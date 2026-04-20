# Architecture

## Overview

littleq is structured as a layered system. The outermost layer (`Queue[I,P,T]`) is what application code touches. The middle layer (`TaskRepository[I,P]`) is what backend drivers implement. The optional `scheduler` package sits alongside both as a network transport.

```
┌─────────────────────────────────────────────────────┐
│  Application code                                   │
│  Queue[I, P, T]  ·  NewJSONQueue  ·  EnqueueOption  │
└────────────────────────┬────────────────────────────┘
                         │ codec + upcasters
┌────────────────────────▼────────────────────────────┐
│  TaskRepository[I, P]  (interface)                  │
│  Push · Pop · Heartbeat · Complete · Fail           │
│  AgeQueued · RequeueZombies · Metrics · Close       │
└──────┬─────────────────┬──────────────┬─────────────┘
       │                 │              │
  ┌────▼────┐      ┌─────▼────┐  ┌─────▼──────┐
  │  Redis  │      │ MongoDB  │  │ PostgreSQL │
  │ string  │      │ string   │  │  int64     │
  └────┬────┘      └─────┬────┘  └─────┬──────┘
       │                 │              │
       └─────────────────┴──────────────┘
                         │
               ┌─────────▼──────────┐
               │  scheduler.Server  │
               │  gRPC · mTLS · API │
               └────────────────────┘
```

## Two-level generic split

The design separates codec concerns from storage concerns. This pattern is borrowed from the ledger library.

**`TaskRepository[I comparable, P any]`** — the backend knows only about its native ID type (`string` for Redis/MongoDB, `int64` for PostgreSQL) and its native payload type (always `json.RawMessage` in practice). It has no knowledge of domain types.

**`Queue[I comparable, P any, T any]`** — the user-facing handle owns the codec (`PayloadCodec[T, P]`) that converts between the domain type `T` and the backend wire type `P`. It also owns the upcaster chain for schema migration.

This means backend implementations never need to be generic over `T`, keeping driver code straightforward. Adding a new codec (e.g. protobuf) requires no backend changes.

## Task lifecycle

```
Enqueue ──► Queued ──► Claimed ──► Running ──► Done
                          │           │
                          │    Heartbeat timeout
                          │           │
                          └───────────┴──► Re-queued (zombie reclaim)
                                │
                              Fail
                                │
                         retries remain? ──yes──► Queued (priority reset)
                                │
                               no
                                │
                              Dead
```

States:

| Status | Meaning |
|--------|---------|
| `Queued` | Sitting in the priority queue, waiting for a worker |
| `Claimed` | Atomically taken by a worker; heartbeat not yet received |
| `Running` | Worker has sent at least one heartbeat |
| `Done` | Completed successfully |
| `Failed` | Failed; re-enqueued if retries remain |
| `Dead` | Retries exhausted; terminal |

## Priority and aging

Every task carries a base priority set at enqueue time. The scheduler periodically runs `AgeQueued` (a leader-only operation) which recomputes the effective priority for all queued tasks:

```
Peff = Pbase + (now - createdAt) × α
```

`α` (AgingAlpha) is set per policy. Higher α means older tasks accelerate faster toward the front of the queue, preventing starvation under sustained high load.

## Pressure-based dispatch

When a worker calls `Pop`, the backend selects the task with the highest effective priority — which incorporates both the original urgency and accumulated age. The pressure score can be computed independently:

```
Pressure = currentWait / Policy.MaxWait
```

A pressure ≥ 1.0 means the task has already breached its SLA. The score is useful for metrics and alerting.

## Little's Law scaling

The `ScalingEvents` gRPC stream applies Little's Law to recommend how many workers are needed:

```
N = (λ/μ) × (1 + 1/(μ × Wmax))
```

Where:
- `λ` = arrival rate (tasks/second, from `Metrics`)
- `μ` = service rate (tasks/second, from `Metrics`)
- `Wmax` = `Policy.MaxWait`

The recommendation is then adjusted by `ScalingMode`:

| Mode | Adjustment |
|------|-----------|
| `Aggressive` | `⌈N × 1.25⌉` — headroom above SLA |
| `Balanced` | `⌈N⌉` — exact theoretical minimum |
| `Economic` | `⌊N × 0.85⌋` — maximize utilization, accept some SLA stretch |

## Backend internals

### Redis

All mutations are Lua scripts loaded at startup via `SCRIPT LOAD` / `EVALSHA`, giving atomicity without transactions.

| Operation | Script | Key structure |
|-----------|--------|---------------|
| Push | `push.lua` | `lq:{type}:queue` (sorted set, score = Peff) |
| Pop | `pop.lua` | `lq:{type}:claimed:{id}` (string, TTL = heartbeat timeout) |
| Heartbeat | `heartbeat.lua` | PEXPIRE on claimed key |
| Complete | `complete.lua` | DEL claimed key + HSET done hash |
| Fail | `fail.lua` | Atomic ownership check + re-enqueue or HSET dead |
| Release (cap mismatch) | `release.lua` | Atomic re-enqueue without ownership change |
| Age | `age.lua` | ZSCORE + ZADD on queue sorted set |
| Zombie reclaim | `requeue_zombies.lua` | Scan claimed keys, re-enqueue expired |

Task metadata (priority, created_at, alpha) is stored as a hash in `lq:{type}:meta:{id}`, written atomically inside `push.lua`.

Heartbeat TTL uses millisecond precision (`PX` / `PEXPIRE`) so sub-second timeouts work correctly in tests.

`Pop` uses a `popOutcome` enum (`matched` / `empty` / `capMismatch`) to prevent capability-mismatch livelock in single-worker deployments: on `capMismatch`, Pop yields for `pollInterval` before retrying instead of immediately re-claiming the same task.

### MongoDB

Requires a replica set. Uses `findOneAndUpdate` with `ReturnDocument: After` for atomic claim. The capability filter is expressed as a single MongoDB query predicate:

```
{ $not: { $elemMatch: { $nin: workerCaps } } }
```

This reads "no element of the task's capability list is absent from the worker's capabilities". When `workerCaps` is `nil`, the predicate is omitted entirely (accept any task).

Leader election uses a single atomic upsert:

```js
filter: { _id: leaderKey, $or: [{ expires_at: { $lt: now } }, { node_id: nodeID }] }
update: { $set: { node_id: nodeID, expires_at: now + leaseTTL } }
upsert: true
```

A node wins if the result doc's `node_id` equals its own.

### PostgreSQL

Uses `database/sql` with `lib/pq`. Schema migrations are embedded as SQL files and run automatically at startup.

`Pop` builds the claim query with `SELECT FOR UPDATE SKIP LOCKED`, which lets multiple workers run `Pop` concurrently without contention. The capability filter is:

```sql
($4::text[] IS NULL OR $4::text[] @> capabilities)
```

`$4::text[] @> capabilities` means "worker caps contains all task requirements". When caps is nil, the predicate is omitted.

Low-latency blocking uses `pq.NewListener` with `LISTEN/NOTIFY`: the store sends `NOTIFY lq_{type}` on every `Push`, so waiting workers wake up within microseconds instead of polling.

Leader election uses:

```sql
INSERT INTO lq_leader (key, node_id, expires_at)
VALUES ($1, $2, NOW() + $3)
ON CONFLICT (key) DO UPDATE
  SET node_id = $2, expires_at = NOW() + $3
  WHERE lq_leader.expires_at < NOW()
```

## gRPC Scheduler

`scheduler.Server[I]` implements the `lqpb.SchedulerServer` interface and is generic over the backend ID type. An `IDCodec[I]` interface bridges between backend-native IDs and the proto `string` task ID:

```go
type IDCodec[I comparable] interface {
    Encode(I) string
    Decode(string) (I, error)
}
```

`StringIDCodec` is a no-op for Redis/MongoDB. `Int64IDCodec` uses `strconv` for PostgreSQL.

The `Subscribe` RPC is a server-streaming loop: `Pop` blocks until a task is available, then sends it to the worker's stream. When the client disconnects, the stream context is cancelled, unblocking `Pop` and exiting cleanly.

`ScalingEvents` ticks on a configurable interval, calls `Metrics`, applies the Little's Law formula, and sends a `ScalingEvent` proto message.

Background operations (`AgeQueued`, `RequeueZombies`) run in `StartBackground` goroutines, gated by `LeaderElector.IsLeader()` so only one server node executes them at a time.

### Auth

`scheduler/auth` provides two independent security layers that can be used together or separately:

**mTLS** — `auth.ServerTLS(cert, key, ca)` returns gRPC `TransportCredentials` that require client certificate verification. `auth.ClientTLS` is the corresponding client helper. Both enforce TLS 1.3 minimum.

**API key** — `auth.APIKeyInterceptors(keys...)` returns a `(UnaryServerInterceptor, StreamServerInterceptor)` pair. Both interceptors read the `x-api-key` gRPC metadata header and reject requests with `codes.Unauthenticated` if the key is absent or not in the allow-list. `auth.APIKeyCredential` implements `grpc.PerRPCCredentials` for clients.

## Conformance test suite

`storetest.RunRepositoryTests[I]` is a generic test function that any backend can call to verify it satisfies the `TaskRepository` contract. It covers 19 cases:

- push_and_pop, priority_ordering
- pop_empty_times_out, pop_ctx_cancel
- heartbeat, heartbeat_missing_task
- complete, complete_twice
- fail_requeue, fail_not_owner
- dedup, dedup_distinct_types
- capabilities, nil_caps_accepts_any
- zombie_requeue, age_queued
- metrics, invalid_args, closed_store

Integration tests for each backend are tagged `integration` and require the corresponding Docker container (started via `just test-redis/mongo/pg`).

## Package layout

```
littleq/
├── store.go              # TaskRepository, LeaderElector, HealthChecker interfaces
├── task.go               # Task, RawTask, RawTaskEntry, Policy, TaskMetrics types
├── queue.go              # Queue[I,P,T], NewQueue, NewJSONQueue
├── codec.go              # PayloadCodec, JSONCodec
├── schema.go             # Upcaster, UpcasterFunc, applyUpcasters
├── options.go            # PopOption, ResolvePopOptions
├── errors.go             # Sentinel errors
├── internal/
│   ├── math/             # EffectivePriority, RequiredWorkers, PressureScore
│   ├── popopt/           # Resolved struct (backend-internal pop plumbing)
│   └── uid/              # Time-ordered hex ID generator
├── redis/                # Redis backend (Lua scripts, leader election)
├── mongo/                # MongoDB backend (replica set, leader election)
├── postgres/             # PostgreSQL backend (SKIP LOCKED, LISTEN/NOTIFY, migrations)
├── storetest/            # Generic conformance test suite
├── scheduler/
│   ├── server.go         # Server[I], IDCodec, StringIDCodec, Int64IDCodec
│   ├── options.go        # Scheduler functional options
│   └── auth/             # ServerTLS, ClientTLS, APIKeyInterceptors
└── proto/
    └── littleq/v1/       # scheduler.proto + generated Go code
```
