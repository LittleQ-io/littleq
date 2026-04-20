# littleq

A high-performance, SLA-aware async task engine for Go. Inspired by [Little's Law](https://en.wikipedia.org/wiki/Little%27s_law), littleq combines priority aging, pressure-based dispatch, and pluggable backends to keep queues operating within defined latency targets — and to tell you when they aren't.

## Features

- **Priority aging** — `Peff = Pbase + age × α` prevents task starvation under sustained load
- **Pressure-based dispatch** — `Pressure = currentWait / MaxWait` dispatches the most SLA-critical task first
- **Little's Law scaling** — emits recommended worker counts via gRPC streaming: `N = (λ/μ) × (1 + 1/(μ × Wmax))`
- **Three backends** — Redis (Lua atomics), MongoDB (replica set), PostgreSQL (`SKIP LOCKED` + `LISTEN/NOTIFY`)
- **Typed generics** — two-level split: `TaskRepository[I, P]` for backends, `Queue[I, P, T]` for callers
- **Schema versioning** — upcaster chains migrate old payloads on read without downtime
- **Deduplication** — idempotent enqueue via per-type unique `dedup_key`
- **gRPC Scheduler** — central server with server-streaming `Subscribe`, mTLS, and API key auth
- **Leader election** — all three backends implement `LeaderElector` for safe background operations

## Quick start

```go
import (
    lq "github.com/LittleQ-io/littleq"
    lqredis "github.com/LittleQ-io/littleq/redis"
    rdb "github.com/redis/go-redis/v9"
)

// 1. Create a backend store
client := rdb.NewClient(&rdb.Options{Addr: "localhost:6379"})
store, err := lqredis.New(client)

// 2. Define your domain type
type SendEmail struct {
    To      string
    Subject string
}

// 3. Build a typed queue (JSON codec, no boilerplate)
q := lq.NewJSONQueue[string, SendEmail](store, "email",
    lq.WithPolicy[string, json.RawMessage, SendEmail](lq.Policy{
        MaxWait:    30 * time.Second,
        AgingAlpha: 0.1,
        MaxRetries: 3,
    }),
)

// 4. Enqueue
id, err := q.Enqueue(ctx, SendEmail{To: "user@example.com", Subject: "Hello"})

// 5. Pop and process (blocking)
task, err := q.Pop(ctx, "worker-1", nil) // nil caps = accept any task
// ... process task.Payload (type SendEmail) ...
_ = q.Complete(ctx, task.ID, "worker-1")
```

## Installation

```sh
go get github.com/LittleQ-io/littleq
```

Backend drivers are separate modules within the same repository:

```sh
go get github.com/LittleQ-io/littleq/redis     # Redis
go get github.com/LittleQ-io/littleq/mongo     # MongoDB
go get github.com/LittleQ-io/littleq/postgres  # PostgreSQL
```

## Backends

| Backend | ID type | Atomic pop | Blocking wait | Leader election |
|---------|---------|------------|---------------|-----------------|
| Redis | `string` | Lua `ZPOPMAX` | pub/sub notify | `SET NX EX` |
| MongoDB | `string` | `findOneAndUpdate` | poll | `findOneAndUpdate` upsert |
| PostgreSQL | `int64` | `SELECT FOR UPDATE SKIP LOCKED` | `LISTEN/NOTIFY` | `INSERT ON CONFLICT DO UPDATE WHERE expires_at < NOW()` |

### Redis

```go
store, err := lqredis.New(client,
    lqredis.WithHeartbeatTTL(30*time.Second),
    lqredis.WithKeyPrefix("myapp"),
)
```

### MongoDB

Requires a replica set (for consistent reads). The driver creates its own collection and indexes on first use.

```go
db := mongoClient.Database("myapp")
store, err := lqmongo.New(db)
```

### PostgreSQL

```go
sqlDB, err := sql.Open("postgres", dsn)
store, err := lqpostgres.New(sqlDB, dsn) // runs embedded migrations on startup
```

## Capabilities

Workers declare what they can do; tasks declare what they need. A task is only dispatched to a worker whose capability set is a superset of the task's requirements.

```go
// Enqueue a task that requires GPU processing
id, err := q.Enqueue(ctx, payload,
    lq.WithCapabilities("gpu", "cuda-12"),  // worker must have both
)

// Worker claims tasks it can handle
task, err := q.Pop(ctx, "worker-gpu-1", []string{"gpu", "cuda-12", "nvme"})

// nil caps = accept any task regardless of requirements
task, err := q.Pop(ctx, "worker-general", nil)

// empty caps = only tasks with no requirements
task, err := q.Pop(ctx, "worker-basic", []string{})
```

## Deduplication

```go
id, err := q.Enqueue(ctx, payload, lq.WithDedupKey("invoice-42"))
// Second call with the same key returns (existingID, ErrDuplicate)
id2, err := q.Enqueue(ctx, payload, lq.WithDedupKey("invoice-42"))
errors.Is(err, lq.ErrDuplicate) // true
```

## Schema versioning

```go
q := lq.NewJSONQueue[string, EmailV2](store, "email",
    lq.WithSchemaVersion[string, json.RawMessage, EmailV2](2),
    lq.WithUpcaster[string, json.RawMessage, EmailV2](
        lq.UpcasterFunc[json.RawMessage](func(ctx context.Context, p json.RawMessage, from, to int) (json.RawMessage, error) {
            // migrate v1 → v2 payload
            return migrateEmailV1toV2(p)
        }),
    ),
)
```

Old tasks are migrated on read; no downtime or backfill job required.

## gRPC Scheduler

The optional `scheduler` package exposes the full task API over gRPC, enabling workers written in any language and centralized scaling signal emission.

```go
import (
    "github.com/LittleQ-io/littleq/scheduler"
    "github.com/LittleQ-io/littleq/scheduler/auth"
)

// Build the server
srv, err := scheduler.NewServer(store, scheduler.StringIDCodec{},
    scheduler.WithPopTimeout(5*time.Second),
    scheduler.WithZombieTimeout(30*time.Second),
)

// mTLS credentials
creds, err := auth.ServerTLS("server.crt", "server.key", "ca.crt")

// API key interceptors
unary, stream := auth.APIKeyInterceptors("key-abc", "key-def")

grpcSrv := grpc.NewServer(
    grpc.Creds(creds),
    grpc.ChainUnaryInterceptor(unary),
    grpc.ChainStreamInterceptor(stream),
)
lqpb.RegisterSchedulerServer(grpcSrv, srv)

// Start background aging + zombie cleanup (leader-only when LeaderElector is implemented)
srv.StartBackground(ctx, "email")
```

Workers connect and receive tasks via server-streaming `Subscribe`. The `ScalingEvents` RPC emits Little's Law worker-count recommendations at a configurable interval for integration with autoscalers.

### Proto

The service definition lives in `proto/littleq/v1/scheduler.proto`. Regenerate with:

```sh
just proto
```

## Running tests

```sh
# Unit tests (no external services)
just test

# Integration tests (starts Docker containers automatically)
just test-redis     # Redis only
just test-mongo     # MongoDB replica set
just test-pg        # PostgreSQL
just test-integration  # all three
```

Integration tests require Docker. The `just` recipes handle container lifecycle including MongoDB replica set initialisation.

## Development

Tools are managed via [mise](https://mise.jdx.dev). After cloning:

```sh
mise install   # installs Go, golangci-lint, just, protoc, govulncheck, gosec
just build
just test
just lint
```

## License

MIT
