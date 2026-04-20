package littleq

import (
	"context"
	"io"
	"time"
)

// TaskRepository is the backend interface. I is the native ID type, P is the native payload type.
// Implementations must be safe for concurrent use.
//
// The typ parameter is the logical queue / task-type name, carried on every
// method both for namespacing (Redis key prefixes) and for observability
// (logs and traces). Backends that do not need it for storage MUST still
// validate it against the stored row for Heartbeat/Complete/Fail.
//
// Concrete types:
//   - Redis:    TaskRepository[string, json.RawMessage]
//   - MongoDB:  TaskRepository[string, json.RawMessage]
//   - Postgres: TaskRepository[int64, json.RawMessage]
type TaskRepository[I comparable, P any] interface {
	// Push atomically enqueues a new task and returns its assigned ID.
	// Returns ErrDuplicate wrapped in the returned error if DedupKey is
	// non-empty and already exists; in that case the returned ID is the
	// existing task's ID.
	Push(ctx context.Context, entry RawTaskEntry[P]) (I, error)

	// Pop atomically claims the highest-pressure task of type typ whose
	// capabilities are satisfied by the worker's caps. A nil caps slice
	// means the worker accepts any task; an empty slice restricts to
	// tasks with no capability requirements.
	//
	// Pop blocks until a task is available or ctx is cancelled. It returns
	// the zero RawTask and a non-nil error on failure or cancellation.
	Pop(ctx context.Context, typ, workerID string, caps []string, opts ...PopOption) (RawTask[I, P], error)

	// Heartbeat refreshes the worker's hold on a task, preventing zombie reclaim.
	// Returns a wrapped ErrNotOwner if workerID does not match the current owner,
	// or a wrapped ErrNotFound if the task no longer exists.
	Heartbeat(ctx context.Context, typ string, taskID I, workerID string) error

	// Complete marks a task as done.
	// Returns a wrapped ErrNotOwner if workerID does not match the current owner.
	Complete(ctx context.Context, typ string, taskID I, workerID string) error

	// Fail marks a task as failed. Re-enqueues if retries remain, otherwise StatusDead.
	// Returns a wrapped ErrNotOwner if workerID does not match the current owner.
	Fail(ctx context.Context, typ string, taskID I, workerID string, reason error) error

	// AgeQueued is a leader-only operation. Recalculates Peff for all queued tasks:
	//   Peff = Pbase + (now - created) * alpha
	AgeQueued(ctx context.Context, typ string, alpha float64) error

	// RequeueZombies is a leader-only operation. Re-enqueues tasks whose heartbeat
	// has not been updated within the given timeout.
	RequeueZombies(ctx context.Context, typ string, timeout time.Duration) error

	// Metrics returns throughput and queue depth measurements over the given window.
	Metrics(ctx context.Context, typ string, window time.Duration) (TaskMetrics, error)

	io.Closer
}

// LeaderElector is an optional interface backends may implement for leader election.
// Detected via type assertion at Scheduler construction time.
type LeaderElector interface {
	Campaign(ctx context.Context) error
	Resign(ctx context.Context) error
	IsLeader() bool
}

// HealthChecker is an optional interface for liveness probes.
type HealthChecker interface {
	Health(ctx context.Context) error
}
