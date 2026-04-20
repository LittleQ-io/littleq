package littleq

import "time"

// Status represents the lifecycle state of a task.
type Status int8

const (
	StatusQueued  Status = iota // waiting in priority queue
	StatusClaimed               // atomically picked up, not yet running
	StatusRunning               // worker confirmed start via heartbeat
	StatusDone                  // completed successfully
	StatusFailed                // terminal failure
	StatusDead                  // exhausted retries
)

func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "queued"
	case StatusClaimed:
		return "claimed"
	case StatusRunning:
		return "running"
	case StatusDone:
		return "done"
	case StatusFailed:
		return "failed"
	case StatusDead:
		return "dead"
	default:
		return "unknown"
	}
}

// ScalingMode controls how aggressively the scheduler provisions workers.
type ScalingMode int8

const (
	ScalingAggressive ScalingMode = iota // minimize wait at any cost
	ScalingBalanced                      // balance cost and SLA
	ScalingEconomic                      // maximize utilization, stretch SLA during peaks
)

// Policy defines the SLA and aging behaviour for a task queue.
type Policy struct {
	Name        string
	MaxWait     time.Duration // Wmax — maximum allowed wait time (SLA target)
	AgingAlpha  float64       // α — aging coefficient for anti-starvation
	MaxRetries  int           // 0 = no retries
	ScalingMode ScalingMode
}

// RawTaskEntry is the input to TaskRepository.Push. P is the backend-native payload type.
type RawTaskEntry[P any] struct {
	Type          string   // logical queue / task type name
	Payload       P        // backend-encoded payload
	Capabilities  []string // worker must satisfy ALL capabilities
	Priority      int      // base priority; higher = more urgent
	Policy        Policy
	SchemaVersion int    // stamped at enqueue time; used for upcasting on read
	DedupKey      string // empty = no dedup
	SourceID      string // for idempotent re-enqueue (e.g. from outbox)
	MaxRetries    int
}

// RawTask is what TaskRepository.Pop returns. I is the backend-native ID type.
type RawTask[I comparable, P any] struct {
	ID            I
	Type          string
	Payload       P
	Capabilities  []string
	Priority      int
	EffPriority   float64 // Peff at time of dispatch
	Policy        Policy
	Status        Status
	SchemaVersion int
	DedupKey      string
	SourceID      string
	RetryCount    int
	MaxRetries    int
	CreatedAt     time.Time
	ClaimedAt     time.Time
	WorkerID      string

	// Reserved for future DAG support — zero value means no dependency.
	// DependsOn []I
}

// Task is the decoded user-facing task. T is the caller's domain type.
type Task[I comparable, T any] struct {
	ID           I
	Type         string
	Payload      T
	Capabilities []string
	Priority     int
	Policy       Policy
	Status       Status
	RetryCount   int
	CreatedAt    time.Time
	ClaimedAt    time.Time
	WorkerID     string
}

// TaskMetrics holds throughput measurements over a sampling window.
type TaskMetrics struct {
	Type        string
	ArrivalRate float64 // λ — tasks per second arriving
	ServiceRate float64 // μ — tasks per second completed (1/avg_duration)
	QueueDepth  int64   // current number of queued tasks
	AvgWait     float64 // seconds
}
