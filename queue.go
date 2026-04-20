package littleq

import (
	"context"
	"encoding/json"
	"fmt"
)

// Queue is the typed user-facing handle. It wraps a TaskRepository and owns
// the codec and upcaster chain. T is the caller's domain payload type.
// Value type — cheap to copy and create per queue name.
type Queue[I comparable, P any, T any] struct {
	repo          TaskRepository[I, P]
	codec         PayloadCodec[T, P]
	qtype         string
	schemaVersion int
	upcasters     []Upcaster[P]
	capabilities  []string
	policy        Policy
}

// QueueOption is the functional-options alias used by Queue constructors.
// Fully qualified, it reads WithXxx[I, P, T].
type QueueOption[I comparable, P any, T any] func(*Queue[I, P, T])

// NewQueue constructs a typed Queue backed by repo.
func NewQueue[I comparable, P any, T any](
	repo TaskRepository[I, P],
	qtype string,
	codec PayloadCodec[T, P],
	opts ...QueueOption[I, P, T],
) Queue[I, P, T] {
	q := Queue[I, P, T]{
		repo:          repo,
		codec:         codec,
		qtype:         qtype,
		schemaVersion: 1,
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&q)
		}
	}
	return q
}

// WithSchemaVersion stamps every enqueued task with the given schema version.
func WithSchemaVersion[I comparable, P any, T any](v int) QueueOption[I, P, T] {
	return func(q *Queue[I, P, T]) { q.schemaVersion = v }
}

// WithUpcaster registers a migration step for reading old-schema tasks.
func WithUpcaster[I comparable, P any, T any](u Upcaster[P]) QueueOption[I, P, T] {
	return func(q *Queue[I, P, T]) { q.upcasters = append(q.upcasters, u) }
}

// WithPolicy sets the default SLA policy for tasks enqueued via this Queue.
func WithPolicy[I comparable, P any, T any](p Policy) QueueOption[I, P, T] {
	return func(q *Queue[I, P, T]) { q.policy = p }
}

// WithCapabilities sets default required capabilities for tasks enqueued via this Queue.
func WithCapabilities[I comparable, P any, T any](caps ...string) QueueOption[I, P, T] {
	return func(q *Queue[I, P, T]) { q.capabilities = caps }
}

// EnqueueOption configures a single Enqueue call.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	priority     int
	policy       *Policy
	capabilities []string
	dedupKey     string
	sourceID     string
}

// WithPriority overrides the task priority for a single enqueue.
// Negative priorities are clamped to zero.
func WithPriority(p int) EnqueueOption {
	return func(o *enqueueOptions) {
		if p < 0 {
			p = 0
		}
		o.priority = p
	}
}

// WithDedupKey sets a deduplication key. Empty string disables dedup.
func WithDedupKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.dedupKey = key }
}

// WithSourceID sets the idempotency source ID (e.g. outbox entry ID).
func WithSourceID(id string) EnqueueOption {
	return func(o *enqueueOptions) { o.sourceID = id }
}

// Enqueue marshals payload and pushes a task into the queue.
func (q Queue[I, P, T]) Enqueue(ctx context.Context, payload T, opts ...EnqueueOption) (I, error) {
	var zero I
	if q.repo == nil {
		return zero, fmt.Errorf("littleq: Enqueue on zero-value Queue")
	}

	o := enqueueOptions{
		capabilities: q.capabilities,
		policy:       &q.policy,
	}
	for _, fn := range opts {
		fn(&o)
	}

	p, err := q.codec.Marshal(payload)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", ErrEncode, err)
	}

	policy := q.policy
	if o.policy != nil {
		policy = *o.policy
	}

	return q.repo.Push(ctx, RawTaskEntry[P]{
		Type:          q.qtype,
		Payload:       p,
		Capabilities:  o.capabilities,
		Priority:      o.priority,
		Policy:        policy,
		SchemaVersion: q.schemaVersion,
		DedupKey:      o.dedupKey,
		SourceID:      o.sourceID,
	})
}

// Pop claims the next task and decodes its payload into T. On any error the
// returned Task is the zero value.
func (q Queue[I, P, T]) Pop(ctx context.Context, workerID string, caps []string, opts ...PopOption) (Task[I, T], error) {
	var zero Task[I, T]
	raw, err := q.repo.Pop(ctx, q.qtype, workerID, caps, opts...)
	if err != nil {
		return zero, err
	}

	payload := raw.Payload
	if raw.SchemaVersion < q.schemaVersion {
		payload, err = applyUpcasters(ctx, payload, raw.SchemaVersion, q.schemaVersion, q.upcasters)
		if err != nil {
			return zero, err
		}
	}

	var v T
	if err := q.codec.Unmarshal(payload, &v); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrDecode, err)
	}

	return Task[I, T]{
		ID:           raw.ID,
		Type:         raw.Type,
		Payload:      v,
		Capabilities: raw.Capabilities,
		Priority:     raw.Priority,
		Policy:       raw.Policy,
		Status:       raw.Status,
		RetryCount:   raw.RetryCount,
		CreatedAt:    raw.CreatedAt,
		ClaimedAt:    raw.ClaimedAt,
		WorkerID:     raw.WorkerID,
	}, nil
}

// Heartbeat delegates to the underlying repository.
func (q Queue[I, P, T]) Heartbeat(ctx context.Context, taskID I, workerID string) error {
	return q.repo.Heartbeat(ctx, q.qtype, taskID, workerID)
}

// Complete delegates to the underlying repository.
func (q Queue[I, P, T]) Complete(ctx context.Context, taskID I, workerID string) error {
	return q.repo.Complete(ctx, q.qtype, taskID, workerID)
}

// Fail delegates to the underlying repository.
func (q Queue[I, P, T]) Fail(ctx context.Context, taskID I, workerID string, reason error) error {
	return q.repo.Fail(ctx, q.qtype, taskID, workerID, reason)
}

// JSONQueue is a convenience alias for a JSON-encoded queue.
type JSONQueue[I comparable, T any] = Queue[I, json.RawMessage, T]

// NewJSONQueue constructs a Queue using JSONCodec.
func NewJSONQueue[I comparable, T any](
	repo TaskRepository[I, json.RawMessage],
	qtype string,
	opts ...QueueOption[I, json.RawMessage, T],
) Queue[I, json.RawMessage, T] {
	return NewQueue(repo, qtype, JSONCodec[T]{}, opts...)
}
