package littleq_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	lq "github.com/LittleQ-io/littleq"
)

// fakeRepo is an in-memory TaskRepository used to exercise the Queue wrapper
// and the codec/upcaster pipeline without any external backend.
type fakeRepo struct {
	mu       sync.Mutex
	nextID   int
	byID     map[string]lq.RawTask[string, json.RawMessage]
	queue    []string
	pushed   int
	lastType string
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{byID: map[string]lq.RawTask[string, json.RawMessage]{}}
}

func (f *fakeRepo) Push(_ context.Context, entry lq.RawTaskEntry[json.RawMessage]) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := "t" + itoa(f.nextID)
	f.lastType = entry.Type
	f.pushed++
	f.byID[id] = lq.RawTask[string, json.RawMessage]{
		ID:            id,
		Type:          entry.Type,
		Payload:       entry.Payload,
		Capabilities:  entry.Capabilities,
		Priority:      entry.Priority,
		Policy:        entry.Policy,
		SchemaVersion: entry.SchemaVersion,
		DedupKey:      entry.DedupKey,
		SourceID:      entry.SourceID,
		MaxRetries:    entry.MaxRetries,
		CreatedAt:     time.Now(),
	}
	f.queue = append(f.queue, id)
	return id, nil
}

func (f *fakeRepo) Pop(ctx context.Context, typ, workerID string, _ []string, _ ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	_ = ctx
	_ = typ
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		return lq.RawTask[string, json.RawMessage]{}, lq.ErrNotFound
	}
	id := f.queue[0]
	f.queue = f.queue[1:]
	t := f.byID[id]
	t.WorkerID = workerID
	t.Status = lq.StatusClaimed
	t.ClaimedAt = time.Now()
	f.byID[id] = t
	return t, nil
}

func (f *fakeRepo) Heartbeat(context.Context, string, string, string) error      { return nil }
func (f *fakeRepo) Complete(context.Context, string, string, string) error       { return nil }
func (f *fakeRepo) Fail(context.Context, string, string, string, error) error    { return nil }
func (f *fakeRepo) AgeQueued(context.Context, string, float64) error             { return nil }
func (f *fakeRepo) RequeueZombies(context.Context, string, time.Duration) error  { return nil }
func (f *fakeRepo) Metrics(context.Context, string, time.Duration) (lq.TaskMetrics, error) {
	return lq.TaskMetrics{}, nil
}
func (f *fakeRepo) Close() error { return nil }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}

type v1Payload struct {
	Name string `json:"name"`
}

type v2Payload struct {
	FullName string `json:"full_name"`
}

func TestJSONQueue_EnqueuePop(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	q := lq.NewJSONQueue[string, v2Payload](repo, "demo")

	id, err := q.Enqueue(ctx, v2Payload{FullName: "grace"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("Enqueue returned empty id")
	}

	task, err := q.Pop(ctx, "worker-1", nil)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if task.Payload.FullName != "grace" {
		t.Errorf("payload = %q, want grace", task.Payload.FullName)
	}
	if task.WorkerID != "worker-1" {
		t.Errorf("worker id = %q, want worker-1", task.WorkerID)
	}
}

func TestJSONQueue_Upcaster(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()

	// Simulate a v1 payload already in the store.
	raw, _ := json.Marshal(v1Payload{Name: "grace"})
	repo.byID["legacy"] = lq.RawTask[string, json.RawMessage]{
		ID:            "legacy",
		Type:          "demo",
		Payload:       raw,
		SchemaVersion: 1,
	}
	repo.queue = append(repo.queue, "legacy")

	up := lq.UpcasterFunc[json.RawMessage](1, 2, func(_ context.Context, p json.RawMessage) (json.RawMessage, error) {
		var v v1Payload
		if err := json.Unmarshal(p, &v); err != nil {
			return nil, err
		}
		return json.Marshal(v2Payload{FullName: v.Name})
	})

	q := lq.NewJSONQueue[string, v2Payload](repo, "demo",
		lq.WithSchemaVersion[string, json.RawMessage, v2Payload](2),
		lq.WithUpcaster[string, json.RawMessage, v2Payload](up),
	)

	task, err := q.Pop(ctx, "w", nil)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if task.Payload.FullName != "grace" {
		t.Errorf("FullName = %q, want grace", task.Payload.FullName)
	}
}

func TestJSONQueue_MissingUpcaster(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	repo.byID["legacy"] = lq.RawTask[string, json.RawMessage]{
		ID:            "legacy",
		Type:          "demo",
		Payload:       json.RawMessage(`{}`),
		SchemaVersion: 1,
	}
	repo.queue = append(repo.queue, "legacy")

	q := lq.NewJSONQueue[string, v2Payload](repo, "demo",
		lq.WithSchemaVersion[string, json.RawMessage, v2Payload](2),
	)
	_, err := q.Pop(ctx, "w", nil)
	if !errors.Is(err, lq.ErrNoUpcaster) {
		t.Errorf("expected ErrNoUpcaster, got %v", err)
	}
}

func TestQueue_ZeroValuePanicGuard(t *testing.T) {
	var q lq.Queue[string, json.RawMessage, v2Payload]
	_, err := q.Enqueue(context.Background(), v2Payload{FullName: "x"})
	if err == nil {
		t.Fatal("expected error enqueuing on zero-value Queue")
	}
}

func TestPopTimeoutOption(t *testing.T) {
	r := lq.ResolvePopOptions(lq.PopTimeout(42 * time.Millisecond))
	if r.Timeout != 42*time.Millisecond {
		t.Errorf("timeout = %v, want 42ms", r.Timeout)
	}
	r = lq.ResolvePopOptions(lq.PopTimeout(-1), lq.PopTimeout(0))
	if r.Timeout != 0 {
		t.Errorf("timeout after non-positive = %v, want 0", r.Timeout)
	}
}

func TestEnqueueOption_Priority(t *testing.T) {
	repo := newFakeRepo()
	q := lq.NewJSONQueue[string, v2Payload](repo, "demo")

	_, err := q.Enqueue(context.Background(), v2Payload{FullName: "n"}, lq.WithPriority(-5))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The fake repo stores the RawTask; verify priority got clamped to zero.
	var got int
	for _, v := range repo.byID {
		got = v.Priority
	}
	if got != 0 {
		t.Errorf("priority = %d, want 0 (clamp)", got)
	}
}
