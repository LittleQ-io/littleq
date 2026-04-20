// Package storetest provides a generic backend-agnostic conformance suite for
// TaskRepository implementations. Each backend test file calls RunRepositoryTests.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	lq "github.com/LittleQ-io/littleq"
)

// TestConfig holds backend-specific helpers needed by the suite.
// I is the ID type of the repository under test.
type TestConfig[I comparable] struct {
	// HeartbeatTTL must match the TTL the store was configured with so the
	// zombie test can wait long enough for expiry.
	HeartbeatTTL time.Duration
}

// RunRepositoryTests exercises a TaskRepository[I, json.RawMessage] against the
// full conformance suite. Call this from each backend's _test.go.
func RunRepositoryTests[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage], cfg TestConfig[I]) {
	t.Helper()

	t.Run("push_and_pop", func(t *testing.T) { testPushAndPop(t, repo) })
	t.Run("priority_ordering", func(t *testing.T) { testPriorityOrdering(t, repo) })
	t.Run("pop_empty_times_out", func(t *testing.T) { testPopEmptyTimesOut(t, repo) })
	t.Run("pop_ctx_cancel", func(t *testing.T) { testPopCtxCancel(t, repo) })
	t.Run("heartbeat", func(t *testing.T) { testHeartbeat(t, repo) })
	t.Run("heartbeat_missing_task", func(t *testing.T) { testHeartbeatMissing(t, repo) })
	t.Run("complete", func(t *testing.T) { testComplete(t, repo) })
	t.Run("complete_twice", func(t *testing.T) { testCompleteTwice(t, repo) })
	t.Run("fail_requeue", func(t *testing.T) { testFailRequeue(t, repo) })
	t.Run("fail_not_owner", func(t *testing.T) { testFailNotOwner(t, repo) })
	t.Run("dedup", func(t *testing.T) { testDedup(t, repo) })
	t.Run("dedup_distinct_types", func(t *testing.T) { testDedupDistinctTypes(t, repo) })
	t.Run("capabilities", func(t *testing.T) { testCapabilities(t, repo) })
	t.Run("nil_caps_accepts_any", func(t *testing.T) { testNilCapsAcceptsAny(t, repo) })
	t.Run("zombie_requeue", func(t *testing.T) { testZombieRequeue(t, repo, cfg.HeartbeatTTL) })
	t.Run("age_queued", func(t *testing.T) { testAgeQueued(t, repo) })
	t.Run("metrics", func(t *testing.T) { testMetrics(t, repo) })
	t.Run("invalid_args", func(t *testing.T) { testInvalidArgs(t, repo) })
	// closed_store must be last — it's irreversible.
	t.Run("closed_store", func(t *testing.T) { testClosedStore(t, repo) })
}

// --- helpers ---

func makeEntry(typ string, priority int, caps []string, dedupKey string, maxRetries int) lq.RawTaskEntry[json.RawMessage] {
	payload, _ := json.Marshal(map[string]string{"hello": "world"})
	return lq.RawTaskEntry[json.RawMessage]{
		Type:          typ,
		Payload:       payload,
		Capabilities:  caps,
		Priority:      priority,
		SchemaVersion: 1,
		DedupKey:      dedupKey,
		MaxRetries:    maxRetries,
		Policy: lq.Policy{
			Name:       "test",
			MaxWait:    30 * time.Second,
			AgingAlpha: 0.1,
			MaxRetries: maxRetries,
		},
	}
}

func mustPush[I comparable](t *testing.T, ctx context.Context, repo lq.TaskRepository[I, json.RawMessage], entry lq.RawTaskEntry[json.RawMessage]) I {
	t.Helper()
	id, err := repo.Push(ctx, entry)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	return id
}

func mustPop[I comparable](t *testing.T, typ string, repo lq.TaskRepository[I, json.RawMessage], workerID string) lq.RawTask[I, json.RawMessage] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	task, err := repo.Pop(ctx, typ, workerID, nil)
	if err != nil {
		t.Fatalf("Pop(%q) failed: %v", typ, err)
	}
	return task
}

// --- test cases ---

func testPushAndPop[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:push_pop"
	id := mustPush(t, context.Background(), repo, makeEntry(typ, 5, nil, "", 0))
	task := mustPop(t, typ, repo, "w1")
	if task.ID != id {
		t.Errorf("got task ID %v, want %v", task.ID, id)
	}
	if task.Priority != 5 {
		t.Errorf("priority = %d, want 5", task.Priority)
	}
}

func testPriorityOrdering[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:priority"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	mustPush(t, context.Background(), repo, makeEntry(typ, 10, nil, "", 0))
	mustPush(t, context.Background(), repo, makeEntry(typ, 5, nil, "", 0))

	first := mustPop(t, typ, repo, "w1")
	if first.Priority != 10 {
		t.Errorf("expected priority 10 first, got %d", first.Priority)
	}
	second := mustPop(t, typ, repo, "w1")
	if second.Priority != 5 {
		t.Errorf("expected priority 5 second, got %d", second.Priority)
	}
}

func testHeartbeat[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:heartbeat"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	task := mustPop(t, typ, repo, "w1")

	if err := repo.Heartbeat(context.Background(), typ, task.ID, "w1"); err != nil {
		t.Errorf("Heartbeat failed: %v", err)
	}
	if err := repo.Heartbeat(context.Background(), typ, task.ID, "wrong"); !errors.Is(err, lq.ErrNotOwner) {
		t.Errorf("expected ErrNotOwner for wrong worker, got %v", err)
	}
}

func testComplete[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:complete"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	task := mustPop(t, typ, repo, "w1")

	if err := repo.Complete(context.Background(), typ, task.ID, "w1"); err != nil {
		t.Errorf("Complete failed: %v", err)
	}
	// Wrong owner.
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	task2 := mustPop(t, typ, repo, "w2")
	if err := repo.Complete(context.Background(), typ, task2.ID, "wrong"); !errors.Is(err, lq.ErrNotOwner) {
		t.Errorf("expected ErrNotOwner, got %v", err)
	}
}

func testFailRequeue[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:fail"
	mustPush(t, context.Background(), repo, makeEntry(typ, 5, nil, "", 1))
	task := mustPop(t, typ, repo, "w1")

	// First failure: should requeue (maxRetries=1, retryCount=0 < 1).
	if err := repo.Fail(context.Background(), typ, task.ID, "w1", errors.New("transient")); err != nil {
		t.Fatalf("Fail (first) failed: %v", err)
	}
	task2 := mustPop(t, typ, repo, "w2")
	if task2.ID != task.ID {
		t.Errorf("expected same task after requeue, got %v", task2.ID)
	}
	if task2.RetryCount != 1 {
		t.Errorf("expected RetryCount=1, got %d", task2.RetryCount)
	}

	// Second failure: exhausted → dead.
	if err := repo.Fail(context.Background(), typ, task2.ID, "w2", errors.New("fatal")); err != nil {
		t.Fatalf("Fail (second) failed: %v", err)
	}
}

func testDedup[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:dedup"
	entry := makeEntry(typ, 1, nil, "my-key", 0)
	id1, err := repo.Push(context.Background(), entry)
	if err != nil {
		t.Fatalf("first push: %v", err)
	}
	id2, err := repo.Push(context.Background(), entry)
	if !errors.Is(err, lq.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate on second push, got %v", err)
	}
	if id2 != id1 {
		t.Errorf("duplicate push returned different ID: %v vs %v", id2, id1)
	}
}

func testCapabilities[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:caps"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, []string{"gpu"}, "", 0))

	// CPU-only worker should not receive GPU task — times out.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := repo.Pop(ctx, typ, "cpu-worker", []string{"cpu"})
	if err == nil {
		t.Error("expected error (timeout) for mismatching caps, got nil")
	}

	// GPU worker with explicit gpu cap receives it.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	task2, err := repo.Pop(ctx2, typ, "gpu-worker", []string{"gpu"})
	if err != nil {
		t.Fatalf("gpu-worker pop: %v", err)
	}
	if task2.Priority != 1 {
		t.Errorf("gpu-worker pop: unexpected priority %d", task2.Priority)
	}
}

func testZombieRequeue[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage], hbTTL time.Duration) {
	typ := "st:zombie"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	mustPop(t, typ, repo, "dying-worker")
	// No heartbeat — wait for TTL to expire.
	if hbTTL <= 0 {
		hbTTL = 100 * time.Millisecond
	}
	time.Sleep(hbTTL + 50*time.Millisecond)

	if err := repo.RequeueZombies(context.Background(), typ, hbTTL); err != nil {
		t.Fatalf("RequeueZombies: %v", err)
	}
	task := mustPop(t, typ, repo, "healthy-worker")
	var zeroID I
	if task.ID == zeroID {
		t.Fatal("expected task after zombie requeue")
	}
}

func testMetrics[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:metrics"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))

	m, err := repo.Metrics(context.Background(), typ, 1*time.Minute)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if m.QueueDepth < 1 {
		t.Errorf("QueueDepth = %d, want >= 1", m.QueueDepth)
	}
	if m.ArrivalRate <= 0 {
		t.Errorf("ArrivalRate = %f, want > 0", m.ArrivalRate)
	}
}

func testClosedStore[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := repo.Push(context.Background(), makeEntry("st:closed", 1, nil, "", 0))
	if !errors.Is(err, lq.ErrClosed) {
		t.Errorf("expected ErrClosed after Close, got %v", err)
	}
}

func testPopEmptyTimesOut[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := repo.Pop(ctx, "st:empty_no_tasks", "w1", nil)
	if err == nil {
		t.Fatal("expected error on empty queue pop, got nil")
	}
}

func testPopCtxCancel[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := repo.Pop(ctx, "st:ctxcancel", "w1", nil)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected non-nil error after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop did not return after context cancel")
	}
}

func testHeartbeatMissing[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	// Heartbeat on a finalized task must not silently succeed. We don't
	// prescribe a specific sentinel — backends may map this to ErrNotFound
	// or ErrNotOwner depending on whether the row has been removed.
	typ := "st:hb_after_complete"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	task := mustPop(t, typ, repo, "w1")
	if err := repo.Complete(context.Background(), typ, task.ID, "w1"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	err := repo.Heartbeat(context.Background(), typ, task.ID, "w1")
	if err == nil {
		t.Fatal("expected error heartbeating completed task, got nil")
	}
}

func testCompleteTwice[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:complete_twice"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	task := mustPop(t, typ, repo, "w1")
	if err := repo.Complete(context.Background(), typ, task.ID, "w1"); err != nil {
		t.Fatalf("first Complete: %v", err)
	}
	// Second complete should not silently succeed.
	if err := repo.Complete(context.Background(), typ, task.ID, "w1"); err == nil {
		t.Error("expected error on double-complete, got nil")
	}
}

func testFailNotOwner[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:fail_not_owner"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 1))
	task := mustPop(t, typ, repo, "w1")
	err := repo.Fail(context.Background(), typ, task.ID, "not-the-owner", errors.New("x"))
	if !errors.Is(err, lq.ErrNotOwner) {
		t.Errorf("expected ErrNotOwner, got %v", err)
	}
}

func testDedupDistinctTypes[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	// The same dedup key across different types must coexist.
	id1 := mustPush(t, context.Background(), repo, makeEntry("st:dedup_a", 1, nil, "shared-key", 0))
	id2, err := repo.Push(context.Background(), makeEntry("st:dedup_b", 1, nil, "shared-key", 0))
	if err != nil {
		t.Fatalf("cross-type dedup should not collide: %v", err)
	}
	var zeroID I
	if id1 == zeroID || id2 == zeroID {
		t.Fatal("got zero-valued ids")
	}
	if id1 == id2 {
		t.Error("different types got same ID")
	}
}

func testNilCapsAcceptsAny[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:nilcaps"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, []string{"gpu"}, "", 0))
	// Worker with nil caps opts out of filtering entirely — should still match.
	task := mustPop(t, typ, repo, "any-worker")
	var zeroID I
	if task.ID == zeroID {
		t.Fatal("expected task, got zero-value")
	}
}

func testAgeQueued[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	typ := "st:age"
	mustPush(t, context.Background(), repo, makeEntry(typ, 1, nil, "", 0))
	if err := repo.AgeQueued(context.Background(), typ, 0.1); err != nil {
		t.Errorf("AgeQueued returned %v, want nil", err)
	}
}

func testInvalidArgs[I comparable](t *testing.T, repo lq.TaskRepository[I, json.RawMessage]) {
	ctx := context.Background()
	if _, err := repo.Pop(ctx, "", "w1", nil); err == nil {
		t.Error("expected error from Pop with empty type")
	}
	if _, err := repo.Pop(ctx, "st:invargs", "", nil); err == nil {
		t.Error("expected error from Pop with empty worker id")
	}
}
