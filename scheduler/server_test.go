package scheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	lq "github.com/LittleQ-io/littleq"
	lqpb "github.com/LittleQ-io/littleq/proto/littleq/v1"
	"github.com/LittleQ-io/littleq/scheduler"
)

// ─── in-memory repo stub ──────────────────────────────────────────────────────

type stubRepo struct {
	tasks  []lq.RawTaskEntry[json.RawMessage]
	popped []lq.RawTaskEntry[json.RawMessage]
}

func (r *stubRepo) Push(_ context.Context, e lq.RawTaskEntry[json.RawMessage]) (string, error) {
	r.tasks = append(r.tasks, e)
	return "task-1", nil
}

func (r *stubRepo) Pop(ctx context.Context, typ, workerID string, caps []string, opts ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	if len(r.tasks) == 0 {
		<-ctx.Done()
		return lq.RawTask[string, json.RawMessage]{}, ctx.Err()
	}
	e := r.tasks[0]
	r.tasks = r.tasks[1:]
	r.popped = append(r.popped, e)
	return lq.RawTask[string, json.RawMessage]{
		ID:      "task-1",
		Type:    e.Type,
		Payload: e.Payload,
	}, nil
}

func (r *stubRepo) Heartbeat(_ context.Context, _ string, _ string, _ string) error { return nil }
func (r *stubRepo) Complete(_ context.Context, _ string, _ string, _ string) error  { return nil }
func (r *stubRepo) Fail(_ context.Context, _ string, _ string, _ string, _ error) error {
	return nil
}
func (r *stubRepo) AgeQueued(_ context.Context, _ string, _ float64) error          { return nil }
func (r *stubRepo) RequeueZombies(_ context.Context, _ string, _ time.Duration) error { return nil }
func (r *stubRepo) Metrics(_ context.Context, _ string, _ time.Duration) (lq.TaskMetrics, error) {
	return lq.TaskMetrics{ArrivalRate: 2, ServiceRate: 4, QueueDepth: 1}, nil
}
func (r *stubRepo) Close() error { return nil }

var _ lq.TaskRepository[string, json.RawMessage] = (*stubRepo)(nil)

// ─── test helpers ─────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, repo lq.TaskRepository[string, json.RawMessage]) lqpb.SchedulerClient {
	t.Helper()
	srv, err := scheduler.NewServer(repo, scheduler.StringIDCodec{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	grpcSrv := grpc.NewServer()
	lqpb.RegisterSchedulerServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); srv.Close() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return lqpb.NewSchedulerClient(conn)
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestEnqueue(t *testing.T) {
	repo := &stubRepo{}
	client := newTestServer(t, repo)

	resp, err := client.Enqueue(context.Background(), &lqpb.EnqueueRequest{
		Type:    "email",
		Payload: []byte(`{"to":"user@example.com"}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if resp.TaskId == "" {
		t.Fatal("expected non-empty task_id")
	}
	if len(repo.tasks) != 1 {
		t.Fatalf("expected 1 task in repo, got %d", len(repo.tasks))
	}
}

func TestEnqueue_missingType(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	_, err := client.Enqueue(context.Background(), &lqpb.EnqueueRequest{Payload: []byte(`{}`)})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", code)
	}
}

func TestSubscribe(t *testing.T) {
	payload := json.RawMessage(`{"key":"value"}`)
	repo := &stubRepo{
		tasks: []lq.RawTaskEntry[json.RawMessage]{{Type: "work", Payload: payload}},
	}
	client := newTestServer(t, repo)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Subscribe(ctx, &lqpb.SubscribeRequest{
		WorkerId: "w1",
		Type:     "work",
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	assignment, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if assignment.TaskId != "task-1" {
		t.Errorf("task_id = %q, want task-1", assignment.TaskId)
	}
	if string(assignment.Payload) != string(payload) {
		t.Errorf("payload mismatch")
	}
}

func TestSubscribe_missingWorkerID(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	stream, err := client.Subscribe(context.Background(), &lqpb.SubscribeRequest{Type: "work"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_, err = stream.Recv()
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", code)
	}
}

func TestHeartbeat(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	_, err := client.Heartbeat(context.Background(), &lqpb.HeartbeatRequest{
		Type: "work", TaskId: "task-1", WorkerId: "w1",
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

func TestComplete(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	_, err := client.Complete(context.Background(), &lqpb.CompleteRequest{
		Type: "work", TaskId: "task-1", WorkerId: "w1",
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

func TestFail(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	_, err := client.Fail(context.Background(), &lqpb.FailRequest{
		Type: "work", TaskId: "task-1", WorkerId: "w1", Reason: "timeout",
	})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
}

func TestGetMetrics(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	resp, err := client.GetMetrics(context.Background(), &lqpb.MetricsRequest{Type: "work"})
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if resp.ArrivalRate != 2 {
		t.Errorf("arrival_rate = %v, want 2", resp.ArrivalRate)
	}
}

func TestHealth(t *testing.T) {
	client := newTestServer(t, &stubRepo{})
	resp, err := client.Health(context.Background(), &lqpb.HealthRequest{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true for backend without HealthChecker")
	}
}

func TestGrpcErr_notFound(t *testing.T) {
	repo := &errRepo{heartbeatErr: lq.ErrNotFound}
	client := newTestServer(t, repo)
	_, err := client.Heartbeat(context.Background(), &lqpb.HeartbeatRequest{
		Type: "work", TaskId: "task-1", WorkerId: "w1",
	})
	if code := status.Code(err); code != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", code)
	}
}

func TestGrpcErr_notOwner(t *testing.T) {
	repo := &errRepo{completeErr: lq.ErrNotOwner}
	client := newTestServer(t, repo)
	_, err := client.Complete(context.Background(), &lqpb.CompleteRequest{
		Type: "work", TaskId: "task-1", WorkerId: "w1",
	})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", code)
	}
}

func TestNewServer_nilRepo(t *testing.T) {
	_, err := scheduler.NewServer[string](nil, scheduler.StringIDCodec{})
	if err == nil {
		t.Fatal("expected error for nil repo")
	}
}

func TestNewServer_nilCodec(t *testing.T) {
	_, err := scheduler.NewServer[string](&stubRepo{}, nil)
	if err == nil {
		t.Fatal("expected error for nil codec")
	}
}

// Compile-time interface checks.
var (
	_ lq.TaskRepository[string, json.RawMessage] = (*stubRepo)(nil)
	_ lq.TaskRepository[string, json.RawMessage] = (*errRepo)(nil)
)

// ─── errRepo for error mapping tests ─────────────────────────────────────────

type errRepo struct {
	heartbeatErr error
	completeErr  error
	failErr      error
}

func (r *errRepo) Push(_ context.Context, _ lq.RawTaskEntry[json.RawMessage]) (string, error) {
	return "", nil
}
func (r *errRepo) Pop(ctx context.Context, _, _ string, _ []string, _ ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	<-ctx.Done()
	return lq.RawTask[string, json.RawMessage]{}, ctx.Err()
}
func (r *errRepo) Heartbeat(_ context.Context, _ string, _ string, _ string) error {
	return r.heartbeatErr
}
func (r *errRepo) Complete(_ context.Context, _ string, _ string, _ string) error {
	return r.completeErr
}
func (r *errRepo) Fail(_ context.Context, _ string, _ string, _ string, _ error) error {
	return r.failErr
}
func (r *errRepo) AgeQueued(_ context.Context, _ string, _ float64) error          { return nil }
func (r *errRepo) RequeueZombies(_ context.Context, _ string, _ time.Duration) error { return nil }
func (r *errRepo) Metrics(_ context.Context, _ string, _ time.Duration) (lq.TaskMetrics, error) {
	return lq.TaskMetrics{}, nil
}
func (r *errRepo) Close() error { return nil }

// ─── TestScalingEvents ────────────────────────────────────────────────────────

func TestScalingEvents(t *testing.T) {
	client := newTestServer(t, &stubRepo{})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.ScalingEvents(ctx, &lqpb.ScalingEventsRequest{
		Type: "work",
	})
	if err != nil {
		t.Fatalf("ScalingEvents: %v", err)
	}

	// With a 30s default interval we won't get an event in time, but we can
	// verify the stream opens and cancels cleanly.
	cancel()
	_, err = stream.Recv()
	if err != nil && !errors.Is(err, context.Canceled) {
		if code := status.Code(err); code != codes.Canceled && code != codes.DeadlineExceeded {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
