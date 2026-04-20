package auth_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	lq "github.com/LittleQ-io/littleq"
	lqpb "github.com/LittleQ-io/littleq/proto/littleq/v1"
	"github.com/LittleQ-io/littleq/scheduler"
	"github.com/LittleQ-io/littleq/scheduler/auth"
)

// ─── stub repo ────────────────────────────────────────────────────────────────

type noopRepo struct{}

func (noopRepo) Push(_ context.Context, _ lq.RawTaskEntry[json.RawMessage]) (string, error) {
	return "id", nil
}
func (noopRepo) Pop(ctx context.Context, _, _ string, _ []string, _ ...lq.PopOption) (lq.RawTask[string, json.RawMessage], error) {
	<-ctx.Done()
	return lq.RawTask[string, json.RawMessage]{}, ctx.Err()
}
func (noopRepo) Heartbeat(_ context.Context, _ string, _ string, _ string) error      { return nil }
func (noopRepo) Complete(_ context.Context, _ string, _ string, _ string) error       { return nil }
func (noopRepo) Fail(_ context.Context, _ string, _ string, _ string, _ error) error  { return nil }
func (noopRepo) AgeQueued(_ context.Context, _ string, _ float64) error               { return nil }
func (noopRepo) RequeueZombies(_ context.Context, _ string, _ time.Duration) error    { return nil }
func (noopRepo) Metrics(_ context.Context, _ string, _ time.Duration) (lq.TaskMetrics, error) {
	return lq.TaskMetrics{}, nil
}
func (noopRepo) Close() error { return nil }

var _ lq.TaskRepository[string, json.RawMessage] = noopRepo{}

// ─── helpers ──────────────────────────────────────────────────────────────────

func newServer(t *testing.T, guard auth.SecurityGuard) lqpb.SchedulerClient {
	t.Helper()
	srv, err := scheduler.NewServer[string](noopRepo{}, scheduler.StringIDCodec{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryInterceptor(guard)),
		grpc.StreamInterceptor(auth.StreamInterceptor(guard)),
	)
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

// ─── simple in-process guard implementations used only in tests ───────────────

// apiKeyGuard is a test-only SecurityGuard that checks x-api-key metadata.
type apiKeyGuard struct{ validKeys map[string]struct{} }

func newAPIKeyGuard(keys ...string) *apiKeyGuard {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return &apiKeyGuard{m}
}

func (g *apiKeyGuard) Authenticate(ctx context.Context) (auth.Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}
	vals := md.Get("x-api-key")
	if len(vals) == 0 {
		return nil, fmt.Errorf("missing x-api-key")
	}
	if _, ok := g.validKeys[vals[0]]; !ok {
		return nil, fmt.Errorf("invalid key")
	}
	return &simpleIdentity{vals[0]}, nil
}
func (g *apiKeyGuard) Authorize(_ context.Context, _ auth.Identity, _ string) (auth.Decision, error) {
	return auth.Decision{Allowed: true, Scope: "all"}, nil
}

// denyAllGuard always fails authentication.
type denyAllGuard struct{}

func (denyAllGuard) Authenticate(_ context.Context) (auth.Identity, error) {
	return nil, fmt.Errorf("no credentials")
}
func (denyAllGuard) Authorize(_ context.Context, _ auth.Identity, _ string) (auth.Decision, error) {
	return auth.Decision{}, nil
}

// actionDenyGuard authenticates everyone but denies a specific gRPC action.
type actionDenyGuard struct{ denied string }

func (g *actionDenyGuard) Authenticate(_ context.Context) (auth.Identity, error) {
	return &simpleIdentity{"user"}, nil
}
func (g *actionDenyGuard) Authorize(_ context.Context, _ auth.Identity, action string) (auth.Decision, error) {
	if action == g.denied {
		return auth.Decision{Allowed: false, Reason: "action not permitted"}, nil
	}
	return auth.Decision{Allowed: true, Scope: "all"}, nil
}

type simpleIdentity struct{ id string }

func (s *simpleIdentity) UserID() string         { return s.id }
func (s *simpleIdentity) Claims() map[string]any { return nil }

// ─── tests ────────────────────────────────────────────────────────────────────

func TestUnaryInterceptor_authenticated(t *testing.T) {
	client := newServer(t, newAPIKeyGuard("good-key"))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "good-key")
	resp, err := client.Health(ctx, &lqpb.HealthRequest{})
	if err != nil {
		t.Fatalf("authenticated request rejected: %v", err)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true")
	}
}

func TestUnaryInterceptor_missingCredentials(t *testing.T) {
	client := newServer(t, newAPIKeyGuard("good-key"))
	_, err := client.Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", code)
	}
}

func TestUnaryInterceptor_invalidCredentials(t *testing.T) {
	client := newServer(t, newAPIKeyGuard("good-key"))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "bad-key")
	_, err := client.Health(ctx, &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", code)
	}
}

func TestUnaryInterceptor_authorizationDenied(t *testing.T) {
	client := newServer(t, &actionDenyGuard{denied: "/littleq.v1.Scheduler/Health"})
	_, err := client.Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", code)
	}
}

func TestStreamInterceptor_authenticated(t *testing.T) {
	client := newServer(t, newAPIKeyGuard("key"))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "key")
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	stream, err := client.Subscribe(ctx, &lqpb.SubscribeRequest{WorkerId: "w1", Type: "t"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Stream opens; no tasks means Recv blocks until ctx cancels.
	_, err = stream.Recv()
	if code := status.Code(err); code == codes.Unauthenticated || code == codes.PermissionDenied {
		t.Fatalf("authenticated stream was rejected: %v", err)
	}
}

func TestStreamInterceptor_unauthenticated(t *testing.T) {
	client := newServer(t, denyAllGuard{})
	stream, err := client.Subscribe(context.Background(), &lqpb.SubscribeRequest{WorkerId: "w1", Type: "t"})
	if err != nil {
		t.Fatalf("Subscribe open: %v", err)
	}
	_, err = stream.Recv()
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", code)
	}
}

func TestIdentityFromContext_empty(t *testing.T) {
	if id := auth.IdentityFromContext(context.Background()); id != nil {
		t.Errorf("expected nil, got %v", id)
	}
}
