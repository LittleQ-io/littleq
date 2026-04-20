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

// noopRepo satisfies TaskRepository[string, json.RawMessage] for tests.
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

func newServerWithGuard(t *testing.T, guard auth.SecurityGuard) string {
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
	return lis.Addr().String()
}

func dial(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// ─── APIKeyGuard tests ────────────────────────────────────────────────────────

func TestAPIKeyGuard_valid(t *testing.T) {
	addr := newServerWithGuard(t, auth.NewAPIKeyGuard("secret-key"))
	conn := dial(t, addr,
		grpc.WithPerRPCCredentials(auth.APIKeyCredential{Key: "secret-key", Insecure: true}),
	)
	resp, err := lqpb.NewSchedulerClient(conn).Health(context.Background(), &lqpb.HealthRequest{})
	if err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true")
	}
}

func TestAPIKeyGuard_missingKey(t *testing.T) {
	addr := newServerWithGuard(t, auth.NewAPIKeyGuard("secret-key"))
	conn := dial(t, addr)
	_, err := lqpb.NewSchedulerClient(conn).Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", code)
	}
}

func TestAPIKeyGuard_wrongKey(t *testing.T) {
	addr := newServerWithGuard(t, auth.NewAPIKeyGuard("secret-key"))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "bad")
	conn := dial(t, addr)
	_, err := lqpb.NewSchedulerClient(conn).Health(ctx, &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", code)
	}
}

// ─── Custom SecurityGuard ─────────────────────────────────────────────────────

// denyAllGuard always fails authentication.
type denyAllGuard struct{}

func (denyAllGuard) Authenticate(_ context.Context) (auth.Identity, error) {
	return nil, fmt.Errorf("no credentials")
}
func (denyAllGuard) Authorize(_ context.Context, _ auth.Identity, _ string) (auth.Decision, error) {
	return auth.Decision{}, nil
}

func TestCustomGuard_denyAll(t *testing.T) {
	addr := newServerWithGuard(t, denyAllGuard{})
	conn := dial(t, addr)
	_, err := lqpb.NewSchedulerClient(conn).Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated from denyAllGuard, got %v", code)
	}
}

// authorizeOnlyGuard authenticates everyone but denies specific actions.
type authorizeOnlyGuard struct{ deniedAction string }

func (g *authorizeOnlyGuard) Authenticate(_ context.Context) (auth.Identity, error) {
	return &fixedIdentity{"user-1"}, nil
}
func (g *authorizeOnlyGuard) Authorize(_ context.Context, _ auth.Identity, action string) (auth.Decision, error) {
	if action == g.deniedAction {
		return auth.Decision{Allowed: false, Reason: "action not permitted"}, nil
	}
	return auth.Decision{Allowed: true, Scope: "all"}, nil
}

type fixedIdentity struct{ id string }

func (f *fixedIdentity) UserID() string         { return f.id }
func (f *fixedIdentity) Claims() map[string]any { return nil }

func TestCustomGuard_authorizeDenied(t *testing.T) {
	guard := &authorizeOnlyGuard{deniedAction: "/littleq.v1.Scheduler/Health"}
	addr := newServerWithGuard(t, guard)
	conn := dial(t, addr)
	_, err := lqpb.NewSchedulerClient(conn).Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", code)
	}
}

// ─── IdentityFromContext ──────────────────────────────────────────────────────

func TestIdentityFromContext_empty(t *testing.T) {
	if id := auth.IdentityFromContext(context.Background()); id != nil {
		t.Errorf("expected nil, got %v", id)
	}
}

// ─── APIKeyCredential ─────────────────────────────────────────────────────────

func TestAPIKeyCredential_requireTransportSecurity(t *testing.T) {
	if !( auth.APIKeyCredential{Key: "k"}.RequireTransportSecurity()) {
		t.Error("expected true when Insecure=false")
	}
	if (auth.APIKeyCredential{Key: "k", Insecure: true}.RequireTransportSecurity()) {
		t.Error("expected false when Insecure=true")
	}
}

func TestAPIKeyCredential_metadata(t *testing.T) {
	md, err := auth.APIKeyCredential{Key: "my-key", Insecure: true}.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["x-api-key"] != "my-key" {
		t.Errorf("x-api-key = %q, want my-key", md["x-api-key"])
	}
}
