package auth_test

import (
	"context"
	"encoding/json"
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

// noopRepo is the minimal TaskRepository used by the scheduler under test.
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

func serverWithAPIKey(t *testing.T, keys ...string) string {
	t.Helper()
	srv, err := scheduler.NewServer[string](noopRepo{}, scheduler.StringIDCodec{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	unary, stream := auth.APIKeyInterceptors(keys...)
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(unary),
		grpc.StreamInterceptor(stream),
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

// dial opens a client connection to addr.
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

func TestAPIKeyInterceptor_valid(t *testing.T) {
	addr := serverWithAPIKey(t, "secret-key")
	conn := dial(t, addr,
		grpc.WithPerRPCCredentials(auth.APIKeyCredential{Key: "secret-key", Insecure: true}),
	)
	client := lqpb.NewSchedulerClient(conn)
	resp, err := client.Health(context.Background(), &lqpb.HealthRequest{})
	if err != nil {
		t.Fatalf("valid key was rejected: %v", err)
	}
	if !resp.Healthy {
		t.Error("expected healthy=true")
	}
}

func TestAPIKeyInterceptor_missing(t *testing.T) {
	addr := serverWithAPIKey(t, "secret-key")
	conn := dial(t, addr)
	client := lqpb.NewSchedulerClient(conn)

	_, err := client.Health(context.Background(), &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for missing key, got %v", code)
	}
}

func TestAPIKeyInterceptor_wrong(t *testing.T) {
	addr := serverWithAPIKey(t, "secret-key")
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "wrong-key")
	conn := dial(t, addr)
	client := lqpb.NewSchedulerClient(conn)

	_, err := client.Health(ctx, &lqpb.HealthRequest{})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for wrong key, got %v", code)
	}
}

func TestAPIKeyCredential_requireTransportSecurity(t *testing.T) {
	cred := auth.APIKeyCredential{Key: "k", Insecure: false}
	if !cred.RequireTransportSecurity() {
		t.Error("expected RequireTransportSecurity=true when Insecure=false")
	}
	insecureCred := auth.APIKeyCredential{Key: "k", Insecure: true}
	if insecureCred.RequireTransportSecurity() {
		t.Error("expected RequireTransportSecurity=false when Insecure=true")
	}
}

func TestAPIKeyCredential_metadata(t *testing.T) {
	cred := auth.APIKeyCredential{Key: "my-key", Insecure: true}
	md, err := cred.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["x-api-key"] != "my-key" {
		t.Errorf("x-api-key = %q, want my-key", md["x-api-key"])
	}
}
