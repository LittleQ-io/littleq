// Package auth provides identity, authorization, and TLS helpers for the
// littleq gRPC Scheduler service.
package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// ─── Core interfaces ──────────────────────────────────────────────────────────

// Identity represents an authenticated subject. Implementations are returned
// by SecurityGuard.Authenticate and passed to SecurityGuard.Authorize.
type Identity interface {
	UserID() string
	// Claims carries arbitrary metadata such as roles, scopes, or org_id.
	Claims() map[string]any
}

// Decision is the outcome of an authorization check.
type Decision struct {
	Allowed bool
	// Scope defines the breadth of access (e.g. "all", "owned", "tenant").
	Scope string
	// Reason aids debugging of denied requests.
	Reason string
}

// SecurityGuard is the single interface callers implement to plug authentication
// and authorization into the Scheduler's gRPC interceptors.
//
// Authenticate extracts and validates credentials from the incoming gRPC
// context (metadata headers, TLS peer cert, etc.).
//
// Authorize decides whether the identity may perform action. For gRPC calls
// action is the full method path "/package.Service/Method".
type SecurityGuard interface {
	Authenticate(ctx context.Context) (Identity, error)
	Authorize(ctx context.Context, id Identity, action string) (Decision, error)
}

// ─── Context key ─────────────────────────────────────────────────────────────

type ctxKey struct{}

// IdentityFromContext returns the Identity stored by the interceptor, or nil.
func IdentityFromContext(ctx context.Context) Identity {
	id, _ := ctx.Value(ctxKey{}).(Identity)
	return id
}

func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// ─── gRPC interceptors ───────────────────────────────────────────────────────

// UnaryInterceptor returns a gRPC unary server interceptor that calls
// guard.Authenticate then guard.Authorize on every request.
func UnaryInterceptor(guard SecurityGuard) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authorize(ctx, guard, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream server interceptor that calls
// guard.Authenticate then guard.Authorize on stream open.
func StreamInterceptor(guard SecurityGuard) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authorize(ss.Context(), guard, info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ss, ctx})
	}
}

func authorize(ctx context.Context, guard SecurityGuard, action string) (context.Context, error) {
	id, err := guard.Authenticate(ctx)
	if err != nil {
		return ctx, status.Errorf(codes.Unauthenticated, "auth: authenticate: %v", err)
	}
	d, err := guard.Authorize(ctx, id, action)
	if err != nil {
		return ctx, status.Errorf(codes.Internal, "auth: authorize: %v", err)
	}
	if !d.Allowed {
		return ctx, status.Errorf(codes.PermissionDenied, "auth: denied: %s", d.Reason)
	}
	return withIdentity(ctx, id), nil
}

// wrappedStream replaces the stream's context with the authenticated one.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// ─── Built-in SecurityGuard implementations ──────────────────────────────────

// APIKeyGuard is a SecurityGuard that authenticates via an x-api-key metadata
// header and allows all authenticated identities unconditionally.
//
// It is a convenience default; replace with a real guard in production.
type APIKeyGuard struct {
	// validKeys is the allow-list; any non-empty matching value is accepted.
	validKeys map[string]struct{}
}

// NewAPIKeyGuard constructs an APIKeyGuard from the given keys.
// At least one key must be provided; empty keys are ignored.
func NewAPIKeyGuard(keys ...string) *APIKeyGuard {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k != "" {
			m[k] = struct{}{}
		}
	}
	return &APIKeyGuard{validKeys: m}
}

func (g *APIKeyGuard) Authenticate(ctx context.Context) (Identity, error) {
	key, err := apiKeyFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := g.validKeys[key]; !ok {
		return nil, fmt.Errorf("invalid API key")
	}
	return &apiKeyIdentity{userID: key}, nil
}

func (g *APIKeyGuard) Authorize(_ context.Context, _ Identity, _ string) (Decision, error) {
	return Decision{Allowed: true, Scope: "all"}, nil
}

// apiKeyIdentity is the Identity produced by APIKeyGuard.
type apiKeyIdentity struct{ userID string }

func (i *apiKeyIdentity) UserID() string            { return i.userID }
func (i *apiKeyIdentity) Claims() map[string]any    { return nil }

// ─── TLS helpers ─────────────────────────────────────────────────────────────

// ServerTLS builds gRPC TransportCredentials for mTLS.
// caFile is the CA that signed client certs; pass "" for one-way TLS.
func ServerTLS(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("auth: failed to parse CA certificate")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(cfg), nil
}

// ClientTLS builds TransportCredentials for mTLS clients.
func ClientTLS(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("auth: failed to parse CA certificate")
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// ─── Client credential helper ─────────────────────────────────────────────────

// APIKeyCredential implements grpc.PerRPCCredentials, injecting the API key
// into every outgoing call's x-api-key metadata header.
type APIKeyCredential struct {
	Key      string
	Insecure bool // set true when not using TLS (testing only)
}

func (a APIKeyCredential) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{apiKeyHeader: a.Key}, nil
}

func (a APIKeyCredential) RequireTransportSecurity() bool { return !a.Insecure }
