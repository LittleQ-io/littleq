// Package auth defines the authentication and authorization seam for the
// littleq gRPC Scheduler service.
//
// The library provides the interfaces and interceptors; callers supply the
// concrete SecurityGuard implementation (JWT, mTLS peer cert, API key, RBAC,
// OPA — anything).
package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Identity represents an authenticated subject returned by SecurityGuard.Authenticate.
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

// SecurityGuard is the interface callers implement to plug authentication and
// authorization into the Scheduler interceptors.
//
// Authenticate extracts and validates credentials from the incoming gRPC
// context (metadata headers, TLS peer cert, etc.) and returns the caller's
// Identity on success.
//
// Authorize decides whether the identity may perform action. For gRPC calls
// action is the full method path "/package.Service/Method".
type SecurityGuard interface {
	Authenticate(ctx context.Context) (Identity, error)
	Authorize(ctx context.Context, id Identity, action string) (Decision, error)
}

// ─── Context ──────────────────────────────────────────────────────────────────

type ctxKey struct{}

// IdentityFromContext returns the Identity stored by the interceptor, or nil
// when no interceptor is in use.
func IdentityFromContext(ctx context.Context) Identity {
	id, _ := ctx.Value(ctxKey{}).(Identity)
	return id
}

func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// ─── gRPC interceptors ───────────────────────────────────────────────────────

// UnaryInterceptor returns a gRPC unary server interceptor that calls
// guard.Authenticate then guard.Authorize before passing control to the handler.
// The authenticated Identity is available to handlers via IdentityFromContext.
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
// guard.Authenticate then guard.Authorize when the stream is opened.
// The authenticated Identity is available to handlers via IdentityFromContext.
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
		return ctx, status.Errorf(codes.Unauthenticated, "auth: %v", err)
	}
	d, err := guard.Authorize(ctx, id, action)
	if err != nil {
		return ctx, status.Errorf(codes.Internal, "auth: authorize: %v", err)
	}
	if !d.Allowed {
		return ctx, status.Errorf(codes.PermissionDenied, "auth: %s", d.Reason)
	}
	return withIdentity(ctx, id), nil
}

// wrappedStream replaces the stream context with the authenticated one.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
