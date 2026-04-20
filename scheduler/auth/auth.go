// Package auth provides mTLS configuration and API-key gRPC interceptors.
package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const apiKeyHeader = "x-api-key"

// ServerTLS builds a gRPC TransportCredentials from PEM files for mTLS.
// caFile is the CA that signed client certificates; pass "" to skip client cert
// verification (one-way TLS).
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
			return nil, status.Error(codes.Internal, "auth: failed to parse CA certificate")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(cfg), nil
}

// ClientTLS builds TransportCredentials for a client connecting with mTLS.
// certFile/keyFile are the client cert pair; caFile is the CA that signed the server cert.
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
		return nil, status.Error(codes.Internal, "auth: failed to parse CA certificate")
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}
	return credentials.NewTLS(cfg), nil
}

// APIKeyInterceptors returns unary and stream server interceptors that require
// a valid API key in the x-api-key gRPC metadata header.
// Pass at least one key; requests missing or presenting an invalid key are
// rejected with codes.Unauthenticated.
func APIKeyInterceptors(validKeys ...string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	allowed := make(map[string]struct{}, len(validKeys))
	for _, k := range validKeys {
		if k != "" {
			allowed[k] = struct{}{}
		}
	}
	check := func(ctx context.Context) error {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "auth: missing metadata")
		}
		vals := md.Get(apiKeyHeader)
		if len(vals) == 0 {
			return status.Errorf(codes.Unauthenticated, "auth: missing %s header", apiKeyHeader)
		}
		if _, ok := allowed[vals[0]]; !ok {
			return status.Error(codes.Unauthenticated, "auth: invalid API key")
		}
		return nil
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}

// APIKeyCredential is a grpc.PerRPCCredentials implementation for clients.
type APIKeyCredential struct {
	Key      string
	Insecure bool // set true when not using TLS (testing only)
}

func (a APIKeyCredential) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{apiKeyHeader: a.Key}, nil
}

func (a APIKeyCredential) RequireTransportSecurity() bool { return !a.Insecure }
