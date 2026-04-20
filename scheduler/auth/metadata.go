package auth

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const apiKeyHeader = "x-api-key"

func apiKeyFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("missing gRPC metadata")
	}
	vals := md.Get(apiKeyHeader)
	if len(vals) == 0 {
		return "", fmt.Errorf("missing %s header", apiKeyHeader)
	}
	return vals[0], nil
}
