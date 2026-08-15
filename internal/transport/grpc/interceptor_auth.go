package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"Fisher-Mapper/internal/platform/tenantauth"
	pb "Fisher-Mapper/internal/transport/grpc/pb/payment/v1"
)

// paymentServiceMethodPrefix scopes TenantAuthInterceptor to PaymentService
// RPCs only (task requirement: not the reflection service, not a future
// health check) -- derived from the generated ServiceDesc rather than a
// hand-typed literal so a proto package rename can't silently widen or
// narrow what this interceptor protects.
var paymentServiceMethodPrefix = "/" + pb.PaymentService_ServiceDesc.ServiceName + "/"

// TenantAuthInterceptor builds a grpc.UnaryServerInterceptor that resolves
// the caller's tenant identity from the "x-api-key" metadata value against
// store -- the gRPC-idiomatic equivalent of REST's tenantAuth middleware
// (internal/transport/rest/tenantauth.go): same tenant_api_keys table, same
// Store.Resolve, only the credential's transport differs (gRPC has no
// headers, only metadata). CRITICAL security fix: today nothing on this
// service verifies who is calling at all.
//
// enabled is checked on every RPC, matching RateLimitInterceptor's own
// dynamic-config toggle pattern -- but there is no toggle for THIS
// interceptor (see cmd/server wiring): authentication on a money-moving
// endpoint is not something an operator should be able to flip off live,
// unlike rate limiting or the circuit breaker.
func TenantAuthInterceptor(store *tenantauth.Store) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !strings.HasPrefix(info.FullMethod, paymentServiceMethodPrefix) {
			return handler(ctx, req)
		}

		apiKey := apiKeyFromMetadata(ctx)
		if apiKey == "" {
			return nil, status.Error(codes.Unauthenticated, "missing x-api-key metadata")
		}

		tenantID, err := store.Resolve(ctx, apiKey)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid x-api-key")
		}

		return handler(withTenantID(ctx, tenantID), req)
	}
}

func apiKeyFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-api-key")
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}
