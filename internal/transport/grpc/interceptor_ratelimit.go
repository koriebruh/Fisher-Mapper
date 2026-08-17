package grpc

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"Fisher-Mapper/internal/resilience/ratelimit"
)

// RateLimitInterceptor builds a grpc.UnaryServerInterceptor backed by the
// SAME *ratelimit.Limiter instance the REST transport's
// ratelimit.Middleware uses (wired by cmd/server) -- one shared token-bucket
// set per key, so gRPC cannot be used to bypass REST's rate limiting.
//
// Kept in this package rather than resilience/ratelimit itself: that
// package already couples to fiber for its REST-side Middleware, but adding
// a google.golang.org/grpc dependency there too would make a
// stub-cheap resilience primitive depend on BOTH transport stacks. Transport
// packages own transport-specific glue; resilience/ratelimit only exposes
// the transport-agnostic Limiter.Allow.
//
// Keying mirrors REST's ratelimit.APIKeyOrIP: the "x-api-key" metadata
// value if present (gRPC lower-cases all metadata keys on the wire,
// regardless of how a client sets it), otherwise the caller's IP -- host
// only, via net.SplitHostPort, NOT peer.Addr.String() verbatim. Keying on
// the full host:port would give every new TCP connection (a fresh ephemeral
// port) its own token bucket, making the limiter a no-op against a real
// client that reconnects, even though a single long-lived grpc.ClientConn
// (as used by grpcurl and this template's own test client) would appear to
// work correctly under that broken keying.
// enabled is checked on every RPC (not just once at construction) so the
// dynamic-config ratelimit.enabled toggle can flip behavior without a
// redeploy; nil means "always enabled".
func RateLimitInterceptor(l *ratelimit.Limiter, enabled func() bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if enabled != nil && !enabled() {
			return handler(ctx, req)
		}
		key := rateLimitKey(ctx)
		if key != "" && !l.Allow(key) {
			return nil, status.Error(codes.ResourceExhausted, "too many requests")
		}
		return handler(ctx, req)
	}
}

func rateLimitKey(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-api-key"); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}

	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		// Addr wasn't in host:port form (e.g. some in-process/bufconn
		// transports) -- fall back to the raw string rather than dropping
		// rate limiting for this request entirely.
		return p.Addr.String()
	}
	return host
}
