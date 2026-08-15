package grpc

import "context"

// tenantIDContextKey is an unexported type so no other package can collide
// with this key by accident (the standard context-key-type idiom).
type tenantIDContextKey struct{}

// withTenantID stores the AUTHENTICATED tenant_id TenantAuthInterceptor
// resolved for this RPC -- the only place a handler may get a tenant_id
// from (never a request field, see CreatePaymentRequest's proto doc).
func withTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantIDContextKey{}, tenantID)
}

// tenantIDFromContext reads back what withTenantID stored. Empty if the
// interceptor never ran (e.g. a non-PaymentService RPC, or a test server
// built without it) -- callers pass that empty string straight into
// Service.GetPayment/GetRefund/GetPayout's tenant-scoped WHERE clause, which
// then correctly matches nothing rather than silently reading any row.
func tenantIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(tenantIDContextKey{}).(string)
	return v
}
