package grpc

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/platform/tenantauth"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
	"Fisher-Mapper/internal/resilience/ratelimit"
	pb "Fisher-Mapper/internal/transport/grpc/pb/payment/v1"
)

// This test proves the thing Fase 6 exists to prove: gRPC is a thin
// transport over the exact same payment.Service REST uses, not a
// reimplementation with different behavior. It starts a REAL grpc.Server
// (backed by a real Postgres-backed Service) on a loopback port, drives it
// with a real grpc client, and follows the same "call the worker handler
// directly instead of waiting on a real asynq round-trip" pattern
// internal/domain/payment/service_integration_test.go already established
// for exercising the async pending -> processing -> succeeded flow
// deterministically (see that file's chargeInputFor doc) -- cmd/server
// never runs the worker itself, so a test of cmd/server's own transport has
// no asynq consumer to wait on either way.
//
// Gated on TEST_POSTGRES_DSN, same as every other DB-backed test in this
// repo; migrations 00001+ (including 00010_tenant_api_keys) must already be
// applied.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping DB-backed integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect test postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type singleProviderRegistry struct {
	p provider.Provider
}

func (r singleProviderRegistry) Get(string) (provider.Provider, error) {
	return r.p, nil
}

// startTestServer wires a real payment.Service (Postgres-backed) behind a
// real grpc.Server on an ephemeral loopback port, WITH TenantAuthInterceptor
// wired exactly like cmd/server does (ChainUnaryInterceptor), and returns a
// connected client plus the Service itself (the test needs the latter to
// invoke ProcessCharge/ProcessRefund directly, standing in for the worker)
// plus the *tenantauth.Store backing the interceptor, so tests can mint
// their own per-test tenant API keys (never a shared/seeded key -- see
// migration 00010's doc for why no such key exists to share).
func startTestServer(t *testing.T, pool *pgxpool.Pool) (pb.PaymentServiceClient, *payment.Service, *tenantauth.Store) {
	t.Helper()

	mockProv := mock.New(mock.Config{Name: "mock"})
	repo := payment.NewPGRepository(pool)
	idemStore := idempotency.NewPGStore(pool)
	svc := payment.NewService(repo, idemStore, singleProviderRegistry{mockProv})
	tenantStore := tenantauth.NewStore(pool)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(TenantAuthInterceptor(tenantStore)))
	pb.RegisterPaymentServiceServer(grpcServer, NewServer(svc))

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewPaymentServiceClient(conn), svc, tenantStore
}

// newTenant mints a fresh, unique tenant_id + API key pair via the real
// tenantauth.Store.CreateKey path (the same one cmd/migrate's
// -create-tenant-key flag uses) and returns an authenticated context ready
// to pass to a client call (metadata carrying x-api-key). Each test gets its
// own never-reused tenant, exactly like the pre-existing suite's
// uuid.NewString() tenant IDs -- no dependence on a shared/seeded key.
func newTenant(ctx context.Context, t *testing.T, store *tenantauth.Store) (tenantID string, authedCtx context.Context) {
	t.Helper()
	tenantID = uuid.NewString()
	apiKey, err := store.CreateKey(ctx, tenantID)
	if err != nil {
		t.Fatalf("create tenant key: %v", err)
	}
	return tenantID, metadata.AppendToOutgoingContext(ctx, "x-api-key", apiKey)
}

func TestGRPC_CreatePayment_AsyncFlow_MatchesRESTBehavior(t *testing.T) {
	pool := testPool(t)
	client, svc, tenantStore := startTestServer(t, pool)
	tenantID, ctx := newTenant(context.Background(), t, tenantStore)

	key := uuid.NewString()

	createResp, err := client.CreatePayment(ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		Currency:       "USD",
		Amount:         1500,
		Provider:       "mock",
	})
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if createResp.GetStatus() != string(payment.StatusPending) {
		t.Fatalf("CreatePayment status = %q, want %q (async: must not block for the provider call)", createResp.GetStatus(), payment.StatusPending)
	}
	if createResp.GetReplayed() {
		t.Fatalf("fresh CreatePayment call reported Replayed=true")
	}

	paymentID, err := uuid.Parse(createResp.GetPaymentId())
	if err != nil {
		t.Fatalf("parse payment id: %v", err)
	}

	// Stand in for the worker: same ChargeTaskInput shape a real outbox
	// dispatch would have produced for this exact CreatePayment call.
	if err := svc.ProcessCharge(ctx, payment.ChargeTaskInput{
		PaymentID:      paymentID,
		IdempotencyKey: key,
		TenantID:       tenantID,
		Currency:       "USD",
		Amount:         1500,
		Provider:       "mock",
	}); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	getResp, err := client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: createResp.GetPaymentId()})
	if err != nil {
		t.Fatalf("GetPayment: %v", err)
	}
	if getResp.GetStatus() != string(payment.StatusSucceeded) {
		t.Fatalf("GetPayment status = %q, want %q after ProcessCharge", getResp.GetStatus(), payment.StatusSucceeded)
	}
	if getResp.GetProviderRef() == "" {
		t.Fatal("GetPayment: provider_ref empty after succeeded charge")
	}
	if getResp.GetTenantId() != tenantID {
		t.Errorf("GetPayment TenantId = %q, want %q (the AUTHENTICATED caller's tenant, not a request field -- CreatePaymentRequest has none)", getResp.GetTenantId(), tenantID)
	}
}

func TestGRPC_CreatePayment_IdempotencyReplayAndConflict(t *testing.T) {
	pool := testPool(t)
	client, _, tenantStore := startTestServer(t, pool)
	_, ctx := newTenant(context.Background(), t, tenantStore)

	key := uuid.NewString()

	req := &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		Currency:       "USD",
		Amount:         2000,
		Provider:       "mock",
	}

	first, err := client.CreatePayment(ctx, req)
	if err != nil {
		t.Fatalf("first CreatePayment: %v", err)
	}

	// Same key + same body -> replay, same payment id, Replayed=true.
	replay, err := client.CreatePayment(ctx, req)
	if err != nil {
		t.Fatalf("replay CreatePayment: %v", err)
	}
	if !replay.GetReplayed() {
		t.Error("replay call: Replayed = false, want true")
	}
	if replay.GetPaymentId() != first.GetPaymentId() {
		t.Errorf("replay PaymentId = %q, want %q (same as first call)", replay.GetPaymentId(), first.GetPaymentId())
	}

	// Same key + different body -> AlreadyExists (codeFor's documented
	// choice for apperror.CodeIdempotencyConflict).
	conflicting := &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		Currency:       "USD",
		Amount:         9999, // different from req's 2000
		Provider:       "mock",
	}
	_, err = client.CreatePayment(ctx, conflicting)
	if err == nil {
		t.Fatal("conflicting CreatePayment: want error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("conflicting CreatePayment: error is not a grpc status: %v", err)
	}
	if st.Code() != codes.AlreadyExists {
		t.Errorf("conflicting CreatePayment: code = %v, want %v", st.Code(), codes.AlreadyExists)
	}
}

func TestGRPC_GetPayment_NotFound(t *testing.T) {
	pool := testPool(t)
	client, _, tenantStore := startTestServer(t, pool)
	_, ctx := newTenant(context.Background(), t, tenantStore)

	_, err := client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: uuid.NewString()})
	if err == nil {
		t.Fatal("GetPayment for a nonexistent id: want error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a grpc status: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want %v", st.Code(), codes.NotFound)
	}
}

// TestGRPC_TenantAuth_MissingAPIKey_Unauthenticated is the CRITICAL fix's
// central gRPC proof: a call with no x-api-key metadata at all must be
// rejected before it ever reaches the service layer -- not merely receive a
// wrong/empty answer.
func TestGRPC_TenantAuth_MissingAPIKey_Unauthenticated(t *testing.T) {
	pool := testPool(t)
	client, _, _ := startTestServer(t, pool)

	_, err := client.GetPayment(context.Background(), &pb.GetPaymentRequest{PaymentId: uuid.NewString()})
	if err == nil {
		t.Fatal("GetPayment with no x-api-key metadata: want error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a grpc status: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want %v", st.Code(), codes.Unauthenticated)
	}
}

// TestGRPC_TenantAuth_InvalidAPIKey_Unauthenticated proves an API key that
// simply does not exist in tenant_api_keys is rejected the same way a
// missing one is -- not silently treated as some default/empty tenant.
func TestGRPC_TenantAuth_InvalidAPIKey_Unauthenticated(t *testing.T) {
	pool := testPool(t)
	client, _, _ := startTestServer(t, pool)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "not-a-real-key-"+uuid.NewString())
	_, err := client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: uuid.NewString()})
	if err == nil {
		t.Fatal("GetPayment with an unknown x-api-key: want error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a grpc status: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("code = %v, want %v", st.Code(), codes.Unauthenticated)
	}
}

// TestGRPC_TenantIsolation_CrossTenantGetPayment_NotFound is the CRITICAL
// finding's core proof, gRPC side: tenant-1 creates a payment; tenant-1's
// own key can read it back; tenant-2's key (a completely different,
// independently-issued API key/tenant) gets CodeNotFound for the exact same
// payment id, never the row. This is the read-side leak the finding
// describes ("Any caller who knows or enumerates a payment ... UUID can
// read another tenant's full record") -- proven closed, not just asserted.
func TestGRPC_TenantIsolation_CrossTenantGetPayment_NotFound(t *testing.T) {
	pool := testPool(t)
	client, svc, tenantStore := startTestServer(t, pool)
	background := context.Background()

	tenant1ID, tenant1Ctx := newTenant(background, t, tenantStore)
	_, tenant2Ctx := newTenant(background, t, tenantStore)

	key := uuid.NewString()
	createResp, err := client.CreatePayment(tenant1Ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		Currency:       "USD",
		Amount:         777,
		Provider:       "mock",
	})
	if err != nil {
		t.Fatalf("CreatePayment (tenant1): %v", err)
	}
	paymentID, err := uuid.Parse(createResp.GetPaymentId())
	if err != nil {
		t.Fatalf("parse payment id: %v", err)
	}
	if err := svc.ProcessCharge(background, payment.ChargeTaskInput{
		PaymentID:      paymentID,
		IdempotencyKey: key,
		TenantID:       tenant1ID,
		Currency:       "USD",
		Amount:         777,
		Provider:       "mock",
	}); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	// Owner reads its own payment: succeeds.
	if _, err := client.GetPayment(tenant1Ctx, &pb.GetPaymentRequest{PaymentId: createResp.GetPaymentId()}); err != nil {
		t.Fatalf("GetPayment as owning tenant: want success, got: %v", err)
	}

	// A DIFFERENT tenant, with its own genuinely valid API key, tries the
	// SAME payment id: must be CodeNotFound, never the row.
	_, err = client.GetPayment(tenant2Ctx, &pb.GetPaymentRequest{PaymentId: createResp.GetPaymentId()})
	if err == nil {
		t.Fatal("GetPayment for another tenant's payment id: want error, got nil (DATA LEAK)")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a grpc status: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("cross-tenant GetPayment code = %v, want %v (not-found, not a leaked row)", st.Code(), codes.NotFound)
	}
}

// TestGRPC_TenantIsolation_CreatePayment_UsesAuthenticatedTenant_NotSpoofable
// proves the write-side half of the CRITICAL finding is closed:
// CreatePaymentRequest has no tenant_id field at all (see its proto doc), so
// there is nothing left to spoof -- the payment this call creates is owned
// by whichever tenant the CALLER'S OWN api key resolves to, full stop. This
// asserts that concretely: create as tenant-2, then prove tenant-2's key can
// read it back (it must, since GetPayment is tenant-scoped) while a THIRD,
// unrelated tenant's key cannot.
func TestGRPC_TenantIsolation_CreatePayment_UsesAuthenticatedTenant_NotSpoofable(t *testing.T) {
	pool := testPool(t)
	client, _, tenantStore := startTestServer(t, pool)
	background := context.Background()

	_, tenant2Ctx := newTenant(background, t, tenantStore)
	_, otherTenantCtx := newTenant(background, t, tenantStore)

	createResp, err := client.CreatePayment(tenant2Ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: uuid.NewString(),
		Currency:       "USD",
		Amount:         4242,
		Provider:       "mock",
	})
	if err != nil {
		t.Fatalf("CreatePayment (tenant2): %v", err)
	}

	// The creating tenant's own key reads it back: proves the row really
	// was persisted under THAT tenant's identity, not some other/empty one.
	if _, err := client.GetPayment(tenant2Ctx, &pb.GetPaymentRequest{PaymentId: createResp.GetPaymentId()}); err != nil {
		t.Fatalf("creating tenant reading back its own payment: want success, got: %v", err)
	}

	// An unrelated tenant's key cannot -- confirms the payment is NOT
	// visible/owned by anyone but the authenticated creator.
	_, err = client.GetPayment(otherTenantCtx, &pb.GetPaymentRequest{PaymentId: createResp.GetPaymentId()})
	if err == nil {
		t.Fatal("unrelated tenant reading tenant2's payment: want error, got nil (DATA LEAK)")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Errorf("unrelated tenant GetPayment error = %v, want NotFound", err)
	}
}

func TestGRPC_CreateRefund_AsyncFlow(t *testing.T) {
	pool := testPool(t)
	client, svc, tenantStore := startTestServer(t, pool)
	background := context.Background()
	tenantID, ctx := newTenant(background, t, tenantStore)

	chargeKey := uuid.NewString()

	createResp, err := client.CreatePayment(ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: chargeKey,
		Currency:       "USD",
		Amount:         5000,
		Provider:       "mock",
	})
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	paymentID, err := uuid.Parse(createResp.GetPaymentId())
	if err != nil {
		t.Fatalf("parse payment id: %v", err)
	}

	if err := svc.ProcessCharge(background, payment.ChargeTaskInput{
		PaymentID:      paymentID,
		IdempotencyKey: chargeKey,
		TenantID:       tenantID,
		Currency:       "USD",
		Amount:         5000,
		Provider:       "mock",
	}); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	refundKey := uuid.NewString()
	refundResp, err := client.CreateRefund(ctx, &pb.CreateRefundRequest{
		IdempotencyKey: refundKey,
		PaymentId:      createResp.GetPaymentId(),
		Currency:       "USD",
		Amount:         1000,
	})
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	if refundResp.GetStatus() != string(payment.StatusPending) {
		t.Fatalf("CreateRefund status = %q, want %q (async, same as CreatePayment)", refundResp.GetStatus(), payment.StatusPending)
	}

	refundID, err := uuid.Parse(refundResp.GetRefundId())
	if err != nil {
		t.Fatalf("parse refund id: %v", err)
	}

	if err := svc.ProcessRefund(background, payment.RefundTaskInput{
		RefundID:           refundID,
		PaymentID:          paymentID,
		IdempotencyKey:     refundKey,
		TenantID:           tenantID,
		Currency:           "USD",
		Amount:             1000,
		Provider:           "mock",
		PaymentProviderRef: mustGetProviderRef(ctx, t, svc, paymentID, tenantID),
	}); err != nil {
		t.Fatalf("ProcessRefund: %v", err)
	}

	getResp, err := client.GetRefund(ctx, &pb.GetRefundRequest{RefundId: refundResp.GetRefundId()})
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if getResp.GetStatus() != string(payment.StatusSucceeded) {
		t.Fatalf("GetRefund status = %q, want %q after ProcessRefund", getResp.GetStatus(), payment.StatusSucceeded)
	}
}

func mustGetProviderRef(ctx context.Context, t *testing.T, svc *payment.Service, paymentID uuid.UUID, tenantID string) string {
	t.Helper()
	p, err := svc.GetPayment(ctx, paymentID, tenantID)
	if err != nil {
		t.Fatalf("GetPayment (fetch provider ref): %v", err)
	}
	if p.ProviderRef == nil {
		t.Fatal("payment has no provider_ref after ProcessCharge")
	}
	return *p.ProviderRef
}

// stubPaymentServer is a pb.PaymentServiceServer that never touches a real
// payment.Service -- just enough to let TestGRPC_RateLimitInterceptor_Rejects
// exercise the interceptor over a real grpc.Server/client pair without a
// Postgres dependency. Deliberately served with NO TenantAuthInterceptor
// (see the two rate-limit tests below): they exist to prove the rate
// limiter in isolation, not auth-plus-rate-limit interaction.
type stubPaymentServer struct {
	pb.UnimplementedPaymentServiceServer
}

func (stubPaymentServer) GetPayment(context.Context, *pb.GetPaymentRequest) (*pb.GetPaymentResponse, error) {
	return &pb.GetPaymentResponse{Status: string(payment.StatusPending)}, nil
}

// TestGRPC_RateLimitInterceptor_Rejects proves the interceptor itself
// (unit-level, no DB): a Limiter with a 1-token burst rejects the second
// call with ResourceExhausted, on a real in-process grpc.Server/client pair
// so this exercises the exact interceptor wiring cmd/server uses, not just
// Limiter.Allow directly.
func TestGRPC_RateLimitInterceptor_Rejects(t *testing.T) {
	limiter := ratelimit.New(1, 1) // 1 token/sec, burst 1 -- second call in the same instant has nothing left

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(RateLimitInterceptor(limiter, nil)))
	pb.RegisterPaymentServiceServer(grpcServer, stubPaymentServer{})
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewPaymentServiceClient(conn)

	// First call reaches the handler (not rate-limited): the stub always
	// returns a plain pending response, no error.
	ctx := context.Background()
	if _, err := client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: uuid.NewString()}); err != nil {
		t.Fatalf("first call unexpectedly failed: %v", err)
	}

	// Second call, same source (one shared grpc.ClientConn -> one peer
	// address), within the same burst window: ResourceExhausted.
	_, err = client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: uuid.NewString()})
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("second call: error is not a grpc status: %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("second call code = %v, want %v", st.Code(), codes.ResourceExhausted)
	}
}

// TestGRPC_RateLimitInterceptor_EnabledFalseBypassesLimiter proves the
// ratelimit.enabled dynamic-config toggle actually changes behavior: the
// exact same 1-token-burst Limiter that rejects a second call in the
// previous test lets an unbounded number of calls through once enabled
// reports false, since the interceptor short-circuits before ever touching
// the limiter.
func TestGRPC_RateLimitInterceptor_EnabledFalseBypassesLimiter(t *testing.T) {
	limiter := ratelimit.New(1, 1)
	disabled := func() bool { return false }

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(RateLimitInterceptor(limiter, disabled)))
	pb.RegisterPaymentServiceServer(grpcServer, stubPaymentServer{})
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewPaymentServiceClient(conn)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := client.GetPayment(ctx, &pb.GetPaymentRequest{PaymentId: uuid.NewString()}); err != nil {
			t.Fatalf("call %d: want no error with ratelimit disabled, got: %v", i, err)
		}
	}
}
