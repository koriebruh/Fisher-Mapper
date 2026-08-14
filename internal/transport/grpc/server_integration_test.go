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
	"google.golang.org/grpc/status"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
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
// repo; migrations 00001+ must already be applied.
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
// real grpc.Server on an ephemeral loopback port, and returns a connected
// client plus the Service itself (the test needs the latter to invoke
// ProcessCharge directly, standing in for the worker).
func startTestServer(t *testing.T, pool *pgxpool.Pool) (pb.PaymentServiceClient, *payment.Service) {
	t.Helper()

	mockProv := mock.New(mock.Config{Name: "mock"})
	repo := payment.NewPGRepository(pool)
	idemStore := idempotency.NewPGStore(pool)
	svc := payment.NewService(repo, idemStore, singleProviderRegistry{mockProv})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
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

	return pb.NewPaymentServiceClient(conn), svc
}

func TestGRPC_CreatePayment_AsyncFlow_MatchesRESTBehavior(t *testing.T) {
	pool := testPool(t)
	client, svc := startTestServer(t, pool)
	ctx := context.Background()

	tenantID := uuid.NewString()
	key := uuid.NewString()

	createResp, err := client.CreatePayment(ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		TenantId:       tenantID,
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
}

func TestGRPC_CreatePayment_IdempotencyReplayAndConflict(t *testing.T) {
	pool := testPool(t)
	client, _ := startTestServer(t, pool)
	ctx := context.Background()

	tenantID := uuid.NewString()
	key := uuid.NewString()

	req := &pb.CreatePaymentRequest{
		IdempotencyKey: key,
		TenantId:       tenantID,
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
		TenantId:       tenantID,
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
	client, _ := startTestServer(t, pool)
	ctx := context.Background()

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

func TestGRPC_CreateRefund_AsyncFlow(t *testing.T) {
	pool := testPool(t)
	client, svc := startTestServer(t, pool)
	ctx := context.Background()

	tenantID := uuid.NewString()
	chargeKey := uuid.NewString()

	createResp, err := client.CreatePayment(ctx, &pb.CreatePaymentRequest{
		IdempotencyKey: chargeKey,
		TenantId:       tenantID,
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

	if err := svc.ProcessCharge(ctx, payment.ChargeTaskInput{
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

	if err := svc.ProcessRefund(ctx, payment.RefundTaskInput{
		RefundID:           refundID,
		PaymentID:          paymentID,
		IdempotencyKey:     refundKey,
		TenantID:           tenantID,
		Currency:           "USD",
		Amount:             1000,
		Provider:           "mock",
		PaymentProviderRef: mustGetProviderRef(ctx, t, svc, paymentID),
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

func mustGetProviderRef(ctx context.Context, t *testing.T, svc *payment.Service, paymentID uuid.UUID) string {
	t.Helper()
	p, err := svc.GetPayment(ctx, paymentID)
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
// Postgres dependency.
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
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(RateLimitInterceptor(limiter)))
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
