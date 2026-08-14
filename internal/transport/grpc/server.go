// Package grpc is the gRPC transport (Fase 6): a thin PaymentServiceServer
// implementation calling into the SAME internal/domain/payment.Service the
// REST transport (internal/transport/rest) uses. Per plan "satu service
// layer, dua transport tipis" — no business logic lives here, only
// request/response mapping and error-code translation (see errors.go).
package grpc

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/domain/payment"
	pb "Fisher-Mapper/internal/transport/grpc/pb/payment/v1"
)

// Server implements pb.PaymentServiceServer over payment.Service.
type Server struct {
	pb.UnimplementedPaymentServiceServer

	service *payment.Service
}

func NewServer(service *payment.Service) *Server {
	return &Server{service: service}
}

// fingerprintBasis builds the deterministic byte sequence
// Service.CreatePayment/CreateRefund hash for the idempotency fingerprint.
//
// It must NOT be a raw protobuf marshal of the request: protobuf explicitly
// documents that deterministic marshaling is not canonical across library
// versions or builds, and this fingerprint is persisted in Postgres and
// compared against on a later replay -- if the bytes shifted after a stub
// regen, a legitimate client retry would come back as an idempotency
// CONFLICT instead of a replay, on the money path. json.Marshal of a plain
// Go struct IS documented-stable (map keys sorted, field order = struct
// declaration order), so the basis is built from a value shaped like the
// domain input, not from the wire message.
//
// idempotency_key is excluded, matching REST: there, the key travels in an
// HTTP header and is never part of the hashed body.
func fingerprintBasis(v any) ([]byte, error) {
	return json.Marshal(v)
}

// CreatePayment mirrors rest.handleCreatePayment: decode -> call
// payment.Service.CreatePayment -> map result/error. Never blocks for the
// provider call -- see the proto doc on CreatePayment.
func (s *Server) CreatePayment(ctx context.Context, req *pb.CreatePaymentRequest) (*pb.CreatePaymentResponse, error) {
	in := payment.CreatePaymentInput{
		TenantID:      req.GetTenantId(),
		Livemode:      req.GetLivemode(),
		Currency:      req.GetCurrency(),
		Amount:        req.GetAmount(),
		Provider:      req.GetProvider(),
		PaymentMethod: req.GetPaymentMethod(),
		Metadata:      req.GetMetadata(),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	out, err := s.service.CreatePayment(ctx, in, req.GetIdempotencyKey(), raw)
	if err != nil {
		return nil, statusFromError(err)
	}

	return &pb.CreatePaymentResponse{
		PaymentId:   out.PaymentID.String(),
		Status:      string(out.Status),
		ProviderRef: out.ProviderRef,
		Replayed:    out.Replayed,
	}, nil
}

// GetPayment mirrors rest.handleGetPayment, but returns the full payment
// row (REST's paymentView is deliberately narrower -- id/status/provider_ref
// only -- for this transport there is no reason to hide the rest of the
// row from an authenticated RPC caller).
func (s *Server) GetPayment(ctx context.Context, req *pb.GetPaymentRequest) (*pb.GetPaymentResponse, error) {
	id, err := uuid.Parse(req.GetPaymentId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payment id"))
	}

	p, err := s.service.GetPayment(ctx, id)
	if err != nil {
		return nil, statusFromError(err)
	}

	resp := &pb.GetPaymentResponse{
		PaymentId:     p.ID.String(),
		TenantId:      p.TenantID,
		Livemode:      p.Livemode,
		Currency:      p.Currency,
		Amount:        p.Amount,
		OperationType: string(p.OperationType),
		Provider:      p.Provider,
		Status:        string(p.Status),
	}
	if p.ProviderRef != nil {
		resp.ProviderRef = *p.ProviderRef
	}
	return resp, nil
}

// CreateRefund mirrors rest.handleCreateRefund: fetch the parent payment
// first (so TenantID/Livemode are derived server-side, never client-
// supplied -- see the proto doc on CreateRefundRequest), then call
// payment.Service.CreateRefund.
func (s *Server) CreateRefund(ctx context.Context, req *pb.CreateRefundRequest) (*pb.CreateRefundResponse, error) {
	paymentID, err := uuid.Parse(req.GetPaymentId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payment id"))
	}

	p, err := s.service.GetPayment(ctx, paymentID)
	if err != nil {
		return nil, statusFromError(err)
	}

	in := payment.CreateRefundInput{
		PaymentID: paymentID,
		TenantID:  p.TenantID,
		Livemode:  p.Livemode,
		Currency:  req.GetCurrency(),
		Amount:    req.GetAmount(),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	out, err := s.service.CreateRefund(ctx, in, req.GetIdempotencyKey(), raw)
	if err != nil {
		return nil, statusFromError(err)
	}

	return &pb.CreateRefundResponse{
		RefundId:  out.RefundID.String(),
		PaymentId: out.PaymentID.String(),
		Status:    string(out.Status),
		Replayed:  out.Replayed,
	}, nil
}

// GetRefund mirrors rest.handleGetRefund (again returning the full row, see
// GetPayment's doc for why).
func (s *Server) GetRefund(ctx context.Context, req *pb.GetRefundRequest) (*pb.GetRefundResponse, error) {
	id, err := uuid.Parse(req.GetRefundId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid refund id"))
	}

	r, err := s.service.GetRefund(ctx, id)
	if err != nil {
		return nil, statusFromError(err)
	}

	resp := &pb.GetRefundResponse{
		RefundId:  r.ID.String(),
		PaymentId: r.PaymentID.String(),
		TenantId:  r.TenantID,
		Livemode:  r.Livemode,
		Currency:  r.Currency,
		Amount:    r.Amount,
		Provider:  r.Provider,
		Status:    string(r.Status),
	}
	if r.ProviderRef != nil {
		resp.ProviderRef = *r.ProviderRef
	}
	if r.ProviderRefundRef != nil {
		resp.ProviderRefundRef = *r.ProviderRefundRef
	}
	return resp, nil
}

// CreatePayout mirrors rest.handleCreatePayout: a standalone money-OUT
// operation, so (unlike CreateRefund) there is no parent row to fetch first
// -- provider/destination come straight from the request.
func (s *Server) CreatePayout(ctx context.Context, req *pb.CreatePayoutRequest) (*pb.CreatePayoutResponse, error) {
	in := payment.CreatePayoutInput{
		TenantID:    req.GetTenantId(),
		Livemode:    req.GetLivemode(),
		Currency:    req.GetCurrency(),
		Amount:      req.GetAmount(),
		Provider:    req.GetProvider(),
		Destination: req.GetDestination(),
		Metadata:    req.GetMetadata(),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	out, err := s.service.CreatePayout(ctx, in, req.GetIdempotencyKey(), raw)
	if err != nil {
		return nil, statusFromError(err)
	}

	return &pb.CreatePayoutResponse{
		PayoutId: out.PayoutID.String(),
		Status:   string(out.Status),
		Replayed: out.Replayed,
	}, nil
}

// GetPayout mirrors rest.handleGetPayout, returning the full row (same
// reasoning as GetPayment/GetRefund's doc).
func (s *Server) GetPayout(ctx context.Context, req *pb.GetPayoutRequest) (*pb.GetPayoutResponse, error) {
	id, err := uuid.Parse(req.GetPayoutId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payout id"))
	}

	p, err := s.service.GetPayout(ctx, id)
	if err != nil {
		return nil, statusFromError(err)
	}

	resp := &pb.GetPayoutResponse{
		PayoutId:      p.ID.String(),
		TenantId:      p.TenantID,
		Livemode:      p.Livemode,
		Currency:      p.Currency,
		Amount:        p.Amount,
		OperationType: string(payment.OperationPayout),
		Provider:      p.Provider,
		Destination:   p.Destination,
		Status:        string(p.Status),
	}
	if p.ProviderRef != nil {
		resp.ProviderRef = *p.ProviderRef
	}
	return resp, nil
}
