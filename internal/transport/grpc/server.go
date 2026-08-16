// Package grpc is the gRPC transport (Fase 6): a thin PaymentServiceServer
// implementation calling into the SAME internal/domain/payment.Service the
// REST transport (internal/transport/rest) uses. Per plan "satu service
// layer, dua transport tipis" — no business logic lives here, only
// request/response mapping and error-code translation (see errors.go).
package grpc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/provider/payload"
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
//
// CreatePaymentInput/CreateRefundInput/CreatePayoutInput's embedded Envelope
// is deliberately INCLUDED here (via a plain json.Marshal of the whole
// input struct) rather than stripped out before hashing: SourceApp/
// Description are caller-supplied business fields, and REST's fingerprint
// (a hash of the raw request body) already includes them when a client
// sends them, by construction. Excluding them from gRPC's fingerprint would
// make the two transports inconsistent -- a same-key-different-description
// retry would 409 on REST but silently replay the old response on gRPC.
// Channel/InitiatedBy are hashed too, but are compile-time constants per
// transport (buildEnvelope) and so never vary between retries of the same
// logical call; TraceID/RequestIP/RequestUserAgent are always nil at this
// point (see CreatePayment/CreateRefund/CreatePayout's call order --
// populateRequestContext runs strictly AFTER fingerprintBasis) for the same
// reason. Net effect: adding Envelope to these input structs DID change the
// hashed byte sequence's shape one time, as of this change -- any
// idempotency_keys row a gRPC caller created before this deploy will not
// match a retry with the same key after it (falls to CodeIdempotencyConflict
// rather than a replay). Accepted as a one-time migration cost on a
// template with no live production traffic; a real deployment carrying live
// in-flight idempotency keys across this exact change would need a
// dual-read migration window instead.
//
// Adding CallbackURL to CreatePaymentInput/CreatePayoutInput is the SAME
// class of change, applied deliberately a second time for the identical
// reason (a real caller-supplied field, not derived) -- accepted with the
// same one-time-migration-cost reasoning as above, not an oversight.
func fingerprintBasis(v any) ([]byte, error) {
	return json.Marshal(v)
}

// stringPtrOrNil treats proto3's empty-string zero value as "not set" --
// the same convention CreatePaymentRequest/CreatePayoutRequest's
// source_app/description fields already use (see their proto doc).
func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// timePtrToRFC3339 converts payload's *time.Time fields to the proto string
// wire representation (see MethodPayload's proto doc on why a plain RFC3339
// string, not google.protobuf.Timestamp). No inverse is needed: MethodPayload
// only ever flows server -> client (GetPayment), never the other way.
func timePtrToRFC3339(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

// methodPayloadToPB mirrors REST's paymentView.MethodPayload exposure --
// only the field matching mp.Method carries data, the rest are left as
// proto3's absent-message zero value (nil), the wire-level equivalent of
// REST's JSON null.
func methodPayloadToPB(mp *payload.MethodPayload) *pb.MethodPayload {
	if mp == nil {
		return nil
	}
	out := &pb.MethodPayload{Method: mp.Method}
	if mp.QRIS != nil {
		out.Qris = &pb.QRIS{
			QrString:  derefOrEmpty(mp.QRIS.QRString),
			ExpiresAt: timePtrToRFC3339(mp.QRIS.ExpiresAt),
		}
	}
	if mp.VirtualAccount != nil {
		out.VirtualAccount = &pb.VirtualAccount{
			BankCode:  mp.VirtualAccount.BankCode,
			VaNumber:  mp.VirtualAccount.VANumber,
			ExpiresAt: timePtrToRFC3339(mp.VirtualAccount.ExpiresAt),
		}
	}
	if mp.Card != nil {
		out.Card = &pb.Card{
			MaskedPan:   derefOrEmpty(mp.Card.MaskedPAN),
			RedirectUrl: derefOrEmpty(mp.Card.RedirectURL),
		}
	}
	if mp.EWallet != nil {
		out.Ewallet = &pb.EWallet{
			RedirectUrl: derefOrEmpty(mp.EWallet.RedirectURL),
			ExpiresAt:   timePtrToRFC3339(mp.EWallet.ExpiresAt),
		}
	}
	return out
}

// CreatePayment mirrors rest.handleCreatePayment: decode -> call
// payment.Service.CreatePayment -> map result/error. Never blocks for the
// provider call -- see the proto doc on CreatePayment.
func (s *Server) CreatePayment(ctx context.Context, req *pb.CreatePaymentRequest) (*pb.CreatePaymentResponse, error) {
	in := payment.CreatePaymentInput{
		// TenantID comes from the AUTHENTICATED caller (TenantAuthInterceptor),
		// never the request -- CreatePaymentRequest has no tenant_id field
		// (see its proto doc) precisely so this can never be anything else.
		TenantID:      tenantIDFromContext(ctx),
		Livemode:      req.GetLivemode(),
		Currency:      req.GetCurrency(),
		Amount:        req.GetAmount(),
		Provider:      req.GetProvider(),
		PaymentMethod: req.GetPaymentMethod(),
		Metadata:      req.GetMetadata(),
		CallbackURL:   stringPtrOrNil(req.GetCallbackUrl()),
		Envelope:      buildEnvelope(req.GetSourceApp(), req.GetDescription()),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	// Populated AFTER the fingerprint above -- see buildEnvelope's doc.
	populateRequestContext(ctx, &in.Envelope)

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
// only -- for this transport there is no reason to hide the rest of the row
// from an authenticated RPC caller). "Authenticated" is no longer aspirational
// here: TenantAuthInterceptor (cmd/server wiring) resolves the caller's
// tenant_id and GetPayment is scoped to it below, so a request UUID that
// belongs to a different tenant reports CodeNotFound, never that tenant's row.
func (s *Server) GetPayment(ctx context.Context, req *pb.GetPaymentRequest) (*pb.GetPaymentResponse, error) {
	id, err := uuid.Parse(req.GetPaymentId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payment id"))
	}

	p, err := s.service.GetPayment(ctx, id, tenantIDFromContext(ctx))
	if err != nil {
		return nil, statusFromError(err)
	}

	env := newEnvelopeView(p.Envelope)
	resp := &pb.GetPaymentResponse{
		PaymentId:        p.ID.String(),
		TenantId:         p.TenantID,
		Livemode:         p.Livemode,
		Currency:         p.Currency,
		Amount:           p.Amount,
		OperationType:    string(p.OperationType),
		Provider:         p.Provider,
		Status:           string(p.Status),
		SourceApp:        env.SourceApp,
		Channel:          env.Channel,
		TraceId:          env.TraceID,
		Description:      env.Description,
		InitiatedBy:      env.InitiatedBy,
		RequestIp:        env.RequestIP,
		RequestUserAgent: env.RequestUserAgent,
	}
	if p.ProviderRef != nil {
		resp.ProviderRef = *p.ProviderRef
	}
	resp.PaymentMethod = p.PaymentMethod
	resp.MethodPayload = methodPayloadToPB(p.MethodPayload)
	resp.CallbackUrl = derefOrEmpty(p.CallbackURL)
	return resp, nil
}

// CreateRefund mirrors rest.handleCreateRefund: fetch the parent payment
// first (so TenantID/Livemode are derived server-side, never client-
// supplied -- see the proto doc on CreateRefundRequest), then call
// payment.Service.CreateRefund. The fetch is tenant-scoped to the
// AUTHENTICATED caller, so a request naming another tenant's payment id
// reports CodeNotFound rather than creating a refund against it.
func (s *Server) CreateRefund(ctx context.Context, req *pb.CreateRefundRequest) (*pb.CreateRefundResponse, error) {
	paymentID, err := uuid.Parse(req.GetPaymentId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payment id"))
	}

	p, err := s.service.GetPayment(ctx, paymentID, tenantIDFromContext(ctx))
	if err != nil {
		return nil, statusFromError(err)
	}

	in := payment.CreateRefundInput{
		PaymentID: paymentID,
		TenantID:  p.TenantID,
		Livemode:  p.Livemode,
		Currency:  req.GetCurrency(),
		Amount:    req.GetAmount(),
		Envelope:  buildEnvelope(req.GetSourceApp(), req.GetDescription()),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	// Populated AFTER the fingerprint above -- see buildEnvelope's doc.
	populateRequestContext(ctx, &in.Envelope)

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
// GetPayment's doc for why, including the tenant-scoping).
func (s *Server) GetRefund(ctx context.Context, req *pb.GetRefundRequest) (*pb.GetRefundResponse, error) {
	id, err := uuid.Parse(req.GetRefundId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid refund id"))
	}

	r, err := s.service.GetRefund(ctx, id, tenantIDFromContext(ctx))
	if err != nil {
		return nil, statusFromError(err)
	}

	env := newEnvelopeView(r.Envelope)
	resp := &pb.GetRefundResponse{
		RefundId:         r.ID.String(),
		PaymentId:        r.PaymentID.String(),
		TenantId:         r.TenantID,
		Livemode:         r.Livemode,
		Currency:         r.Currency,
		Amount:           r.Amount,
		Provider:         r.Provider,
		Status:           string(r.Status),
		SourceApp:        env.SourceApp,
		Channel:          env.Channel,
		TraceId:          env.TraceID,
		Description:      env.Description,
		InitiatedBy:      env.InitiatedBy,
		RequestIp:        env.RequestIP,
		RequestUserAgent: env.RequestUserAgent,
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
		// TenantID comes from the AUTHENTICATED caller -- see CreatePayment's
		// identical comment.
		TenantID:    tenantIDFromContext(ctx),
		Livemode:    req.GetLivemode(),
		Currency:    req.GetCurrency(),
		Amount:      req.GetAmount(),
		Provider:    req.GetProvider(),
		Destination: req.GetDestination(),
		Metadata:    req.GetMetadata(),
		CallbackURL: stringPtrOrNil(req.GetCallbackUrl()),
		Envelope:    buildEnvelope(req.GetSourceApp(), req.GetDescription()),
	}

	raw, err := fingerprintBasis(in)
	if err != nil {
		return nil, statusFromError(apperror.Wrap(apperror.CodeInternal, "grpc: marshal fingerprint basis", err))
	}

	// Populated AFTER the fingerprint above -- see buildEnvelope's doc.
	populateRequestContext(ctx, &in.Envelope)

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
// reasoning as GetPayment/GetRefund's doc, including the tenant-scoping).
func (s *Server) GetPayout(ctx context.Context, req *pb.GetPayoutRequest) (*pb.GetPayoutResponse, error) {
	id, err := uuid.Parse(req.GetPayoutId())
	if err != nil {
		return nil, statusFromError(apperror.New(apperror.CodeValidation, "invalid payout id"))
	}

	p, err := s.service.GetPayout(ctx, id, tenantIDFromContext(ctx))
	if err != nil {
		return nil, statusFromError(err)
	}

	env := newEnvelopeView(p.Envelope)
	resp := &pb.GetPayoutResponse{
		PayoutId:         p.ID.String(),
		TenantId:         p.TenantID,
		Livemode:         p.Livemode,
		Currency:         p.Currency,
		Amount:           p.Amount,
		OperationType:    string(payment.OperationPayout),
		Provider:         p.Provider,
		Destination:      p.Destination,
		Status:           string(p.Status),
		SourceApp:        env.SourceApp,
		Channel:          env.Channel,
		TraceId:          env.TraceID,
		Description:      env.Description,
		InitiatedBy:      env.InitiatedBy,
		RequestIp:        env.RequestIP,
		RequestUserAgent: env.RequestUserAgent,
	}
	if p.ProviderRef != nil {
		resp.ProviderRef = *p.ProviderRef
	}
	resp.CallbackUrl = derefOrEmpty(p.CallbackURL)
	return resp, nil
}
