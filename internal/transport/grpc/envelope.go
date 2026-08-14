package grpc

import (
	"context"
	"net"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"Fisher-Mapper/internal/domain/payment"
)

// buildEnvelope mirrors rest.buildEnvelope for the gRPC transport:
// sourceApp/description are the two caller-supplied fields (already decoded
// off the request message by the caller), everything else is derived from
// ctx by populateRequestContext below -- kept as a SEPARATE call, invoked
// only after the idempotency fingerprint has been computed from the input
// struct, so RequestIP/RequestUserAgent/TraceID (which vary per attempt/
// peer) never affect replay matching (see CreatePaymentInput.Envelope's
// doc).
func buildEnvelope(sourceApp, description string) payment.Envelope {
	env := payment.Envelope{
		// Every gRPC creation call today is a direct client hitting this
		// API on behalf of itself, never this template acting on its own --
		// "customer" per Envelope.InitiatedBy's taxonomy, same as REST.
		Channel:     payment.ChannelGRPC,
		InitiatedBy: payment.InitiatedByCustomer,
	}
	if sourceApp != "" {
		env.SourceApp = &sourceApp
	}
	if description != "" {
		env.Description = &description
	}
	return env
}

// populateRequestContext fills in env's context-derived fields best-effort
// -- a missing peer/metadata/span (e.g. an in-process test client) leaves
// the corresponding field nil rather than erroring the whole request.
func populateRequestContext(ctx context.Context, env *payment.Envelope) {
	// Host only, via net.SplitHostPort -- mirrors rateLimitKey's reasoning
	// exactly (peer.Addr.String() includes an ephemeral port that varies per
	// TCP connection, not per logical caller).
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr := p.Addr.String()
		if host, _, err := net.SplitHostPort(addr); err == nil {
			addr = host
		}
		if addr != "" {
			env.RequestIP = &addr
		}
	}

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("user-agent"); len(vals) > 0 && vals[0] != "" {
			ua := vals[0]
			env.RequestUserAgent = &ua
		}
	}

	// otelgrpc's server interceptor (wired at cmd/server bootstrap) starts a
	// span per RPC on this same ctx -- an all-zeros trace id (no valid span,
	// e.g. otel_enabled=false) is worse than no trace id on a financial
	// record, so this stays nil rather than recording a meaningless value.
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		id := sc.TraceID().String()
		env.TraceID = &id
	}
}

// envelopeView is the plain-string shape of payment.Envelope shared by
// GetPayment/GetRefund/GetPayout's response mapping -- pointer fields
// dereferenced to "" when nil, mirroring REST's paymentView/refundView/
// payoutView exactly.
type envelopeView struct {
	SourceApp        string
	Channel          string
	TraceID          string
	Description      string
	InitiatedBy      string
	RequestIP        string
	RequestUserAgent string
}

func newEnvelopeView(env payment.Envelope) envelopeView {
	v := envelopeView{Channel: env.Channel, InitiatedBy: env.InitiatedBy}
	if env.SourceApp != nil {
		v.SourceApp = *env.SourceApp
	}
	if env.TraceID != nil {
		v.TraceID = *env.TraceID
	}
	if env.Description != nil {
		v.Description = *env.Description
	}
	if env.RequestIP != nil {
		v.RequestIP = *env.RequestIP
	}
	if env.RequestUserAgent != nil {
		v.RequestUserAgent = *env.RequestUserAgent
	}
	return v
}
