package grpc

import (
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"Fisher-Mapper/internal/domain/apperror"
)

// codeFor maps the transport-agnostic apperror taxonomy to a gRPC status
// code. Mirrors internal/transport/rest/errors.go's httpStatusFor — same
// taxonomy, different transport, so the two must be read side by side when
// either changes.
//
// Two codes need a documented choice because gRPC's status space doesn't
// line up 1:1 with HTTP's:
//   - CodeIdempotencyConflict -> AlreadyExists: REST's 409 here means "this
//     idempotency key already exists, bound to a different request body" --
//     AlreadyExists is the canonical gRPC code for "the resource you tried
//     to create already exists", which is exactly this shape.
//   - CodeIdempotencyInProgress -> Aborted: a concurrent request holding the
//     same key is still in flight. Aborted ("the operation was aborted, the
//     caller should retry") fits a transient race better than
//     AlreadyExists, which implies a durable conflict.
//
// CodeTerminalState/CodeInvalidTransition/CodeStaleEvent/CodeRefundLimitExceeded
// all map to FailedPrecondition (not AlreadyExists) — each is "the system is
// not in a state where this operation is valid", the textbook
// FailedPrecondition case, independent of the fact REST happens to also
// answer 409 for all of them.
func codeFor(code apperror.Code) codes.Code {
	switch code {
	case apperror.CodeValidation:
		return codes.InvalidArgument
	case apperror.CodeUnauthorized:
		return codes.Unauthenticated
	case apperror.CodeNotFound:
		return codes.NotFound
	case apperror.CodeIdempotencyConflict:
		return codes.AlreadyExists
	case apperror.CodeIdempotencyInProgress:
		return codes.Aborted
	case apperror.CodeDuplicateEvent:
		return codes.AlreadyExists
	case apperror.CodeTerminalState, apperror.CodeInvalidTransition, apperror.CodeStaleEvent:
		return codes.FailedPrecondition
	case apperror.CodeRefundLimitExceeded:
		return codes.FailedPrecondition
	case apperror.CodeProviderNotRegistered:
		return codes.InvalidArgument
	case apperror.CodeProviderTimeout:
		return codes.DeadlineExceeded
	case apperror.CodeProviderError:
		return codes.Unavailable
	case apperror.CodeProviderDisabled:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// statusFromError converts err (expected to be, or wrap, an *apperror.Error)
// into a *status.Status, applying the same redaction rule
// rest/errors.go.writeError does: an Internal-class error never leaks its
// wrapped cause (which may embed raw DB/provider error text) to the caller,
// only to the server log.
func statusFromError(err error) error {
	code := apperror.CodeOf(err)
	grpcCode := codeFor(code)

	message := err.Error()
	if grpcCode == codes.Internal {
		slog.Error("grpc request failed", "error", err, "code", code)
		message = "internal error"
	}

	return status.Error(grpcCode, message)
}
