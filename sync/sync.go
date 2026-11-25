// Package sync is the Go side of the sync protocol v1 (ADR-0002, ADR-0014 A2): the one parser
// for sync op events (ParseOp), the entity registry loaded from the api-specs entity YAML, the
// conformance primitives (canonical JSON, hashes, signed messages, HLC) and the projection engine
// that applies the owned and replicated entities of one service per field policy.
//
// The package name shadows the standard library's sync inside this package only; importers use
// an alias when they need both.
package sync

import (
	"errors"

	"github.com/hostryhq/go-common/inbox"
)

// Limits of protocol v1 (sync/protocol-v1.md, vectors size_limit and emit_batch).
const (
	MaxBodyBytes = 262144 // op body, UTF-8 bytes of the JSON string that h covers
	MaxPIIBytes  = 16384  // pii object, compact JSON bytes
	MaxEmitOps   = 200    // ops per emit request
	MinEmitOps   = 1
	// MaxSafeInt is the largest integer an op body may carry (2^53-1).
	MaxSafeInt = 1<<53 - 1
)

// Errors returned by the parser and the registry. Match with errors.Is.
var (
	ErrTooLarge     error = permanentErr("sync: op body or pii over the size limit")
	ErrHashMismatch error = permanentErr("sync: h is not the SHA-256 of body")
	ErrMalformed    error = permanentErr("sync: malformed op event")
	// ErrNotOp means the event is a gateway event (sync.op.flagged, sync.quarantine.recorded,
	// sync.pii.erased) or not a sync event at all, not an op. It is not an error of the message:
	// route it with OpNamespace first.
	ErrNotOp        = errors.New("sync: not an op event")
	ErrNotCanonical = errors.New("sync: not canonical JSON (RFC 8785, integers only)")
	ErrBatchSize    = errors.New("sync: emit batch must hold 1 to 200 ops")
)

// CheckSize reports whether a body of bodyBytes and a pii object of piiBytes are within the limits.
func CheckSize(bodyBytes, piiBytes int) error {
	if bodyBytes > MaxBodyBytes || piiBytes > MaxPIIBytes {
		return ErrTooLarge
	}
	return nil
}

// CheckBatch reports whether n is a valid emit batch size.
func CheckBatch(n int) error {
	if n < MinEmitOps || n > MaxEmitOps {
		return ErrBatchSize
	}
	return nil
}

// permanentErr is a refusal that retrying cannot change: inbox parks the event on its first delivery.
type permanentErr string

func (e permanentErr) Error() string { return string(e) }
func (e permanentErr) Unwrap() error { return inbox.ErrPermanent }
