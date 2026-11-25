// Package emit is the client of the sync gateway's cloud-origin emit API, POST /internal/v1/emit
// (ADR-0014 A2.2): the only way a service writes data to devices. The gateway alone stamps
// origin, seq, ph and hlc and seals the batch; a caller supplies only the intent, the mutations
// and one idempotency key per op.
//
// Two ways to call it:
//   - interactive: Client.Emit from a request handler. When the gateway is unreachable, answers 5xx or
//     is not ready, the error is *UnavailableError (code emit.unavailable, 503 with Retry-After) and
//     nothing changed; the caller's client retries with the same Idempotency-Key.
//   - batch: Queue (queue.go), the emit_queue outbox. Ops are enqueued in the caller's transaction,
//     and a worker sends them in order per tenant, waiting and retrying while the gateway is down.
//
// A refusal by the gateway (422, 403) is a defect in the caller: *RejectedError, which a handler
// answers as 500 emit.rejected and alerts on; it is never retried.
package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
	gsync "github.com/hostryhq/go-common/sync"
)

// ServiceTokenHeader carries the caller's short-lived service token.
const ServiceTokenHeader = "Hostry-Service-Token"

// Path is the emit endpoint.
const Path = "/internal/v1/emit"

// Op is one cloud-origin op. Key is its idempotency key: unique per (tenant, caller), deterministic
// where the source is (see DeterministicKey), derived from the caller's Idempotency-Key for an
// interactive edit (see DeriveKey). Muts must be of entities and kinds the intent lists.
type Op struct {
	Key     string                               `json:"key"`
	Stream  string                               `json:"stream"`
	Intent  string                               `json:"intent"`
	IntentV int                                  `json:"intent_v"`
	Muts    []gsync.Mut                          `json:"muts"`
	PII     map[string]map[string]map[string]any `json:"pii,omitempty"`
	Actor   string                               `json:"actor"`
	// AuthorisedBy is the permission from the intent's authorised_online_by that the caller
	// checked; nil (sent as null) for a system emit.
	AuthorisedBy *string `json:"authorised_by"`
	// Approval is the approval or vendor-grant id the caller verified, when there was one.
	Approval string `json:"approval,omitempty"`
}

// Result says where an op landed.
type Result struct {
	Key      string `json:"key"`
	Origin   string `json:"origin"`
	Stream   string `json:"stream"`
	Seq      int64  `json:"seq"`
	Replayed bool   `json:"replayed"`
}

// Emitter sends a batch for one tenant. Client implements it; tests use a fake.
type Emitter interface {
	Emit(ctx context.Context, tenant ids.ID, ops []Op) ([]Result, error)
}

// TokenSource returns the service token for the next call to the gateway for one tenant. Service
// tokens for emit are minted per tenant (provisional, O2-11): the gateway requires the token's
// tenant to equal the request tenant. Tokens are short-lived (no static
// shared secrets between services); never log the value.
type TokenSource func(ctx context.Context, tenant ids.ID) (string, error)

// Client calls the gateway. Zero HTTP uses a client with a 10 second timeout.
type Client struct {
	BaseURL string
	Token   TokenSource
	HTTP    *http.Client
}

// Errors.
var (
	// ErrInvalid is a defect found before sending (bad key, empty ops, over a limit).
	ErrInvalid = errors.New("emit: invalid request")
)

// UnavailableError is emit.unavailable: the gateway is unreachable, errored or is not ready. Nothing changed.
type UnavailableError struct {
	App *apperr.Error
	// After is the gateway's Retry-After, zero when absent.
	After time.Duration
}

func (e *UnavailableError) Error() string { return e.App.Error() }

// Unwrap lets errors.As find the *apperr.Error, so apperr maps it to the 503 problem.
func (e *UnavailableError) Unwrap() error { return e.App }

// RejectedError is a refusal by the gateway (422 or 403). It is a defect in the caller: answer 500
// emit.rejected, alert, never retry.
type RejectedError struct {
	Status int
	// Code is the gateway's problem code, for example idempotency.mismatch or emit.entity_not_allowed.
	Code string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("emit: gateway refused the emit (%d %s)", e.Status, e.Code)
}

// AppError is the safe problem to return to the caller's client (500 emit.rejected). The gateway's
// code stays in Code, for logs and alerts only.
func (e *RejectedError) AppError() *apperr.Error {
	return apperr.Wrap(apperr.Internal, "emit.rejected", "", e)
}

// IsTransient reports whether err means "try again later with the same keys": the gateway was
// unavailable, or a token could not be obtained or was refused with 401.
func IsTransient(err error) bool {
	var u *UnavailableError
	return errors.As(err, &u)
}

// RetryAfter returns the gateway's Retry-After carried by err.
func RetryAfter(err error) time.Duration {
	var u *UnavailableError
	if errors.As(err, &u) {
		return u.After
	}
	return 0
}

// NewUnavailable builds the emit.unavailable error; for test doubles and fakes of Emitter.
func NewUnavailable(cause error, after time.Duration) *UnavailableError {
	return unavailable(cause, after)
}

func unavailable(cause error, after time.Duration) *UnavailableError {
	return &UnavailableError{App: apperr.Wrap(apperr.Unavailable, "emit.unavailable", "cloud emit is unavailable, retry later", cause), After: after}
}

// DeterministicKey builds a key from the parts of a deterministic source (import job and row,
// tenant and rule-set version, day and revision), as a UUIDv5.
func DeterministicKey(parts ...string) string {
	return gsync.UUIDv5("emit|" + strings.Join(parts, "|"))
}

// DeriveKey builds the key of the n-th op of an interactive edit from the caller's Idempotency-Key,
// so a retried edit is one op (ADR-0014 A2.2 item 4).
func DeriveKey(idempotencyKey string, n int) string {
	return DeterministicKey("idem", idempotencyKey, strconv.Itoa(n))
}

// Validate checks an op against what the gateway would refuse as a defect, before sending.
func (o Op) Validate() error {
	switch {
	case o.Key == "" || len(o.Key) > 128:
		return fmt.Errorf("%w: key must be 1 to 128 characters", ErrInvalid)
	case o.Stream == "" || o.Intent == "" || o.IntentV < 1 || o.Actor == "":
		return fmt.Errorf("%w: stream, intent, intent_v and actor are required", ErrInvalid)
	case len(o.Muts) == 0:
		return fmt.Errorf("%w: an op needs at least one mutation", ErrInvalid)
	}
	if o.PII != nil {
		b, err := json.Marshal(o.PII)
		if err != nil {
			return fmt.Errorf("%w: pii: %v", ErrInvalid, err)
		}
		if len(b) > gsync.MaxPIIBytes {
			return fmt.Errorf("%w: pii is over %d bytes (op.too_large)", ErrInvalid, gsync.MaxPIIBytes)
		}
	}
	// The mutations alone are a lower bound of the body; if they exceed the limit the gateway must refuse.
	if b, err := json.Marshal(o.Muts); err != nil {
		return fmt.Errorf("%w: muts: %v", ErrInvalid, err)
	} else if len(b) > gsync.MaxBodyBytes {
		return fmt.Errorf("%w: mutations are over %d bytes (op.too_large); split the op", ErrInvalid, gsync.MaxBodyBytes)
	}
	return nil
}

type request struct {
	TenantID ids.ID `json:"tenant_id"`
	Ops      []Op   `json:"ops"`
}

// Emit sends one atomic batch of 1 to 200 ops for one tenant and returns where each landed, in
// request order. A replayed op has Replayed true and the original origin, stream and seq.
func (c *Client) Emit(ctx context.Context, tenant ids.ID, ops []Op) ([]Result, error) {
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	if err := gsync.CheckBatch(len(ops)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	seen := make(map[string]bool, len(ops))
	for i, o := range ops {
		if err := o.Validate(); err != nil {
			return nil, fmt.Errorf("op %d: %w", i, err)
		}
		if seen[o.Key] {
			return nil, fmt.Errorf("%w: op %d repeats key within the batch", ErrInvalid, i)
		}
		seen[o.Key] = true
	}
	body, err := json.Marshal(request{TenantID: tenant, Ops: ops})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	tok, err := c.Token(ctx, tenant)
	if err != nil {
		return nil, unavailable(fmt.Errorf("service token: %w", err), 0)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+Path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(ServiceTokenHeader, tok)
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, unavailable(errors.New("gateway unreachable"), 0) // the url error could carry a token-free host; keep text out anyway
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, unavailable(errors.New("gateway response unreadable"), 0)
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return decodeResults(raw, ops)
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusUnauthorized:
		return nil, unavailable(fmt.Errorf("gateway answered %d", resp.StatusCode), retryAfter(resp))
	default:
		var p struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &p)
		return nil, &RejectedError{Status: resp.StatusCode, Code: p.Code}
	}
}

func decodeResults(raw []byte, ops []Op) ([]Result, error) {
	var r struct {
		Ops []Result `json:"ops"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || len(r.Ops) != len(ops) {
		return nil, unavailable(errors.New("gateway answered 200 with a malformed result"), 0)
	}
	for i := range ops {
		if r.Ops[i].Key != ops[i].Key || r.Ops[i].Origin == "" || r.Ops[i].Seq < 1 {
			return nil, unavailable(fmt.Errorf("gateway result %d does not match the request", i), 0)
		}
	}
	return r.Ops, nil
}

func retryAfter(resp *http.Response) time.Duration {
	if s := resp.Header.Get("Retry-After"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}
