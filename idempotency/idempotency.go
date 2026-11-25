// Package idempotency supports the Idempotency-Key contract for mutating
// endpoints: the first request with a key runs; a replay with the same key and
// same request returns the stored response; the same key with a different
// request is rejected. Offline clients retry, so this must be exact.
//
// Keys are scoped per tenant. For Postgres use DoTx (pg.go): the claim, the business write and
// the stored result commit in one transaction, so a crash cannot leave a key stuck, and records
// expire (24 h default). Never store secrets (tokens, refresh tokens, codes, packs) in a result:
// use TxResult.OmitBody, which keeps only a SHA-256 fingerprint of the body. Store and Do remain
// for work that cannot share the transaction; their records also expire and stale claims are
// taken over.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
)

// Header is the request header carrying the key.
const Header = "Idempotency-Key"

const maxKeyLen = 128

// ErrInProgress means another request with the same key has not finished.
var ErrInProgress = apperr.New(apperr.Conflict, "idempotency.in_progress", "a request with this Idempotency-Key is still being processed")

// ErrMismatch means the key was used with a different request.
var ErrMismatch = apperr.New(apperr.IdempotencyMismatch, "idempotency.mismatch", "Idempotency-Key was already used with a different request")

// ErrKeyInvalid means the header value is unusable.
var ErrKeyInvalid = apperr.New(apperr.Validation, "idempotency.key_invalid", "Idempotency-Key must be 1-128 visible ASCII characters")

// KeyFromRequest returns the key; ok is false when the header is absent.
func KeyFromRequest(r *http.Request) (key string, ok bool, err error) {
	k := r.Header.Get(Header)
	if k == "" {
		return "", false, nil
	}
	if len(k) > maxKeyLen {
		return "", false, ErrKeyInvalid
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return "", false, ErrKeyInvalid
		}
	}
	return k, true, nil
}

// Fingerprint identifies a request's meaning: method, path and body hash.
func Fingerprint(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// Response is the stored result replayed on retry.
type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

// Store persists key state. Begin must be atomic: exactly one caller gets
// started=true for a key.
type Store interface {
	// Begin claims the key. If it exists: returns its stored response (done),
	// or ErrInProgress if unfinished, or ErrMismatch if the fingerprint differs.
	Begin(ctx context.Context, tenant ids.ID, key, fingerprint string) (resp *Response, started bool, err error)
	// Complete stores the response for a started key.
	Complete(ctx context.Context, tenant ids.ID, key string, resp Response) error
	// Release drops a started key after failure so the client may retry.
	Release(ctx context.Context, tenant ids.ID, key string) error
}

// Do runs fn at most once per (tenant, key). On replay it returns the stored
// response with replayed=true. A fn error releases the key so a retry re-runs.
func Do(ctx context.Context, s Store, tenant ids.ID, key, fingerprint string, fn func() (Response, error)) (resp Response, replayed bool, err error) {
	prev, started, err := s.Begin(ctx, tenant, key, fingerprint)
	if err != nil {
		return Response{}, false, err
	}
	if !started {
		return *prev, true, nil
	}
	resp, err = fn()
	if err != nil {
		_ = s.Release(ctx, tenant, key)
		return Response{}, false, err
	}
	if err := s.Complete(ctx, tenant, key, resp); err != nil {
		return Response{}, false, err
	}
	return resp, false, nil
}

type entry struct {
	fp      string
	resp    *Response
	created time.Time
}

// Memory is an in-memory Store for tests and doubles. Records expire after TTL (DefaultTTL) and
// unfinished claims older than ClaimTTL (DefaultClaimTTL) are taken over, as in PGStore.
type Memory struct {
	mu       sync.Mutex
	m        map[string]*entry
	TTL      time.Duration
	ClaimTTL time.Duration
	Now      func() time.Time
}

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{m: map[string]*entry{}} }

func (s *Memory) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Memory) stale(e *entry) bool {
	ttl, cttl := s.TTL, s.ClaimTTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if cttl <= 0 {
		cttl = DefaultClaimTTL
	}
	age := s.now().Sub(e.created)
	return age >= ttl || (e.resp == nil && age >= cttl)
}

func k(t ids.ID, key string) string { return t.String() + "\x00" + key }

// Begin implements Store.
func (s *Memory) Begin(_ context.Context, t ids.ID, key, fp string) (*Response, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[k(t, key)]
	if ok && s.stale(e) {
		ok = false
	}
	if !ok {
		s.m[k(t, key)] = &entry{fp: fp, created: s.now()}
		return nil, true, nil
	}
	if e.fp != fp {
		return nil, false, ErrMismatch
	}
	if e.resp == nil {
		return nil, false, ErrInProgress
	}
	return e.resp, false, nil
}

// Complete implements Store.
func (s *Memory) Complete(_ context.Context, t ids.ID, key string, r Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[k(t, key)]
	if !ok {
		return errors.New("idempotency: complete on unknown key")
	}
	e.resp = &r
	return nil
}

// Release implements Store.
func (s *Memory) Release(_ context.Context, t ids.ID, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k(t, key))
	return nil
}
