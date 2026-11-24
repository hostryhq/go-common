// Package testkit holds helpers for tests across services: a fake clock,
// deterministic ids, ready-made tenant contexts (two tenants for isolation
// tests) and HTTP helpers. Import it from _test.go files and doubles only.
package testkit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/tenancy"
)

// Epoch is the default start of a FakeClock.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// FakeClock is a manually advanced clock. It implements clock.Clock.
type FakeClock struct {
	mu sync.Mutex
	t  time.Time
}

var _ clock.Clock = (*FakeClock)(nil)

// NewClock starts at Epoch.
func NewClock() *FakeClock { return &FakeClock{t: Epoch} }

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves time forward.
func (c *FakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Set jumps to t (UTC).
func (c *FakeClock) Set(t time.Time) { c.mu.Lock(); c.t = t.UTC(); c.mu.Unlock() }

// IDs returns a UUIDv7 generator driven by the clock with a deterministic
// random source, so ids are reproducible and sort by fake time.
func (c *FakeClock) IDs() *ids.Generator { return ids.NewGenerator(c.Now, &counterReader{}) }

type counterReader struct {
	mu sync.Mutex
	n  byte
}

func (r *counterReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

// Fixed, valid UUIDv7 ids for two tenants and their outlets.
const (
	TenantA ids.ID = "018f0000-0000-7000-8000-00000000000a"
	TenantB ids.ID = "018f0000-0000-7000-8000-00000000000b"
	OutletA ids.ID = "018f0000-0000-7000-8000-0000000000a1"
	OutletB ids.ID = "018f0000-0000-7000-8000-0000000000b1"
)

// ContextA and ContextB are tenant contexts for isolation tests: tenant A must
// never read or write tenant B's data.
func ContextA() tenancy.Context {
	return tenancy.Context{TenantID: TenantA, OutletID: OutletA, ActorID: "member-a", DeviceID: "device-a"}
}

// ContextB is the second tenant; see ContextA.
func ContextB() tenancy.Context {
	return tenancy.Context{TenantID: TenantB, OutletID: OutletB, ActorID: "member-b", DeviceID: "device-b"}
}

// Ctx returns a background context carrying tc.
func Ctx(tc tenancy.Context) context.Context { return tenancy.With(context.Background(), tc) }

// FixedExtractor is a tenancy.Extractor for tests that always yields tc.
func FixedExtractor(tc tenancy.Context) tenancy.Extractor {
	return func(*http.Request) (tenancy.Context, error) { return tc, nil }
}

// HeaderExtractor reads "X-Test-Tenant" (and optional "X-Test-Outlet"),
// letting one test server act as either tenant. For tests only; real services
// derive tenancy from a verified token.
func HeaderExtractor(r *http.Request) (tenancy.Context, error) {
	return tenancy.Context{
		TenantID: ids.ID(r.Header.Get("X-Test-Tenant")),
		OutletID: ids.ID(r.Header.Get("X-Test-Outlet")),
		ActorID:  "test",
	}, nil
}

// Do sends a JSON request to h (body may be nil) and returns the recorder.
func Do(t testing.TB, h http.Handler, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *bytes.Reader
	if body == nil {
		rd = bytes.NewReader(nil)
	} else {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("testkit.Do: marshal: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// AsTenant returns headers that make HeaderExtractor act as tc.
func AsTenant(tc tenancy.Context) map[string]string {
	return map[string]string{"X-Test-Tenant": tc.TenantID.String(), "X-Test-Outlet": tc.OutletID.String()}
}

// DecodeJSON decodes a recorder body into T, failing the test on error.
func DecodeJSON[T any](t testing.TB, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("testkit.DecodeJSON: %v; body=%s", err, rec.Body)
	}
	return v
}

// RequireProblem asserts rec is a problem+json with the given status and code.
func RequireProblem(t testing.TB, rec *httptest.ResponseRecorder, status int, code string) apperr.Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json; body=%s", ct, rec.Body)
	}
	p := DecodeJSON[apperr.Problem](t, rec)
	if rec.Code != status || p.Code != code {
		t.Fatalf("got %d %q, want %d %q", rec.Code, p.Code, status, code)
	}
	return p
}

// RequireNotFoundAcrossTenants asserts the tenant-isolation rule: tenant B
// asking for tenant A's resource gets 404 (never 403).
func RequireNotFoundAcrossTenants(t testing.TB, h http.Handler, path string, other tenancy.Context) {
	t.Helper()
	rec := Do(t, h, http.MethodGet, path, nil, AsTenant(other))
	if rec.Code != http.StatusNotFound {
		t.Fatal(fmt.Sprintf("cross-tenant GET %s = %d, want 404", path, rec.Code))
	}
}
