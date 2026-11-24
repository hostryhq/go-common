package idempotency

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hostryhq/go-common/ids"
)

func TestDoRunsOnceAndReplays(t *testing.T) {
	s, tenant, ctx := NewMemory(), ids.New(), context.Background()
	calls := 0
	fn := func() (Response, error) { calls++; return Response{Status: 201, Body: []byte(`{"id":"1"}`)}, nil }
	fp := Fingerprint("POST", "/v1/bills", []byte(`{}`))

	r1, replayed, err := Do(ctx, s, tenant, "k1", fp, fn)
	if err != nil || replayed || r1.Status != 201 {
		t.Fatalf("first: %+v %v %v", r1, replayed, err)
	}
	r2, replayed, err := Do(ctx, s, tenant, "k1", fp, fn)
	if err != nil || !replayed || string(r2.Body) != `{"id":"1"}` || calls != 1 {
		t.Fatalf("replay: %+v replayed=%v err=%v calls=%d", r2, replayed, err, calls)
	}
}

func TestDoRejectsDifferentRequestUnderSameKey(t *testing.T) {
	s, tenant, ctx := NewMemory(), ids.New(), context.Background()
	ok := func() (Response, error) { return Response{Status: 200}, nil }
	_, _, _ = Do(ctx, s, tenant, "k", Fingerprint("POST", "/a", nil), ok)
	_, _, err := Do(ctx, s, tenant, "k", Fingerprint("POST", "/a", []byte("x")), ok)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("err = %v", err)
	}
}

func TestKeysAreTenantScoped(t *testing.T) {
	s, ctx := NewMemory(), context.Background()
	calls := 0
	fn := func() (Response, error) { calls++; return Response{Status: 200}, nil }
	fp := Fingerprint("POST", "/a", nil)
	_, _, _ = Do(ctx, s, ids.New(), "same", fp, fn)
	_, replayed, _ := Do(ctx, s, ids.New(), "same", fp, fn)
	if replayed || calls != 2 {
		t.Fatalf("tenant B saw tenant A's key: replayed=%v calls=%d", replayed, calls)
	}
}

func TestFailureReleasesKeyAndInProgressIsReported(t *testing.T) {
	s, tenant, ctx := NewMemory(), ids.New(), context.Background()
	fp := Fingerprint("POST", "/a", nil)
	boom := errors.New("boom")
	if _, _, err := Do(ctx, s, tenant, "k", fp, func() (Response, error) { return Response{}, boom }); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	// released: retry runs again
	if _, replayed, err := Do(ctx, s, tenant, "k", fp, func() (Response, error) { return Response{Status: 200}, nil }); err != nil || replayed {
		t.Fatalf("retry after failure: replayed=%v err=%v", replayed, err)
	}
	// unfinished claim
	_, _, _ = s.Begin(ctx, tenant, "busy", fp)
	if _, _, err := s.Begin(ctx, tenant, "busy", fp); !errors.Is(err, ErrInProgress) {
		t.Fatalf("err = %v", err)
	}
}

func TestKeyFromRequestValidation(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	if _, ok, err := KeyFromRequest(r); ok || err != nil {
		t.Fatal("absent header should be ok=false")
	}
	r.Header.Set(Header, "bad key")
	if _, _, err := KeyFromRequest(r); err == nil {
		t.Fatal("space accepted")
	}
	r.Header.Set(Header, "0190-abc")
	if k, ok, err := KeyFromRequest(r); !ok || err != nil || k != "0190-abc" {
		t.Fatalf("%q %v %v", k, ok, err)
	}
}
