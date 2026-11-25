package emit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
	gsync "github.com/hostryhq/go-common/sync"
)

const tenant = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"

func okOp(key string) Op {
	perm := "menu.tax_class.edit"
	return Op{Key: key, Stream: "brand:0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a6a", Intent: "menu.tax_class.set", IntentV: 1, Actor: "service:catalog",
		AuthorisedBy: &perm, Muts: []gsync.Mut{{E: "item", ID: "i1", K: "set", F: map[string]any{"tax_class": "fnb_standard"}}}}
}

// token mints per tenant, as the gateway requires token tenant = request tenant.
func token(_ context.Context, t ids.ID) (string, error) { return "tok-for-" + string(t), nil }

func TestEmitSendsTokenAndPerOpKeysAndReadsResults(t *testing.T) {
	var got struct {
		TenantID string `json:"tenant_id"`
		Ops      []Op   `json:"ops"`
	}
	var hdr, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr, path = r.Header.Get(ServiceTokenHeader), r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ops":[{"key":"k1","origin":"cloud-1","stream":"brand:b","seq":1,"replayed":false},{"key":"k2","origin":"cloud-1","stream":"brand:b","seq":2,"replayed":true}]}`))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, Token: token}
	res, err := c.Emit(context.Background(), tenant, []Op{okOp("k1"), okOp("k2")})
	if err != nil {
		t.Fatal(err)
	}
	if hdr != "tok-for-"+tenant || path != Path || got.TenantID != tenant || len(got.Ops) != 2 || got.Ops[1].Key != "k2" {
		t.Fatalf("request: hdr=%q path=%q got=%+v", hdr, path, got)
	}
	if res[0].Seq != 1 || res[0].Replayed || !res[1].Replayed || res[1].Origin != "cloud-1" {
		t.Fatalf("results: %+v", res)
	}
}

func TestEmitUnavailableIsSurfacedAs503WithRetryAfter(t *testing.T) {
	for name, status := range map[string]int{"503 not ready": 503, "500": 500, "502": 502} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "30")
				w.WriteHeader(status)
			}))
			defer srv.Close()
			_, err := (&Client{BaseURL: srv.URL, Token: token}).Emit(context.Background(), tenant, []Op{okOp("k1")})
			var ae *apperr.Error
			if !errors.As(err, &ae) || ae.Code != "emit.unavailable" || ae.Kind.Status() != 503 {
				t.Fatalf("err = %v", err)
			}
			if !IsTransient(err) || RetryAfter(err) != 30*time.Second {
				t.Fatalf("transient=%v after=%v", IsTransient(err), RetryAfter(err))
			}
		})
	}
	// An unreachable gateway is the same error, and no token or host text leaks into it.
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := (&Client{BaseURL: url, Token: token}).Emit(context.Background(), tenant, []Op{okOp("k1")})
	if !IsTransient(err) || strings.Contains(err.Error(), "tok-from-test-source") {
		t.Fatalf("unreachable: %v", err)
	}
	// A token source failure is also "unavailable", not a rejection.
	_, err = (&Client{BaseURL: url, Token: func(context.Context, ids.ID) (string, error) { return "", errors.New("identity down") }}).
		Emit(context.Background(), tenant, []Op{okOp("k1")})
	if !IsTransient(err) {
		t.Fatalf("token failure: %v", err)
	}
}

func TestEmitRefusalIsADefectNotTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(422)
		_, _ = w.Write([]byte(`{"type":"urn:hostry:problem:idempotency.mismatch","title":"x","status":422,"code":"idempotency.mismatch"}`))
	}))
	defer srv.Close()
	_, err := (&Client{BaseURL: srv.URL, Token: token}).Emit(context.Background(), tenant, []Op{okOp("k1")})
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != "idempotency.mismatch" || IsTransient(err) {
		t.Fatalf("err = %v", err)
	}
	if ae := rej.AppError(); ae.Code != "emit.rejected" || ae.Kind.Status() != 500 {
		t.Fatalf("app error: %+v", ae)
	}
}

func TestEmitValidatesBeforeSending(t *testing.T) {
	c := &Client{BaseURL: "http://127.0.0.1:1", Token: token}
	big := okOp("big")
	big.Muts[0].F = map[string]any{"x": strings.Repeat("a", gsync.MaxBodyBytes)}
	hugePII := okOp("pii")
	hugePII.PII = map[string]map[string]map[string]any{"e": {"i": {"x": strings.Repeat("b", gsync.MaxPIIBytes)}}}
	noKey := okOp("")
	long := okOp(strings.Repeat("k", 129))
	many := make([]Op, gsync.MaxEmitOps+1)
	for i := range many {
		many[i] = okOp(DeriveKey("idem", i))
	}
	tests := map[string][]Op{
		"empty batch": nil, "201 ops": many, "no key": {noKey}, "key over 128": {long},
		"op over size": {big}, "pii over size": {hugePII}, "repeated key": {okOp("a"), okOp("a")},
	}
	for name, ops := range tests {
		if _, err := c.Emit(context.Background(), tenant, ops); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid (and nothing sent)", name, err)
		}
	}
	if _, err := c.Emit(context.Background(), "", []Op{okOp("a")}); !errors.Is(err, ErrInvalid) {
		t.Errorf("no tenant: %v", err)
	}
}

func TestEmitRefusesAResultThatDoesNotMatchTheRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ops":[{"key":"other","origin":"cloud-1","stream":"s","seq":1,"replayed":false}]}`))
	}))
	defer srv.Close()
	if _, err := (&Client{BaseURL: srv.URL, Token: token}).Emit(context.Background(), tenant, []Op{okOp("k1")}); !IsTransient(err) {
		t.Fatalf("mismatched result accepted: %v", err)
	}
}

func TestKeysAreDeterministicAndDistinct(t *testing.T) {
	if DeriveKey("abc", 0) != DeriveKey("abc", 0) || DeriveKey("abc", 0) == DeriveKey("abc", 1) || DeriveKey("abc", 0) == DeriveKey("abd", 0) {
		t.Fatal("DeriveKey must be stable per (key, n) and differ otherwise")
	}
	if len(DeriveKey("x", 1)) > 128 {
		t.Fatal("derived key too long")
	}
	if DeterministicKey("job", "1", "row", "2") == DeterministicKey("job", "1", "row", "3") {
		t.Fatal("parts must matter")
	}
}

func TestBackoffGrowsIsCappedHasJitterAndHonoursRetryAfter(t *testing.T) {
	b, m := time.Second, time.Minute
	if d := Backoff(1, b, m, 0, 0); d != 500*time.Millisecond {
		t.Errorf("first, no jitter: %v", d)
	}
	if d := Backoff(1, b, m, 0, 0.999); d >= time.Second || d < 900*time.Millisecond {
		t.Errorf("first, max jitter: %v", d)
	}
	prev := time.Duration(0)
	for n := 1; n <= 5; n++ {
		d := Backoff(n, b, m, 0, 0)
		if d <= prev {
			t.Errorf("not growing at %d: %v after %v", n, d, prev)
		}
		prev = d
	}
	if d := Backoff(50, b, m, 0, 0.999); d > m {
		t.Errorf("not capped: %v", d)
	}
	if d := Backoff(1, b, m, 45*time.Second, 0); d != 45*time.Second {
		t.Errorf("Retry-After ignored: %v", d)
	}
}
