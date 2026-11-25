package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDAcceptedOrReplacedAndAppearsInProblems(t *testing.T) {
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom: secret") }), RequestID, Recover)

	req := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	req.Header.Set(RequestIDHeader, "client-id-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 500 || rec.Header().Get(RequestIDHeader) != "client-id-1" ||
		!strings.Contains(rec.Body.String(), `"request_id":"client-id-1"`) || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("status=%d hdr=%q body=%s", rec.Code, rec.Header().Get(RequestIDHeader), rec.Body)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	req.Header.Set(RequestIDHeader, "bad id\nwith newline")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if id := rec.Header().Get(RequestIDHeader); len(id) != 36 {
		t.Fatalf("hostile id not replaced: %q", id)
	}
}
