package apperr

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteProblemMapsKindAndHidesInternals(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantLeak   string
	}{
		{"not found", New(NotFound, "bill.not_found", "bill not found"), 404, "bill.not_found", ""},
		{"validation with fields", New(Validation, "bill.invalid", "bad").WithFields(FieldError{Field: "qty", Code: "min"}), 422, "bill.invalid", ""},
		{"wrapped internal cause is not leaked", Wrap(Internal, "db.failed", "oops", errors.New("pq: relation \"bills\" does not exist")), 500, "db.failed", "pq:"},
		{"unknown error is generic 500", errors.New("select * from secrets"), 500, "internal", "select"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			WriteProblem(rec, httptest.NewRequest(http.MethodGet, "/v1/bills/1", nil), tc.err)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d", rec.Code, tc.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("content-type %q", ct)
			}
			var p Problem
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatal(err)
			}
			if p.Code != tc.wantCode || p.Status != tc.wantStatus || p.Type != TypeBase+tc.wantCode || p.Instance != "/v1/bills/1" {
				t.Fatalf("problem = %+v", p)
			}
			if tc.wantLeak != "" && strings.Contains(rec.Body.String(), tc.wantLeak) {
				t.Fatalf("body leaks internals: %s", rec.Body.String())
			}
		})
	}
}
