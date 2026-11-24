package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadyReportsFailuresWithoutLeakingErrorText(t *testing.T) {
	h := New(time.Second)
	h.Add("db", func(context.Context) error { return nil })
	h.Add("bus", func(context.Context) error { return errors.New("dial tcp 10.0.0.5: secret-host refused") })
	rec := httptest.NewRecorder()
	h.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "secret-host") || !strings.Contains(body, `"bus":"fail"`) || !strings.Contains(body, `"db":"ok"`) {
		t.Fatalf("body %s", body)
	}
}

func TestLiveIgnoresDependencies(t *testing.T) {
	h := New(0)
	h.Add("db", func(context.Context) error { return errors.New("down") })
	rec := httptest.NewRecorder()
	h.Live(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}
