// Package health serves /healthz (liveness) and /readyz (dependencies).
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Check reports whether one dependency is usable.
type Check func(ctx context.Context) error

// Handler holds readiness checks.
type Handler struct {
	timeout time.Duration
	checks  map[string]Check
}

// New returns a Handler; each readiness check gets timeout (default 2s).
func New(timeout time.Duration) *Handler {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Handler{timeout: timeout, checks: map[string]Check{}}
}

// Add registers a named readiness check. Not safe for use after serving starts.
func (h *Handler) Add(name string, c Check) { h.checks[name] = c }

// Register mounts /healthz and /readyz on mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.Live)
	mux.HandleFunc("GET /readyz", h.Ready)
}

// Live always answers 200 while the process runs; it checks no dependency.
func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready runs all checks concurrently. The response names failing checks but
// never includes error text, which could leak internals.
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	res := make(map[string]string, len(h.checks))
	for name, c := range h.checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := "ok"
			if err := c(ctx); err != nil {
				s = "fail"
			}
			mu.Lock()
			res[name] = s
			mu.Unlock()
		}()
	}
	wg.Wait()
	status, code := "ok", http.StatusOK
	names := make([]string, 0, len(res))
	for n, s := range res {
		names = append(names, n)
		if s != "ok" {
			status, code = "unavailable", http.StatusServiceUnavailable
		}
	}
	sort.Strings(names)
	checks := make(map[string]string, len(res))
	for _, n := range names {
		checks[n] = res[n]
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": checks})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
