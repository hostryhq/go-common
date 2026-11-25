// Package httpx has small HTTP middleware shared by services: request id and
// panic recovery. Compose with tenancy.Middleware (from go-auth's extractor).
package httpx

import (
	"net/http"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/logx"
)

// RequestIDHeader carries the correlation id.
const RequestIDHeader = "X-Request-ID"

func init() {
	apperr.RequestIDFunc = func(r *http.Request) string { return logx.RequestID(r.Context()) }
}

func validRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// RequestID accepts a sane inbound X-Request-ID or mints a UUIDv7, stores it in
// the context for logging and echoes it on the response.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = ids.New().String()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(logx.WithRequestID(r.Context(), id)))
	})
}

// Recover turns a handler panic into a generic 500 problem.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				apperr.WriteProblem(w, r, apperr.New(apperr.Internal, "internal", ""))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Chain applies middleware so the first listed is outermost.
func Chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
