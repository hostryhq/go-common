// Package logx configures log/slog with request/tenant correlation and
// key-based PII/secret redaction. Never log request bodies or tokens; redaction
// is a safety net, not a licence.
package logx

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"github.com/hostryhq/go-common/tenancy"
)

// Redacted replaces sensitive values.
const Redacted = "[REDACTED]"

// sensitive key fragments, matched case-insensitively against the attribute key.
var sensitive = []string{
	"password", "passwd", "secret", "token", "authorization", "cookie",
	"api_key", "apikey", "credential", "otp", "pin", "phone", "mobile",
	"email", "aadhaar", "pan", "card", "cvv", "name", "address",
}

// exactOnly keys are short and would over-match as fragments ("pin" in
// "shipping", "pan" in "company"); they redact only when equal to the key.
var exactOnly = map[string]bool{"pin": true, "pan": true, "otp": true, "name": true, "card": true}

func isSensitive(key string) bool {
	k := strings.ToLower(key)
	if exactOnly[k] {
		return true
	}
	for _, s := range sensitive {
		if !exactOnly[s] && strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// Redact is a slog ReplaceAttr that masks sensitive keys at any depth.
func Redact(_ []string, a slog.Attr) slog.Attr {
	if isSensitive(a.Key) && a.Value.Kind() != slog.KindGroup {
		return slog.String(a.Key, Redacted)
	}
	return a
}

type ctxKey struct{}

// WithRequestID stores the request id in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// RequestID returns the request id in ctx, or "".
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(ctxKey{}).(string)
	return s
}

type ctxHandler struct{ slog.Handler }

func (h ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	if tc, ok := tenancy.From(ctx); ok {
		r.AddAttrs(slog.String("tenant_id", tc.TenantID.String()))
		if tc.OutletID != "" {
			r.AddAttrs(slog.String("outlet_id", tc.OutletID.String()))
		}
		if tc.ActorID != "" {
			r.AddAttrs(slog.String("actor_id", tc.ActorID))
		}
		if tc.DeviceID != "" {
			r.AddAttrs(slog.String("device_id", tc.DeviceID))
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h ctxHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return ctxHandler{h.Handler.WithAttrs(a)}
}

func (h ctxHandler) WithGroup(n string) slog.Handler { return ctxHandler{h.Handler.WithGroup(n)} }

// New returns a JSON logger writing to w. Use the *Context logging methods so
// request and tenant ids are attached.
func New(w io.Writer, level slog.Level, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: Redact})
	return slog.New(ctxHandler{h}).With("service", service)
}
