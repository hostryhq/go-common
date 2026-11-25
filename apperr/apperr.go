// Package apperr is the typed domain error model, mapped to RFC 9457
// application/problem+json. Responses never include internal error text.
package apperr

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

// Kind classifies an error and fixes its HTTP status.
type Kind int

// Error kinds.
const (
	Internal Kind = iota
	Validation
	Unauthenticated
	Forbidden
	NotFound // also used for another tenant's resource: never 403
	Conflict
	IdempotencyMismatch
	RateLimited
	Unavailable
)

var kindInfo = map[Kind]struct {
	status int
	title  string
}{
	Internal:            {http.StatusInternalServerError, "Internal error"},
	Validation:          {http.StatusUnprocessableEntity, "Validation failed"},
	Unauthenticated:     {http.StatusUnauthorized, "Authentication required"},
	Forbidden:           {http.StatusForbidden, "Permission denied"},
	NotFound:            {http.StatusNotFound, "Not found"},
	Conflict:            {http.StatusConflict, "Conflict"},
	IdempotencyMismatch: {http.StatusUnprocessableEntity, "Idempotency key reused with a different request"},
	RateLimited:         {http.StatusTooManyRequests, "Too many requests"},
	Unavailable:         {http.StatusServiceUnavailable, "Service unavailable"},
}

// Status returns the HTTP status for k.
func (k Kind) Status() int { return kindInfo[k].status }

// FieldError describes one invalid input field. Message is a safe, i18n-able
// text or key; never echo secrets or PII.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// Error is a typed domain error.
type Error struct {
	Kind   Kind
	Code   string // stable machine code, e.g. "bill.already_void"; forms the problem type
	Detail string // safe, user-visible
	Fields []FieldError
	cause  error
}

// New creates an error. Detail must be safe to show to API clients.
func New(kind Kind, code, detail string) *Error {
	return &Error{Kind: kind, Code: code, Detail: detail}
}

// Wrap attaches an internal cause (logged, never sent).
func Wrap(kind Kind, code, detail string, cause error) *Error {
	return &Error{Kind: kind, Code: code, Detail: detail, cause: cause}
}

// WithFields returns a copy with field errors.
func (e *Error) WithFields(f ...FieldError) *Error {
	c := *e
	c.Fields = append([]FieldError(nil), f...)
	return &c
}

func (e *Error) Error() string {
	s := e.Code
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.cause != nil {
		s += ": " + e.cause.Error()
	}
	return s
}

// Unwrap exposes the internal cause.
func (e *Error) Unwrap() error { return e.cause }

// TypeBase is the prefix of problem type URIs. A URN keeps types stable and
// independent of any hostname; the registry of types is owned by api-specs.
var TypeBase = "urn:hostry:problem:"

// Problem is the RFC 9457 body. Extensions: code, request_id, errors.
type Problem struct {
	Type      string       `json:"type"`
	Title     string       `json:"title"`
	Status    int          `json:"status"`
	Detail    string       `json:"detail,omitempty"`
	Instance  string       `json:"instance,omitempty"`
	Code      string       `json:"code"`
	RequestID string       `json:"request_id,omitempty"`
	Errors    []FieldError `json:"errors,omitempty"`
}

// RequestIDFunc lets httpx/logx supply the request id without an import cycle.
var RequestIDFunc func(r *http.Request) string

// ToProblem converts any error to a Problem. Non-apperr errors become a
// generic Internal problem; their text is never exposed.
func ToProblem(r *http.Request, err error) Problem {
	var e *Error
	if !errors.As(err, &e) {
		e = New(Internal, "internal", "")
	}
	info := kindInfo[e.Kind]
	detail := e.Detail
	if e.Kind == Internal {
		detail = "" // never leak internals
	}
	p := Problem{
		Type:   TypeBase + strings.ReplaceAll(e.Code, " ", "-"),
		Title:  info.title,
		Status: info.status,
		Detail: detail,
		Code:   e.Code,
		Errors: e.Fields,
	}
	if r != nil {
		p.Instance = r.URL.Path
		if RequestIDFunc != nil {
			p.RequestID = RequestIDFunc(r)
		}
	}
	return p
}

// WriteProblem writes err as application/problem+json. Internal errors are
// logged with their cause.
func WriteProblem(w http.ResponseWriter, r *http.Request, err error) {
	p := ToProblem(r, err)
	if p.Status >= 500 {
		ctx := r.Context()
		slog.ErrorContext(ctx, "request failed", "code", p.Code, "error", err)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}
