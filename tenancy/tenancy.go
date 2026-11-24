// Package tenancy carries the tenant context (ADR-0003) through a request and
// into the database.
//
// tenant_id is the owning organisation. It is derived from a verified token
// or link context (go-auth), never from a body, query or path. This package
// does not verify credentials; it only stores what the caller's verified
// extractor produced and fails closed when it is missing.
package tenancy

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
)

// Context is the verified identity of the current operation.
type Context struct {
	// TenantID is the owning organisation (data owner).
	TenantID ids.ID
	// OutletID is the outlet/property; empty for organisation-level calls.
	OutletID ids.ID
	// ActorID is the member, device or service performing the action (audit).
	ActorID string
	// DeviceID is the calling device, if any.
	DeviceID string
	// ActingTenantID and LinkID are set only on link calls (ADR-0003 §3), where
	// TenantID is the data owner and ActingTenantID is the caller's own tenant.
	ActingTenantID ids.ID
	LinkID         string
}

type ctxKey struct{}

// With returns ctx carrying tc.
func With(ctx context.Context, tc Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, tc)
}

// From returns the tenant context, if present and with a tenant id.
func From(ctx context.Context) (Context, bool) {
	tc, ok := ctx.Value(ctxKey{}).(Context)
	return tc, ok && tc.TenantID != ""
}

// Require returns the tenant context or a typed unauthenticated error. Use it
// at the top of service methods so no path runs without a tenant.
func Require(ctx context.Context) (Context, error) {
	tc, ok := From(ctx)
	if !ok {
		return Context{}, apperr.New(apperr.Unauthenticated, "auth.tenant_missing", "tenant context missing")
	}
	return tc, nil
}

// Extractor reads the verified principal for a request (provided by go-auth).
type Extractor func(r *http.Request) (Context, error)

// Middleware puts the extracted tenant context on the request context. An
// extractor error or an empty tenant is rejected with a problem response, so
// handlers behind it always have a tenant (fail closed).
func Middleware(extract Extractor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tc, err := extract(r)
			if err == nil && tc.TenantID == "" {
				err = apperr.New(apperr.Unauthenticated, "auth.tenant_missing", "tenant context missing")
			}
			if err != nil {
				apperr.WriteProblem(w, r, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(With(r.Context(), tc)))
		})
	}
}

// Execer is the part of *sql.Tx used to set the database context.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// SetDBContext sets app.tenant_id for the current transaction so row-level
// security (ADR-0003 §6) applies. The setting is transaction-local (third
// argument true), which is safe with connection pooling.
func SetDBContext(ctx context.Context, tx Execer, tenantID ids.ID) error {
	if tenantID == "" {
		return errors.New("tenancy: empty tenant id")
	}
	_, err := tx.ExecContext(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID.String())
	return err
}

// TxBeginner is satisfied by *sql.DB.
type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// WithTx runs fn in a transaction whose database context is the tenant in
// ctx. Repositories should obtain handles only through this helper, so there
// is no code path with a connection and no tenant.
func WithTx(ctx context.Context, db TxBeginner, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tc, err := Require(ctx)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := SetDBContext(ctx, tx, tc.TenantID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
