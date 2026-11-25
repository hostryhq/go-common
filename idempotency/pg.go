package idempotency

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/hostryhq/go-common/ids"
)

// DefaultTTL is how long a finished record is kept and replayed. Offline retry queues
// are bounded well under this; after expiry the key is treated as new.
const DefaultTTL = 24 * time.Hour

// DefaultClaimTTL is how long an unfinished claim blocks retries in the non-transactional
// PGStore before a retry may take it over. Not used by DoTx, whose claim is atomic.
const DefaultClaimTTL = 5 * time.Minute

// Schema is the forward-only migration for the Postgres records. Services copy it into
// repository/migrations. With DoTx a row exists only as the result of a committed claim: the
// claim, the business write and the result commit or roll back together, so a crash can never
// leave a key stuck. Secrets must never be stored here; see TxResult.OmitBody.
const Schema = `
CREATE TABLE IF NOT EXISTS idempotency_records (
  tenant_id    text        NOT NULL,
  key          text        NOT NULL,
  fingerprint  text        NOT NULL,
  done         boolean     NOT NULL DEFAULT false,
  status       integer     NOT NULL DEFAULT 0,
  content_type text        NOT NULL DEFAULT '',
  body         bytea,
  body_sha256  text        NOT NULL DEFAULT '',
  created_at   timestamptz NOT NULL,
  expires_at   timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, key)
);
CREATE INDEX IF NOT EXISTS idempotency_records_expires ON idempotency_records (expires_at);
`

// PGOptions tunes the Postgres helpers. Zero values use the defaults.
type PGOptions struct {
	TTL      time.Duration    // retention of finished records (DefaultTTL)
	ClaimTTL time.Duration    // stale-claim takeover for PGStore (DefaultClaimTTL)
	Now      func() time.Time // injectable clock for tests
}

func (o PGOptions) ttl() time.Duration {
	if o.TTL > 0 {
		return o.TTL
	}
	return DefaultTTL
}

func (o PGOptions) claimTTL() time.Duration {
	if o.ClaimTTL > 0 {
		return o.ClaimTTL
	}
	return DefaultClaimTTL
}

func (o PGOptions) now() time.Time {
	if o.Now != nil {
		return o.Now().UTC()
	}
	return time.Now().UTC()
}

// ErrBodyOmitted is returned alongside a replayed Response whose body was deliberately not
// stored (OmitBody). The caller maps it to a safe reply, for example the resource reference,
// or the fact that the secret was already issued once. Status and ContentType are set, Body is nil.
var ErrBodyOmitted = errors.New("idempotency: stored response body was omitted")

// TxResult is what a DoTx callback returns.
type TxResult struct {
	Response Response
	// OmitBody stores only status, content type and the SHA-256 of the body, never the body.
	// Use it for any response carrying a token, refresh token, one-time code, invitation code,
	// pack or other secret.
	OmitBody bool
}

// DoTx runs fn at most once per (tenant, key), with the claim, the business write (done on the
// tx passed to fn) and the stored result in ONE transaction. If fn errors or the process dies
// before commit, everything rolls back and the key is free again; nothing is left stuck. A
// concurrent request with the same key waits on the claim row until the first commits (then it
// replays) or rolls back (then it runs). Expired records are replaced.
//
// fn must not commit or roll back the tx. replayed is true when a stored result is returned; a
// replay of an OmitBody result also returns ErrBodyOmitted.
func DoTx(ctx context.Context, db *sql.DB, o PGOptions, tenant ids.ID, key, fingerprint string, fn func(tx *sql.Tx) (TxResult, error)) (resp Response, replayed bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Response{}, false, err
	}
	defer func() { _ = tx.Rollback() }() // no-op after commit
	now := o.now()
	t := tenant.String()
	if _, err = tx.ExecContext(ctx, `DELETE FROM idempotency_records WHERE tenant_id=$1 AND key=$2 AND expires_at <= $3`, t, key, now); err != nil {
		return Response{}, false, err
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_records (tenant_id, key, fingerprint, created_at, expires_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
		t, key, fingerprint, now, now.Add(o.ttl()))
	if err != nil {
		return Response{}, false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return replay(ctx, tx, t, key, fingerprint)
	}
	r, err := fn(tx)
	if err != nil {
		return Response{}, false, err
	}
	body := r.Response.Body
	if r.OmitBody {
		body = nil
	}
	sum := sha256.Sum256(r.Response.Body)
	if _, err = tx.ExecContext(ctx,
		`UPDATE idempotency_records SET done=true, status=$3, content_type=$4, body=$5, body_sha256=$6 WHERE tenant_id=$1 AND key=$2`,
		t, key, r.Response.Status, r.Response.ContentType, body, hex.EncodeToString(sum[:])); err != nil {
		return Response{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Response{}, false, err
	}
	return r.Response, false, nil
}

func replay(ctx context.Context, tx *sql.Tx, t, key, fp string) (Response, bool, error) {
	var (
		storedFP, ct string
		done         bool
		status       int
		body         []byte
		sum          string
	)
	err := tx.QueryRowContext(ctx, `SELECT fingerprint, done, status, content_type, body, body_sha256 FROM idempotency_records WHERE tenant_id=$1 AND key=$2`, t, key).
		Scan(&storedFP, &done, &status, &ct, &body, &sum)
	if err != nil {
		return Response{}, false, err
	}
	if storedFP != fp {
		return Response{}, false, ErrMismatch
	}
	if !done { // an unfinished PGStore claim on the same key
		return Response{}, false, ErrInProgress
	}
	r := Response{Status: status, ContentType: ct, Body: body}
	if body == nil && sum != emptySum {
		return r, true, ErrBodyOmitted
	}
	return r, true, nil
}

var emptySum = func() string { s := sha256.Sum256(nil); return hex.EncodeToString(s[:]) }()

// Sweep deletes expired records; run it periodically (for example hourly). It returns the count.
func Sweep(ctx context.Context, db *sql.DB, o PGOptions) (int64, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM idempotency_records WHERE expires_at <= $1`, o.now())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PGStore is a Store for work that cannot share the business transaction (a call to another
// service, say). Prefer DoTx. An unfinished claim older than ClaimTTL is taken over by the next
// retry, so a crash between Begin and Complete costs one ClaimTTL of 409s, not for ever. Because
// the business write is separate, fn must itself tolerate a re-run after a takeover.
type PGStore struct {
	DB  *sql.DB
	Opt PGOptions
}

var _ Store = (*PGStore)(nil)

// Begin implements Store.
func (s *PGStore) Begin(ctx context.Context, tenant ids.ID, key, fp string) (*Response, bool, error) {
	now, t := s.Opt.now(), tenant.String()
	if _, err := s.DB.ExecContext(ctx, `DELETE FROM idempotency_records WHERE tenant_id=$1 AND key=$2 AND (expires_at <= $3 OR (NOT done AND created_at <= $4))`,
		t, key, now, now.Add(-s.Opt.claimTTL())); err != nil {
		return nil, false, err
	}
	res, err := s.DB.ExecContext(ctx, `INSERT INTO idempotency_records (tenant_id, key, fingerprint, created_at, expires_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
		t, key, fp, now, now.Add(s.Opt.ttl()))
	if err != nil {
		return nil, false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil, true, nil
	}
	var (
		storedFP, ct string
		done         bool
		status       int
		body         []byte
	)
	if err = s.DB.QueryRowContext(ctx, `SELECT fingerprint, done, status, content_type, body FROM idempotency_records WHERE tenant_id=$1 AND key=$2`, t, key).
		Scan(&storedFP, &done, &status, &ct, &body); err != nil {
		return nil, false, err
	}
	switch {
	case storedFP != fp:
		return nil, false, ErrMismatch
	case !done:
		return nil, false, ErrInProgress
	}
	return &Response{Status: status, ContentType: ct, Body: body}, false, nil
}

// Complete implements Store. Do not pass secrets in r.Body; pass a reference instead.
func (s *PGStore) Complete(ctx context.Context, tenant ids.ID, key string, r Response) error {
	res, err := s.DB.ExecContext(ctx, `UPDATE idempotency_records SET done=true, status=$3, content_type=$4, body=$5 WHERE tenant_id=$1 AND key=$2`,
		tenant.String(), key, r.Status, r.ContentType, r.Body)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("idempotency: complete on unknown or expired key")
	}
	return nil
}

// Release implements Store.
func (s *PGStore) Release(ctx context.Context, tenant ids.ID, key string) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM idempotency_records WHERE tenant_id=$1 AND key=$2 AND NOT done`, tenant.String(), key)
	return err
}
