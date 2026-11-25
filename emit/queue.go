package emit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/ids"
	gsync "github.com/hostryhq/go-common/sync"
)

// QueueSchema is the forward-only migration for the emit_queue table. Services copy it into
// repository/migrations. It is the outbox of cloud-origin emissions: a batch job (menu import,
// rule-set publish, day revision) enqueues in the same transaction as its own state change, and a
// worker sends the rows to the gateway, waiting and retrying while the gateway is down.
const QueueSchema = `
CREATE TABLE IF NOT EXISTS emit_queue (
  id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id       text        NOT NULL,
  key             text        NOT NULL,
  op              jsonb       NOT NULL,
  state           text        NOT NULL DEFAULT 'pending',
  attempts        int         NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,
  last_error      text        NOT NULL DEFAULT '',
  origin          text,
  stream          text,
  seq             bigint,
  replayed        boolean,
  created_at      timestamptz NOT NULL,
  sent_at         timestamptz,
  UNIQUE (tenant_id, key)
);
CREATE INDEX IF NOT EXISTS emit_queue_pending ON emit_queue (tenant_id, id) WHERE state = 'pending';
`

// Queue states.
const (
	StatePending = "pending"
	StateSent    = "sent"
	StateFailed  = "failed" // refused by the gateway: a defect in the caller, never retried, needs a person
)

// DBTX is the part of *sql.DB and *sql.Tx Enqueue needs.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Enqueue adds ops for one tenant inside the caller's transaction, so they exist exactly when the
// state change that caused them commits. An op whose (tenant, key) is already queued is ignored.
// Ops of one tenant are sent in enqueue order.
func Enqueue(ctx context.Context, db DBTX, now time.Time, tenant ids.ID, ops ...Op) error {
	for i, o := range ops {
		if err := o.Validate(); err != nil {
			return fmt.Errorf("op %d: %w", i, err)
		}
		b, err := json.Marshal(o)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO emit_queue (tenant_id, key, op, next_attempt_at, created_at) VALUES ($1,$2,$3,$4,$4) ON CONFLICT (tenant_id, key) DO NOTHING`,
			tenant.String(), o.Key, string(b), now.UTC()); err != nil {
			return err
		}
	}
	return nil
}

// Queue drains emit_queue. Run one worker per service instance; an advisory lock per tenant keeps
// two instances from sending one tenant's rows out of order.
type Queue struct {
	DB      *sql.DB
	Emitter Emitter
	Clock   clock.Clock
	Log     *slog.Logger
	// BatchSize is the ops per request, 1 to 200 (default 100).
	BatchSize int
	// BaseDelay and MaxDelay bound the wait after a transient failure (defaults 2s and 5m).
	BaseDelay, MaxDelay time.Duration
	// Jitter returns a value in [0,1); default math/rand. Inject for tests.
	Jitter func() float64
}

func (q *Queue) now() time.Time {
	if q.Clock != nil {
		return q.Clock.Now()
	}
	return time.Now().UTC()
}

func (q *Queue) batch() int {
	if q.BatchSize >= 1 && q.BatchSize <= gsync.MaxEmitOps {
		return q.BatchSize
	}
	return 100
}

func (q *Queue) log() *slog.Logger {
	if q.Log != nil {
		return q.Log
	}
	return slog.Default()
}

// Backoff returns the wait before attempt n (1 for the first retry): exponential from base, capped
// at max, with full jitter in the upper half, and never shorter than the gateway's Retry-After.
func Backoff(n int, base, max, retryAfter time.Duration, jitter float64) time.Duration {
	if base <= 0 {
		base = 2 * time.Second
	}
	if max <= 0 {
		max = 5 * time.Minute
	}
	d := base
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	d = d/2 + time.Duration(jitter*float64(d/2))
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

// Drain sends every due row it can and returns how many ops the gateway stored. A tenant whose
// oldest row is not yet due is skipped, so order within a tenant is kept. It stops at the first
// transient failure of a tenant (the rows wait with backoff) and goes on with other tenants. It
// returns an error only for database failures.
func (q *Queue) Drain(ctx context.Context) (sent int, err error) {
	skip := map[string]bool{}
	for ctx.Err() == nil {
		n, done, err := q.drainOne(ctx, skip)
		sent += n
		if err != nil {
			return sent, err
		}
		if done {
			return sent, nil
		}
	}
	return sent, ctx.Err()
}

// Run drains every interval until ctx ends.
func (q *Queue) Run(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := q.Drain(ctx); err != nil && ctx.Err() == nil {
			q.log().ErrorContext(ctx, "emit queue drain failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

type row struct {
	id       int64
	key      string
	attempts int
	op       Op
}

// drainOne handles one batch of one tenant in one transaction. done is true when no tenant is due.
func (q *Queue) drainOne(ctx context.Context, skip map[string]bool) (n int, done bool, err error) {
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback() }()
	now := q.now()

	// A tenant is due when its oldest pending row is due.
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id FROM (SELECT DISTINCT ON (tenant_id) tenant_id, next_attempt_at FROM emit_queue WHERE state='pending' ORDER BY tenant_id, id) h WHERE next_attempt_at <= $1 ORDER BY tenant_id`, now)
	if err != nil {
		return 0, false, err
	}
	var due []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return 0, false, err
		}
		if !skip[t] {
			due = append(due, t)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	var tenant string
	for _, t := range due {
		var got bool
		if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, "emit_queue|"+t).Scan(&got); err != nil {
			return 0, false, err
		}
		if got {
			tenant = t
			break
		}
		skip[t] = true
	}
	if tenant == "" {
		return 0, true, nil // nothing due, or what is due is held by another worker
	}

	batch, err := q.claim(ctx, tx, tenant)
	if err != nil {
		return 0, false, err
	}
	n, err = q.send(ctx, tx, tenant, batch, now)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return n, false, nil
}

func (q *Queue) claim(ctx context.Context, tx *sql.Tx, tenant string) ([]row, error) {
	rs, err := tx.QueryContext(ctx, `SELECT id, key, attempts, op FROM emit_queue WHERE tenant_id=$1 AND state='pending' ORDER BY id LIMIT $2`, tenant, q.batch())
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []row
	for rs.Next() {
		var r row
		var raw []byte
		if err := rs.Scan(&r.id, &r.key, &r.attempts, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &r.op); err != nil {
			return nil, fmt.Errorf("emit_queue row %d: %w", r.id, err)
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

// send emits the batch; on a refusal it isolates the defective ops one by one. It holds the tx (and
// the tenant lock) across the gateway call, which the client bounds with a timeout; idempotency
// keys make a lost commit after a successful send harmless (the retry is a replay).
func (q *Queue) send(ctx context.Context, tx *sql.Tx, tenant string, batch []row, now time.Time) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	tid := ids.ID(tenant)
	ops := make([]Op, len(batch))
	for i, r := range batch {
		ops[i] = r.op
	}
	res, err := q.Emitter.Emit(ctx, tid, ops)
	switch {
	case err == nil:
		return len(batch), q.markSent(ctx, tx, batch, res, now)
	case IsTransient(err):
		return 0, q.markRetry(ctx, tx, batch, err, now)
	}
	var rej *RejectedError
	if !errors.As(err, &rej) {
		// Our own validation or an unexpected error: park the batch with the error class, not content.
		return 0, q.markFailed(ctx, tx, batch, "invalid", now)
	}
	if len(batch) == 1 {
		q.log().ErrorContext(ctx, "emit refused by the gateway: defect in the caller", "tenant_id", tenant, "key", batch[0].key, "status", rej.Status, "code", rej.Code)
		return 0, q.markFailed(ctx, tx, batch, rej.Code, now)
	}
	// The batch is atomic: one bad op refuses all. Send singly to find it; keep the order.
	sent := 0
	for i, r := range batch {
		one, err := q.send(ctx, tx, tenant, batch[i:i+1], now)
		if err != nil {
			return sent, err
		}
		sent += one
		if one == 0 && q.stillPending(ctx, tx, r.id) { // a transient failure: stop, keep order
			break
		}
	}
	return sent, nil
}

func (q *Queue) stillPending(ctx context.Context, tx *sql.Tx, id int64) bool {
	var st string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM emit_queue WHERE id=$1`, id).Scan(&st); err != nil {
		return true
	}
	return st == StatePending
}

func (q *Queue) markSent(ctx context.Context, tx *sql.Tx, batch []row, res []Result, now time.Time) error {
	for i, r := range batch {
		if _, err := tx.ExecContext(ctx,
			`UPDATE emit_queue SET state='sent', origin=$2, stream=$3, seq=$4, replayed=$5, sent_at=$6, last_error='' WHERE id=$1`,
			r.id, res[i].Origin, res[i].Stream, res[i].Seq, res[i].Replayed, now); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) markRetry(ctx context.Context, tx *sql.Tx, batch []row, cause error, now time.Time) error {
	after := RetryAfter(cause)
	jit := rand.Float64
	if q.Jitter != nil {
		jit = q.Jitter
	}
	for _, r := range batch {
		d := Backoff(r.attempts+1, q.BaseDelay, q.MaxDelay, after, jit())
		if _, err := tx.ExecContext(ctx,
			`UPDATE emit_queue SET attempts=attempts+1, next_attempt_at=$2, last_error='emit.unavailable' WHERE id=$1`,
			r.id, now.Add(d)); err != nil {
			return err
		}
	}
	q.log().WarnContext(ctx, "emit unavailable, queued ops wait", "ops", len(batch))
	return nil
}

func (q *Queue) markFailed(ctx context.Context, tx *sql.Tx, batch []row, code string, now time.Time) error {
	if code == "" {
		code = "emit.rejected"
	}
	for _, r := range batch {
		if _, err := tx.ExecContext(ctx, `UPDATE emit_queue SET state='failed', attempts=attempts+1, last_error=$2 WHERE id=$1`, r.id, code); err != nil {
			return err
		}
	}
	return nil
}

// Counts returns pending and failed rows, for metrics and /readyz (failed needs a person).
func Counts(ctx context.Context, db *sql.DB) (pending, failed int, err error) {
	err = db.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state='pending'), count(*) FILTER (WHERE state='failed') FROM emit_queue`).Scan(&pending, &failed)
	return
}
