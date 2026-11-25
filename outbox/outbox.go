// Package outbox is the transactional outbox of ADR-0011 B4: events are inserted in the
// caller's database transaction and relayed asynchronously to the bus. A service never
// publishes to the bus any other way. It uses database/sql only; the caller picks the driver.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hostryhq/go-common/events"
)

// Schema is the forward-only migration for the outbox table (service-internal, ADR-0003 section 2).
// Services copy it into repository/migrations. payload holds the whole envelope JSON.
const Schema = `
CREATE TABLE IF NOT EXISTS outbox (
  id           uuid        PRIMARY KEY,
  seq          bigint      GENERATED ALWAYS AS IDENTITY,
  type         text        NOT NULL,
  subject      text        NOT NULL,
  tenant_id    text        NOT NULL,
  key          text        NOT NULL DEFAULT '',
  key_seq      bigint      NOT NULL DEFAULT 0,
  payload      bytea       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;
`

// RelayStateSchema is the forward-only migration of outbox_relay_state: the bus generation the
// relay last republished for (ADR-0011 B9 item 2, Note N1). It is rolled back with the database.
const RelayStateSchema = `
CREATE TABLE IF NOT EXISTS outbox_relay_state (
  stream     text        PRIMARY KEY,
  generation text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
`

// Publish writes e to the outbox inside tx (E1: a rolled-back transaction publishes nothing).
// The envelope is validated and marshalled; it is the bus message as is.
func Publish(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
	b, err := e.Marshal()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (id, type, subject, tenant_id, key, key_seq, payload) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		e.ID.String(), e.Type, e.Subject(), e.TenantID.String(), e.Key, e.KeySeq, b)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	// Wake in-database listeners; the relay itself also polls, so a lost NOTIFY costs only latency.
	_, err = tx.ExecContext(ctx, `SELECT pg_notify('outbox', '')`)
	return err
}

// Sink publishes one message and returns after the stream acknowledged it. bus.Publisher satisfies it.
type Sink interface {
	Publish(ctx context.Context, subject, id string, data []byte) error
}

// Relay moves unpublished rows to the Sink in seq order. Exactly one relay is active per
// database (Postgres advisory lock); the others wait.
type Relay struct {
	DB   *sql.DB
	Sink Sink
	// LockKey is the advisory lock key, unique per service database.
	LockKey int64
	// Poll is the idle poll interval (default 1s); Batch the rows per round (default 100).
	Poll  time.Duration
	Batch int
	// MaxBackoff caps the wait after a sink error (default 30s).
	MaxBackoff time.Duration
	// Retention of published rows (default 7 days).
	Retention time.Duration
	Log       *slog.Logger
	Now       func() time.Time

	// Generation reads the bus generation of Stream (bus.GenerationFunc). When set, the relay
	// compares it with outbox_relay_state at every connect and every GenerationEvery (default one
	// minute). On a difference it marks the outbox rows inside the Retention horizon unpublished,
	// republishes them in order, then records the generation. This covers hostry-bus-reset with
	// services still running, and a restore of the service database.
	Generation      func(ctx context.Context) (string, error)
	Stream          string
	GenerationEvery time.Duration

	wake       chan struct{}
	pendingGen string // generation being republished; recorded once the outbox is drained
	lastGen    time.Time
}

func (r *Relay) init() {
	if r.Poll == 0 {
		r.Poll = time.Second
	}
	if r.Batch == 0 {
		r.Batch = 100
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = 30 * time.Second
	}
	if r.Retention == 0 {
		r.Retention = 7 * 24 * time.Hour
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.wake == nil {
		r.wake = make(chan struct{}, 1)
	}
	if r.GenerationEvery == 0 {
		r.GenerationEvery = time.Minute
	}
	if r.Stream == "" {
		r.Stream = "default"
	}
}

// Wake asks the relay to poll now (call after committing an outbox transaction).
func (r *Relay) Wake() {
	r.init()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run holds the advisory lock and relays until ctx ends. It returns ctx.Err().
func (r *Relay) Run(ctx context.Context) error {
	r.init()
	conn, err := r.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	locked := false
	defer func() {
		if locked {
			_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, r.LockKey)
		}
	}()
	backoff := time.Duration(0)
	lastClean := time.Time{}
	for {
		if !locked {
			if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, r.LockKey).Scan(&locked); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.Log.Error("outbox relay: lock", "err", err)
			}
		}
		wait := r.Poll
		if locked {
			if now := r.Now(); r.Generation != nil && (r.lastGen.IsZero() || now.Sub(r.lastGen) >= r.GenerationEvery) {
				r.lastGen = now
				if err := r.checkGeneration(ctx); err != nil && ctx.Err() == nil {
					r.Log.Warn("outbox relay: bus generation check failed", "stream", r.Stream, "err", err)
				}
			}
			n, err := r.relayBatch(ctx)
			switch {
			case err != nil && ctx.Err() != nil:
				return ctx.Err()
			case err != nil:
				backoff = nextBackoff(backoff, r.MaxBackoff)
				wait = backoff
				r.Log.Warn("outbox relay: publish failed; rows stay in the outbox", "err", err, "retry_in", backoff)
			default:
				backoff = 0
				if n == r.Batch {
					wait = 0 // more rows waiting
				} else if r.pendingGen != "" {
					// Everything inside the horizon is republished: now record the generation.
					if err := r.recordGeneration(ctx, r.pendingGen); err != nil {
						r.Log.Warn("outbox relay: record generation", "stream", r.Stream, "err", err)
					} else {
						r.pendingGen = ""
					}
				}
			}
			if now := r.Now(); now.Sub(lastClean) > time.Minute {
				lastClean = now
				if _, err := r.DB.ExecContext(ctx, `DELETE FROM outbox WHERE published_at < $1`, now.Add(-r.Retention)); err != nil && ctx.Err() == nil {
					r.Log.Warn("outbox relay: retention delete", "err", err)
				}
			}
		}
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-r.wake:
				t.Stop()
			case <-t.C:
			}
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur == 0 {
		return time.Second
	}
	if cur *= 2; cur > max {
		return max
	}
	return cur
}

// relayBatch publishes up to Batch unpublished rows in order, stopping at the first failure.
func (r *Relay) relayBatch(ctx context.Context) (int, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, subject, payload FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT $1`, r.Batch)
	if err != nil {
		return 0, err
	}
	type row struct {
		id, subject string
		payload     []byte
	}
	var batch []row
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.id, &x.subject, &x.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, x)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	for i, x := range batch {
		if err := r.Sink.Publish(ctx, x.subject, x.id, x.payload); err != nil {
			return i, err
		}
		// A crash here republishes the row; the stream's de-duplication or the consumer's inbox drops the copy.
		if _, err := r.DB.ExecContext(ctx, `UPDATE outbox SET published_at = $2 WHERE id = $1`, x.id, r.Now().UTC()); err != nil {
			return i, err
		}
	}
	return len(batch), nil
}

// checkGeneration compares the stream's generation with the recorded one. A stream seen for the
// first time on this database records its generation and replays nothing.
func (r *Relay) checkGeneration(ctx context.Context) error {
	gen, err := r.Generation(ctx)
	if err != nil {
		return err
	}
	if gen == "" || gen == r.pendingGen {
		return nil
	}
	var recorded string
	err = r.DB.QueryRowContext(ctx, `SELECT generation FROM outbox_relay_state WHERE stream = $1`, r.Stream).Scan(&recorded)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return r.recordGeneration(ctx, gen)
	case err != nil:
		return err
	case recorded == gen:
		r.pendingGen = ""
		return nil
	}
	horizon := r.Now().Add(-r.Retention)
	res, err := r.DB.ExecContext(ctx, `UPDATE outbox SET published_at = NULL WHERE published_at IS NOT NULL AND created_at >= $1`, horizon)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	r.pendingGen = gen
	r.Log.Warn("outbox relay: bus generation changed; republishing the outbox", "stream", r.Stream, "from", recorded, "to", gen, "rows", n)
	r.Wake()
	return nil
}

func (r *Relay) recordGeneration(ctx context.Context, gen string) error {
	_, err := r.DB.ExecContext(ctx, `INSERT INTO outbox_relay_state (stream, generation, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (stream) DO UPDATE SET generation = excluded.generation, updated_at = excluded.updated_at`, r.Stream, gen)
	return err
}

// Pending returns the number of unpublished rows and the age of the oldest (alert: older than 60 s).
func Pending(ctx context.Context, db *sql.DB, now time.Time) (n int, oldest time.Duration, err error) {
	var t sql.NullTime
	err = db.QueryRowContext(ctx, `SELECT count(*), min(created_at) FROM outbox WHERE published_at IS NULL`).Scan(&n, &t)
	if err != nil {
		return 0, 0, err
	}
	if t.Valid {
		oldest = now.Sub(t.Time)
	}
	return n, oldest, nil
}
