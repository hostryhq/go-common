// Package restore is the one place a Hostry service learns that the platform database was
// restored (ADR-0002 Amendment A2.4, ADR-0006 H6). A restore always stops every service first, so
// the epoch is read once at start; no service reads it per request and none writes its own
// detection.
//
//   - Reader.Current returns the latest stamped epoch from platform_meta.restore_epoch over a
//     read-only connection that is opened for the read and closed after it.
//   - Every service database holds a one-row restore_state table (StateSchema). It is rolled back
//     with the database, so after a restore it differs from the stamped epoch.
//   - Guard compares the two before the service reports ready and before its outbox relay starts.
//     If they differ it runs the registered OnRestore hooks in order and then records the new
//     epoch. A failed hook keeps the service not ready (health /readyz fails) and is logged as an
//     alert.
//
// Hooks must be idempotent by their own bookkeeping: a crash between the hooks and the write of
// restore_state runs them again on the next start.
package restore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Genesis is the epoch id of an instance that was never restored (ADR-0006 H6).
const Genesis = "genesis"

// EpochSchema describes platform_meta.restore_epoch as hostry-restore creates it. Services never
// create or write it; it is exported for restoretest and for documentation.
const EpochSchema = `
CREATE TABLE IF NOT EXISTS restore_epoch (
  epoch_id         text        PRIMARY KEY,
  recovered_to     timestamptz,
  requested_target text        NOT NULL DEFAULT '',
  stamped_at       timestamptz NOT NULL,
  pg_timeline      bigint      NOT NULL DEFAULT 0,
  class            text        NOT NULL DEFAULT '',
  reason           text        NOT NULL DEFAULT ''
);
`

// StateSchema is the forward-only migration of the per-service restore_state table. Services copy
// it into repository/migrations. One row (id = 1) holds the epoch the service last handled.
const StateSchema = `
CREATE TABLE IF NOT EXISTS restore_state (
  id         integer     PRIMARY KEY CHECK (id = 1),
  epoch_id   text        NOT NULL,
  handled_at timestamptz NOT NULL
);
`

// Epoch is the latest stamped restore epoch.
type Epoch struct {
	EpochID     string
	RecoveredTo time.Time // zero for genesis
	StampedAt   time.Time
	Class       string
}

// ErrNotStamped is returned when platform_meta holds no epoch row: the preflight did not run.
var ErrNotStamped = errors.New("restore: no restore epoch is stamped")

// EpochSource gives the current epoch. *Reader implements it.
type EpochSource interface {
	Current(ctx context.Context) (Epoch, error)
}

// Reader reads platform_meta.restore_epoch once and remembers the answer.
type Reader struct {
	open func(ctx context.Context) (*sql.DB, error)
	mu   sync.Mutex
	got  *Epoch
}

// NewReader reads from the Postgres platform_meta database at dsn. The connection is read-only
// (default_transaction_read_only) and is closed after the read.
func NewReader(dsn string) *Reader {
	return NewReaderFunc(func(context.Context) (*sql.DB, error) {
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, fmt.Errorf("restore: bad platform_meta dsn: %w", err)
		}
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = map[string]string{}
		}
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
		return stdlib.OpenDB(*cfg), nil
	})
}

// NewReaderFunc reads through a database the caller opens (tests, SQLite). The reader closes it.
func NewReaderFunc(open func(ctx context.Context) (*sql.DB, error)) *Reader {
	return &Reader{open: open}
}

// Current returns the latest epoch. The database is read on the first successful call only.
func (r *Reader) Current(ctx context.Context) (Epoch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.got != nil {
		return *r.got, nil
	}
	db, err := r.open(ctx)
	if err != nil {
		return Epoch{}, err
	}
	defer db.Close()
	var id, class string
	var rec, stamped any
	err = db.QueryRowContext(ctx,
		`SELECT epoch_id, recovered_to, stamped_at, class FROM restore_epoch ORDER BY stamped_at DESC, epoch_id DESC LIMIT 1`).
		Scan(&id, &rec, &stamped, &class)
	if errors.Is(err, sql.ErrNoRows) {
		return Epoch{}, ErrNotStamped
	}
	if err != nil {
		return Epoch{}, fmt.Errorf("restore: read epoch: %w", err)
	}
	e := Epoch{EpochID: id, RecoveredTo: toTime(rec), StampedAt: toTime(stamped), Class: class}
	r.got = &e
	return e, nil
}

// StateStore holds the service's restore_state row.
type StateStore interface {
	// Load returns the epoch last handled; found is false for a database that never recorded one.
	Load(ctx context.Context) (epochID string, found bool, err error)
	Save(ctx context.Context, epochID string, at time.Time) error
}

// SQLState keeps restore_state in the service's own database.
type SQLState struct {
	DB       *sql.DB
	Postgres bool
}

// NewSQLState wraps db; postgres selects $n placeholders and native timestamps.
func NewSQLState(db *sql.DB, postgres bool) *SQLState { return &SQLState{DB: db, Postgres: postgres} }

// Load implements StateStore.
func (s *SQLState) Load(ctx context.Context) (string, bool, error) {
	var id string
	err := s.DB.QueryRowContext(ctx, `SELECT epoch_id FROM restore_state WHERE id = 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}

// Save implements StateStore.
func (s *SQLState) Save(ctx context.Context, epochID string, at time.Time) error {
	q := `INSERT INTO restore_state (id, epoch_id, handled_at) VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET epoch_id = excluded.epoch_id, handled_at = excluded.handled_at`
	var arg any = at.UTC().Format(time.RFC3339Nano)
	if s.Postgres {
		q = `INSERT INTO restore_state (id, epoch_id, handled_at) VALUES (1, $1, $2)
			ON CONFLICT (id) DO UPDATE SET epoch_id = excluded.epoch_id, handled_at = excluded.handled_at`
		arg = at.UTC()
	}
	_, err := s.DB.ExecContext(ctx, q, epochID, arg)
	return err
}

// Hook runs once per restore epoch. from is the epoch the service last handled.
type Hook func(ctx context.Context, from string, to Epoch) error

type namedHook struct {
	name string
	fn   Hook
}

// Guard runs the OnRestore hooks before a service reports ready.
type Guard struct {
	src EpochSource
	st  StateStore
	log *slog.Logger
	// Now stamps handled_at; defaults to time.Now.
	Now func() time.Time
	// Alert is called when a hook fails (in addition to the error log line).
	Alert func(hook string, err error)

	mu    sync.Mutex
	hooks []namedHook
	ready bool
	err   error
	epoch Epoch
	done  chan struct{}
}

// NewGuard builds a guard. log may be nil.
func NewGuard(src EpochSource, st StateStore, log *slog.Logger) *Guard {
	if log == nil {
		log = slog.Default()
	}
	return &Guard{src: src, st: st, log: log, Now: time.Now, done: make(chan struct{})}
}

// OnRestore registers a hook; hooks run in registration order. Register before Run.
func (g *Guard) OnRestore(name string, fn Hook) {
	g.mu.Lock()
	g.hooks = append(g.hooks, namedHook{name, fn})
	g.mu.Unlock()
}

// Run settles the epoch. A database with no restore_state row is new: the epoch is recorded and
// no hook runs. After a success Run does nothing; after a failure it may be called again and runs
// every hook again (hooks are idempotent by their own bookkeeping).
func (g *Guard) Run(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ready {
		return nil
	}
	ep, err := g.src.Current(ctx)
	if err != nil {
		g.err = err
		return err
	}
	prev, found, err := g.st.Load(ctx)
	if err != nil {
		g.err = err
		return err
	}
	if found && prev != ep.EpochID {
		for _, h := range g.hooks {
			if err := h.fn(ctx, prev, ep); err != nil {
				g.err = fmt.Errorf("restore hook %q: %w", h.name, err)
				g.log.Error("restore hook failed; service stays not ready", "alert", true, "hook", h.name, "from", prev, "to", ep.EpochID, "err", err.Error())
				if g.Alert != nil {
					g.Alert(h.name, err)
				}
				return g.err
			}
		}
		g.log.Info("restore epoch handled", "from", prev, "to", ep.EpochID)
	}
	if !found || prev != ep.EpochID {
		if err := g.st.Save(ctx, ep.EpochID, g.Now()); err != nil {
			g.err = err
			return err
		}
	}
	g.epoch, g.ready, g.err = ep, true, nil
	close(g.done)
	return nil
}

// Ready reports whether Run has finished successfully.
func (g *Guard) Ready() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.ready }

// Done is closed when the guard is ready; start the outbox relay after it.
func (g *Guard) Done() <-chan struct{} { return g.done }

// Epoch is the epoch the service runs under; valid once Ready.
func (g *Guard) Epoch() Epoch { g.mu.Lock(); defer g.mu.Unlock(); return g.epoch }

// Check is a health.Check: /readyz fails until the guard is ready.
func (g *Guard) Check(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ready {
		return nil
	}
	if g.err != nil {
		return g.err
	}
	return errors.New("restore guard has not finished")
}

func toTime(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x.UTC()
	case string:
		return parseTime(x)
	case []byte:
		return parseTime(string(x))
	}
	return time.Time{}
}

func parseTime(s string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
