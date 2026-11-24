// Package inbox is the consumer side of ADR-0011 B5: an idempotent subscriber whose handler
// effect and inbox insert commit in one transaction, with acknowledgement after commit,
// redelivery on failure and parking of poison events after the last attempt.
package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hostryhq/go-common/events"
)

// Schema is the forward-only migration for the inbox and parked-event tables.
const Schema = `
CREATE TABLE IF NOT EXISTS inbox (
  consumer     text        NOT NULL,
  event_id     uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE TABLE IF NOT EXISTS event_parked (
  id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  consumer    text        NOT NULL,
  event_id    text        NOT NULL,
  subject     text        NOT NULL,
  payload     bytea       NOT NULL,
  error       text        NOT NULL,
  attempts    bigint      NOT NULL,
  parked_at   timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);
`

// Delivery is one bus message. bus.Delivery satisfies it.
type Delivery interface {
	Subject() string
	Data() []byte
	NumDelivered() uint64
	Ack() error
	Nak(delay time.Duration) error
}

// Source yields deliveries. bus.Consumer satisfies it through an adapter in the caller.
type Source interface {
	// Next waits for one message; it returns ErrNoMessage on timeout.
	Next(ctx context.Context) (Delivery, error)
}

// ErrNoMessage is returned by Source.Next when nothing arrived in time.
var ErrNoMessage = errors.New("inbox: no message")

// Handler applies an event inside tx. The tenant context is already set on ctx from the
// envelope (B7). Returning an error rolls back and triggers redelivery. The handler must also
// tolerate out-of-order delivery (use key_seq; see Newer).
type Handler func(ctx context.Context, tx *sql.Tx, e events.Envelope) error

// Subscriber processes deliveries for one consumer name.
type Subscriber struct {
	DB       *sql.DB
	Consumer string
	Handle   Handler
	// MaxAttempts is the last attempt; after it the event is parked and acknowledged (default 10).
	MaxAttempts uint64
	// Backoff gives the redelivery delay after attempt n (1-based); default 1s doubling to 10m.
	Backoff func(attempt uint64) time.Duration
	// Retention of inbox rows (default 8 days, longer than the outbox horizon).
	Retention time.Duration
	// OnPark is the alert hook for a parked event.
	OnPark func(consumer, eventID, reason string)
	Log    *slog.Logger
	Now    func() time.Time
}

func (s *Subscriber) init() {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 10
	}
	if s.Backoff == nil {
		s.Backoff = DefaultBackoff
	}
	if s.Retention == 0 {
		s.Retention = 8 * 24 * time.Hour
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.Now == nil {
		s.Now = time.Now
	}
}

// DefaultBackoff is 1s, 2s, 4s ... capped at 10 minutes.
func DefaultBackoff(attempt uint64) time.Duration {
	d := time.Second
	for i := uint64(1); i < attempt && d < 10*time.Minute; i++ {
		d *= 2
	}
	if d > 10*time.Minute {
		d = 10 * time.Minute
	}
	return d
}

// Run processes messages from src until ctx ends.
func (s *Subscriber) Run(ctx context.Context, src Source) error {
	s.init()
	for ctx.Err() == nil {
		d, err := src.Next(ctx)
		if errors.Is(err, ErrNoMessage) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.Log.Warn("inbox: source error", "consumer", s.Consumer, "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if err := s.Process(ctx, d); err != nil {
			s.Log.Error("inbox: process", "consumer", s.Consumer, "err", err)
		}
	}
	return ctx.Err()
}

// Process handles one delivery: dedupe, apply, commit, then acknowledge (E2, E3, E9).
func (s *Subscriber) Process(ctx context.Context, d Delivery) error {
	s.init()
	env, err := events.Unmarshal(d.Data())
	if err == nil {
		err = env.Validate()
	}
	if err != nil {
		// Undecodable or invalid: retrying cannot help.
		return s.park(ctx, d, string(env.ID.String()), "invalid envelope: "+err.Error())
	}
	hctx := env.TenantContext(ctx)

	dup, err := s.apply(hctx, env)
	switch {
	case err == nil:
		_ = dup // a duplicate is acknowledged without effect
		return d.Ack()
	case d.NumDelivered() >= s.MaxAttempts:
		return s.park(ctx, d, env.ID.String(), err.Error())
	default:
		s.Log.Warn("inbox: handler failed; will redeliver", "consumer", s.Consumer, "event_id", env.ID, "attempt", d.NumDelivered())
		return d.Nak(s.Backoff(d.NumDelivered()))
	}
}

// apply runs the inbox insert and the handler in one transaction.
func (s *Subscriber) apply(ctx context.Context, env events.Envelope) (duplicate bool, err error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO inbox (consumer, event_id, processed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
		s.Consumer, env.ID.String(), s.Now().UTC())
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return true, nil
	}
	if err := s.Handle(ctx, tx, env); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

// park records the event in event_parked, fires the alert hook and acknowledges so the consumer moves on.
func (s *Subscriber) park(ctx context.Context, d Delivery, eventID, reason string) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO event_parked (consumer, event_id, subject, payload, error, attempts) VALUES ($1,$2,$3,$4,$5,$6)`,
		s.Consumer, eventID, d.Subject(), d.Data(), reason, d.NumDelivered())
	if err != nil {
		// Do not ack: the event must not be lost.
		return fmt.Errorf("inbox: park: %w", err)
	}
	s.Log.Error("inbox: event parked", "consumer", s.Consumer, "event_id", eventID, "reason", reason)
	if s.OnPark != nil {
		s.OnPark(s.Consumer, eventID, reason)
	}
	return d.Ack()
}

// Cleanup deletes inbox rows older than Retention.
func (s *Subscriber) Cleanup(ctx context.Context) error {
	s.init()
	_, err := s.DB.ExecContext(ctx, `DELETE FROM inbox WHERE processed_at < $1`, s.Now().Add(-s.Retention))
	return err
}

// Newer reports whether an event with keySeq should be applied given the highest key_seq
// already applied for its key: ignore if not newer (B5). Gap handling (hold) is the caller's.
func Newer(keySeq, applied int64) bool { return keySeq > applied }
