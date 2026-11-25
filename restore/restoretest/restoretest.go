// Package restoretest stamps a restore epoch in development and CI with the migrator role
// (ADR-0002 A2.4 item 7). In production only hostry-restore stamps; a service never can.
package restoretest

import (
	"context"
	"database/sql"
	"time"

	"github.com/hostryhq/go-common/restore"
)

const sqliteTime = "2006-01-02T15:04:05.000000000Z"

// Stamp creates restore_epoch if needed and inserts e as the newest epoch. dialect is "postgres" or
// "sqlite". A zero StampedAt is the time of the call; a StampedAt not after the newest existing
// row is moved just past it so that "latest" is always the row stamped last.
func Stamp(ctx context.Context, db *sql.DB, dialect string, e restore.Epoch) error {
	if _, err := db.ExecContext(ctx, restore.EpochSchema); err != nil {
		return err
	}
	if e.StampedAt.IsZero() {
		e.StampedAt = time.Now()
	}
	var newest any
	if err := db.QueryRowContext(ctx, `SELECT stamped_at FROM restore_epoch ORDER BY stamped_at DESC LIMIT 1`).Scan(&newest); err == nil {
		if prev := toTime(newest); !e.StampedAt.After(prev) {
			e.StampedAt = prev.Add(time.Millisecond)
		}
	}
	if e.Class == "" {
		e.Class = "test"
	}
	q := `INSERT INTO restore_epoch (epoch_id, recovered_to, stamped_at, class) VALUES (?, ?, ?, ?)`
	var rec, stamped any
	if dialect == "postgres" {
		q = `INSERT INTO restore_epoch (epoch_id, recovered_to, stamped_at, class) VALUES ($1, $2, $3, $4)`
		stamped = e.StampedAt.UTC()
		if !e.RecoveredTo.IsZero() {
			rec = e.RecoveredTo.UTC()
		}
	} else {
		stamped = e.StampedAt.UTC().Format(sqliteTime)
		if !e.RecoveredTo.IsZero() {
			rec = e.RecoveredTo.UTC().Format(sqliteTime)
		}
	}
	_, err := db.ExecContext(ctx, q, e.EpochID, rec, stamped, e.Class)
	return err
}

func toTime(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		t, _ := time.Parse(time.RFC3339Nano, x)
		return t
	case []byte:
		t, _ := time.Parse(time.RFC3339Nano, string(x))
		return t
	}
	return time.Time{}
}
