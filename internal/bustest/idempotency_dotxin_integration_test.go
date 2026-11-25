//go:build integration

package bustest_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/hostryhq/go-common/idempotency"
	"github.com/hostryhq/go-common/ids"
)

// DoTxIn runs inside the caller's tx: the claim, the write and the result commit or roll back with
// that tx, and a replay in a later tx returns the stored result without running fn.
func TestDoTxInCommitsAndRollsBackWithCallerTx(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	o := idempotency.PGOptions{}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := idempotency.DoTxIn(ctx, tx, o, tenant, "k", "fp", create("w1")); err != nil || replayed {
		t.Fatalf("first: replayed=%v err=%v", replayed, err)
	}
	if err := tx.Rollback(); err != nil { // the caller (audited tx) fails later: nothing may remain
		t.Fatal(err)
	}
	if widgets(t, db) != 0 {
		t.Fatal("write survived the caller's rollback")
	}

	run := func(key, fp string, fn func(*sql.Tx) (idempotency.TxResult, error)) (idempotency.Response, bool, error) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		r, replayed, err := idempotency.DoTxIn(ctx, tx, o, tenant, key, fp, fn)
		if err == nil || errors.Is(err, idempotency.ErrBodyOmitted) {
			if cerr := tx.Commit(); cerr != nil {
				t.Fatal(cerr)
			}
		}
		return r, replayed, err
	}
	if r, replayed, err := run("k", "fp", create("w1")); err != nil || replayed || r.Status != 201 {
		t.Fatalf("retry after rollback: %+v replayed=%v err=%v", r, replayed, err)
	}
	r, replayed, err := run("k", "fp", create("w2"))
	if err != nil || !replayed || string(r.Body) != `{"id":"w1"}` || widgets(t, db) != 1 {
		t.Fatalf("replay: %+v replayed=%v err=%v", r, replayed, err)
	}
	if _, _, err := run("k", "other", create("w3")); !errors.Is(err, idempotency.ErrMismatch) {
		t.Fatalf("mismatch err = %v", err)
	}
	// Another tenant never sees the key.
	tx, _ = db.BeginTx(ctx, nil)
	defer func() { _ = tx.Rollback() }()
	if _, replayed, err := idempotency.DoTxIn(ctx, tx, o, ids.New(), "k", "fp", create("b1")); err != nil || replayed {
		t.Fatalf("tenant isolation: replayed=%v err=%v", replayed, err)
	}
}

// A concurrent request with the same key waits for the first tx and then replays it.
func TestDoTxInConcurrentSameKeyRunsOnce(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	o := idempotency.PGOptions{}
	tx1, _ := db.BeginTx(ctx, nil)
	if _, _, err := idempotency.DoTxIn(ctx, tx1, o, tenant, "k", "fp", create("w1")); err != nil {
		t.Fatal(err)
	}
	type out struct {
		replayed bool
		err      error
	}
	done := make(chan out, 1)
	go func() {
		tx2, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx2.Rollback() }()
		_, replayed, err := idempotency.DoTxIn(ctx, tx2, o, tenant, "k", "fp", create("w2"))
		if err == nil {
			err = tx2.Commit()
		}
		done <- out{replayed, err}
	}()
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := <-done; got.err != nil || !got.replayed || widgets(t, db) != 1 {
		t.Fatalf("second: %+v widgets=%d", got, widgets(t, db))
	}
}

// When fn fails (even with a failed statement that aborts the tx), the claim and fn's writes are
// undone to the savepoint, the caller can still commit its own work (an audit row), and the key is free.
func TestDoTxInFnErrorFreesKeyAndTxStaysUsable(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	o := idempotency.PGOptions{}
	if _, err := db.Exec(`CREATE TABLE audit (n int)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = idempotency.DoTxIn(ctx, tx, o, tenant, "k", "fp", func(tx *sql.Tx) (idempotency.TxResult, error) {
		_, _ = tx.Exec(`INSERT INTO widgets VALUES ('w1')`)
		_, e := tx.Exec(`INSERT INTO widgets VALUES ('w1')`) // duplicate key: aborts the tx unless rolled back to a savepoint
		return idempotency.TxResult{}, e
	})
	if err == nil {
		t.Fatal("expected fn error")
	}
	if _, err := tx.Exec(`INSERT INTO audit VALUES (1)`); err != nil {
		t.Fatalf("tx unusable after fn error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var claims, audits int
	_ = db.QueryRow(`SELECT count(*) FROM idempotency_records`).Scan(&claims)
	_ = db.QueryRow(`SELECT count(*) FROM audit`).Scan(&audits)
	if claims != 0 || widgets(t, db) != 0 || audits != 1 {
		t.Fatalf("claims=%d widgets=%d audits=%d", claims, widgets(t, db), audits)
	}
}
