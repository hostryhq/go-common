//go:build integration

package bustest_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hostryhq/go-common/idempotency"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/internal/bustest"
)

func setup(t *testing.T) *sql.DB {
	t.Helper()
	db := bustest.Shared(t).NewDB(t)
	if _, err := db.Exec(`CREATE TABLE widgets (id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func widgets(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM widgets`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func create(id string) func(tx *sql.Tx) (idempotency.TxResult, error) {
	return func(tx *sql.Tx) (idempotency.TxResult, error) {
		if _, err := tx.Exec(`INSERT INTO widgets VALUES ($1)`, id); err != nil {
			return idempotency.TxResult{}, err
		}
		return idempotency.TxResult{Response: idempotency.Response{Status: 201, ContentType: "application/json", Body: []byte(`{"id":"` + id + `"}`)}}, nil
	}
}

// A crash (or error) after the business write but before commit leaves neither the write nor a
// stuck claim: the retry runs again and succeeds. The old claim-outside-tx design returned 409 for ever.
func TestDoTxCrashBeforeCommitLeavesNoStuckKey(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	o := idempotency.PGOptions{}
	crash := errors.New("process died")
	_, _, err := idempotency.DoTx(ctx, db, o, tenant, "k", "fp", func(tx *sql.Tx) (idempotency.TxResult, error) {
		_, _ = tx.Exec(`INSERT INTO widgets VALUES ('w1')`)
		return idempotency.TxResult{}, crash
	})
	if !errors.Is(err, crash) || widgets(t, db) != 0 {
		t.Fatalf("err=%v widgets=%d", err, widgets(t, db))
	}
	r, replayed, err := idempotency.DoTx(ctx, db, o, tenant, "k", "fp", create("w1"))
	if err != nil || replayed || r.Status != 201 || widgets(t, db) != 1 {
		t.Fatalf("retry: %+v replayed=%v err=%v", r, replayed, err)
	}
	r, replayed, err = idempotency.DoTx(ctx, db, o, tenant, "k", "fp", create("w2"))
	if err != nil || !replayed || string(r.Body) != `{"id":"w1"}` || widgets(t, db) != 1 {
		t.Fatalf("replay: %+v replayed=%v err=%v", r, replayed, err)
	}
	if _, _, err = idempotency.DoTx(ctx, db, o, tenant, "k", "other", create("w3")); !errors.Is(err, idempotency.ErrMismatch) {
		t.Fatalf("mismatch err = %v", err)
	}
}

func TestDoTxConcurrentSameKeyRunsOnce(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	runs, replays := 0, 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replayed, err := idempotency.DoTx(ctx, db, idempotency.PGOptions{}, tenant, "k", "fp", func(tx *sql.Tx) (idempotency.TxResult, error) {
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				runs++
				mu.Unlock()
				return create("w1")(tx)
			})
			if err != nil {
				t.Error(err)
			}
			if replayed {
				mu.Lock()
				replays++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if runs != 1 || replays != 7 || widgets(t, db) != 1 {
		t.Fatalf("runs=%d replays=%d widgets=%d", runs, replays, widgets(t, db))
	}
}

func TestDoTxTenantIsolationAndExpiry(t *testing.T) {
	db, ctx := setup(t), context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := idempotency.PGOptions{Now: func() time.Time { return now }}
	a, b := ids.New(), ids.New()
	if _, _, err := idempotency.DoTx(ctx, db, o, a, "k", "fp", create("a1")); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := idempotency.DoTx(ctx, db, o, b, "k", "fp", create("b1")); err != nil || replayed {
		t.Fatalf("tenant B saw tenant A's key: replayed=%v err=%v", replayed, err)
	}
	now = now.Add(idempotency.DefaultTTL + time.Second)
	if _, replayed, err := idempotency.DoTx(ctx, db, o, a, "k", "fp", create("a2")); err != nil || replayed {
		t.Fatalf("after expiry: replayed=%v err=%v", replayed, err)
	}
	now = now.Add(idempotency.DefaultTTL + time.Second)
	n, err := idempotency.Sweep(ctx, db, o)
	if err != nil || n != 2 {
		t.Fatalf("sweep n=%d err=%v", n, err)
	}
}

func TestDoTxOmitBodyStoresFingerprintNotSecret(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	const secret = "SECRET-REFRESH-TOKEN"
	fn := func(tx *sql.Tx) (idempotency.TxResult, error) {
		return idempotency.TxResult{OmitBody: true, Response: idempotency.Response{Status: 200, ContentType: "application/json", Body: []byte(`{"refresh_token":"` + secret + `"}`)}}, nil
	}
	if r, _, err := idempotency.DoTx(ctx, db, idempotency.PGOptions{}, tenant, "k", "fp", fn); err != nil || !strings.Contains(string(r.Body), secret) {
		t.Fatalf("first response must still carry the secret: %v", err)
	}
	r, replayed, err := idempotency.DoTx(ctx, db, idempotency.PGOptions{}, tenant, "k", "fp", fn)
	if !replayed || !errors.Is(err, idempotency.ErrBodyOmitted) || r.Status != 200 || r.Body != nil {
		t.Fatalf("replay: %+v replayed=%v err=%v", r, replayed, err)
	}
	var leaked int
	if err := db.QueryRow(`SELECT count(*) FROM idempotency_records WHERE convert_from(coalesce(body,''::bytea),'UTF8') LIKE '%' || $1 || '%'`, secret).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("secret stored: %d %v", leaked, err)
	}
}

func TestPGStoreTakesOverStaleClaim(t *testing.T) {
	db, tenant, ctx := setup(t), ids.New(), context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &idempotency.PGStore{DB: db, Opt: idempotency.PGOptions{Now: func() time.Time { return now }}}
	if _, started, err := s.Begin(ctx, tenant, "k", "fp"); !started || err != nil {
		t.Fatal(started, err)
	}
	if _, _, err := s.Begin(ctx, tenant, "k", "fp"); !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("err = %v", err)
	}
	now = now.Add(idempotency.DefaultClaimTTL)
	if _, started, err := s.Begin(ctx, tenant, "k", "fp"); !started || err != nil {
		t.Fatalf("takeover: %v %v", started, err)
	}
	if err := s.Complete(ctx, tenant, "k", idempotency.Response{Status: 201}); err != nil {
		t.Fatal(err)
	}
	if r, started, err := s.Begin(ctx, tenant, "k", "fp"); started || err != nil || r.Status != 201 {
		t.Fatalf("replay: %+v %v %v", r, started, err)
	}
}
