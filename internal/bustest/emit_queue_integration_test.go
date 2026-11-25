//go:build integration

package bustest_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hostryhq/go-common/emit"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/internal/bustest"
	gsync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/go-common/testkit"
)

type fakeEmitter struct {
	mu    sync.Mutex
	calls [][]string // keys per call, per tenant in call order
	byTen map[string][]string
	fail  func(tenant string, ops []emit.Op) error
	seq   int64
}

func (f *fakeEmitter) Emit(_ context.Context, tenant ids.ID, ops []emit.Op) ([]emit.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byTen == nil {
		f.byTen = map[string][]string{}
	}
	keys := make([]string, len(ops))
	for i, o := range ops {
		keys[i] = o.Key
	}
	f.calls = append(f.calls, keys)
	if f.fail != nil {
		if err := f.fail(tenant.String(), ops); err != nil {
			return nil, err
		}
	}
	res := make([]emit.Result, len(ops))
	for i, o := range ops {
		f.seq++
		res[i] = emit.Result{Key: o.Key, Origin: "cloud-1", Stream: o.Stream, Seq: f.seq}
		f.byTen[tenant.String()] = append(f.byTen[tenant.String()], o.Key)
	}
	return res, nil
}

func op(key string) emit.Op {
	return emit.Op{Key: key, Stream: "brand:b1", Intent: "menu.tax_class.set", IntentV: 1, Actor: "service:catalog",
		Muts: []gsync.Mut{{E: "item", ID: "i-" + key, K: "set", F: map[string]any{"tax_class": "fnb_standard"}}}}
}

func queueDB(t *testing.T) *sql.DB {
	t.Helper()
	db := bustest.Shared(t).NewDB(t)
	if _, err := db.Exec(emit.QueueSchema); err != nil {
		t.Fatal(err)
	}
	return db
}

func states(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT key, state FROM emit_queue`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, s string
		if err := rows.Scan(&k, &s); err != nil {
			t.Fatal(err)
		}
		out[k] = s
	}
	return out
}

// An op is queued exactly when the caller's transaction commits, and a repeated key is one row.
func TestEmitQueueEnqueueIsTransactionalAndDeduplicated(t *testing.T) {
	db, ctx, clk := queueDB(t), context.Background(), testkit.NewClock()
	a := ids.ID("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60")
	tx, _ := db.BeginTx(ctx, nil)
	if err := emit.Enqueue(ctx, tx, clk.Now(), a, op("k1")); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if len(states(t, db)) != 0 {
		t.Fatal("rolled-back enqueue left a row")
	}
	tx, _ = db.BeginTx(ctx, nil)
	if err := emit.Enqueue(ctx, tx, clk.Now(), a, op("k1"), op("k1"), op("k2")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := states(t, db); len(got) != 2 {
		t.Fatalf("rows: %v", got)
	}
	if err := emit.Enqueue(ctx, db, clk.Now(), a, emit.Op{Key: "bad"}); !errors.Is(err, emit.ErrInvalid) {
		t.Fatalf("invalid op queued: %v", err)
	}
}

// While the gateway is down the queue waits with backoff and keeps order; when it is back the same
// keys go out in enqueue order and the rows are marked sent with the gateway's identity.
func TestEmitQueueWaitsRetriesAndKeepsOrderPerTenant(t *testing.T) {
	db, ctx, clk := queueDB(t), context.Background(), testkit.NewClock()
	ta, tb := ids.ID("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"), ids.ID("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a99")
	if err := emit.Enqueue(ctx, db, clk.Now(), ta, op("a1"), op("a2"), op("a3")); err != nil {
		t.Fatal(err)
	}
	if err := emit.Enqueue(ctx, db, clk.Now(), tb, op("b1")); err != nil {
		t.Fatal(err)
	}
	down := true
	f := &fakeEmitter{fail: func(string, []emit.Op) error {
		if down {
			return emit.NewUnavailable(errors.New("gateway down"), 10*time.Second)
		}
		return nil
	}}
	q := &emit.Queue{DB: db, Emitter: f, Clock: clk, BatchSize: 2, Jitter: func() float64 { return 0 }}

	sent, err := q.Drain(ctx)
	if err != nil || sent != 0 {
		t.Fatalf("down: sent=%d err=%v", sent, err)
	}
	callsWhileDown := len(f.calls)
	if pending, failed, _ := emit.Counts(ctx, db); pending != 4 || failed != 0 {
		t.Fatalf("counts pending=%d failed=%d", pending, failed)
	}
	// Not due yet: a second drain does not even call the gateway.
	if _, err := q.Drain(ctx); err != nil || len(f.calls) != callsWhileDown {
		t.Fatalf("drained before backoff elapsed: calls=%d", len(f.calls))
	}
	down = false
	clk.Advance(11 * time.Second) // Retry-After was 10s
	sent, err = q.Drain(ctx)
	if err != nil || sent != 4 {
		t.Fatalf("up: sent=%d err=%v", sent, err)
	}
	if got := f.byTen[ta.String()]; len(got) != 3 || got[0] != "a1" || got[1] != "a2" || got[2] != "a3" {
		t.Fatalf("tenant A order: %v", got)
	}
	if got := f.byTen[tb.String()]; len(got) != 1 || got[0] != "b1" {
		t.Fatalf("tenant B: %v", got)
	}
	for k, s := range states(t, db) {
		if s != emit.StateSent {
			t.Errorf("%s is %s", k, s)
		}
	}
	var origin string
	var seq int64
	if err := db.QueryRow(`SELECT origin, seq FROM emit_queue WHERE key='b1'`).Scan(&origin, &seq); err != nil || origin != "cloud-1" || seq < 1 {
		t.Fatalf("sent identity: %q %d %v", origin, seq, err)
	}
	// Nothing is sent twice.
	if sent, _ := q.Drain(ctx); sent != 0 {
		t.Fatalf("redrain sent %d", sent)
	}
}

// A refused batch is atomic at the gateway; the queue isolates the defective op, sends the rest, and
// never retries the failed one.
func TestEmitQueueIsolatesARefusedOp(t *testing.T) {
	db, ctx, clk := queueDB(t), context.Background(), testkit.NewClock()
	ta := ids.ID("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60")
	if err := emit.Enqueue(ctx, db, clk.Now(), ta, op("g1"), op("bad"), op("g2")); err != nil {
		t.Fatal(err)
	}
	f := &fakeEmitter{fail: func(_ string, ops []emit.Op) error {
		for _, o := range ops {
			if o.Key == "bad" {
				return &emit.RejectedError{Status: 422, Code: "emit.entity_not_allowed"}
			}
		}
		return nil
	}}
	q := &emit.Queue{DB: db, Emitter: f, Clock: clk}
	sent, err := q.Drain(ctx)
	if err != nil || sent != 2 {
		t.Fatalf("sent=%d err=%v", sent, err)
	}
	got := states(t, db)
	if got["g1"] != emit.StateSent || got["g2"] != emit.StateSent || got["bad"] != emit.StateFailed {
		t.Fatalf("states: %v", got)
	}
	var code string
	_ = db.QueryRow(`SELECT last_error FROM emit_queue WHERE key='bad'`).Scan(&code)
	if code != "emit.entity_not_allowed" {
		t.Fatalf("last_error = %q", code)
	}
	calls := len(f.calls)
	clk.Advance(time.Hour)
	if _, err := q.Drain(ctx); err != nil || len(f.calls) != calls {
		t.Fatal("a failed op was retried")
	}
	if _, failed, _ := emit.Counts(ctx, db); failed != 1 {
		t.Fatalf("failed count %d", failed)
	}
}
