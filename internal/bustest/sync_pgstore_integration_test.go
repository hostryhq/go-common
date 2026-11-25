//go:build integration

package bustest_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	gids "github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/internal/bustest"
	gsync "github.com/hostryhq/go-common/sync"
)

const (
	sTenantA = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"
	sTenantB = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a99"
	sOutlet  = "outlet:0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a61"
)

func syncDB(t *testing.T) *sql.DB {
	t.Helper()
	db := bustest.Shared(t).NewDB(t)
	if _, err := db.Exec(gsync.Schema); err != nil {
		t.Fatal(err)
	}
	return db
}

func sop(tenant, origin string, seq int64, ms int64, muts ...gsync.Mut) *gsync.Op {
	return &gsync.Op{Ref: gsync.OpRef{Origin: origin, Stream: sOutlet, Seq: seq}, HLC: gsync.HLC{Ms: ms}.String(), TenantID: gids.ID(tenant), Muts: muts}
}

// The Postgres store gives the same rows as the reference MemStore, is idempotent on redelivery,
// applies a late void, keeps pii out of the projection (and erasable), and never mixes tenants.
func TestSyncPGStoreMatchesProjectionRules(t *testing.T) {
	db, ctx := syncDB(t), context.Background()
	reg, err := gsync.LoadRegistry(os.DirFS("../../sync/testdata"), "entities")
	if err != nil {
		t.Fatal(err)
	}
	pg := &gsync.Engine{Reg: reg, Service: "catalog", Store: gsync.PGStore{DB: db}}
	mem := &gsync.Engine{Reg: reg, Service: "catalog", Store: gsync.NewMemStore()}

	ins := gsync.Mut{E: "item", ID: "i1", K: gsync.KindInsert, F: map[string]any{"name_en": "Dosa", "sort_order": int64(3), "tax_class": "fnb_5"}}
	edit := gsync.Mut{E: "item", ID: "i1", K: gsync.KindSet, F: map[string]any{"name_en": "Masala Dosa"}}
	bad := gsync.Mut{E: "item", ID: "i1", K: gsync.KindSet, F: map[string]any{"name_en": "refused"}}
	other := gsync.Mut{E: "item", ID: "i1", K: gsync.KindInsert, F: map[string]any{"name_en": "Tenant B item"}}
	badOp := sop(sTenantA, "dev-b", 9, 300, bad)
	void := &gsync.Op{Ref: gsync.OpRef{Origin: "cloud-1", Stream: sOutlet, Seq: 1}, HLC: gsync.HLC{Ms: 400}.String(), TenantID: gids.ID(sTenantA), Voids: []gsync.OpRef{badOp.Ref}}
	ops := []*gsync.Op{sop(sTenantA, "dev-a", 1, 100, ins), sop(sTenantA, "dev-a", 2, 200, edit), badOp, sop(sTenantB, "dev-a", 1, 100, other), void}
	for _, e := range []*gsync.Engine{pg, mem} {
		for _, i := range []int{4, 2, 0, 1, 3, 0, 2, 4, 1} { // shuffled, with redelivery
			if _, err := e.Apply(ctx, ops[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	keyA, keyB := gsync.RowKey{Tenant: sTenantA, Entity: "item", ID: "i1"}, gsync.RowKey{Tenant: sTenantB, Entity: "item", ID: "i1"}
	for _, k := range []gsync.RowKey{keyA, keyB} {
		p, err := pg.Row(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		m, _ := mem.Row(ctx, k)
		if !sameRow(p, m) {
			t.Fatalf("pg %+v differs from mem %+v", p, m)
		}
	}
	a, _ := pg.Row(ctx, keyA)
	b, _ := pg.Row(ctx, keyB)
	if a.Fields["name_en"] != "Masala Dosa" || a.Fields["sort_order"] != int64(3) || b.Fields["name_en"] != "Tenant B item" {
		t.Fatalf("a=%+v b=%+v", a.Fields, b.Fields)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM sync_candidates WHERE tenant_id=$1`, sTenantA).Scan(&n)
	if n != 3 { // insert, edit, voided edit: redelivery added nothing
		t.Fatalf("candidates for tenant A = %d, want 3", n)
	}

	// pii: stored for the owner in the side table, erasable per row, never in the projection.
	piiOp := sop(sTenantA, "dev-a", 5, 500, gsync.Mut{E: "check", ID: "c1", K: gsync.KindInsert, F: map[string]any{}})
	piiOp.PII = map[string]map[string]map[string]any{"check": {"c1": {"customer_name": "A. Guest"}}}
	orders := &gsync.Engine{Reg: reg, Service: "orders", Store: gsync.PGStore{DB: db}}
	res, err := orders.Apply(ctx, piiOp)
	if err != nil || len(res.PII) != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	st := gsync.PGStore{DB: db}
	for i := 0; i < 2; i++ { // idempotent
		if err := st.PutPII(ctx, res.PII[0]); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.QueryRow(`SELECT count(*) FROM sync_pii`).Scan(&n)
	var leaked int
	_ = db.QueryRow(`SELECT count(*) FROM sync_candidates WHERE fields::text LIKE '%Guest%'`).Scan(&leaked)
	if n != 1 || leaked != 0 {
		t.Fatalf("pii rows=%d leaked into candidates=%d", n, leaked)
	}
	if gone, _ := st.ErasePII(ctx, gsync.RowKey{Tenant: sTenantB, Entity: "check", ID: "c1"}); gone != 0 {
		t.Fatal("erase crossed tenants")
	}
	if gone, err := st.ErasePII(ctx, res.PII[0].Key); err != nil || gone != 1 {
		t.Fatalf("erase: %d %v", gone, err)
	}
}

// A pii erasure op removes values and salts from sync_pii and keeps the commitments, whichever
// of the erasure and the op arrives first; the value is nowhere in the database afterwards.
func TestSyncPGStoreErasureKeepsCommitmentsOnly(t *testing.T) {
	db, ctx := syncDB(t), context.Background()
	reg, err := gsync.LoadRegistry(os.DirFS("../../sync/testdata"), "entities")
	if err != nil {
		t.Fatal(err)
	}
	orders := &gsync.Engine{Reg: reg, Service: "orders", Store: gsync.PGStore{DB: db}}
	mkPII := func(seq int64, row string) *gsync.Op {
		o := sop(sTenantA, "dev-a", seq, 100*seq, gsync.Mut{E: "check", ID: row, K: gsync.KindInsert, F: map[string]any{}})
		o.PII = map[string]map[string]map[string]any{"check": {row: {"customer_name": "Unique Erasure Guest"}}}
		o.PIISalts = map[string]map[string]map[string]string{"check": {row: {"customer_name": "c2FsdHNhbHRzYWx0c2FsdA"}}}
		o.PIICommits = gsync.PIIC{"check": {row: {"customer_name": "00ff"}}}
		return o
	}
	erase := func(target gsync.OpRef) *gsync.Op {
		return &gsync.Op{Ref: gsync.OpRef{Origin: "cloud-1", Stream: sOutlet, Seq: 50 + target.Seq}, HLC: gsync.HLC{Ms: 900}.String(), TenantID: gids.ID(sTenantA), PIIErase: []gsync.OpRef{target}}
	}
	first, second := mkPII(1, "c1"), mkPII(2, "c2")
	for _, o := range []*gsync.Op{first, erase(first.Ref), erase(second.Ref), second, first} { // second: erase before op; first redelivered
		if _, err := orders.Apply(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	var rows, withValue, withSalt, withCommit, leaked int
	_ = db.QueryRow(`SELECT count(*) FROM sync_pii`).Scan(&rows)
	_ = db.QueryRow(`SELECT count(*) FROM sync_pii WHERE fields::text LIKE '%Erasure Guest%'`).Scan(&withValue)
	_ = db.QueryRow(`SELECT count(*) FROM sync_pii WHERE salts::text LIKE '%c2FsdHNhbHRz%'`).Scan(&withSalt)
	_ = db.QueryRow(`SELECT count(*) FROM sync_pii WHERE commits::text LIKE '%00ff%'`).Scan(&withCommit)
	_ = db.QueryRow(`SELECT count(*) FROM sync_candidates WHERE fields::text LIKE '%Erasure Guest%'`).Scan(&leaked)
	// first was stored, then erased (commitment kept); second arrived after its erasure and was never stored.
	if rows != 1 || withValue != 0 || withSalt != 0 || withCommit != 1 || leaked != 0 {
		t.Fatalf("rows=%d value=%d salt=%d commit=%d candidates=%d", rows, withValue, withSalt, withCommit, leaked)
	}
}

func sameRow(a, b gsync.Row) bool {
	if a.Exists != b.Exists || a.Deleted != b.Deleted || len(a.Fields) != len(b.Fields) || len(a.Conflicts) != len(b.Conflicts) {
		return false
	}
	for k, v := range a.Fields {
		if b.Fields[k] != v {
			return false
		}
	}
	return true
}
