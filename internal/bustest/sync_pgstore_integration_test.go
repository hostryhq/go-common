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
