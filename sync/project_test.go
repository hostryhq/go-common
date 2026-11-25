package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	gosync "sync"
	"testing"
)

var (
	regOnce gosync.Once
	regVal  *Registry
	regErr  error
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	regOnce.Do(func() { regVal, regErr = LoadRegistry(os.DirFS("testdata"), "entities") })
	if regErr != nil {
		t.Fatal(regErr)
	}
	return regVal
}

func TestRegistryOwnersAndReplicas(t *testing.T) {
	r := testRegistry(t)
	cases := []struct {
		service, entity string
		want            Role
	}{
		{"catalog", "item", RoleOwner},
		{"orders", "item", RoleNone},
		{"orders", "check", RoleOwner},
		{"billing", "check", RoleNone},
		{"orders", "station", RoleReplica}, // replicated_to
		{"orders", "dining_table", RoleReplica},
		{"orders", "day_close", RoleReplica},
		{"catalog", "station", RoleOwner},
		{"billing", "station", RoleNone},
		{"orders", "no_such_entity", RoleNone},
	}
	for _, c := range cases {
		if got := r.RoleOf(c.service, c.entity); got != c.want {
			t.Errorf("RoleOf(%s,%s)=%d want %d", c.service, c.entity, got, c.want)
		}
	}
	for _, e := range r.Entities {
		if len(e.ReplicatedTo) > 0 {
			for _, f := range e.Fields {
				if f.PII {
					t.Errorf("replicated entity %s has pii field %s", e.Name, f.Name)
				}
			}
		}
	}
	if f := r.Entities["check"].Fields["status"]; f.Policy != PolicyRank || len(f.Order) != 3 {
		t.Errorf("check.status: %+v", f)
	}
}

func TestUUIDv5IsDeterministicAndVersioned(t *testing.T) {
	a, b := UUIDv5("item_outlet|o|i"), UUIDv5("item_outlet|o|i")
	if a != b || a == UUIDv5("item_outlet|o|j") {
		t.Fatal("not deterministic or not name-sensitive")
	}
	if a[14] != '5' || (a[19] != '8' && a[19] != '9' && a[19] != 'a' && a[19] != 'b') {
		t.Fatalf("not a v5 uuid: %s", a)
	}
}

// --- projection tests ---

const (
	ra = "dev-a"
	rb = "dev-b"
)

func hlc(ms int64) string { return HLC{Ms: ms}.String() }

func mkOp(origin string, seq int64, ms int64, muts ...Mut) *Op {
	return &Op{
		Ref: OpRef{Origin: origin, Stream: "outlet:" + tOutlet, Seq: seq}, HLC: hlc(ms), TenantID: tTenant,
		Intent: "x.y", IntentV: 1, Muts: muts,
	}
}

func apply(t *testing.T, e *Engine, ops ...*Op) Result {
	t.Helper()
	var last Result
	for _, op := range ops {
		var err error
		if last, err = e.Apply(context.Background(), op); err != nil {
			t.Fatal(err)
		}
	}
	return last
}

func row(t *testing.T, e *Engine, entity, id string) Row {
	t.Helper()
	r, err := e.Row(context.Background(), RowKey{Tenant: tTenant, Entity: entity, ID: id})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newEngine(t *testing.T, service string) *Engine {
	return &Engine{Reg: testRegistry(t), Service: service, Store: NewMemStore()}
}

func TestLWWHighestStampWinsPerGroup(t *testing.T) {
	e := newEngine(t, "catalog")
	ins := Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "Dosa", "status": "active", "tax_class": "fnb_5"}}
	apply(t, e,
		mkOp(ra, 1, 100, ins),
		mkOp(rb, 1, 300, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "Masala Dosa"}}),
		mkOp(ra, 2, 200, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "Plain Dosa"}}),
	)
	r := row(t, e, "item", "i1")
	if !r.Exists || r.Fields["name_en"] != "Masala Dosa" || r.Fields["status"] != "active" {
		t.Fatalf("row: %+v", r)
	}
}

func TestSameHLCTieBreaksByOriginThenSeq(t *testing.T) {
	e := newEngine(t, "catalog")
	apply(t, e,
		mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "x"}}),
		mkOp(rb, 1, 500, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "from-b"}}),
		mkOp(ra, 2, 500, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "from-a"}}),
	)
	if got := row(t, e, "item", "i1").Fields["name_en"]; got != "from-b" { // "dev-b" > "dev-a"
		t.Fatalf("got %v", got)
	}
}

func TestLWWReviewKeepsWinnerAndRetainsLoser(t *testing.T) {
	e := newEngine(t, "catalog")
	ins := Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "Dosa", "tax_class": "fnb_5"}}
	seen := hlc(100)
	apply(t, e, mkOp(ra, 1, 100, ins))
	// Two devices edit the tax class concurrently; both saw the insert.
	apply(t, e,
		mkOp(ra, 2, 200, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"tax_class": "fnb_12"}, Prev: map[string]string{"tax_class": seen}}),
		mkOp(rb, 1, 250, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"tax_class": "fnb_18"}, Prev: map[string]string{"tax_class": seen}}),
	)
	r := row(t, e, "item", "i1")
	if r.Fields["tax_class"] != "fnb_18" || len(r.Conflicts) != 1 {
		t.Fatalf("row: %+v", r)
	}
	c := r.Conflicts[0]
	if c.Group != "tax" || c.LoserValues["tax_class"] != "fnb_12" || c.Winner.Origin != rb || c.Loser.Origin != ra {
		t.Fatalf("conflict: %+v", c)
	}
	// An edit made after seeing the winner is not a conflict.
	apply(t, e, mkOp(ra, 3, 400, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"tax_class": "fnb_5"}, Prev: map[string]string{"tax_class": hlc(250)}}))
	r = row(t, e, "item", "i1")
	if r.Fields["tax_class"] != "fnb_5" || len(r.Conflicts) != 1 {
		t.Fatalf("after informed edit: %+v", r)
	}
}

func TestDeltaSumsAndIsIdempotent(t *testing.T) {
	e := newEngine(t, "orders")
	ins := Mut{E: "order_line", ID: "l1", K: KindInsert, F: map[string]any{"qty_milli": int64(1000), "name": "Dosa"}}
	d := func(n int64) Mut {
		return Mut{E: "order_line", ID: "l1", K: KindDelta, F: map[string]any{"qty_milli": n}}
	}
	o1, o2 := mkOp(ra, 1, 100, ins), mkOp(rb, 1, 110, d(2000))
	apply(t, e, o1, o2, o2, o1, mkOp(ra, 2, 120, d(-1000)))
	if got := row(t, e, "order_line", "l1").Fields["qty_milli"]; got != int64(2000) {
		t.Fatalf("qty = %v, want 2000 (replays must not double count)", got)
	}
}

func TestRankIsForwardOnly(t *testing.T) {
	e := newEngine(t, "orders")
	rk := func(v string) Mut { return Mut{E: "check", ID: "c1", K: KindRank, F: map[string]any{"status": v}} }
	apply(t, e,
		mkOp(ra, 1, 100, Mut{E: "check", ID: "c1", K: KindInsert, F: map[string]any{"order_type": "dine_in", "business_date": "2026-11-24", "status": "open"}}),
		mkOp(ra, 2, 300, rk("billed")),
		mkOp(rb, 1, 400, rk("open")), // later stamp, lower rank: never moves backward
		mkOp(rb, 2, 410, rk("not_a_status")),
	)
	if got := row(t, e, "check", "c1").Fields["status"]; got != "billed" {
		t.Fatalf("status = %v", got)
	}
}

func TestTombstoneDelUndelByStamp(t *testing.T) {
	e := newEngine(t, "catalog")
	apply(t, e,
		mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "x"}}),
		mkOp(ra, 2, 200, Mut{E: "item", ID: "i1", K: KindDel}),
	)
	if r := row(t, e, "item", "i1"); !r.Exists || !r.Deleted {
		t.Fatalf("after del: %+v", r)
	}
	apply(t, e, mkOp(rb, 1, 300, Mut{E: "item", ID: "i1", K: KindUndel}))
	if r := row(t, e, "item", "i1"); r.Deleted {
		t.Fatalf("after undel: %+v", r)
	}
	// del on an entity without tombstone is refused, not applied.
	res := apply(t, newEngine(t, "orders"), mkOp(ra, 1, 100, Mut{E: "check", ID: "c", K: KindDel}))
	if len(res.Skipped) != 1 || len(res.Rows) != 0 {
		t.Fatalf("res: %+v", res)
	}
}

func TestPolicyViolationsAreSkippedNotApplied(t *testing.T) {
	e := newEngine(t, "catalog")
	res := apply(t, e,
		mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "x", "future_field": "ok"}}),
		mkOp(ra, 2, 200, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "y", "sort_order": "not-an-int", "category_id": int64(1)}}),
	)
	if len(res.Skipped) != 2 {
		t.Fatalf("skipped: %+v", res.Skipped)
	}
	r := row(t, e, "item", "i1")
	if r.Fields["name_en"] != "y" || r.Fields["future_field"] != nil || r.Fields["sort_order"] != nil {
		t.Fatalf("row: %+v", r)
	}
	// set may not touch an immutable field.
	res = apply(t, e, mkOp(ra, 3, 300, Mut{E: "station", ID: "s1", K: KindInsert, F: map[string]any{"code": "K1"}}),
		mkOp(ra, 4, 400, Mut{E: "station", ID: "s1", K: KindSet, F: map[string]any{"code": "K2"}}))
	if r := row(t, e, "station", "s1"); r.Fields["code"] != "K1" || len(res.Skipped) != 1 {
		t.Fatalf("immutable overwritten: %+v skipped=%+v", r, res.Skipped)
	}
}

func TestOwnedReplicaAndForeignEntities(t *testing.T) {
	ops := []*Op{
		mkOp(ra, 1, 100,
			Mut{E: "station", ID: "s1", K: KindInsert, F: map[string]any{"code": "K1", "name": "Kitchen"}},
			Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "x"}},
			Mut{E: "bill", ID: "b1", K: KindInsert, F: map[string]any{}},
		),
	}
	orders := newEngine(t, "orders")
	res := apply(t, orders, ops...)
	if len(res.Rows) != 1 || res.Rows[0].Key.Entity != "station" || res.Ignored != 2 {
		t.Fatalf("orders should hold only the station replica: %+v", res)
	}
	if r := row(t, orders, "station", "s1"); r.Fields["name"] != "Kitchen" {
		t.Fatalf("replica not applied: %+v", r)
	}
	billing := newEngine(t, "billing")
	if res := apply(t, billing, ops...); len(res.Rows) != 1 || res.Rows[0].Key.Entity != "bill" {
		t.Fatalf("billing: %+v", res)
	}
}

func TestPIIOnlyForOwnedEntities(t *testing.T) {
	op := mkOp(ra, 1, 100, Mut{E: "check", ID: "c1", K: KindInsert, F: map[string]any{
		"order_type": "takeaway", "customer_name": "must-not-enter-the-projection"}})
	op.PII = map[string]map[string]map[string]any{"check": {"c1": {"customer_name": "A. Guest", "order_type": "not-pii"}}}
	owner := newEngine(t, "orders")
	res := apply(t, owner, op)
	if len(res.PII) != 1 || res.PII[0].Fields["customer_name"] != "A. Guest" || len(res.PII[0].Fields) != 1 {
		t.Fatalf("pii: %+v", res.PII)
	}
	if v, ok := row(t, owner, "check", "c1").Fields["customer_name"]; ok {
		t.Fatalf("pii field leaked into the projection: %v", v)
	}
	// A service that owns nothing of "check" returns no pii, and neither does a replica.
	if res := apply(t, newEngine(t, "billing"), op); len(res.PII) != 0 {
		t.Fatalf("non-owner got pii: %+v", res.PII)
	}
	rep := mkOp(ra, 1, 100, Mut{E: "station", ID: "s1", K: KindInsert, F: map[string]any{"code": "K"}})
	rep.PII = map[string]map[string]map[string]any{"station": {"s1": {"name": "x"}}}
	if res := apply(t, newEngine(t, "orders"), rep); len(res.PII) != 0 {
		t.Fatalf("replica got pii: %+v", res.PII)
	}
}

func TestVoidRemovesAnOpWhetherItArrivesBeforeOrAfter(t *testing.T) {
	bad := mkOp(rb, 7, 300, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "refused edit"}})
	base := mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "Dosa"}})
	void := &Op{Ref: OpRef{Origin: "cloud-1", Stream: "outlet:" + tOutlet, Seq: 1}, HLC: hlc(400), TenantID: tTenant, Intent: VoidIntent, Voids: []OpRef{bad.Ref}}

	for name, order := range map[string][]*Op{"after": {base, bad, void}, "before": {void, bad, base}, "between": {base, void, bad}} {
		e := newEngine(t, "catalog")
		res := apply(t, e, order...)
		r := row(t, e, "item", "i1")
		if r.Fields["name_en"] != "Dosa" {
			t.Errorf("%s: voided edit still applies: %+v", name, r)
		}
		_ = res
	}
	// The void reports the rows it exposed.
	e := newEngine(t, "catalog")
	apply(t, e, base, bad)
	if res := apply(t, e, void); len(res.Rows) != 1 || res.Rows[0].Fields["name_en"] != "Dosa" || len(res.Voided) != 1 {
		t.Fatalf("void result: %+v", res)
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEngine(t, "catalog")
	a := mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "tenant A"}})
	b := mkOp(ra, 1, 100, Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "tenant B"}})
	b.TenantID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a99"
	apply(t, e, a, b)
	if got := row(t, e, "item", "i1").Fields["name_en"]; got != "tenant A" {
		t.Fatalf("tenant A sees %v", got)
	}
	rb, _ := e.Row(context.Background(), RowKey{Tenant: string(b.TenantID), Entity: "item", ID: "i1"})
	if rb.Fields["name_en"] != "tenant B" {
		t.Fatalf("tenant B sees %v", rb.Fields["name_en"])
	}
	// A void in one tenant does not touch the other.
	v := &Op{Ref: OpRef{Origin: "cloud-1", Stream: "s", Seq: 1}, HLC: hlc(1), TenantID: b.TenantID, Intent: VoidIntent, Voids: []OpRef{a.Ref}}
	apply(t, e, v)
	if got := row(t, e, "item", "i1").Fields["name_en"]; got != "tenant A" {
		t.Fatalf("cross-tenant void: %v", got)
	}
}

// The architect's requirement: any arrival order, with duplicates, gives the same rows. A mixed
// workload over catalog (lww, lww_review, tombstone), orders (delta, rank, immutable, replicas) and
// a void is delivered in many shuffles, each with random redeliveries, to fresh stores.
func TestShuffledDeliveryConverges(t *testing.T) {
	seen := hlc(100)
	void1 := &Op{Ref: OpRef{Origin: "cloud-1", Stream: "outlet:" + tOutlet, Seq: 1}, HLC: hlc(900), TenantID: tTenant, Intent: VoidIntent,
		Voids: []OpRef{{Origin: rb, Stream: "outlet:" + tOutlet, Seq: 3}}}
	ops := []*Op{
		mkOp(ra, 1, 100,
			Mut{E: "item", ID: "i1", K: KindInsert, F: map[string]any{"name_en": "Dosa", "tax_class": "fnb_5", "status": "active"}},
			Mut{E: "station", ID: "s1", K: KindInsert, F: map[string]any{"code": "K1", "name": "Kitchen"}},
			Mut{E: "check", ID: "c1", K: KindInsert, F: map[string]any{"order_type": "dine_in", "business_date": "2026-11-24", "status": "open"}},
			Mut{E: "order_line", ID: "l1", K: KindInsert, F: map[string]any{"qty_milli": int64(1000), "name": "Dosa", "check_id": "c1"}}),
		mkOp(ra, 2, 200, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "Masala Dosa"}},
			Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"tax_class": "fnb_12"}, Prev: map[string]string{"tax_class": seen}}),
		mkOp(rb, 1, 210, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"tax_class": "fnb_18"}, Prev: map[string]string{"tax_class": seen}}),
		mkOp(rb, 2, 220, Mut{E: "item", ID: "i1", K: KindDel}),
		mkOp(rb, 3, 230, Mut{E: "item", ID: "i1", K: KindSet, F: map[string]any{"name_en": "VOIDED"}}),
		mkOp(ra, 3, 240, Mut{E: "item", ID: "i1", K: KindUndel}),
		mkOp(ra, 4, 250, Mut{E: "order_line", ID: "l1", K: KindDelta, F: map[string]any{"qty_milli": int64(2000)}}),
		mkOp(rb, 4, 251, Mut{E: "order_line", ID: "l1", K: KindDelta, F: map[string]any{"qty_milli": int64(-500)}}),
		mkOp(rb, 5, 260, Mut{E: "order_line", ID: "l1", K: KindSet, F: map[string]any{"check_id": "c2"}}),
		mkOp(ra, 5, 261, Mut{E: "order_line", ID: "l1", K: KindSet, F: map[string]any{"check_id": "c3"}}),
		mkOp(ra, 6, 300, Mut{E: "check", ID: "c1", K: KindRank, F: map[string]any{"status": "billed"}}),
		mkOp(rb, 6, 310, Mut{E: "check", ID: "c1", K: KindRank, F: map[string]any{"status": "open"}}),
		mkOp(rb, 7, 320, Mut{E: "station", ID: "s1", K: KindSet, F: map[string]any{"name": "Main kitchen"}}),
		// a second, concurrent insert of the same deterministic row with the same immutable values
		mkOp(rb, 8, 105, Mut{E: "item_outlet", ID: "io1", K: KindInsert, F: map[string]any{"item_id": "i1", "available": true}}),
		mkOp(ra, 7, 106, Mut{E: "item_outlet", ID: "io1", K: KindInsert, F: map[string]any{"item_id": "i1", "available": false}}),
		void1,
	}
	snapshot := func(service string, order []*Op) string {
		e := newEngine(t, service)
		for _, op := range order {
			apply(t, e, op)
		}
		out := map[string]Row{}
		for _, k := range []RowKey{{tTenant, "item", "i1"}, {tTenant, "station", "s1"}, {tTenant, "check", "c1"}, {tTenant, "order_line", "l1"}, {tTenant, "item_outlet", "io1"}} {
			if e.Reg.RoleOf(service, k.Entity) != RoleNone {
				out[k.Entity+"/"+k.ID] = row(t, e, k.Entity, k.ID)
			}
		}
		b, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for _, service := range []string{"catalog", "orders", "billing"} {
		want := snapshot(service, ops)
		if service == "catalog" {
			var m map[string]Row
			_ = json.Unmarshal([]byte(want), &m)
			it := m["item/i1"]
			if it.Fields["name_en"] != "Masala Dosa" || it.Deleted || len(it.Conflicts) != 1 {
				t.Fatalf("catalog baseline wrong (void or tombstone or conflict): %s", want)
			}
		}
		if service == "orders" {
			var m map[string]Row
			_ = json.Unmarshal([]byte(want), &m)
			if q := m["order_line/l1"].Fields["qty_milli"]; q != float64(2500) { // json float after round trip
				t.Fatalf("orders baseline wrong: %s", want)
			}
		}
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 200; i++ {
			order := append([]*Op(nil), ops...)
			rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
			for j := 0; j < 8; j++ { // redeliveries
				order = append(order, ops[rng.Intn(len(ops))])
			}
			rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
			if got := snapshot(service, order); got != want {
				t.Fatalf("%s: shuffle %d diverged\n got: %s\nwant: %s", service, i, got, want)
			}
		}
	}
}

func TestResolveIsPureOverCandidateOrder(t *testing.T) {
	r := testRegistry(t)
	ent := r.Entities["item"]
	cands := []Candidate{
		{Ref: OpRef{ra, "s", 1}, HLC: hlc(1), Kind: KindInsert, F: map[string]any{"name_en": "a"}},
		{Ref: OpRef{ra, "s", 2}, HLC: hlc(2), Kind: KindSet, F: map[string]any{"name_en": "b"}},
		{Ref: OpRef{rb, "s", 1}, HLC: hlc(3), Kind: KindSet, F: map[string]any{"name_en": "c"}},
	}
	want := Resolve(ent, RowKey{}, cands, nil)
	rev := []Candidate{cands[2], cands[1], cands[0]}
	if got := Resolve(ent, RowKey{}, rev, nil); !reflect.DeepEqual(got, want) || got.Fields["name_en"] != "c" {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if got := Resolve(ent, RowKey{}, rev, map[OpRef]bool{cands[2].Ref: true}); got.Fields["name_en"] != "b" {
		t.Fatalf("void fallback: %+v", got)
	}
	_ = fmt.Sprint
}
