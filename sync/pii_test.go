package sync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const (
	secretName  = "Ananya Rao-Unique-Name"
	secretPhone = "+91 98765 00123"
	piiStream   = "outlet:" + tOutlet
)

// piiEvent builds a sync.op event whose pii is committed like svc-sync's CommitPII does it
// (fixed salts, so the test is deterministic). tamper lets a test change the wire pii afterwards.
func piiEvent(t *testing.T, seq int64, values map[string]any, tamper func(fields map[string]any)) []byte {
	t.Helper()
	wire := map[string]any{}
	commits := map[string]any{}
	for name, v := range values {
		salt := []byte(strings.Repeat("s", piiSaltBytes))
		c, err := PIICommit(salt, v)
		if err != nil {
			t.Fatal(err)
		}
		wire[name] = v
		wire[piiSaltPrefix+name] = base64.RawURLEncoding.EncodeToString(salt)
		commits[name] = c
	}
	if tamper != nil {
		tamper(wire)
	}
	pii, _ := json.Marshal(map[string]any{"check": map[string]any{"c1": wire}})
	pc, _ := json.Marshal(map[string]any{"check": map[string]any{"c1": commits}})
	return eventSpec{seq: seq, intent: "orders.check.open", pii: string(pii),
		muts: `[{"e":"check","id":"c1","k":"insert","f":{"order_type":"takeaway"}}]`,
		mutate: func(d, _ map[string]any) {
			b := d["body"].(string)
			b = strings.TrimSuffix(b, "}") + `,"pii_c":` + string(pc) + "}"
			d["body"], d["h"] = b, Hash(b)
		}}.build(t)
}

func eraseEvent(t *testing.T, seq int64, target OpRef) []byte {
	t.Helper()
	return eventSpec{origin: "cloud-1", stream: piiStream, seq: seq, intent: PIIEraseIntent,
		muts: `[{"e":"pii_erase","id":"` + target.String() + `","k":"insert","f":{"origin":"` + target.Origin + `","stream":"` + target.Stream + `","seq":` + itoa(target.Seq) + `}}]`}.build(t)
}

func TestParseOpKeepsOnlyPIIThatMatchesItsCommitment(t *testing.T) {
	good := map[string]any{"customer_name": secretName, "customer_phone": secretPhone}
	tests := []struct {
		name    string
		tamper  func(f map[string]any)
		kept    int
		dropped int
	}{
		{"all verify", nil, 2, 0},
		{"changed value", func(f map[string]any) { f["customer_phone"] = "+91 00000 00000" }, 1, 1},
		{"salt missing", func(f map[string]any) { delete(f, piiSaltPrefix+"customer_name") }, 1, 1},
		{"salt of another field", func(f map[string]any) {
			f[piiSaltPrefix+"customer_name"] = f[piiSaltPrefix+"customer_phone"].(string)[:5]
		}, 1, 1},
		{"field without commitment", func(f map[string]any) { f["customer_gstin"] = "29ABCDE1234F1Z5" }, 2, 1},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			op, err := ParseOp(piiEvent(t, 1, good, c.tamper))
			if err != nil {
				t.Fatal(err)
			}
			if got := len(op.PII["check"]["c1"]); got != c.kept || op.PIIDropped != c.dropped {
				t.Fatalf("kept %d dropped %d: %+v", got, op.PIIDropped, op)
			}
			for n := range op.PII["check"]["c1"] {
				if op.PIISalts["check"]["c1"][n] == "" || op.PIICommits["check"]["c1"][n] == "" {
					t.Errorf("verified field %s lost its salt or commitment", n)
				}
			}
		})
	}
	// pii with no pii_c at all verifies nothing.
	raw := eventSpec{intent: "orders.check.open", pii: `{"check":{"c1":{"customer_name":"x"}}}`}.build(t)
	if op, err := ParseOp(raw); err != nil || len(op.PII) != 0 || op.PIIDropped != 1 {
		t.Fatalf("uncommitted pii kept: %+v err=%v", op, err)
	}
}

func TestPIIEraseIsCloudOnly(t *testing.T) {
	target := OpRef{Origin: tDevice, Stream: piiStream, Seq: 1}
	raw := eventSpec{origin: tDevice, intent: PIIEraseIntent,
		muts: `[{"e":"pii_erase","id":"` + target.String() + `","k":"insert"}]`}.build(t)
	if _, err := ParseOp(raw); !errors.Is(err, ErrMalformed) {
		t.Fatalf("device-origin erase accepted: %v", err)
	}
	op, err := ParseOp(eraseEvent(t, 1, target))
	if err != nil || len(op.PIIErase) != 1 || op.PIIErase[0] != target || len(op.Muts) != 0 {
		t.Fatalf("op=%+v err=%v", op, err)
	}
}

// Erasure removes values and salts wherever the projection engine keeps them and leaves the
// commitments, whatever order the erase and the op arrive in, and redelivery changes nothing.
func TestErasureLeavesNoPersonalValueInAnyRow(t *testing.T) {
	create := piiEvent(t, 1, map[string]any{"customer_name": secretName, "customer_phone": secretPhone}, nil)
	opRef := OpRef{Origin: tDevice, Stream: piiStream, Seq: 1}
	erase := eraseEvent(t, 1, opRef)
	orders := map[string][][]byte{
		"op then erase":   {create, erase},
		"erase then op":   {erase, create},
		"redelivery":      {create, erase, create, erase},
		"erase op erase":  {erase, create, erase},
		"interleaved dup": {create, create, erase},
	}
	for name, seq := range orders {
		t.Run(name, func(t *testing.T) {
			e := newEngine(t, "orders")
			store := e.Store.(*MemStore)
			for _, raw := range seq {
				op, err := ParseOp(raw)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := e.Apply(context.Background(), op); err != nil {
					t.Fatal(err)
				}
			}
			k := RowKey{Tenant: tTenant, Entity: "check", ID: "c1"}
			// Everything the engine holds for the row: the resolved projection row, every candidate,
			// and the pii side table.
			r, _ := e.Row(context.Background(), k)
			cands, _ := store.Candidates(context.Background(), k)
			dump, _ := json.Marshal([]any{r, cands, store.PII(k)})
			for _, secret := range []string{secretName, secretPhone, base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", piiSaltBytes)))} {
				if strings.Contains(string(dump), secret) {
					t.Fatalf("%q survived erasure: %s", secret, dump)
				}
			}
			if r.Fields["order_type"] != "takeaway" {
				t.Errorf("erasure damaged the projection: %+v", r)
			}
			recs := store.PII(k)
			if erased, _ := store.Erased(context.Background(), tTenant, opRef); !erased {
				t.Fatal("erasure not recorded")
			}
			for _, w := range recs { // when the record exists, only commitments remain
				if len(w.Fields) != 0 || len(w.Salts) != 0 || len(w.Commits) != 2 {
					t.Errorf("record after erasure: %+v", w)
				}
			}
		})
	}
}

func TestSeriesCloseIsExposedByParseOp(t *testing.T) {
	raw := eventSpec{intent: "series.close", muts: `[{"e":"series_close","id":"series-1","k":"insert","f":{"code":"B1","fy":"2026-27","last_number":42}}]`}.build(t)
	op, err := ParseOp(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(op.Muts) != 0 || len(op.SeriesClose) != 1 || op.SeriesClose[0] != (SeriesClose{SeriesID: "series-1", Code: "B1", FY: "2026-27", LastNumber: 42}) {
		t.Fatalf("series_close: %+v", op)
	}
	bad := eventSpec{intent: "series.close", muts: `[{"e":"series_close","id":"s","k":"insert","f":{"code":"B1","fy":"2026-27"}}]`}.build(t)
	if _, err := ParseOp(bad); !errors.Is(err, ErrMalformed) {
		t.Fatalf("series_close without last_number: %v", err)
	}
}
