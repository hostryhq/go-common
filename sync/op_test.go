package sync

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const (
	tTenant = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"
	tOutlet = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a61"
	tBrand  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a6a"
	tDevice = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5e"
)

// eventSpec builds a sync.op.<namespace> bus message with a correct hash, so tests change only
// what they probe.
type eventSpec struct {
	origin, stream string
	seq            int64
	intent         string
	hlc            string
	tenantInBody   string
	muts           string // JSON array
	pii            string // JSON object
	padBody        int    // make the body exactly this many bytes
	padPII         int    // make the compact pii exactly this many bytes
	mutate         func(data map[string]any, env map[string]any)
}

func (s eventSpec) build(t *testing.T) []byte {
	t.Helper()
	if s.origin == "" {
		s.origin = tDevice
	}
	if s.stream == "" {
		s.stream = "outlet:" + tOutlet
	}
	if s.seq == 0 {
		s.seq = 1
	}
	if s.intent == "" {
		s.intent = "menu.availability.set"
	}
	if s.hlc == "" {
		s.hlc = "01a1ee565e000000"
	}
	if s.tenantInBody == "" {
		s.tenantInBody = tTenant
	}
	if s.muts == "" {
		s.muts = `[{"e":"item_outlet","f":{"available":false},"id":"a7ac8341-e6c5-58a2-8a75-d00c40f77035","k":"set"}]`
	}
	mk := func(pad string) string {
		return `{"actor":"` + tDevice + `","at":"2026-11-24T13:05:11.120Z","clk":"sync","hlc":"` + s.hlc + `","intent":"` + s.intent +
			`","intent_v":1,"muts":` + s.muts + `,"origin":"` + s.origin + `","ph":"` + Genesis(s.origin, s.stream) + `","seq":` +
			itoa(s.seq) + `,"stream":"` + s.stream + `","tenant_id":"` + s.tenantInBody + `","v":1` + pad + `}`
	}
	body := mk("")
	if s.padBody > 0 {
		base := len(mk(`,"x":""`))
		if s.padBody < base {
			t.Fatalf("padBody %d below minimum %d", s.padBody, base)
		}
		body = mk(`,"x":"` + strings.Repeat("a", s.padBody-base) + `"`)
		if len(body) != s.padBody {
			t.Fatalf("body is %d bytes, want %d", len(body), s.padBody)
		}
	}
	pii := s.pii
	if s.padPII > 0 {
		base := len(`{"e":{"i":{"x":""}}}`)
		pii = `{"e":{"i":{"x":"` + strings.Repeat("b", s.padPII-base) + `"}}}`
	}
	if pii == "" {
		pii = "{}"
	}
	ns, _, _ := strings.Cut(s.intent, ".")
	data := map[string]any{
		"origin": s.origin, "stream": s.stream, "seq": s.seq, "h": Hash(body), "intent": s.intent, "intent_v": 1,
		"status": "accepted", "marks": []string{}, "body": body, "pii": json.RawMessage(pii),
	}
	env := map[string]any{
		"id": "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f9201", "type": "sync.op." + ns, "v": 1, "source": "sync",
		"tenant_id": tTenant, "outlet_id": tOutlet, "key": s.stream + "|" + s.origin, "key_seq": s.seq,
		"occurred_at": "2026-11-24T13:05:12.000Z", "data": data,
	}
	if s.mutate != nil {
		s.mutate(data, env)
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestParseOpReadsEnvelopeAndMutations(t *testing.T) {
	op, err := ParseOp(eventSpec{}.build(t))
	if err != nil {
		t.Fatal(err)
	}
	if op.Ref.Seq != 1 || op.Namespace != "menu" || op.Intent != "menu.availability.set" || op.TenantID != tTenant || op.OutletID != tOutlet {
		t.Fatalf("identity: %+v", op)
	}
	if len(op.Muts) != 1 || op.Muts[0].E != "item_outlet" || op.Muts[0].K != KindSet || op.Muts[0].F["available"] != false {
		t.Fatalf("muts: %+v", op.Muts)
	}
	if op.HLC != "01a1ee565e000000" || op.At.IsZero() {
		t.Fatalf("hlc/at: %+v", op)
	}
}

// The spec's own example event (events/sync/op.v1.yaml) parses, so the parser agrees with the contract.
func TestParseOpAcceptsSpecExample(t *testing.T) {
	body := `{"actor":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a64","at":"2026-11-24T13:05:11.120Z","clk":"sync","hlc":"01a1ee565e000000","intent":"menu.availability.set","intent_v":1,"muts":[{"e":"item_outlet","f":{"available":false},"id":"a7ac8341-e6c5-58a2-8a75-d00c40f77035","k":"set"}],"origin":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5e","ph":"9b47040fc954b008feead3268b905c243a183232441bf09e34e467054c975a12","seq":1,"stream":"outlet:0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a61","tenant_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60","v":1}`
	if Hash(body) != "7a12848dfd8c7c9dce262261d97432158905aeb5f99798239d4583528921b057" {
		t.Fatal("example hash differs: the spec example changed")
	}
	if !IsCanonical([]byte(body)) {
		t.Fatal("example body is not canonical")
	}
	op, err := ParseOp(eventSpec{mutate: func(d, _ map[string]any) {
		d["body"], d["h"] = body, Hash(body)
	}}.build(t))
	if err != nil || op.Ref.Seq != 1 || op.Muts[0].E != "item_outlet" {
		t.Fatalf("op=%+v err=%v", op, err)
	}
}

func TestParseOpRefusals(t *testing.T) {
	tests := []struct {
		name string
		spec eventSpec
		want error
	}{
		{"hash mismatch", eventSpec{mutate: func(d, _ map[string]any) { d["h"] = strings.Repeat("0", 64) }}, ErrHashMismatch},
		{"body altered after hashing", eventSpec{mutate: func(d, _ map[string]any) { d["body"] = strings.Replace(d["body"].(string), "false", "true", 1) }}, ErrHashMismatch},
		{"body over limit", eventSpec{padBody: MaxBodyBytes + 1}, ErrTooLarge},
		{"pii over limit", eventSpec{padPII: MaxPIIBytes + 1}, ErrTooLarge},
		{"tenant in body differs", eventSpec{tenantInBody: "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a99"}, ErrMalformed},
		{"no body (placeholder)", eventSpec{mutate: func(d, _ map[string]any) { delete(d, "body") }}, ErrMalformed},
		{"wrong key", eventSpec{mutate: func(_, e map[string]any) { e["key"] = "outlet:x|y" }}, ErrMalformed},
		{"namespace is not the intent's", eventSpec{mutate: func(_, e map[string]any) { e["type"] = "sync.op.bill" }}, ErrMalformed},
		{"fraction in a mutation", eventSpec{muts: `[{"e":"item","id":"i","k":"set","f":{"x":1.5}}]`}, ErrMalformed},
		{"nested value", eventSpec{muts: `[{"e":"item","id":"i","k":"set","f":{"x":{"a":1}}}]`}, ErrMalformed},
		{"unknown kind", eventSpec{muts: `[{"e":"item","id":"i","k":"merge"}]`}, ErrMalformed},
		{"bad hlc", eventSpec{hlc: "xyz"}, ErrMalformed},
		{"void from a device", eventSpec{intent: VoidIntent, muts: `[{"e":"sync_void","id":"outlet:o|dev|3","k":"insert","f":{"reason":"refused"}}]`}, ErrMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op, err := ParseOp(tc.spec.build(t))
			if !errors.Is(err, tc.want) || op != nil {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "available") {
				t.Fatalf("error leaks body content: %v", err)
			}
		})
	}
}

func TestParseOpVoidFromCloud(t *testing.T) {
	spec := eventSpec{
		origin: "cloud-1", stream: "outlet:" + tOutlet, intent: VoidIntent,
		muts: `[{"e":"sync_void","f":{"origin":"dev-a","reason":"refused","seq":3,"stream":"outlet:` + tOutlet + `"},"id":"outlet:` + tOutlet + `|dev-a|3","k":"insert"}]`,
	}
	op, err := ParseOp(spec.build(t))
	if err != nil {
		t.Fatal(err)
	}
	want := OpRef{Origin: "dev-a", Stream: "outlet:" + tOutlet, Seq: 3}
	if len(op.Voids) != 1 || op.Voids[0] != want || len(op.Muts) != 0 || op.Namespace != "sync" {
		t.Fatalf("voids=%+v muts=%+v", op.Voids, op.Muts)
	}
	// A record whose fields contradict its id is refused.
	bad := spec
	bad.muts = strings.Replace(spec.muts, `"seq":3`, `"seq":4`, 1)
	if _, err := ParseOp(bad.build(t)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("contradicting void record accepted: %v", err)
	}
}

func TestUpcasterRunsAfterVerificationOnTheParsedCopy(t *testing.T) {
	var p Parser
	p.Register("menu.availability.set", 1, func(op *Op) (int, error) {
		op.Muts[0].F["available"] = true // v1 meant the opposite
		return 2, nil
	})
	op, err := p.Parse(eventSpec{}.build(t))
	if err != nil || op.IntentV != 2 || op.Muts[0].F["available"] != true {
		t.Fatalf("op=%+v err=%v", op, err)
	}
	// A tampered body is refused before any upcaster runs.
	if _, err := p.Parse(eventSpec{mutate: func(d, _ map[string]any) { d["h"] = strings.Repeat("1", 64) }}.build(t)); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("upcaster ran on unverified input: %v", err)
	}
	var bad Parser
	bad.Register("menu.availability.set", 1, func(*Op) (int, error) { return 1, nil })
	if _, err := bad.Parse(eventSpec{}.build(t)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("non-advancing upcaster accepted: %v", err)
	}
}

func TestParsePIIShape(t *testing.T) {
	op, err := ParseOp(eventSpec{pii: `{"check":{"c1":{"customer_name":"A. Guest"}}}`}.build(t))
	// Since the commitment rule (ADR-0014 A2.1) a value without pii_c is dropped; see pii_test.go for the kept case.
	if err != nil || len(op.PII) != 0 || op.PIIDropped != 1 {
		t.Fatalf("pii: %+v err=%v", op, err)
	}
	if _, err := ParseOp(eventSpec{pii: `{"customer_name":"flat"}`}.build(t)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("flat pii accepted: %v", err)
	}
}
