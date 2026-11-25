package sync

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

// These tests run the shared conformance vectors (api-specs sync/conformance/v1.json, ADR-0002
// S15) against this package. The vectors are copied to testdata/ and pinned (testdata/PIN);
// TestSpecCopyIsCurrent compares the copy with a checkout when HOSTRY_API_SPECS is set.

type vectors struct {
	Canonical []struct{ In, Out string }
	Hash      []struct{ Body, H string }
	Genesis   []struct{ Origin, Stream, H string }
	SealMsg   []struct {
		Origin, Stream string
		Seq            int64
		H, Message     string
	} `json:"seal_message"`
	EndorseMsg []struct {
		Origin, Stream  string
		Seq             int64
		H, Rec, Message string
	} `json:"endorse_message"`
	AttestMsg []struct {
		Origin, Stream      string
		Seq                 int64
		H, By, Rec, Message string
	} `json:"attest_message"`
	EndorseVerify []struct {
		Case, Pub, Origin, Stream string
		Seq                       int64
		H, Rec, Kid, Sig          string
		Valid                     bool
	} `json:"endorse_verify"`
	HLC []struct {
		Ms      int64
		Counter int
		Hex     string
	}
	SizeLimit []struct {
		Field string
		Bytes int
		Valid bool
	} `json:"size_limit"`
	EmitBatch []struct {
		Ops   int
		Valid bool
	} `json:"emit_batch"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	b, err := os.ReadFile("testdata/conformance/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestConformanceVectors(t *testing.T) {
	v := loadVectors(t)
	if len(v.Canonical) == 0 || len(v.Hash) == 0 || len(v.SizeLimit) < 4 || len(v.EmitBatch) < 4 || len(v.EndorseVerify) == 0 {
		t.Fatal("vector file is missing sections")
	}
	for _, c := range v.Canonical {
		got, err := Canonicalize([]byte(c.In))
		if err != nil || string(got) != c.Out {
			t.Errorf("canonical %q: got %q err=%v, want %q", c.In, got, err, c.Out)
		}
		if !IsCanonical([]byte(c.Out)) {
			t.Errorf("vector output %q is not canonical", c.Out)
		}
	}
	for _, c := range v.Hash {
		if got := Hash(c.Body); got != c.H {
			t.Errorf("hash of %q: %s, want %s", c.Body, got, c.H)
		}
	}
	for _, c := range v.Genesis {
		if got := Genesis(c.Origin, c.Stream); got != c.H {
			t.Errorf("genesis %s/%s: %s, want %s", c.Origin, c.Stream, got, c.H)
		}
	}
	for _, c := range v.SealMsg {
		if got := SealMessage(c.Origin, c.Stream, c.Seq, c.H); got != c.Message {
			t.Errorf("seal message %q, want %q", got, c.Message)
		}
	}
	for _, c := range v.EndorseMsg {
		if got := EndorseMessage(c.Origin, c.Stream, c.Seq, c.H, c.Rec); got != c.Message {
			t.Errorf("endorse message %q, want %q", got, c.Message)
		}
	}
	for _, c := range v.AttestMsg {
		if got := AttestMessage(c.Origin, c.Stream, c.Seq, c.H, c.By, c.Rec); got != c.Message {
			t.Errorf("attest message %q, want %q", got, c.Message)
		}
	}
	for _, c := range v.EndorseVerify {
		if got := VerifyEndorsement(c.Pub, c.Origin, c.Stream, c.Seq, c.H, c.Rec, c.Sig); got != c.Valid {
			t.Errorf("endorsement %q: valid=%v, want %v", c.Case, got, c.Valid)
		}
	}
	for _, c := range v.HLC {
		h := HLC{Ms: c.Ms, Counter: uint16(c.Counter)}
		if h.String() != c.Hex {
			t.Errorf("hlc %d/%d: %s, want %s", c.Ms, c.Counter, h, c.Hex)
		}
		back, err := ParseHLC(c.Hex)
		if err != nil || back != h {
			t.Errorf("parse hlc %s: %+v err=%v", c.Hex, back, err)
		}
	}
	for _, c := range v.SizeLimit {
		body, pii := 0, 0
		if c.Field == "pii" {
			pii = c.Bytes
		} else {
			body = c.Bytes
		}
		if got := CheckSize(body, pii) == nil; got != c.Valid {
			t.Errorf("size %s %d: valid=%v, want %v", c.Field, c.Bytes, got, c.Valid)
		}
	}
	for _, c := range v.EmitBatch {
		if got := CheckBatch(c.Ops) == nil; got != c.Valid {
			t.Errorf("emit batch %d: valid=%v, want %v", c.Ops, got, c.Valid)
		}
	}
}

// The size vectors also hold end to end: ParseOp refuses a body or pii one byte over the limit and
// accepts the limit itself.
func TestParseOpHonoursSizeVectors(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.SizeLimit {
		var ev eventSpec
		switch c.Field {
		case "body":
			ev = eventSpec{padBody: c.Bytes}
		case "pii":
			ev = eventSpec{padPII: c.Bytes}
		default:
			continue
		}
		_, err := ParseOp(ev.build(t))
		if c.Valid && err != nil {
			t.Errorf("%s %d bytes: unexpected %v", c.Field, c.Bytes, err)
		}
		if !c.Valid && !errors.Is(err, ErrTooLarge) {
			t.Errorf("%s %d bytes: want ErrTooLarge, got %v", c.Field, c.Bytes, err)
		}
	}
}

func TestCanonicalizeRefusals(t *testing.T) {
	for _, in := range []string{`{"a":1.5}`, `{"a":1e3}`, `{"a":9007199254740992}`, `{"a":1,"a":2}`, `{"a":1} x`, `[1,`, `{"a":-9007199254740992}`} {
		if _, err := Canonicalize([]byte(in)); !errors.Is(err, ErrNotCanonical) {
			t.Errorf("%s: want ErrNotCanonical, got %v", in, err)
		}
	}
	if _, err := Canonicalize([]byte(strings.Repeat(" ", 3) + `{"a":9007199254740991}`)); err != nil {
		t.Errorf("max safe integer refused: %v", err)
	}
}

// When HOSTRY_API_SPECS points at an api-specs checkout, the pinned copies must equal it.
func TestSpecCopyIsCurrent(t *testing.T) {
	root := os.Getenv("HOSTRY_API_SPECS")
	if root == "" {
		t.Skip("HOSTRY_API_SPECS not set")
	}
	a, err := os.ReadFile(root + "/sync/conformance/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile("testdata/conformance/v1.json")
	if string(a) != string(b) {
		t.Error("testdata/conformance/v1.json differs from api-specs")
	}
	ents, _ := os.ReadDir("testdata/entities/restaurant")
	for _, e := range ents {
		x, err := os.ReadFile(root + "/entities/restaurant/" + e.Name())
		y, _ := os.ReadFile("testdata/entities/restaurant/" + e.Name())
		if err != nil || string(x) != string(y) {
			t.Errorf("entity %s differs from api-specs", e.Name())
		}
	}
}
