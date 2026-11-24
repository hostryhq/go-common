package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hostryhq/go-common/ids"
)

func valid() Envelope {
	return Envelope{
		ID: ids.New(), Type: "identity.device.revoked", V: 1, Source: "identity",
		TenantID: ids.New(), Key: "dev-1", KeySeq: 7,
		OccurredAt: time.Date(2026, 11, 24, 18, 35, 11, 120e6, time.FixedZone("IST", 19800)),
		Data:       json.RawMessage(`{}`),
	}
}

func TestSubjectAndMarshalNormalisesToUTC(t *testing.T) {
	e := valid()
	if got := e.Subject(); got != "evt.identity.device.revoked.v1" {
		t.Fatalf("subject %q", got)
	}
	b, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["occurred_at"] != "2026-11-24T13:05:11.12Z" {
		t.Fatalf("occurred_at = %v", m["occurred_at"])
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Envelope)
		ok     bool
	}{
		{"valid", func(*Envelope) {}, true},
		{"no tenant", func(e *Envelope) { e.TenantID = "" }, false},
		{"type not prefixed by source", func(e *Envelope) { e.Source = "tenancy" }, false},
		{"missing data", func(e *Envelope) { e.Data = nil }, false},
		{"link id without counterparty", func(e *Envelope) { e.LinkID = "l1" }, false},
		{"bad id", func(e *Envelope) { e.ID = "nope" }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := valid()
			tc.mutate(&e)
			if err := e.Validate(); (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestPlatformEventWithoutTenantAndUnknownFieldsIgnored(t *testing.T) {
	e := valid()
	e.TenantID = ""
	if err := e.ValidatePlatform(); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(valid())
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	m["future_field"] = "x"
	b, _ = json.Marshal(m)
	if _, err := Unmarshal(b); err != nil {
		t.Fatalf("unknown field rejected: %v", err)
	}
}
