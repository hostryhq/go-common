// Package events defines the domain event envelope of ADR-0011 §B6 and its
// subject naming. Publishing (outbox) and subscribing (inbox, NATS) live in a
// separate package once the bus is built; this package is wire types only.
//
// Consumers must ignore unknown fields; encoding/json does so by default.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/tenancy"
)

// Envelope is the JSON message published on the bus.
type Envelope struct {
	// ID is the UUIDv7 event id; equals the outbox row id and Nats-Msg-Id.
	ID ids.ID `json:"id"`
	// Type is "<service>.<entity>.<verb>", e.g. "identity.device.revoked".
	Type string `json:"type"`
	// V is the major version of the type's schema.
	V int `json:"v"`
	// Source is the publishing service; must prefix Type.
	Source   string `json:"source"`
	TenantID ids.ID `json:"tenant_id,omitempty"`
	OutletID ids.ID `json:"outlet_id,omitempty"`
	// Key is the aggregate id; KeySeq orders events for the same key.
	Key    string `json:"key,omitempty"`
	KeySeq int64  `json:"key_seq,omitempty"`
	// OccurredAt is UTC, RFC 3339 with milliseconds.
	OccurredAt time.Time `json:"occurred_at"`
	// Actor is the member, device or service id, as data for audit.
	Actor string `json:"actor,omitempty"`
	// Epoch is the publisher's restore epoch (ADR-0006 H6, ADR-0002).
	Epoch string `json:"epoch,omitempty"`
	// Trace is the trace context.
	Trace string `json:"trace,omitempty"`
	// Link-addressed events (ADR-0003 §3) only.
	CounterpartyTenantID ids.ID          `json:"counterparty_tenant_id,omitempty"`
	LinkID               string          `json:"link_id,omitempty"`
	Data                 json.RawMessage `json:"data"`
}

// Validation errors.
var (
	ErrNoTenant = errors.New("events: tenant_id is required")
	ErrBadType  = errors.New("events: type must be <source>.<entity>.<verb> with source as prefix")
)

// Validate checks the envelope for a tenant-scoped event. Events must reference
// people by id and carry no names, phone numbers or free text in Data.
func (e Envelope) Validate() error {
	if e.TenantID == "" {
		return ErrNoTenant
	}
	return e.ValidatePlatform()
}

// ValidatePlatform is Validate without the tenant requirement, for the few
// platform-level types on the schema registry's allow-list (ADR-0011 §B7).
func (e Envelope) ValidatePlatform() error {
	if _, err := ids.Parse(e.ID.String()); err != nil {
		return fmt.Errorf("events: id: %w", err)
	}
	parts := strings.Split(e.Type, ".")
	if len(parts) != 3 || e.Source == "" || parts[0] != e.Source || parts[1] == "" || parts[2] == "" {
		return ErrBadType
	}
	if e.V < 1 {
		return errors.New("events: v must be >= 1")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("events: occurred_at is required")
	}
	if len(e.Data) == 0 {
		return errors.New("events: data is required (use {})")
	}
	if (e.CounterpartyTenantID == "") != (e.LinkID == "") {
		return errors.New("events: counterparty_tenant_id and link_id go together")
	}
	return nil
}

// Subject returns evt.<service>.<entity>.<verb>.v<major>. No tenant id appears
// in subjects.
func (e Envelope) Subject() string { return "evt." + e.Type + fmt.Sprintf(".v%d", e.V) }

// SyncOpSubject returns the subject for sync ops, filterable by intent
// namespace: evt.sync.op.<namespace>.v1.
func SyncOpSubject(intentNamespace string) string {
	return "evt.sync.op." + intentNamespace + ".v1"
}

// Marshal validates and encodes e with OccurredAt normalised to UTC.
func (e Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return json.Marshal(e)
}

// Unmarshal decodes an envelope; unknown fields are ignored.
func Unmarshal(b []byte) (Envelope, error) {
	var e Envelope
	err := json.Unmarshal(b, &e)
	return e, err
}

// TenantContext sets the tenant context from the envelope, as the subscriber
// library does before a handler runs (ADR-0011 §B7). Row-level security still
// applies to the handler's writes.
func (e Envelope) TenantContext(ctx context.Context) context.Context {
	return tenancy.With(ctx, tenancy.Context{
		TenantID: e.TenantID,
		OutletID: e.OutletID,
		ActorID:  e.Actor,
	})
}
