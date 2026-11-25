package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
)

// Mutation kinds (ADR-0002 S4).
const (
	KindInsert = "insert"
	KindSet    = "set"
	KindDelta  = "delta"
	KindRank   = "rank"
	KindDel    = "del"
	KindUndel  = "undel"
)

// VoidIntent is the cloud-origin intent that voids a refused configuration op.
const VoidIntent = "sync.void"

// OpRef is the identity of an op: (origin, stream, seq).
type OpRef struct {
	Origin string `json:"origin"`
	Stream string `json:"stream"`
	Seq    int64  `json:"seq"`
}

// String is the void-record id form: stream|origin|seq.
func (r OpRef) String() string { return r.Stream + "|" + r.Origin + "|" + strconv.FormatInt(r.Seq, 10) }

// Mut is one atomic mutation of one entity row. F holds only strings, booleans, int64 and nil (a
// cleared field); the parser refuses fractions, exponents and nested values.
type Mut struct {
	E    string            `json:"e"`
	ID   string            `json:"id"`
	K    string            `json:"k"`
	F    map[string]any    `json:"f,omitempty"`
	Prev map[string]string `json:"prev,omitempty"`
}

// Doc is the legal document number an op allocated (informational for consumers).
type Doc struct {
	Type   string `json:"type"`
	Number string `json:"number"`
}

// Op is a parsed sync.op.<namespace> event: the envelope identity plus the mutations read from
// data.body. It does not keep the body bytes or pii text; log OpRef, never content.
type Op struct {
	Ref       OpRef
	H         string
	Intent    string
	IntentV   int
	Namespace string
	Status    string // accepted or flagged
	Marks     []string
	TenantID  ids.ID
	OutletID  ids.ID
	Actor     string
	HLC       string // 16 hex characters, as sent
	At        time.Time
	Clock     string
	Doc       *Doc
	Muts      []Mut
	// Voids lists the ops named by sync_void records (intent sync.void, cloud origin only).
	Voids []OpRef
	// PII is the personal data beside the op, as entity -> row id -> field -> value. See Engine.
	PII map[string]map[string]map[string]any
}

// Upcaster rewrites a parsed op of an older intent_v to the shape the service understands
// (ADR-0002 S13). It runs after verification, on the parsed copy; the stored bytes are never touched.
// It returns the intent_v it produced.
type Upcaster func(op *Op) (newV int, err error)

// Parser is ParseOp with registered upcasters. The zero value works and has none.
type Parser struct {
	upcasters map[upKey]Upcaster
}

type upKey struct {
	intent string
	v      int
}

// Register adds an upcaster for (intent, from intent_v).
func (p *Parser) Register(intent string, fromV int, fn Upcaster) {
	if p.upcasters == nil {
		p.upcasters = map[upKey]Upcaster{}
	}
	p.upcasters[upKey{intent, fromV}] = fn
}

// ParseOp is the one parser for sync op events. raw is the bus message (the events.Envelope JSON).
func ParseOp(raw []byte) (*Op, error) { return (&Parser{}).Parse(raw) }

// Parse decodes the envelope, verifies the op and reads its mutations from data.body.
//
// Verified: type sync.op.<namespace>, v 1, source sync and a tenant; body and pii within the size
// limits; h equals the SHA-256 of the body bytes as received; the body's origin, stream, seq and
// tenant equal the data fields and the envelope; the envelope key and key_seq; the namespace is
// the intent's; mutation kinds and values (strings, booleans, safe integers, null only); void
// records come from a cloud origin. Unknown fields are ignored (ADR-0002 S13). Errors wrap
// ErrTooLarge, ErrHashMismatch or ErrMalformed and never carry the body.
func (p *Parser) Parse(raw []byte) (*Op, error) {
	env, err := events.Unmarshal(raw)
	if err != nil {
		return nil, malformed("envelope: %v", err)
	}
	return p.ParseEnvelope(env)
}

// ParseEnvelope is Parse for an already decoded envelope.
func (p *Parser) ParseEnvelope(env events.Envelope) (*Op, error) {
	ns, ok := strings.CutPrefix(env.Type, "sync.op.")
	if !ok || ns == "" || strings.Contains(ns, ".") {
		return nil, malformed("type %q is not sync.op.<namespace>", env.Type)
	}
	if env.V != 1 || env.Source != "sync" {
		return nil, malformed("v/source: not a v1 sync event")
	}
	if env.TenantID == "" {
		return nil, malformed("tenant_id is required")
	}
	var data struct {
		Origin  string          `json:"origin"`
		Stream  string          `json:"stream"`
		Seq     int64           `json:"seq"`
		H       string          `json:"h"`
		Intent  string          `json:"intent"`
		IntentV int             `json:"intent_v"`
		Status  string          `json:"status"`
		Marks   []string        `json:"marks"`
		Body    *string         `json:"body"`
		PII     json.RawMessage `json:"pii"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return nil, malformed("data: %v", err)
	}
	if data.Body == nil {
		return nil, malformed("data.body is required (a placeholder is never published)")
	}
	body := *data.Body
	pii := bytes.TrimSpace(data.PII)
	if len(pii) > 0 {
		var c bytes.Buffer
		if err := json.Compact(&c, pii); err != nil {
			return nil, malformed("pii: %v", err)
		}
		pii = c.Bytes()
	}
	if err := CheckSize(len(body), len(pii)); err != nil {
		return nil, err
	}
	if data.Origin == "" || data.Stream == "" || data.Seq < 1 || data.Intent == "" || data.IntentV < 1 {
		return nil, malformed("data: origin, stream, seq, intent and intent_v are required")
	}
	if data.Status != "accepted" && data.Status != "flagged" {
		return nil, malformed("data.status %q", data.Status)
	}
	if !isHex64(data.H) || Hash(body) != data.H {
		return nil, fmt.Errorf("%w: seq %d of %s", ErrHashMismatch, data.Seq, data.Stream)
	}
	if intentNamespace(data.Intent) != ns {
		return nil, malformed("type namespace %q is not the intent's (%s)", ns, data.Intent)
	}
	if env.Key != data.Stream+"|"+data.Origin || env.KeySeq != data.Seq {
		return nil, malformed("envelope key/key_seq do not match origin, stream and seq")
	}

	var b struct {
		V       int             `json:"v"`
		Origin  string          `json:"origin"`
		Stream  string          `json:"stream"`
		Seq     int64           `json:"seq"`
		HLC     string          `json:"hlc"`
		Clock   string          `json:"clk"`
		Tenant  string          `json:"tenant_id"`
		Actor   string          `json:"actor"`
		Intent  string          `json:"intent"`
		IntentV int             `json:"intent_v"`
		At      string          `json:"at"`
		Doc     *Doc            `json:"doc"`
		Muts    json.RawMessage `json:"muts"`
	}
	if err := json.Unmarshal([]byte(body), &b); err != nil {
		return nil, malformed("body: %v", err)
	}
	if b.V != 1 {
		return nil, malformed("body.v %d is not supported", b.V)
	}
	if b.Origin != data.Origin || b.Stream != data.Stream || b.Seq != data.Seq || b.Intent != data.Intent || b.IntentV != data.IntentV {
		return nil, malformed("body identity differs from data")
	}
	if b.Tenant != env.TenantID.String() {
		return nil, malformed("body tenant_id differs from the envelope")
	}
	if _, err := ParseHLC(b.HLC); err != nil {
		return nil, malformed("body.hlc: %v", err)
	}
	at, err := time.Parse(time.RFC3339Nano, b.At)
	if err != nil {
		return nil, malformed("body.at: %v", err)
	}
	op := &Op{
		Ref:       OpRef{Origin: data.Origin, Stream: data.Stream, Seq: data.Seq},
		H:         data.H,
		Intent:    data.Intent,
		IntentV:   data.IntentV,
		Namespace: ns,
		Status:    data.Status,
		Marks:     data.Marks,
		TenantID:  env.TenantID,
		OutletID:  env.OutletID,
		Actor:     b.Actor,
		HLC:       b.HLC,
		At:        at.UTC(),
		Clock:     b.Clock,
		Doc:       b.Doc,
	}
	muts, err := parseMuts(b.Muts)
	if err != nil {
		return nil, err
	}
	for _, m := range muts {
		if m.E == "sync_void" { // a protocol record, not an entity
			ref, err := parseVoid(m, op)
			if err != nil {
				return nil, err
			}
			op.Voids = append(op.Voids, ref)
			continue
		}
		op.Muts = append(op.Muts, m)
	}
	if len(pii) > 0 && string(pii) != "{}" && string(pii) != "null" {
		if op.PII, err = parsePII(pii); err != nil {
			return nil, err
		}
	}
	return p.upcast(op)
}

func (p *Parser) upcast(op *Op) (*Op, error) {
	for i := 0; i < 16; i++ {
		fn := p.upcasters[upKey{op.Intent, op.IntentV}]
		if fn == nil {
			return op, nil
		}
		v, err := fn(op)
		if err != nil {
			return nil, malformed("upcast %s v%d: %v", op.Intent, op.IntentV, err)
		}
		if v <= op.IntentV {
			return nil, malformed("upcast %s v%d did not advance the version", op.Intent, op.IntentV)
		}
		op.IntentV = v
	}
	return nil, malformed("upcast chain of %s too long", op.Intent)
}

func parseMuts(raw json.RawMessage) ([]Mut, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var items []struct {
		E    string                     `json:"e"`
		ID   string                     `json:"id"`
		K    string                     `json:"k"`
		F    map[string]json.RawMessage `json:"f"`
		Prev map[string]string          `json:"prev"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, malformed("body.muts: %v", err)
	}
	out := make([]Mut, 0, len(items))
	for i, it := range items {
		switch it.K {
		case KindInsert, KindSet, KindDelta, KindRank, KindDel, KindUndel:
		default:
			return nil, malformed("mut %d: kind %q", i, it.K)
		}
		if it.E == "" || it.ID == "" {
			return nil, malformed("mut %d: e and id are required", i)
		}
		m := Mut{E: it.E, ID: it.ID, K: it.K, Prev: it.Prev}
		if len(it.F) > 0 {
			m.F = make(map[string]any, len(it.F))
			for name, rv := range it.F {
				v, err := decodeValue(rv)
				if err != nil {
					return nil, malformed("mut %d field %q: %v", i, name, err)
				}
				m.F[name] = v
			}
		}
		out = append(out, m)
	}
	return out, nil
}

// decodeValue accepts a string, bool, null or an integer within +-(2^53-1) and nothing else.
func decodeValue(rv json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(rv))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case nil, bool, string:
		return t, nil
	case json.Number:
		return parseSafeInt(t.String())
	default:
		return nil, fmt.Errorf("nested values are not allowed")
	}
}

func parseVoid(m Mut, op *Op) (OpRef, error) {
	if !strings.HasPrefix(op.Ref.Origin, "cloud-") || op.Intent != VoidIntent || m.K != KindInsert {
		return OpRef{}, malformed("a void record must be an insert of intent sync.void from a cloud origin")
	}
	parts := strings.Split(m.ID, "|")
	if len(parts) != 3 {
		return OpRef{}, malformed("void id %q is not stream|origin|seq", m.ID)
	}
	seq, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || seq < 1 {
		return OpRef{}, malformed("void id seq")
	}
	ref := OpRef{Stream: parts[0], Origin: parts[1], Seq: seq}
	if f := m.F; f != nil {
		if o, ok := f["origin"].(string); ok && o != ref.Origin {
			return OpRef{}, malformed("void record origin differs from its id")
		}
		if s, ok := f["stream"].(string); ok && s != ref.Stream {
			return OpRef{}, malformed("void record stream differs from its id")
		}
		if n, ok := f["seq"].(int64); ok && n != ref.Seq {
			return OpRef{}, malformed("void record seq differs from its id")
		}
	}
	return ref, nil
}

func parsePII(raw []byte) (map[string]map[string]map[string]any, error) {
	var generic map[string]map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, malformed("pii must be entity -> row id -> field -> value: %v", err)
	}
	out := make(map[string]map[string]map[string]any, len(generic))
	for e, rows := range generic {
		out[e] = make(map[string]map[string]any, len(rows))
		for id, fields := range rows {
			out[e][id] = make(map[string]any, len(fields))
			for name, rv := range fields {
				v, err := decodeValue(rv)
				if err != nil {
					return nil, malformed("pii %s field %q: %v", e, name, err)
				}
				out[e][id][name] = v
			}
		}
	}
	return out, nil
}

func malformed(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, a...))
}

func intentNamespace(intent string) string {
	ns, _, _ := strings.Cut(intent, ".")
	return ns
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
