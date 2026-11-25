package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// The projection model. A service never overwrites a row; it records one Candidate per mutation of
// an entity it owns or replicates, and a row is the pure function Resolve of its candidates and of
// the void set. Adding a candidate is idempotent (identity: op ref and mutation index) and the
// result does not depend on arrival order, so redelivery, duplicates and shuffled delivery give
// the same rows (ADR-0014 A2.1: a consumer needs no order). A void that arrives after the op it
// names simply removes that op's candidates from the next Resolve.
//
// Per field policy (ADR-0002 S6):
//   - immutable: set by insert; if two inserts of one id disagree the lowest stamp wins.
//   - lww: the highest stamp (hlc, origin, seq, index) wins per field group; fields of one group win
//     together, so the winning candidate's values define the group (a field it does not carry is unset).
//   - lww_review: as lww, and a candidate that did not see its predecessor (its prev stamp for the
//     group is not the predecessor's hlc) raises a Conflict: the winner is kept and the loser retained.
//   - delta: the sum over distinct candidates.
//   - rank: forward-only, the highest position in the field's order.
//   - del / undel: a tombstone register, the highest stamp wins.
//
// A row exists once an insert is seen. Mutations that arrive first are kept and become visible
// with the insert, which is how a row whose parent or insert is missing is parked (ADR-0002 S11).
//
// Candidates are kept until the caller compacts them; compaction must not drop a candidate that a
// still-possible void could expose, so it is left to the owning service.

// RowKey identifies one row of one tenant.
type RowKey struct {
	Tenant string
	Entity string
	ID     string
}

// Stamp totally orders candidates: hlc, then origin, then seq, then mutation index.
type Stamp struct {
	HLC    string `json:"hlc"`
	Origin string `json:"origin"`
	Seq    int64  `json:"seq"`
	Idx    int    `json:"idx"`
}

// Compare returns -1, 0 or 1.
func (a Stamp) Compare(b Stamp) int {
	switch {
	case a.HLC != b.HLC:
		return cmpStr(a.HLC, b.HLC)
	case a.Origin != b.Origin:
		return cmpStr(a.Origin, b.Origin)
	case a.Seq != b.Seq:
		return cmpInt(a.Seq, b.Seq)
	}
	return cmpInt(int64(a.Idx), int64(b.Idx))
}

func cmpStr(a, b string) int {
	if a < b {
		return -1
	}
	return 1
}

func cmpInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Candidate is one sanitised mutation: only fields the entity declares, whose policy the kind may
// carry, with a valid type, and never a pii field.
type Candidate struct {
	Ref  OpRef             `json:"ref"`
	Idx  int               `json:"idx"`
	HLC  string            `json:"hlc"`
	Kind string            `json:"kind"`
	F    map[string]any    `json:"f,omitempty"`
	Prev map[string]string `json:"prev,omitempty"`
}

// Stamp is the candidate's ordering key.
func (c Candidate) Stamp() Stamp {
	return Stamp{HLC: c.HLC, Origin: c.Ref.Origin, Seq: c.Ref.Seq, Idx: c.Idx}
}

// Conflict is a concurrent edit of an lww_review group: the winner is kept, the loser retained for
// a manager. It is derived from the candidates, so it is the same on every node and order.
type Conflict struct {
	Group       string         `json:"group"`
	Fields      []string       `json:"fields"`
	Winner      Stamp          `json:"winner"`
	Loser       Stamp          `json:"loser"`
	LoserValues map[string]any `json:"loser_values"`
}

// Row is the resolved state of one row.
type Row struct {
	Key       RowKey
	Exists    bool // an insert was seen and is not voided
	Deleted   bool // tombstone entities: the latest del/undel is del
	Fields    map[string]any
	Conflicts []Conflict
}

// Resolve folds the candidates of one row. voided names ops to ignore. It is pure and does not
// depend on the order of cands.
func Resolve(ent *Entity, key RowKey, cands []Candidate, voided map[OpRef]bool) Row {
	live := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if !voided[c.Ref] {
			live = append(live, c)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Stamp().Compare(live[j].Stamp()) < 0 })

	row := Row{Key: key, Fields: map[string]any{}}
	var tomb *Candidate
	for i := range live {
		c := &live[i]
		switch c.Kind {
		case KindInsert:
			row.Exists = true
		case KindDel, KindUndel:
			tomb = c
		}
	}
	if tomb != nil {
		row.Deleted = tomb.Kind == KindDel
	}
	if !row.Exists {
		return row
	}

	groups := map[string][]string{} // lww group -> field names
	for _, name := range ent.FieldNames() {
		if f := ent.Fields[name]; f.Policy == PolicyLWW || f.Policy == PolicyLWWReview {
			groups[f.groupOf()] = append(groups[f.groupOf()], name)
		}
	}
	groupKeys := make([]string, 0, len(groups))
	for g := range groups {
		groupKeys = append(groupKeys, g)
	}
	sort.Strings(groupKeys)

	// immutable: lowest-stamped insert that carries the field.
	for _, name := range ent.FieldNames() {
		f := ent.Fields[name]
		switch f.Policy {
		case PolicyImmutable:
			for _, c := range live {
				if c.Kind != KindInsert {
					continue
				}
				if v, ok := c.F[name]; ok && v != nil {
					row.Fields[name] = v
					break
				}
			}
		case PolicyDelta:
			var sum int64
			seen := false
			for _, c := range live {
				if v, ok := c.F[name].(int64); ok {
					sum += v
					seen = true
				}
			}
			if seen {
				row.Fields[name] = sum
			}
		case PolicyRank:
			best := -1
			for _, c := range live {
				if v, ok := c.F[name].(string); ok {
					if p := indexOf(f.Order, v); p > best {
						best = p
					}
				}
			}
			if best >= 0 {
				row.Fields[name] = f.Order[best]
			}
		}
	}

	// lww and lww_review: per group, the highest stamp among candidates that carry a group field.
	for _, g := range groupKeys {
		names := groups[g]
		var carriers []Candidate
		for _, c := range live {
			if (c.Kind == KindInsert || c.Kind == KindSet) && carriesAny(c, names) {
				carriers = append(carriers, c)
			}
		}
		if len(carriers) == 0 {
			continue
		}
		win := carriers[len(carriers)-1]
		for _, n := range names {
			if v, ok := win.F[n]; ok && v != nil {
				row.Fields[n] = v
			}
		}
		if ent.Fields[names[0]].Policy == PolicyLWWReview {
			for i := 1; i < len(carriers); i++ {
				if sawPredecessor(carriers[i], carriers[i-1], names) {
					continue
				}
				row.Conflicts = append(row.Conflicts, Conflict{
					Group:       g[2:],
					Fields:      names,
					Winner:      carriers[i].Stamp(),
					Loser:       carriers[i-1].Stamp(),
					LoserValues: pick(carriers[i-1].F, names),
				})
			}
		}
	}
	return row
}

func carriesAny(c Candidate, names []string) bool {
	for _, n := range names {
		if _, ok := c.F[n]; ok {
			return true
		}
	}
	return false
}

// sawPredecessor: the writer of c saw p when, for every review field c carries, its prev is p's hlc.
func sawPredecessor(c, p Candidate, names []string) bool {
	seen := false
	for _, n := range names {
		if _, ok := c.F[n]; !ok {
			continue
		}
		seen = true
		if c.Prev[n] != p.HLC {
			return false
		}
	}
	return seen
}

func pick(f map[string]any, names []string) map[string]any {
	out := map[string]any{}
	for _, n := range names {
		if v, ok := f[n]; ok {
			out[n] = v
		}
	}
	return out
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

// Store persists candidates and the void set. Every method is scoped by tenant. A Store bound to a
// database transaction makes Apply atomic with the caller's other writes (inbox, outbox).
type Store interface {
	// AddCandidate inserts c unless the row already holds a candidate with the same (ref, idx).
	AddCandidate(ctx context.Context, k RowKey, c Candidate) error
	// Candidates returns every candidate of the row.
	Candidates(ctx context.Context, k RowKey) ([]Candidate, error)
	// RowsOfOp lists the rows an op has candidates in.
	RowsOfOp(ctx context.Context, tenant string, r OpRef) ([]RowKey, error)
	// AddVoid records a voided op; adding it twice is a no-op.
	AddVoid(ctx context.Context, tenant string, r OpRef) error
	// Voided returns the subset of refs that are voided.
	Voided(ctx context.Context, tenant string, refs []OpRef) (map[OpRef]bool, error)
}

// Engine applies parsed ops for one service: Service names the owner whose entities it owns and
// whose replicated_to entries it holds as replicas.
type Engine struct {
	Reg     *Registry
	Service string
	Store   Store
}

// Skip reports a mutation field the engine did not apply (policy or type); content is never included.
type Skip struct {
	Entity string
	ID     string
	Field  string
	Reason string // policy, type, rank_value
}

// PIIWrite is personal data to put in the owner's erasable side table, never in a projection row.
type PIIWrite struct {
	Key    RowKey
	Ref    OpRef
	Fields map[string]any
}

// Result is what Apply did.
type Result struct {
	Rows    []Row      // every row touched or exposed by a void, resolved, in (entity, id) order
	PII     []PIIWrite // owned entities only
	Skipped []Skip
	Ignored int // mutations of entities this service neither owns nor replicates
	Voided  []OpRef
}

// ErrNoTenant is returned for an op without a tenant.
var ErrNoTenant = errors.New("sync: op has no tenant")

// Apply records the op: voids first, then one candidate per applicable mutation. It is idempotent
// and order-free; calling it again for the same op, or for ops in another order, ends in the same
// rows. Rows returned are for the caller to write to its own tables.
func (e *Engine) Apply(ctx context.Context, op *Op) (Result, error) {
	if op.TenantID == "" {
		return Result{}, ErrNoTenant
	}
	tenant := op.TenantID.String()
	var res Result
	touched := map[RowKey]struct{}{}

	for _, v := range op.Voids {
		if err := e.Store.AddVoid(ctx, tenant, v); err != nil {
			return Result{}, err
		}
		rows, err := e.Store.RowsOfOp(ctx, tenant, v)
		if err != nil {
			return Result{}, err
		}
		for _, k := range rows {
			touched[k] = struct{}{}
		}
		res.Voided = append(res.Voided, v)
	}

	for i, m := range op.Muts {
		role := e.Reg.RoleOf(e.Service, m.E)
		if role == RoleNone {
			res.Ignored++
			continue
		}
		ent := e.Reg.Entities[m.E]
		c, skips := candidate(ent, op, i, m)
		res.Skipped = append(res.Skipped, skips...)
		if c == nil {
			continue
		}
		k := RowKey{Tenant: tenant, Entity: m.E, ID: m.ID}
		if err := e.Store.AddCandidate(ctx, k, *c); err != nil {
			return Result{}, fmt.Errorf("sync: add candidate: %w", err)
		}
		touched[k] = struct{}{}
	}

	for entName, rows := range op.PII {
		if e.Reg.RoleOf(e.Service, entName) != RoleOwner { // a replica never stores pii
			continue
		}
		ent := e.Reg.Entities[entName]
		for id, fields := range rows {
			kept := map[string]any{}
			for n, v := range fields {
				if f := ent.Fields[n]; f != nil && f.PII {
					kept[n] = v
				}
			}
			if len(kept) > 0 {
				res.PII = append(res.PII, PIIWrite{Key: RowKey{Tenant: tenant, Entity: entName, ID: id}, Ref: op.Ref, Fields: kept})
			}
		}
	}
	sort.Slice(res.PII, func(i, j int) bool { return keyLess(res.PII[i].Key, res.PII[j].Key) })

	keys := make([]RowKey, 0, len(touched))
	for k := range touched {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keyLess(keys[i], keys[j]) })
	for _, k := range keys {
		row, err := e.Row(ctx, k)
		if err != nil {
			return Result{}, err
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
}

// Row resolves one row from the store.
func (e *Engine) Row(ctx context.Context, k RowKey) (Row, error) {
	ent := e.Reg.Entities[k.Entity]
	if ent == nil {
		return Row{}, fmt.Errorf("sync: unknown entity %q", k.Entity)
	}
	cands, err := e.Store.Candidates(ctx, k)
	if err != nil {
		return Row{}, err
	}
	refs := make([]OpRef, 0, len(cands))
	for _, c := range cands {
		refs = append(refs, c.Ref)
	}
	voided, err := e.Store.Voided(ctx, k.Tenant, refs)
	if err != nil {
		return Row{}, err
	}
	return Resolve(ent, k, cands, voided), nil
}

func keyLess(a, b RowKey) bool {
	if a.Entity != b.Entity {
		return a.Entity < b.Entity
	}
	return a.ID < b.ID
}

// candidate sanitises one mutation. It returns nil when nothing is left to record.
func candidate(ent *Entity, op *Op, idx int, m Mut) (*Candidate, []Skip) {
	c := &Candidate{Ref: op.Ref, Idx: idx, HLC: op.HLC, Kind: m.K, Prev: m.Prev}
	var skips []Skip
	switch m.K {
	case KindDel, KindUndel:
		if !ent.Tombstone {
			return nil, []Skip{{ent.Name, m.ID, "", "policy"}}
		}
		return c, nil
	case KindInsert, KindSet, KindDelta, KindRank:
	default:
		return nil, nil
	}
	c.F = map[string]any{}
	for name, v := range m.F {
		f := ent.Fields[name]
		if f == nil || f.PII {
			continue // unknown fields are ignored (S13); pii never enters a projection
		}
		if !kindAllows(m.K, f.Policy) {
			skips = append(skips, Skip{ent.Name, m.ID, name, "policy"})
			continue
		}
		if !typeOK(f, v) {
			skips = append(skips, Skip{ent.Name, m.ID, name, "type"})
			continue
		}
		if f.Policy == PolicyRank && indexOf(f.Order, v.(string)) < 0 {
			skips = append(skips, Skip{ent.Name, m.ID, name, "rank_value"})
			continue
		}
		c.F[name] = v
	}
	if m.K != KindInsert && len(c.F) == 0 {
		return nil, skips
	}
	if len(c.Prev) > 0 {
		prev := map[string]string{}
		for n, s := range c.Prev {
			if f := ent.Fields[n]; f != nil && f.Policy == PolicyLWWReview {
				prev[n] = s
			}
		}
		c.Prev = prev
	}
	return c, skips
}

func kindAllows(kind, policy string) bool {
	switch kind {
	case KindInsert:
		return true
	case KindSet:
		return policy == PolicyLWW || policy == PolicyLWWReview
	case KindDelta:
		return policy == PolicyDelta
	case KindRank:
		return policy == PolicyRank
	}
	return false
}

func typeOK(f *Field, v any) bool {
	if v == nil {
		return f.Policy == PolicyLWW || f.Policy == PolicyLWWReview // a cleared optional value
	}
	switch f.Type {
	case "bool":
		_, ok := v.(bool)
		return ok
	case "int", "paise", "qty_milli", "bp":
		_, ok := v.(int64)
		return ok
	default: // id, string, date, ts, enum (new enum values from newer clients are accepted)
		_, ok := v.(string)
		return ok
	}
}
