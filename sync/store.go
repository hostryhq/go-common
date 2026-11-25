package sync

import (
	"context"
	gosync "sync"
)

// MemStore is the in-memory Store for unit tests and test doubles.
type MemStore struct {
	mu    gosync.Mutex
	cands map[RowKey]map[candID]Candidate
	voids map[string]map[OpRef]bool
	pii   map[RowKey]map[OpRef]PIIWrite
	gone  map[string]map[OpRef]bool // erased ops by tenant
}

type candID struct {
	ref OpRef
	idx int
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{cands: map[RowKey]map[candID]Candidate{}, voids: map[string]map[OpRef]bool{},
		pii: map[RowKey]map[OpRef]PIIWrite{}, gone: map[string]map[OpRef]bool{}}
}

// AddCandidate implements Store.
func (s *MemStore) AddCandidate(_ context.Context, k RowKey, c Candidate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.cands[k]
	if m == nil {
		m = map[candID]Candidate{}
		s.cands[k] = m
	}
	id := candID{c.Ref, c.Idx}
	if _, ok := m[id]; !ok {
		m[id] = c
	}
	return nil
}

// Candidates implements Store.
func (s *MemStore) Candidates(_ context.Context, k RowKey) ([]Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Candidate, 0, len(s.cands[k]))
	for _, c := range s.cands[k] {
		out = append(out, c)
	}
	return out, nil
}

// RowsOfOp implements Store.
func (s *MemStore) RowsOfOp(_ context.Context, tenant string, r OpRef) ([]RowKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []RowKey
	for k, m := range s.cands {
		if k.Tenant != tenant {
			continue
		}
		for id := range m {
			if id.ref == r {
				out = append(out, k)
				break
			}
		}
	}
	return out, nil
}

// AddVoid implements Store.
func (s *MemStore) AddVoid(_ context.Context, tenant string, r OpRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.voids[tenant] == nil {
		s.voids[tenant] = map[OpRef]bool{}
	}
	s.voids[tenant][r] = true
	return nil
}

// Voided implements Store.
func (s *MemStore) Voided(_ context.Context, tenant string, refs []OpRef) (map[OpRef]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[OpRef]bool{}
	for _, r := range refs {
		if s.voids[tenant][r] {
			out[r] = true
		}
	}
	return out, nil
}

// PutPII implements Store.
func (s *MemStore) PutPII(_ context.Context, w PIIWrite) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gone[w.Key.Tenant][w.Ref] {
		return nil
	}
	if s.pii[w.Key] == nil {
		s.pii[w.Key] = map[OpRef]PIIWrite{}
	}
	if _, ok := s.pii[w.Key][w.Ref]; !ok {
		s.pii[w.Key][w.Ref] = w
	}
	return nil
}

// AddErasure implements Store: values and salts go, commitments stay.
func (s *MemStore) AddErasure(_ context.Context, tenant string, r OpRef) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gone[tenant] == nil {
		s.gone[tenant] = map[OpRef]bool{}
	}
	s.gone[tenant][r] = true
	var n int64
	for k, m := range s.pii {
		if k.Tenant != tenant {
			continue
		}
		if w, ok := m[r]; ok && len(w.Fields) > 0 {
			w.Fields, w.Salts = nil, nil
			m[r] = w
			n++
		}
	}
	return n, nil
}

// Erased implements Store.
func (s *MemStore) Erased(_ context.Context, tenant string, r OpRef) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gone[tenant][r], nil
}

// PII returns the side-table records of a row (for tests and doubles), in no particular order.
func (s *MemStore) PII(k RowKey) []PIIWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []PIIWrite
	for _, w := range s.pii[k] {
		out = append(out, w)
	}
	return out
}
