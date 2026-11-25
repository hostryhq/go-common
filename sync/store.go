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
}

type candID struct {
	ref OpRef
	idx int
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{cands: map[RowKey]map[candID]Candidate{}, voids: map[string]map[OpRef]bool{}}
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
