package ids

import (
	"bytes"
	"testing"
	"time"
)

func TestNewIsValidV7AndTimeRoundTrips(t *testing.T) {
	at := time.Date(2026, 11, 24, 13, 5, 11, 120e6, time.UTC)
	g := NewGenerator(func() time.Time { return at }, bytes.NewReader(make([]byte, 64)))
	id := g.New()
	if _, err := Parse(string(id)); err != nil {
		t.Fatalf("Parse(%q): %v", id, err)
	}
	got, ok := id.Time()
	if !ok || !got.Equal(at) {
		t.Fatalf("Time() = %v, %v; want %v", got, ok, at)
	}
}

func TestSameMillisecondIdsSortInCreationOrder(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	g := NewGenerator(func() time.Time { return at }, nil)
	prev := g.New()
	for i := 0; i < 5000; i++ { // more than one ms of sequence space: forces carry
		next := g.New()
		if next <= prev {
			t.Fatalf("id %d not after previous: %s <= %s", i, next, prev)
		}
		prev = next
	}
}

func TestClockStepBackStaysMonotonic(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	g := NewGenerator(func() time.Time { return now }, nil)
	a := g.New()
	now = now.Add(-time.Hour)
	if b := g.New(); b <= a {
		t.Fatalf("%s <= %s after clock step back", b, a)
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{"", "xyz", "018f0000-0000-4000-8000-000000000000", "018F0000-0000-7000-8000-000000000000", "018f0000-0000-7000-c000-000000000000"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted", s)
		}
	}
}
