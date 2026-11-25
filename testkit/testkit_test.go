package testkit

import (
	"testing"
	"time"

	"github.com/hostryhq/go-common/ids"
)

func TestFakeClockDrivesDeterministicSortableIDs(t *testing.T) {
	c := NewClock()
	g := c.IDs()
	a := g.New()
	c.Advance(time.Millisecond)
	b := g.New()
	if b <= a {
		t.Fatalf("%s not after %s", b, a)
	}
	if got, _ := a.Time(); !got.Equal(Epoch) {
		t.Fatalf("id time %v, want %v", got, Epoch)
	}
	// reproducible across runs
	if again := NewClock().IDs().New(); again != a {
		t.Fatalf("not deterministic: %s vs %s", again, a)
	}
}

func TestFixedTenantIDsAreValid(t *testing.T) {
	for _, id := range []ids.ID{TenantA, TenantB, OutletA, OutletB} {
		if _, err := ids.Parse(id.String()); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
}
