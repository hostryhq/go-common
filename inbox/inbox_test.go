package inbox

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestDefaultBackoffDoublesAndCaps(t *testing.T) {
	for _, c := range []struct {
		attempt uint64
		want    time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {4, 8 * time.Second}, {10, 8*time.Minute + 32*time.Second}, {11, 10 * time.Minute}, {40, 10 * time.Minute}} {
		if got := DefaultBackoff(c.attempt); got != c.want {
			t.Errorf("attempt %d: %v want %v", c.attempt, got, c.want)
		}
	}
}

func TestNewer(t *testing.T) {
	if !Newer(3, 2) || Newer(2, 2) || Newer(1, 2) {
		t.Fatal("Newer must be strictly greater")
	}
}

func TestShouldParkPermanentAtOnceTransientAtLastAttempt(t *testing.T) {
	boom := errors.New("boom")
	for name, c := range map[string]struct {
		err      error
		attempt  uint64
		wantPark bool
	}{
		"transient first":   {boom, 1, false},
		"transient last":    {boom, 10, true},
		"permanent first":   {Permanent(boom), 1, true},
		"wrapped permanent": {fmt.Errorf("x: %w", Permanent(boom)), 1, true},
	} {
		if got := shouldPark(c.err, c.attempt, 10); got != c.wantPark {
			t.Errorf("%s: %v", name, got)
		}
	}
}
