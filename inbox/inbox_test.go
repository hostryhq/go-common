package inbox

import (
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
