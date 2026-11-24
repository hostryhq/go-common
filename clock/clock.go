// Package clock abstracts time so logic is deterministic in tests.
// Production code takes a Clock; testkit provides a fake. Times are UTC.
package clock

import "time"

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

type system struct{}

func (system) Now() time.Time { return time.Now().UTC() }

// System is the wall clock.
var System Clock = system{}

// Func adapts a function to Clock.
type Func func() time.Time

// Now implements Clock.
func (f Func) Now() time.Time { return f().UTC() }

// IST is the display zone (UTC+05:30, no DST). Storage stays UTC.
var IST = time.FixedZone("IST", 5*3600+30*60)

// InIST converts t to IST for display only.
func InIST(t time.Time) time.Time { return t.In(IST) }
