// Package ids generates time-sortable UUIDv7 identifiers (RFC 9562).
package ids

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"
)

// ID is a UUID in canonical lower-case 36-character form.
type ID string

// ErrInvalid is returned by Parse for malformed or non-v7 ids.
var ErrInvalid = errors.New("ids: invalid uuidv7")

// Generator creates UUIDv7 values. It is monotonic within one generator: ids
// created in the same millisecond, or after the clock stepped back, still sort
// in creation order.
type Generator struct {
	mu     sync.Mutex
	now    func() time.Time
	rand   io.Reader
	lastMS int64
	lastSq uint16 // 12-bit sequence held in rand_a
}

// NewGenerator returns a generator. A nil now uses the wall clock and a nil
// r uses crypto/rand; tests pass fakes.
func NewGenerator(now func() time.Time, r io.Reader) *Generator {
	if now == nil {
		now = time.Now
	}
	if r == nil {
		r = rand.Reader
	}
	return &Generator{now: now, rand: r}
}

var defaultGen = NewGenerator(nil, nil)

// New returns a new UUIDv7 from the default generator.
func New() ID { return defaultGen.New() }

// New returns a new UUIDv7.
func (g *Generator) New() ID {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := g.now().UnixMilli()
	if ms <= g.lastMS {
		ms = g.lastMS
		g.lastSq++
		if g.lastSq > 0x0fff {
			ms++
			g.lastSq = 0
		}
	} else {
		var b [2]byte
		_, _ = io.ReadFull(g.rand, b[:])
		// Start in the lower half of the space so the sequence has headroom.
		g.lastSq = (uint16(b[0])<<8 | uint16(b[1])) & 0x07ff
	}
	g.lastMS = ms

	var u [16]byte
	u[0], u[1], u[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	u[3], u[4], u[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	u[6] = 0x70 | byte(g.lastSq>>8)
	u[7] = byte(g.lastSq)
	_, _ = io.ReadFull(g.rand, u[8:])
	u[8] = 0x80 | (u[8] & 0x3f)
	return ID(format(u))
}

func format(u [16]byte) string {
	var out [36]byte
	hex.Encode(out[0:8], u[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], u[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], u[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], u[8:10])
	out[23] = '-'
	hex.Encode(out[24:], u[10:])
	return string(out[:])
}

// Parse validates s as a canonical lower-case UUIDv7.
func Parse(s string) (ID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return "", ErrInvalid
	}
	for i := 0; i < 36; i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", ErrInvalid
		}
	}
	if s[14] != '7' || !(s[19] == '8' || s[19] == '9' || s[19] == 'a' || s[19] == 'b') {
		return "", ErrInvalid
	}
	return ID(s), nil
}

// String returns the canonical form.
func (i ID) String() string { return string(i) }

// Time extracts the embedded millisecond timestamp; ok is false for invalid ids.
func (i ID) Time() (t time.Time, ok bool) {
	if _, err := Parse(string(i)); err != nil {
		return time.Time{}, false
	}
	b, err := hex.DecodeString(string(i[0:8]) + string(i[9:13]))
	if err != nil {
		return time.Time{}, false
	}
	var ms int64
	for _, x := range b {
		ms = ms<<8 | int64(x)
	}
	return time.UnixMilli(ms).UTC(), true
}
