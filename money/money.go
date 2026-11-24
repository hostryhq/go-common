// Package money holds integer-paise helpers. Floats are never used for money
// and tax rates are never hard-coded here: callers pass rates from configuration.
package money

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// INR is the default currency code.
const INR = "INR"

// Errors returned by arithmetic helpers.
var (
	ErrOverflow         = errors.New("money: overflow")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrInvalidArgument  = errors.New("money: invalid argument")
)

// Money matches the API shape {"amount_paise": int64, "currency": "INR"}.
type Money struct {
	AmountPaise int64  `json:"amount_paise"`
	Currency    string `json:"currency"`
}

// New returns an INR amount.
func New(paise int64) Money { return Money{AmountPaise: paise, Currency: INR} }

// Add returns m+o with overflow and currency checks.
func (m Money) Add(o Money) (Money, error) {
	if m.Currency != o.Currency {
		return Money{}, ErrCurrencyMismatch
	}
	s := m.AmountPaise + o.AmountPaise
	if (o.AmountPaise > 0 && s < m.AmountPaise) || (o.AmountPaise < 0 && s > m.AmountPaise) {
		return Money{}, ErrOverflow
	}
	return Money{s, m.Currency}, nil
}

// Sub returns m-o with overflow and currency checks.
func (m Money) Sub(o Money) (Money, error) {
	if m.Currency != o.Currency {
		return Money{}, ErrCurrencyMismatch
	}
	d := m.AmountPaise - o.AmountPaise
	if (o.AmountPaise < 0 && d < m.AmountPaise) || (o.AmountPaise > 0 && d > m.AmountPaise) {
		return Money{}, ErrOverflow
	}
	return Money{d, m.Currency}, nil
}

// String renders for logs, e.g. "INR 1234.50". User-facing formatting
// (grouping, symbols, locale) belongs to clients.
func (m Money) String() string {
	sign := ""
	u := uint64(m.AmountPaise)
	if m.AmountPaise < 0 {
		sign = "-"
		u = -u
	}
	return fmt.Sprintf("%s %s%d.%02d", m.Currency, sign, u/100, u%100)
}

// Rounding selects how a fractional paisa result is resolved.
type Rounding int

const (
	// HalfUp rounds halves away from zero.
	HalfUp Rounding = iota
	// Down truncates toward zero.
	Down
)

// MulRatio returns amount*num/den using a 128-bit intermediate, so values like
// amount*basisPoints cannot overflow midway. den must be positive.
func MulRatio(amount, num, den int64, r Rounding) (int64, error) {
	if den <= 0 {
		return 0, ErrInvalidArgument
	}
	neg := (amount < 0) != (num < 0)
	hi, lo := bits.Mul64(abs(amount), abs(num))
	d := uint64(den)
	if hi >= d {
		return 0, ErrOverflow
	}
	q, rem := bits.Div64(hi, lo, d)
	if r == HalfUp && rem >= d-rem { // rem >= d/2 without overflow
		q++
	}
	if q > math.MaxInt64 {
		if neg && q == 1<<63 {
			return math.MinInt64, nil
		}
		return 0, ErrOverflow
	}
	if neg {
		return -int64(q), nil
	}
	return int64(q), nil
}

// Bps applies basis points (1% = 100 bps) to amount, rounding half up.
func Bps(amount, bps int64) (int64, error) { return MulRatio(amount, bps, 10000, HalfUp) }

// Split divides amount into n parts that differ by at most one paisa and sum
// exactly to amount; earlier parts take the remainder.
func Split(amount int64, n int) ([]int64, error) {
	if n <= 0 {
		return nil, ErrInvalidArgument
	}
	base, rem := amount/int64(n), amount%int64(n)
	step := int64(1)
	if rem < 0 {
		step, rem = -1, -rem
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = base
		if int64(i) < rem {
			out[i] += step
		}
	}
	return out, nil
}

func abs(v int64) uint64 {
	if v < 0 {
		return -uint64(v)
	}
	return uint64(v)
}
