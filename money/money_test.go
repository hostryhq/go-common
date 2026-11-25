package money

import (
	"errors"
	"math"
	"testing"
)

func TestBps(t *testing.T) {
	tests := []struct {
		name        string
		amount, bps int64
		want        int64
	}{
		{"5 percent of 100.00", 10000, 500, 500},
		{"half rounds up", 10, 500, 1}, // 0.5 paisa
		{"below half rounds down", 9, 500, 0},
		{"negative rounds away from zero", -10, 500, -1},
		{"large amount no overflow", math.MaxInt64 / 2, 10000, math.MaxInt64 / 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Bps(tc.amount, tc.bps)
			if err != nil || got != tc.want {
				t.Fatalf("Bps(%d,%d) = %d, %v; want %d", tc.amount, tc.bps, got, err, tc.want)
			}
		})
	}
}

func TestMulRatioErrors(t *testing.T) {
	if _, err := MulRatio(1, 1, 0, HalfUp); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("zero den: %v", err)
	}
	if _, err := MulRatio(math.MaxInt64, 2, 1, Down); !errors.Is(err, ErrOverflow) {
		t.Errorf("overflow: %v", err)
	}
}

func TestSplitSumsExactly(t *testing.T) {
	for _, amt := range []int64{100, 101, -101, 0, 7} {
		for n := 1; n <= 7; n++ {
			parts, err := Split(amt, n)
			if err != nil {
				t.Fatal(err)
			}
			var sum int64
			min, max := parts[0], parts[0]
			for _, p := range parts {
				sum += p
				if p < min {
					min = p
				}
				if p > max {
					max = p
				}
			}
			if sum != amt || max-min > 1 {
				t.Errorf("Split(%d,%d) = %v", amt, n, parts)
			}
		}
	}
}

func TestAddSubGuards(t *testing.T) {
	if _, err := New(math.MaxInt64).Add(New(1)); !errors.Is(err, ErrOverflow) {
		t.Errorf("add overflow: %v", err)
	}
	if _, err := New(math.MinInt64).Sub(New(1)); !errors.Is(err, ErrOverflow) {
		t.Errorf("sub overflow: %v", err)
	}
	if _, err := New(1).Add(Money{1, "USD"}); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("mismatch: %v", err)
	}
}

func TestString(t *testing.T) {
	if got := New(-123450).String(); got != "INR -1234.50" {
		t.Errorf("got %q", got)
	}
}
