//go:build arm64 && !noasm

package compute

import (
	"math"
	"math/rand/v2"
	"testing"
)

// neonTestLengths covers every tail shape of the 16-wide main loops
// plus a few sizes that span many iterations.
func neonTestLengths() []int {
	var out []int
	for n := range 70 {
		out = append(out, n)
	}
	return append(out, 127, 128, 129, 1000, 4097, 65537)
}

func randInt64Slice(r *rand.Rand, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		switch r.IntN(8) {
		case 0:
			out[i] = math.MinInt64 + r.Int64N(4)
		case 1:
			out[i] = math.MaxInt64 - r.Int64N(4)
		default:
			out[i] = r.Int64() - r.Int64()
		}
	}
	return out
}

// unalignedViews returns the slice itself plus views that start one
// and three elements in, so the kernels see 8-byte-aligned but not
// 16-byte-aligned base pointers.
func unalignedViews[T any](s []T) [][]T {
	out := [][]T{s}
	if len(s) > 1 {
		out = append(out, s[1:])
	}
	if len(s) > 4 {
		out = append(out, s[3:len(s)-1])
	}
	return out
}

func TestNEONSumInt64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range neonTestLengths() {
		for _, v := range unalignedViews(randInt64Slice(r, n+3)) {
			var want int64
			for _, x := range v {
				want += x
			}
			if got := simdSumInt64NEON(v); got != want {
				t.Fatalf("n=%d: got %d want %d", len(v), got, want)
			}
		}
	}
}

func TestNEONSumFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, n := range neonTestLengths() {
		// Small integers sum exactly in any order, so the result must
		// match the scalar sum bit for bit.
		exact := make([]float64, n+3)
		for i := range exact {
			exact[i] = float64(r.IntN(2001) - 1000)
		}
		for _, v := range unalignedViews(exact) {
			var want float64
			for _, x := range v {
				want += x
			}
			if got := simdSumFloat64NEON(v); got != want {
				t.Fatalf("n=%d: got %v want %v", len(v), got, want)
			}
		}
		// Non-finite inputs must propagate.
		if n > 0 {
			v := append([]float64(nil), exact[:n]...)
			v[r.IntN(n)] = math.Inf(1)
			if got := simdSumFloat64NEON(v); !math.IsInf(got, 1) {
				t.Fatalf("n=%d: +Inf lost, got %v", n, got)
			}
			v[r.IntN(n)] = math.NaN()
			if got := simdSumFloat64NEON(v); !math.IsNaN(got) {
				t.Fatalf("n=%d: NaN lost, got %v", n, got)
			}
		}
	}
}

func TestNEONMinMaxFloat64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for _, n := range neonTestLengths() {
		if n == 0 {
			continue
		}
		base := make([]float64, n+3)
		for i := range base {
			base[i] = r.NormFloat64() * 1e6
		}
		if n > 4 {
			base[r.IntN(n)] = math.Inf(-1)
			base[r.IntN(n)] = math.Inf(1)
		}
		for _, v := range unalignedViews(base) {
			wantMin, wantMax := v[0], v[0]
			for _, x := range v {
				wantMin = min(wantMin, x)
				wantMax = max(wantMax, x)
			}
			gotMin, nanMin := simdMinFloat64NEON(v)
			gotMax, nanMax := simdMaxFloat64NEON(v)
			if gotMin != wantMin || nanMin {
				t.Fatalf("min n=%d: got %v,%v want %v", len(v), gotMin, nanMin, wantMin)
			}
			if gotMax != wantMax || nanMax {
				t.Fatalf("max n=%d: got %v,%v want %v", len(v), gotMax, nanMax, wantMax)
			}
		}
		// A NaN anywhere must be reported.
		for _, pos := range []int{0, n / 2, n - 1} {
			v := append([]float64(nil), base[:n]...)
			v[pos] = math.NaN()
			if _, nan := simdMinFloat64NEON(v); !nan {
				t.Fatalf("min n=%d: NaN at %d not reported", n, pos)
			}
			if _, nan := simdMaxFloat64NEON(v); !nan {
				t.Fatalf("max n=%d: NaN at %d not reported", n, pos)
			}
		}
	}
}

func TestNEONMinMaxInt64MatchesScalar(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	for _, n := range neonTestLengths() {
		if n == 0 {
			continue
		}
		for _, v := range unalignedViews(randInt64Slice(r, n+3)) {
			if got, want := simdMinInt64NEON(v), minMaxInt64Scalar(v, false); got != want {
				t.Fatalf("min n=%d: got %d want %d", len(v), got, want)
			}
			if got, want := simdMaxInt64NEON(v), minMaxInt64Scalar(v, true); got != want {
				t.Fatalf("max n=%d: got %d want %d", len(v), got, want)
			}
		}
	}
}

func FuzzNEONReductions(f *testing.F) {
	f.Add(uint64(1), uint16(17), uint8(0))
	f.Add(uint64(2), uint16(1000), uint8(3))
	f.Fuzz(func(t *testing.T, seed uint64, n uint16, off uint8) {
		r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		iv := randInt64Slice(r, int(n)+int(off%4))[off%4:]
		var want int64
		for _, x := range iv {
			want += x
		}
		if got := simdSumInt64NEON(iv); got != want {
			t.Fatalf("sum: got %d want %d", got, want)
		}
		if len(iv) == 0 {
			return
		}
		if got, want := simdMinInt64NEON(iv), minMaxInt64Scalar(iv, false); got != want {
			t.Fatalf("min: got %d want %d", got, want)
		}
		if got, want := simdMaxInt64NEON(iv), minMaxInt64Scalar(iv, true); got != want {
			t.Fatalf("max: got %d want %d", got, want)
		}
	})
}
