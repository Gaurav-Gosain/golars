package radix

import (
	"math/rand/v2"
	"slices"
	"testing"
)

func TestSortUint64MatchesSlicesSort(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 2, 255, 256, 257, 5000, parallelCutoff - 1, parallelCutoff, 300_001} {
		for _, sig := range []int{1, 7, 11, 12, 22, 23, 38, 63, 64} {
			vals := make([]uint64, n)
			for i := range vals {
				v := r.Uint64()
				if sig < 64 {
					v &= (uint64(1) << sig) - 1
				}
				vals[i] = v
			}
			want := slices.Clone(vals)
			slices.Sort(want)
			SortUint64(vals, sig)
			if !slices.Equal(vals, want) {
				t.Fatalf("n=%d sig=%d: mismatch", n, sig)
			}
		}
	}
}

func TestSortUint64ConstantDigits(t *testing.T) {
	t.Parallel()
	// Every digit but one is constant: skipped passes must still leave
	// the data in vals.
	for _, n := range []int{1000, parallelCutoff + 5} {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = uint64(n-i)<<33 | 0x5
		}
		want := slices.Clone(vals)
		slices.Sort(want)
		SortUint64(vals, 64)
		if !slices.Equal(vals, want) {
			t.Fatalf("n=%d: mismatch", n)
		}
	}
}
