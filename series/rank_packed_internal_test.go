package series

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestRankPackedMatchesIndirect checks the packed-radix rank against
// the indirect argsort + tie walk it short-circuits, for every method,
// across duplicate-heavy, negative, wide-span and parallel-size inputs.
func TestRankPackedMatchesIndirect(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(3, 4))
	gens := map[string]func(i int) int64{
		"dups":     func(int) int64 { return r.Int64N(50) },
		"negative": func(int) int64 { return r.Int64N(1<<20) - 1<<19 },
		"wide":     func(int) int64 { return r.Int64() - r.Int64() },
		"sorted":   func(i int) int64 { return int64(i / 3) },
	}
	methods := []RankMethod{RankAverage, RankMin, RankMax, RankDense, RankOrdinal}
	for name, gen := range gens {
		for _, n := range []int{1, 2, 63, 64, 1000, 200_000} {
			vals := make([]int64, n)
			for i := range vals {
				vals[i] = gen(i)
			}
			// Indirect reference: stable argsort then rank walk.
			idx := make([]int, n)
			for i := range idx {
				idx[i] = i
			}
			argRadixSortInt64Local(vals, idx)
			for _, m := range methods {
				want, err := rankSortedInt64("x", vals, idx, n, m, memory.DefaultAllocator)
				if err != nil {
					t.Fatal(err)
				}
				got, ok, err := rankPackedInt64("x", vals, m, memory.DefaultAllocator)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					// Only spans that cannot pack may decline.
					if name != "wide" {
						t.Fatalf("%s n=%d: packed path declined", name, n)
					}
					want.Release()
					continue
				}
				w := want.Chunk(0).(*array.Float64).Float64Values()
				g := got.Chunk(0).(*array.Float64).Float64Values()
				if !slices.Equal(w, g) {
					t.Fatalf("%s n=%d method=%d: ranks differ", name, n, m)
				}
				want.Release()
				got.Release()
			}
		}
	}
}

func TestPackInt64KeysExtremes(t *testing.T) {
	t.Parallel()
	// Full int64 span cannot pack with any index bits.
	if _, _, _, ok := packInt64Keys([]int64{math.MinInt64, math.MaxInt64}); ok {
		t.Fatal("full-span input should not pack")
	}
	// A span that needs exactly 63 bits plus one index bit packs.
	vals := []int64{math.MinInt64 / 2, math.MaxInt64 / 2}
	packed, idxBits, sig, ok := packInt64Keys(vals)
	if !ok || idxBits != 1 || sig != 64 {
		t.Fatalf("got ok=%v idxBits=%d sig=%d", ok, idxBits, sig)
	}
	if packed[0] >= packed[1] {
		t.Fatal("packed order does not follow value order")
	}
	idx, ok := argSortInt64Packed([]int64{5, -3, 5, -3, 0})
	if !ok || !slices.Equal(idx, []int{1, 3, 4, 0, 2}) {
		t.Fatalf("argsort = %v, want [1 3 4 0 2] (stable)", idx)
	}
}
