package series

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestTopKInt64NoNullMatchesHeap checks the threshold scan (serial and
// per-worker merge) against the general heap path, including heavy ties
// where only the row-index tie break decides membership.
func TestTopKInt64NoNullMatchesHeap(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(9, 10))
	for _, n := range []int{1, 5, 100, topKParallelCutoff - 1, topKParallelCutoff, 300_000} {
		for _, bound := range []int64{3, 1000, 1 << 40} {
			vals := make([]int64, n)
			for i := range vals {
				vals[i] = r.Int64N(bound) - bound/2
			}
			s, _ := FromInt64("x", vals, nil)
			a := s.Chunk(0).(*array.Int64)
			for _, k := range []int{1, 10, 100} {
				if k >= n {
					continue
				}
				for _, desc := range []bool{true, false} {
					want := heapTopKInt64(a, n, k, desc)
					got := topKInt64NoNull(vals, k, desc)
					if !slices.Equal(got, want) {
						t.Fatalf("n=%d bound=%d k=%d desc=%v: got %v want %v", n, bound, k, desc, got[:min(5, len(got))], want[:min(5, len(want))])
					}
				}
			}
			s.Release()
		}
	}
}
