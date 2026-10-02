package compute

import (
	"math/rand/v2"
	"testing"
)

// TestGatherIntoMatchesLoop checks the 8-byte fast path against a plain
// loop for lengths around the 8-row block size and for repeated and
// boundary indices.
func TestGatherIntoMatchesLoop(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	src := make([]int64, 100)
	for i := range src {
		src[i] = r.Int64()
	}
	fsrc := make([]float64, len(src))
	for i := range fsrc {
		fsrc[i] = r.NormFloat64()
	}
	for _, n := range []int{0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 250} {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = r.IntN(len(src))
		}
		if n > 2 {
			idx[0] = 0
			idx[n-1] = len(src) - 1
		}
		out := make([]int64, n)
		gatherInto(out, src, idx)
		fout := make([]float64, n)
		gatherInto(fout, fsrc, idx)
		i32 := make([]int32, n)
		src32 := make([]int32, len(src))
		for i := range src32 {
			src32[i] = int32(src[i])
		}
		gatherInto(i32, src32, idx)
		for j, i := range idx {
			if out[j] != src[i] || fout[j] != fsrc[i] || i32[j] != src32[i] {
				t.Fatalf("n=%d j=%d: got %d %v %d want %d %v %d", n, j, out[j], fout[j], i32[j], src[i], fsrc[i], src32[i])
			}
		}
	}
}

// TestGatherIntoOutOfRangePanics checks that a bad index anywhere in a
// block, including a negative one, still panics instead of reading
// outside src.
func TestGatherIntoOutOfRangePanics(t *testing.T) {
	src := make([]int64, 32)
	for _, bad := range []int{32, 1 << 40, -1, -(1 << 62)} {
		for _, pos := range []int{0, 3, 7, 8, 13, 23} {
			idx := make([]int, 24)
			for i := range idx {
				idx[i] = i
			}
			idx[pos] = bad
			func() {
				defer func() {
					if recover() == nil {
						t.Fatalf("bad=%d pos=%d: expected panic", bad, pos)
					}
				}()
				gatherInto(make([]int64, len(idx)), src, idx)
			}()
		}
	}
}
