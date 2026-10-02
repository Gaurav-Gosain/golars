package compute_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestFilterStringByBitmap checks the fused string filter against the
// mask, for sparse, dense, empty and full masks, sliced sources, and
// lengths that are not multiples of 64 on both sides of the parallel
// cutoff.
func TestFilterStringByBitmap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(2, 3))
	for _, n := range []int{0, 1, 63, 65, 1000, 64*1024 + 7, 300_001} {
		for _, density := range []float64{0, 0.05, 0.7, 1} {
			vals := make([]string, n+3)
			for i := range vals {
				if r.IntN(5) == 0 {
					vals[i] = ""
				} else {
					vals[i] = fmt.Sprintf("s%dé", r.IntN(1_000_000))
				}
			}
			keep := make([]bool, n)
			for i := range keep {
				keep[i] = r.Float64() < density
			}
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			full, _ := series.FromString("s", vals, nil, series.WithAllocator(mem))
			src, err := full.Slice(3, n)
			if err != nil {
				t.Fatal(err)
			}
			mask, _ := series.FromBool("m", keep, nil, series.WithAllocator(mem))
			out, err := compute.Filter(ctx, src, mask, compute.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			a := out.Chunk(0).(*array.String)
			j := 0
			for i, k := range keep {
				if !k {
					continue
				}
				if j >= a.Len() || a.Value(j) != vals[i+3] {
					t.Fatalf("n=%d density=%v: output row %d wrong", n, density, j)
				}
				j++
			}
			if j != a.Len() {
				t.Fatalf("n=%d density=%v: len %d, want %d", n, density, a.Len(), j)
			}
			out.Release()
			mask.Release()
			src.Release()
			full.Release()
			mem.AssertSize(t, 0)
		}
	}
}

// TestTakeStringDirect checks the direct-buffer string gather against
// per-row expectations: nulls, empty strings, non-ASCII, repeated and
// out-of-order indices, a sliced source, and sizes on both sides of the
// parallel cutoff (including lengths that are not multiples of 8).
func TestTakeStringDirect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 1))
	for _, n := range []int{0, 1, 9, 1000, 64*1024 + 3, 200_001} {
		vals := make([]string, n+10)
		valid := make([]bool, n+10)
		for i := range vals {
			switch r.IntN(4) {
			case 0:
				vals[i] = ""
			case 1:
				vals[i] = "héllo wörld"
			default:
				vals[i] = fmt.Sprintf("v%d", r.IntN(1_000_000))
			}
			valid[i] = r.IntN(7) != 0
		}
		for _, withNulls := range []bool{false, true} {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			var v []bool
			if withNulls {
				v = valid
			}
			full, _ := series.FromString("s", vals, v, series.WithAllocator(mem))
			// Slice off the first 10 rows so the source has an offset.
			src, err := full.Slice(10, n)
			if err != nil {
				t.Fatal(err)
			}
			idx := make([]int, n+n/2)
			for j := range idx {
				idx[j] = r.IntN(max(n, 1))
			}
			if n == 0 {
				idx = nil
			}
			out, err := compute.Take(ctx, src, idx, compute.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			a := out.Chunk(0).(*array.String)
			if a.Len() != len(idx) {
				t.Fatalf("len %d, want %d", a.Len(), len(idx))
			}
			for j, i := range idx {
				wantNull := withNulls && !valid[i+10]
				if a.IsNull(j) != wantNull {
					t.Fatalf("n=%d row %d: null=%v want %v", n, j, a.IsNull(j), wantNull)
				}
				if !wantNull && a.Value(j) != vals[i+10] {
					t.Fatalf("n=%d row %d: %q want %q", n, j, a.Value(j), vals[i+10])
				}
			}
			out.Release()
			src.Release()
			full.Release()
			mem.AssertSize(t, 0)
		}
	}
}
