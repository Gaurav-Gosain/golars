package compute_test

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// aggCase builds an int64 and a float64 Series over the same values,
// with optional nulls, sliced at off so the arrow offset is non-zero.
type aggCase struct {
	vals  []int64
	valid []bool
}

func (c aggCase) refSum() (int64, int) {
	var s int64
	cnt := 0
	for i, v := range c.vals {
		if c.valid == nil || c.valid[i] {
			s += v
			cnt++
		}
	}
	return s, cnt
}

func (c aggCase) refMinMax() (lo, hi int64, ok bool) {
	for i, v := range c.vals {
		if c.valid != nil && !c.valid[i] {
			continue
		}
		if !ok {
			lo, hi, ok = v, v, true
			continue
		}
		lo, hi = min(lo, v), max(hi, v)
	}
	return lo, hi, ok
}

func newAggCase(r *rand.Rand, n int, nulls bool) aggCase {
	c := aggCase{vals: make([]int64, n)}
	for i := range c.vals {
		// Small magnitudes keep float64 sums exact.
		c.vals[i] = r.Int64N(1<<20) - 1<<19
	}
	if nulls {
		c.valid = make([]bool, n)
		for i := range c.valid {
			// Runs of all-valid and all-null bytes plus mixed ones.
			switch (i / 64) % 3 {
			case 0:
				c.valid[i] = true
			case 1:
				c.valid[i] = false
			default:
				c.valid[i] = r.IntN(3) != 0
			}
		}
	}
	return c
}

// sizes around the serial/parallel and SIMD block cutoffs.
var aggSizes = []int{0, 1, 7, 15, 16, 17, 33, 1000, 16*1024 - 1, 16 * 1024, 16*1024 + 5, 256*1024 - 3, 256*1024 + 3}

func TestAggKernelsAgainstReference(t *testing.T) {
	ctx := context.Background()
	r := rand.New(rand.NewPCG(21, 22))
	for _, n := range aggSizes {
		for _, nulls := range []bool{false, true} {
			for _, off := range []int{0, 3} {
				mem := testutil.NewCheckedAllocator(t)
				c := newAggCase(r, n+off, nulls)
				fv := make([]float64, len(c.vals))
				for i, v := range c.vals {
					fv[i] = float64(v)
				}
				fullI, _ := series.FromInt64("x", c.vals, c.valid, series.WithAllocator(mem))
				fullF, _ := series.FromFloat64("x", fv, c.valid, series.WithAllocator(mem))
				si, err := fullI.Slice(off, n)
				if err != nil {
					t.Fatal(err)
				}
				sf, _ := fullF.Slice(off, n)
				view := aggCase{vals: c.vals[off:]}
				if c.valid != nil {
					view.valid = c.valid[off:]
				}
				wantSum, cnt := view.refSum()
				lo, hi, ok := view.refMinMax()

				gotSum, err := compute.SumInt64(ctx, si, compute.WithAllocator(mem))
				if err != nil || gotSum != wantSum {
					t.Fatalf("SumInt64 n=%d nulls=%v off=%d: got %d, %v want %d", n, nulls, off, gotSum, err, wantSum)
				}
				gotF, err := compute.SumFloat64(ctx, sf, compute.WithAllocator(mem))
				if err != nil || gotF != float64(wantSum) {
					t.Fatalf("SumFloat64 n=%d nulls=%v off=%d: got %v, %v want %d", n, nulls, off, gotF, err, wantSum)
				}
				if cnt > 0 {
					mean, mok, _ := compute.MeanFloat64(ctx, sf, compute.WithAllocator(mem))
					if !mok || math.Abs(mean-float64(wantSum)/float64(cnt)) > 1e-9 {
						t.Fatalf("MeanFloat64 n=%d: got %v", n, mean)
					}
				}
				gLo, gok, _ := compute.MinInt64(ctx, si, compute.WithAllocator(mem))
				gHi, _, _ := compute.MaxInt64(ctx, si, compute.WithAllocator(mem))
				if gok != ok || (ok && (gLo != lo || gHi != hi)) {
					t.Fatalf("Min/MaxInt64 n=%d nulls=%v off=%d: got %d %d %v want %d %d %v", n, nulls, off, gLo, gHi, gok, lo, hi, ok)
				}
				fLo, fok, _ := compute.MinFloat64(ctx, sf, compute.WithAllocator(mem))
				fHi, _, _ := compute.MaxFloat64(ctx, sf, compute.WithAllocator(mem))
				if fok != ok || (ok && (fLo != float64(lo) || fHi != float64(hi))) {
					t.Fatalf("Min/MaxFloat64 n=%d nulls=%v off=%d: got %v %v %v want %d %d %v", n, nulls, off, fLo, fHi, fok, lo, hi, ok)
				}
				si.Release()
				sf.Release()
				fullI.Release()
				fullF.Release()
			}
		}
	}
}

func TestMinMaxFloat64NaNAtCutoffSizes(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{16, 17, 16 * 1024, 256*1024 + 1} {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = float64(i % 1000)
		}
		vals[n/2] = math.NaN()
		s, _ := series.FromFloat64("x", vals, nil)
		lo, _, _ := compute.MinFloat64(ctx, s)
		hi, _, _ := compute.MaxFloat64(ctx, s)
		if lo != 0 || hi != 999 && n > 1000 {
			t.Fatalf("n=%d: NaN not ignored, min %v max %v", n, lo, hi)
		}
		for i := range vals {
			vals[i] = math.NaN()
		}
		all, _ := series.FromFloat64("x", vals, nil)
		if v, ok, _ := compute.MinFloat64(ctx, all); !ok || !math.IsNaN(v) {
			t.Fatalf("n=%d: all-NaN min = %v", n, v)
		}
		s.Release()
		all.Release()
	}
}
