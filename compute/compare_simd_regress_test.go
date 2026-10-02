package compute_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Every comparison op at sizes that reach the SIMD kernels must agree
// with plain Go comparisons. The arm64 float kernels once had every
// ordering op reversed, which small tests never reached.
func TestCompareOpsAtSIMDSizes(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	type cmpFn func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error)
	type litFn func(context.Context, *series.Series, any, ...compute.Option) (*series.Series, error)
	ops := []struct {
		name string
		col  cmpFn
		lit  litFn
		ref  func(a, b float64) bool
	}{
		{"eq", compute.Eq, compute.EqLit, func(a, b float64) bool { return a == b }},
		{"ne", compute.Ne, compute.NeLit, func(a, b float64) bool { return a != b }},
		{"lt", compute.Lt, compute.LtLit, func(a, b float64) bool { return a < b }},
		{"le", compute.Le, compute.LeLit, func(a, b float64) bool { return a <= b }},
		{"gt", compute.Gt, compute.GtLit, func(a, b float64) bool { return a > b }},
		{"ge", compute.Ge, compute.GeLit, func(a, b float64) bool { return a >= b }},
	}
	check := func(t *testing.T, label string, out *series.Series, want func(i int) bool) {
		t.Helper()
		arr := out.Chunk(0).(*array.Boolean)
		for i := range arr.Len() {
			if arr.Value(i) != want(i) {
				t.Fatalf("%s: row %d = %v, want %v", label, i, arr.Value(i), want(i))
			}
		}
		out.Release()
	}
	for _, n := range []int{8, 16, 33, 1000, 200_003} {
		af := make([]float64, n)
		bf := make([]float64, n)
		ai := make([]int64, n)
		bi := make([]int64, n)
		for i := range n {
			af[i], bf[i] = float64(i%10), float64((i*7)%10)
			ai[i], bi[i] = int64(i%10), int64((i*7)%10)
		}
		sa, _ := series.FromFloat64("a", af, nil, series.WithAllocator(mem))
		sb, _ := series.FromFloat64("b", bf, nil, series.WithAllocator(mem))
		ia, _ := series.FromInt64("a", ai, nil, series.WithAllocator(mem))
		ib, _ := series.FromInt64("b", bi, nil, series.WithAllocator(mem))
		for _, op := range ops {
			out, err := op.col(ctx, sa, sb, compute.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			check(t, "f64 col "+op.name, out, func(i int) bool { return op.ref(af[i], bf[i]) })
			out, _ = op.lit(ctx, sa, 4.0, compute.WithAllocator(mem))
			check(t, "f64 lit "+op.name, out, func(i int) bool { return op.ref(af[i], 4) })
			out, _ = op.col(ctx, ia, ib, compute.WithAllocator(mem))
			check(t, "i64 col "+op.name, out, func(i int) bool { return op.ref(float64(ai[i]), float64(bi[i])) })
			out, _ = op.lit(ctx, ia, int64(4), compute.WithAllocator(mem))
			check(t, "i64 lit "+op.name, out, func(i int) bool { return op.ref(float64(ai[i]), 4) })
		}
		sa.Release()
		sb.Release()
		ia.Release()
		ib.Release()
	}
}
