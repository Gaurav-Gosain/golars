package lazy_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// The optimizer rewrites col.is_between(lit, lit) into two comparisons.
// Both forms must agree on bounds, nulls and NaN for every closed mode.
func TestIsBetweenRewriteMatchesKernel(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	nan := math.NaN()
	f, _ := series.FromFloat64("f", []float64{-1, 0, 0.5, 1, 2, nan, 0, math.Inf(1)},
		[]bool{true, true, true, true, true, true, false, true}, o)
	i, _ := series.FromInt64("i", []int64{-1, 0, 1, 2, 3, 0, 5, 1}, []bool{true, true, true, true, true, false, true, true}, o)
	d, _ := series.FromDate("d", []int32{9, 10, 11, 12, 13, 10, 11, 12}, []bool{true, true, true, true, true, true, false, true}, o)
	df, err := dataframe.New(f, i, d)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	type bounds struct {
		col    string
		lo, hi expr.Expr
	}
	for _, b := range []bounds{
		{"f", expr.LitFloat64(0), expr.LitFloat64(1)},
		{"f", expr.LitFloat64(0), expr.LitFloat64(nan)},
		{"f", expr.LitFloat64(1), expr.LitFloat64(0)},
		{"i", expr.LitInt64(0), expr.LitInt64(2)},
		{"i", expr.LitFloat64(0.5), expr.LitInt64(2)},
		{"d", expr.LitDate(1970, 1, 11), expr.LitDate(1970, 1, 13)},
	} {
		for _, c := range []expr.IntervalClosed{expr.ClosedBoth, expr.ClosedLeft, expr.ClosedRight, expr.ClosedNone} {
			lf := lazy.FromDataFrame(df).Select(expr.Col(b.col).IsBetween(b.lo, b.hi, c).Alias("r"))
			if !strings.Contains(lf.ExplainString(), ">") {
				t.Fatalf("rewrite did not fire:\n%s", lf.ExplainString())
			}
			got, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			want, err := lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				got.Release()
				t.Fatal(err)
			}
			if got.String() != want.String() {
				t.Errorf("%s between %s and %s closed=%s:\ngot\n%s\nwant\n%s", b.col, b.lo, b.hi, c, got, want)
			}
			got.Release()
			want.Release()
		}
	}
}
