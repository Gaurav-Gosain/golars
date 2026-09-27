package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// Wrapped aggregations are split into plain aggregations plus a
// projection. The answer must match the eager group-by, which evaluates
// the wrapped expressions per group.
func TestAggWrapperLiftMatchesEager(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := morselFrame(t, series.WithAllocator(mem), 300)
	defer df.Release()
	x, k := expr.Col("x"), expr.Col("k")
	aggs := []expr.Expr{
		x.Mul(expr.LitFloat64(1).Sub(k)).Sum().Round(2).Alias("rev"),
		x.Sum().Div(x.Count()).Alias("avg"),
		expr.LitFloat64(100).Mul(x.Max()).Alias("scaled"),
		x.Min().Alias("plain"),
		x.Sum().IsNull().Alias("isnull"),
	}
	lf := lazy.FromDataFrame(df).GroupBy("g").Agg(aggs...)
	if !strings.Contains(lf.ExplainString(), "__aggw_") {
		t.Fatalf("wrappers not lifted:\n%s", lf.ExplainString())
	}
	got, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	want, err := df.GroupBy("g").Agg(ctx, aggs)
	if err != nil {
		t.Fatal(err)
	}
	defer want.Release()
	assertSameRows(t, got, want)
}
