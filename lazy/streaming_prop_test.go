package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestStreamingRegressions pins bugs TestLazyModesAgreeProp found in
// the streaming engine and one in fill_null. Expected values are what
// polars 1.39 returns for the same query (streaming and in-memory
// engines agree there):
//
//	df = pl.DataFrame({"id": range(20), "a": [1] * 20, "b": [0.0] * 19 + [1.0]})
//	df.lazy().with_columns(cs=pl.col("a").cum_sum()).collect()["cs"]  -> 1..20
//	df.lazy().filter(pl.col("b") > 0.5).select("id").collect()        -> id [19]
//	df.lazy().select("id", "a").filter(pl.col("a") > 5).collect()    -> 0 rows, columns id and a
//	df.lazy().filter(pl.col("a") > 5).with_columns(pl.col("a").fill_null(0)).collect()  -> 0 rows
func TestStreamingRegressions(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	ids := make([]int64, 20)
	ones := make([]int64, 20)
	bs := make([]float64, 20)
	for i := range ids {
		ids[i] = int64(i)
		ones[i] = 1
	}
	bs[19] = 1
	id, _ := series.FromInt64("id", ids, nil, series.WithAllocator(mem))
	a, _ := series.FromInt64("a", ones, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", bs, nil, series.WithAllocator(mem))
	df, err := dataframe.New(id, a, b)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	collect := func(name string, q lazy.LazyFrame) *dataframe.DataFrame {
		t.Helper()
		out, err := q.Collect(ctx, lazy.WithExecAllocator(mem), lazy.WithStreaming(), lazy.WithStreamingMorselRows(7))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return out
	}

	// cum_sum ran once per morsel and restarted at every morsel.
	out := collect("cum_sum", lf.WithColumns(expr.Col("a").CumSum().Alias("cs")))
	cs, _ := out.Column("cs")
	if got := cs.ToList(); got[19] != int64(20) || got[7] != int64(8) {
		t.Errorf("streaming cum_sum = %v, want 1..20", got)
	}
	out.Release()

	// The scan predicate ran after the pushed-down projection had
	// already dropped the column it reads.
	out = collect("filter then select", lf.Filter(expr.Col("b").Gt(expr.LitFloat64(0.5))).Select(expr.Col("id")))
	if got := out.ColumnAt(0).ToList(); out.Width() != 1 || len(got) != 1 || got[0] != int64(19) {
		t.Errorf("filter then select = %v", got)
	}
	out.Release()

	// A result with no rows lost its columns.
	out = collect("empty", lf.Select(expr.Col("id"), expr.Col("a")).Filter(expr.Col("a").Gt(expr.LitInt64(5))))
	if out.Height() != 0 || out.Width() != 2 {
		t.Errorf("empty streaming result has shape %dx%d, want 0x2", out.Height(), out.Width())
	}
	out.Release()

	// fill_null with a literal failed on a zero-row frame because the
	// broadcast literal had no value to read.
	out, err = lf.Filter(expr.Col("a").Gt(expr.LitInt64(5))).
		WithColumns(expr.Col("a").FillNull(int64(0))).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("fill_null on empty frame: %v", err)
	}
	if out.Height() != 0 || out.Width() != 3 {
		t.Errorf("fill_null on empty frame: shape %dx%d", out.Height(), out.Width())
	}
	out.Release()
}
