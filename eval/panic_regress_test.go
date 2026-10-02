package eval_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Regression tests for panics found by the differential harness
// (internal/difftest). Expected values come from polars 1.39.3.

// polars: pl.DataFrame({"c0": []}, schema={"c0": pl.Int64})
// .select(pl.col("c0").is_between(2, 2)) is an empty bool column.
func TestIsBetweenEmptyFrame(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	c0, _ := series.FromInt64("c0", nil, nil, series.WithAllocator(mem))
	df, err := dataframe.New(c0)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	e := expr.Col("c0").IsBetween(expr.LitInt64(2), expr.LitInt64(2), expr.IntervalClosed("both"))
	if _, got := evalRepr(t, mem, df, e); got != "[]" {
		t.Errorf("is_between on empty frame = %s, want []", got)
	}
}
