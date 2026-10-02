package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// is_between evaluates its bounds as scalars. Date literals used to fail
// there with "cannot store 1994-01-01 as date".
func TestIsBetweenDateLiterals(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	// 8766 = 1994-01-01, 9131 = 1995-01-01.
	d, _ := series.FromDate("d", []int32{8765, 8766, 9000, 9131, 9200}, []bool{true, true, true, true, false}, series.WithAllocator(mem))
	df, _ := dataframe.New(d)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("d").IsBetween(expr.LitDate(1994, 1, 1), expr.LitDate(1995, 1, 1), expr.ClosedLeft).Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Boolean)
	want := []bool{false, true, true, false}
	for i, w := range want {
		if arr.IsNull(i) || arr.Value(i) != w {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), w)
		}
	}
	if !arr.IsNull(4) {
		t.Fatal("null row should stay null")
	}
}
