package eval_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars 1.39: a group whose values are all null has sum 0 and a null
// mean, min and max under over().
func TestOverAllNullGroup(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	g, _ := series.FromInt64("g", []int64{1, 1, 2, 2}, nil, o)
	x, _ := series.FromFloat64("x", []float64{0, 0, 1, 0}, []bool{false, false, true, false}, o)
	df, _ := dataframe.New(g, x)
	defer df.Release()
	want := map[string][]any{
		"sum":   {0.0, 0.0, 1.0, 1.0},
		"mean":  {nil, nil, 1.0, 1.0},
		"min":   {nil, nil, 1.0, 1.0},
		"max":   {nil, nil, 1.0, 1.0},
		"count": {uint32(0), uint32(0), uint32(1), uint32(1)},
	}
	col := expr.Col("x")
	exprs := map[string]expr.Expr{
		"sum": col.Sum(), "mean": col.Mean(), "min": col.Min(), "max": col.Max(), "count": col.Count(),
	}
	for name, e := range exprs {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e.Over("g"), df)
		if err != nil {
			t.Fatal(err)
		}
		for i, w := range want[name] {
			if v, _ := out.Get(i); fmt.Sprint(v) != fmt.Sprint(w) {
				t.Errorf("%s row %d: got %v want %v", name, i, v, w)
			}
		}
		out.Release()
	}
}
