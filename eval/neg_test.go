package eval_test

import (
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestNegInt64Float64(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	i, _ := series.FromInt64("i", []int64{1, -2, 0, math.MinInt64, 5}, []bool{true, true, true, true, false}, o)
	f, _ := series.FromFloat64("f", []float64{1.5, -2, math.NaN(), math.Inf(1), 3}, []bool{true, true, true, true, false}, o)
	df, _ := dataframe.New(i, f)
	defer df.Release()
	ctx := context.Background()

	gi, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("i").Neg(), df)
	if err != nil {
		t.Fatal(err)
	}
	defer gi.Release()
	for r, w := range []any{int64(-1), int64(2), int64(0), int64(math.MinInt64), nil} {
		if v, _ := gi.Get(r); v != w {
			t.Fatalf("i row %d: got %v want %v", r, v, w)
		}
	}
	if gi.Name() != "i" {
		t.Fatalf("name = %q", gi.Name())
	}
	gf, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, expr.Col("f").Neg(), df)
	if err != nil {
		t.Fatal(err)
	}
	defer gf.Release()
	v0, _ := gf.Get(0)
	v2, _ := gf.Get(2)
	v3, _ := gf.Get(3)
	v4, _ := gf.Get(4)
	if v0 != -1.5 || !math.IsNaN(v2.(float64)) || v3 != math.Inf(-1) || v4 != nil {
		t.Fatalf("f = %v %v %v %v", v0, v2, v3, v4)
	}
}
