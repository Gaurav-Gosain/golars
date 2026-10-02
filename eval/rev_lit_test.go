package eval_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// lit - col and lit / col run without broadcasting the literal.
func TestLiteralLeftSubDiv(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	f, _ := series.FromFloat64("f", []float64{0.25, 0, math.NaN(), -2, 9}, []bool{true, true, true, true, false}, o)
	i, _ := series.FromInt64("i", []int64{3, math.MinInt64, 0, -7, 1}, []bool{true, true, true, true, false}, o)
	df, _ := dataframe.New(f, i)
	defer df.Release()
	ctx := context.Background()
	cases := []struct {
		e    expr.Expr
		want []string
	}{
		{expr.LitFloat64(1).Sub(expr.Col("f")), []string{"0.75", "1", "NaN", "3", "<nil>"}},
		{expr.LitInt64(1).Sub(expr.Col("f")), []string{"0.75", "1", "NaN", "3", "<nil>"}},
		{expr.LitFloat64(1).Div(expr.Col("f")), []string{"4", "+Inf", "NaN", "-0.5", "<nil>"}},
		{expr.LitInt64(10).Sub(expr.Col("i")), []string{"7", "-9223372036854775798", "10", "17", "<nil>"}},
		{expr.LitFloat64(0.5).Sub(expr.Col("i")), []string{"-2.5", fmt.Sprint(0.5 - float64(math.MinInt64)), "0.5", "7.5", "<nil>"}},
	}
	// Shapes revArithLit does not cover take the generic operand path
	// and must match it exactly; wrapping the literal in a cast forces
	// the generic path for the reference.
	k, _ := series.FromInt32("k", []int32{3, -4, 0, 9}, []bool{true, true, false, true}, o)
	df32, _ := dataframe.New(k)
	defer df32.Release()
	for _, mk := range []func(expr.Expr) expr.Expr{
		func(l expr.Expr) expr.Expr { return l.Sub(expr.Col("k")) },
		func(l expr.Expr) expr.Expr { return l.Div(expr.Col("k")) },
	} {
		got, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, mk(expr.LitInt64(10)), df32)
		if err != nil {
			t.Fatal(err)
		}
		want, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, mk(expr.LitInt64(10).Cast(expr.LitInt64(0).Node().(expr.LitNode).DType)), df32)
		if err != nil {
			t.Fatal(err)
		}
		gr, wr := got.Rename("r"), want.Rename("r")
		same := gr.String() == wr.String() && got.DType().Equal(want.DType())
		gr.Release()
		wr.Release()
		if !same {
			t.Errorf("fallback differs:\n%s\n%s", got, want)
		}
		got.Release()
		want.Release()
	}
	for _, tc := range cases {
		out, err := eval.Eval(ctx, eval.EvalContext{Alloc: mem}, tc.e, df)
		if err != nil {
			t.Fatalf("%s: %v", tc.e, err)
		}
		for r, w := range tc.want {
			if v, _ := out.Get(r); fmt.Sprint(v) != w {
				t.Errorf("%s row %d: got %v want %s", tc.e, r, v, w)
			}
		}
		out.Release()
	}
}
