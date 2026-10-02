package eval_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Expression evaluation benchmarks for common shapes, at a large height
// (kernel cost) and a morsel-sized one (per-evaluation overhead, which
// the streaming engine pays once per morsel).

func exprBenchFrame(b *testing.B, n int) *dataframe.DataFrame {
	b.Helper()
	x := make([]float64, n)
	k := make([]int64, n)
	g := make([]int64, n)
	s := make([]string, n)
	for i := range n {
		x[i] = float64(i%1000) / 7
		k[i] = int64(i * 7919 % 1000)
		g[i] = int64(i % 64)
		s[i] = fmt.Sprintf("item_%d_%s", i%97, []string{"red", "green", "blue"}[i%3])
	}
	xs, _ := series.FromFloat64("x", x, nil)
	ks, _ := series.FromInt64("k", k, nil)
	gs, _ := series.FromInt64("g", g, nil)
	ss, _ := series.FromString("s", s, nil)
	df, err := dataframe.New(xs, ks, gs, ss)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func BenchmarkExprEval(b *testing.B) {
	x, k := expr.Col("x"), expr.Col("k")
	cases := map[string]expr.Expr{
		"arith_chain": x.Mul(expr.LitFloat64(2)).Add(k).Div(expr.LitFloat64(3)).Sub(x),
		"when_then": expr.When(k.Gt(expr.LitInt64(500))).Then(x).
			Otherwise(x.Neg()),
		"str_contains": expr.Col("s").Str().Contains("green"),
		"over_sum":     x.Sum().Over("g"),
		"filter_pred":  x.Gt(expr.LitFloat64(50)).And(k.Lt(expr.LitInt64(900))),
		"disc_price":   x.Mul(expr.LitFloat64(1).Sub(x.Div(expr.LitFloat64(1000)))),
	}
	for _, n := range []int{64 << 10, 1 << 20} {
		df := exprBenchFrame(b, n)
		ctx := context.Background()
		for name, e := range cases {
			b.Run(fmt.Sprintf("%s/%d", name, n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := eval.Eval(ctx, eval.EvalContext{}, e, df)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
		df.Release()
	}
}
