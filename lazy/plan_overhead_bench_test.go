package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkPlanOverhead measures optimizer plus executor cost on a tiny
// frame, where per-node work (schema inference, expression rewriting,
// dispatch) dominates the kernels.
func BenchmarkPlanOverhead(b *testing.B) {
	n := 256
	k := make([]int64, n)
	v := make([]float64, n)
	s := make([]string, n)
	for i := range n {
		k[i] = int64(i % 7)
		v[i] = float64(i)
		s[i] = []string{"a", "b"}[i%2]
	}
	ks, _ := series.FromInt64("k", k, nil)
	vs, _ := series.FromFloat64("v", v, nil)
	ss, _ := series.FromString("s", s, nil)
	df, _ := dataframe.New(ks, vs, ss)
	defer df.Release()
	other, _ := dataframe.New(func() *series.Series { x, _ := series.FromInt64("k", []int64{0, 1, 2, 3}, nil); return x }(),
		func() *series.Series { x, _ := series.FromString("name", []string{"z", "o", "t", "h"}, nil); return x }())
	defer other.Release()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		out, err := lazy.FromDataFrame(df).
			Filter(expr.Col("v").Gt(expr.LitFloat64(10))).
			WithColumns(expr.Col("v").Mul(expr.LitFloat64(2)).Alias("v2")).
			WithColumns(expr.Col("v2").Add(expr.Col("v")).Alias("v3")).
			Join(lazy.FromDataFrame(other), []string{"k"}, dataframe.InnerJoin).
			GroupBy("name", "s").
			Agg(expr.Col("v3").Sum().Alias("t"), expr.Col("v").Mean().Round(1).Alias("m")).
			Sort("t", true).
			Collect(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
