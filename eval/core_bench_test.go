package eval_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

const coreBenchRows = 1 << 20

func coreBenchFrame(b *testing.B) *dataframe.DataFrame {
	b.Helper()
	r := rand.New(rand.NewPCG(42, 43))
	x := make([]float64, coreBenchRows)
	k := make([]int64, coreBenchRows)
	g := make([]int64, coreBenchRows)
	for i := range x {
		x[i] = r.Float64() * 100
		k[i] = r.Int64N(1000)
		g[i] = r.Int64N(64)
	}
	xs, _ := series.FromFloat64("x", x, nil)
	ks, _ := series.FromInt64("k", k, nil)
	gs, _ := series.FromInt64("g", g, nil)
	df, err := dataframe.New(xs, ks, gs)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func benchSelect(b *testing.B, e expr.Expr) {
	df := coreBenchFrame(b)
	defer df.Release()
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		out, err := lazy.FromDataFrame(df).Select(e).Collect(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkCoreCut(b *testing.B) {
	benchSelect(b, expr.Col("x").Cut([]float64{10, 20, 30, 40, 50, 60, 70, 80, 90}, expr.CutOptions{}))
}

func BenchmarkCoreQCut(b *testing.B) {
	benchSelect(b, expr.Col("x").QCutN(10, expr.CutOptions{}))
}

func BenchmarkCoreReplaceStrict(b *testing.B) {
	old := make([]any, 500)
	nw := make([]any, 500)
	for i := range old {
		old[i] = int64(i * 2)
		nw[i] = int64(i * 10)
	}
	d := expr.Lit(int64(-1))
	benchSelect(b, expr.Col("k").ReplaceStrict(old, nw, expr.ReplaceStrictOptions{Default: &d}))
}

func BenchmarkCoreRankOver(b *testing.B) {
	benchSelect(b, expr.Col("k").Rank("dense").Over("g"))
}

func BenchmarkCoreFilterAggGroup(b *testing.B) {
	df := coreBenchFrame(b)
	defer df.Release()
	ctx := context.Background()
	e := expr.Col("k").Filter(expr.Col("x").Gt(expr.Lit(50.0))).Sum().Alias("s")
	b.ResetTimer()
	for b.Loop() {
		out, err := lazy.FromDataFrame(df).GroupBy("g").Agg(e).Collect(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
