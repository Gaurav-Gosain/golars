package main

import (
	"context"
	"math/rand/v2"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// Benchmarks for the core expression API (cut, qcut, replace_strict,
// per-group filter aggregation). Mirrored in bench/polars-compare/bench.py
// under the same names.

func coreExprBenches(ctx context.Context) []result {
	var runs []result
	for _, n := range []int{256 * 1024, 1024 * 1024} {
		runs = append(runs,
			benchCut(ctx, n),
			benchQCut(ctx, n),
			benchReplaceStrict(ctx, n),
		)
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs, benchFilterAggGroup(ctx, n))
	}
	return runs
}

func uniformFloats(n int, seed uint64, scale float64) []float64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]float64, n)
	for i := range out {
		out[i] = r.Float64() * scale
	}
	return out
}

func runSelect(ctx context.Context, df *dataframe.DataFrame, e expr.Expr) {
	out, err := lazy.FromDataFrame(df).Select(e).Collect(ctx)
	if err != nil {
		panic(err)
	}
	out.Release()
}

func benchCut(ctx context.Context, rows int) result {
	x, _ := series.FromFloat64("x", uniformFloats(rows, 42, 100), nil)
	df, _ := dataframe.New(x)
	defer df.Release()
	breaks := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90}
	e := expr.Col("x").Cut(breaks, expr.CutOptions{})
	t := timeNs(func() { runSelect(ctx, df, e) }, 1, 5)
	return result{Name: "Cut(10 bins)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchQCut(ctx context.Context, rows int) result {
	x, _ := series.FromFloat64("x", uniformFloats(rows, 43, 100), nil)
	df, _ := dataframe.New(x)
	defer df.Release()
	e := expr.Col("x").QCutN(10, expr.CutOptions{})
	t := timeNs(func() { runSelect(ctx, df, e) }, 1, 5)
	return result{Name: "QCut(10)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchReplaceStrict(ctx context.Context, rows int) result {
	k, _ := series.FromInt64("k", randInt64s(rows, 44, 1000), nil)
	df, _ := dataframe.New(k)
	defer df.Release()
	old := make([]any, 500)
	nw := make([]any, 500)
	for i := range old {
		old[i] = int64(i * 2)
		nw[i] = int64(i * 10)
	}
	dflt := expr.Lit(int64(-1))
	e := expr.Col("k").ReplaceStrict(old, nw, expr.ReplaceStrictOptions{Default: &dflt})
	t := timeNs(func() { runSelect(ctx, df, e) }, 1, 5)
	return result{Name: "ReplaceStrict(500 keys)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchFilterAggGroup(ctx context.Context, rows int) result {
	keys := randInt64s(rows, 45, 64)
	vals := randInt64s(rows, 46, 1<<20)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()
	e := expr.Col("v").Filter(expr.Col("v").Gt(expr.Lit(int64(1 << 19)))).Sum().Alias("s")
	fn := func() {
		out, err := lazy.FromDataFrame(df).GroupBy("k").Agg(e).Collect(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "FilterSumPerGroup(groups=64)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}
