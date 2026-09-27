package compute_test

// Benchmarks that mirror the numeric workloads in cmd/bench (the
// polars comparison suite) at the three suite sizes. They exist so the
// kernels can be profiled and compared with benchstat without running
// the whole harness:
//
//	go test ./compute -run '^$' -bench 'Workload/SumInt64' -count 10

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

var workloadSizes = []int{16 * 1024, 256 * 1024, 1024 * 1024}

func wlInt64s(n int, seed uint64, bound int64) []int64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int64N(bound)
	}
	return out
}

func wlFloat64s(n int, seed uint64) []float64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]float64, n)
	for i := range out {
		out[i] = r.Float64()
	}
	return out
}

func wlNullable(n int) *series.Series {
	vals := make([]int64, n)
	valid := make([]bool, n)
	r := rand.New(rand.NewPCG(7, 8))
	for i := range vals {
		vals[i] = int64(i)
		valid[i] = r.Float64() >= 0.3
	}
	s, _ := series.FromInt64("x", vals, valid)
	return s
}

func wlGroupFrame(n int) *dataframe.DataFrame {
	keys := make([]int64, n)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(64)
	}
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", wlInt64s(n, 44, 1<<20), nil)
	df, _ := dataframe.New(k, v)
	return df
}

// runWorkload runs setup once per size and reports throughput using
// bytesPerRow like the harness does.
func runWorkload(b *testing.B, bytesPerRow int, fn func(b *testing.B, n int) func()) {
	for _, n := range workloadSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			op := fn(b, n)
			b.SetBytes(int64(n * bytesPerRow))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				op()
			}
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func BenchmarkWorkload(b *testing.B) {
	ctx := context.Background()

	b.Run("SumInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			b.Cleanup(s.Release)
			return func() { must(compute.SumInt64(ctx, s)) }
		})
	})
	b.Run("SumFloat64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromFloat64("a", wlFloat64s(n, 42), nil))
			b.Cleanup(s.Release)
			return func() { must(compute.SumFloat64(ctx, s)) }
		})
	})
	b.Run("MeanFloat64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromFloat64("a", wlFloat64s(n, 42), nil))
			b.Cleanup(s.Release)
			return func() { _, _, _ = compute.MeanFloat64(ctx, s) }
		})
	})
	b.Run("MinFloat64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromFloat64("a", wlFloat64s(n, 42), nil))
			b.Cleanup(s.Release)
			return func() { _, _, _ = compute.MinFloat64(ctx, s) }
		})
	})
	b.Run("MaxFloat64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromFloat64("a", wlFloat64s(n, 42), nil))
			b.Cleanup(s.Release)
			return func() { _, _, _ = compute.MaxFloat64(ctx, s) }
		})
	})
	b.Run("MinInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			s := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			b.Cleanup(s.Release)
			return func() { _, _, _ = compute.MinInt64(ctx, s) }
		})
	})
	b.Run("AddInt64", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			y := must(series.FromInt64("b", wlInt64s(n, 43, 1<<20), nil))
			b.Cleanup(x.Release)
			b.Cleanup(y.Release)
			return func() { must(compute.Add(ctx, x, y)).Release() }
		})
	})
	b.Run("AddFloat64", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			x := must(series.FromFloat64("a", wlFloat64s(n, 42), nil))
			y := must(series.FromFloat64("b", wlFloat64s(n, 43), nil))
			b.Cleanup(x.Release)
			b.Cleanup(y.Release)
			return func() { must(compute.Add(ctx, x, y)).Release() }
		})
	})
	b.Run("MulInt64", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<10), nil))
			y := must(series.FromInt64("b", wlInt64s(n, 43, 1<<10), nil))
			b.Cleanup(x.Release)
			b.Cleanup(y.Release)
			return func() { must(compute.Mul(ctx, x, y)).Release() }
		})
	})
	b.Run("GtInt64", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			y := must(series.FromInt64("b", wlInt64s(n, 43, 1<<20), nil))
			b.Cleanup(x.Release)
			b.Cleanup(y.Release)
			return func() { must(compute.Gt(ctx, x, y)).Release() }
		})
	})
	b.Run("GtLitInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			b.Cleanup(x.Release)
			return func() { must(compute.GtLit(ctx, x, int64(1<<19))).Release() }
		})
	})
	b.Run("CastI64ToF64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
			b.Cleanup(x.Release)
			return func() { must(compute.Cast(ctx, x, dtype.Float64())).Release() }
		})
	})
	b.Run("CumSumInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("x", wlInt64s(n, 42, 1<<16), nil))
			b.Cleanup(x.Release)
			return func() { must(x.CumSum()).Release() }
		})
	})
	b.Run("ShiftInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			x := must(series.FromInt64("x", wlInt64s(n, 42, 1<<20), nil))
			b.Cleanup(x.Release)
			return func() { must(x.Shift(1)).Release() }
		})
	})
	b.Run("FillNullValue", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			df := must(dataframe.New(wlNullable(n)))
			b.Cleanup(df.Release)
			return func() { must(df.FillNull(int64(0))).Release() }
		})
	})
	b.Run("ForwardFillInt64", func(b *testing.B) {
		runWorkload(b, 8, func(b *testing.B, n int) func() {
			x := wlNullable(n)
			b.Cleanup(x.Release)
			return func() { must(x.ForwardFill(0)).Release() }
		})
	})
	for _, kind := range []string{"Sum", "Min", "Max", "Mean"} {
		b.Run("Rolling"+kind, func(b *testing.B) {
			runWorkload(b, 8, func(b *testing.B, n int) func() {
				x := must(series.FromInt64("x", wlInt64s(n, 42, 1<<20), nil))
				b.Cleanup(x.Release)
				opts := series.RollingOptions{WindowSize: 32}
				var f func(series.RollingOptions, ...series.Option) (*series.Series, error)
				switch kind {
				case "Sum":
					f = x.RollingSum
				case "Min":
					f = x.RollingMin
				case "Max":
					f = x.RollingMax
				default:
					f = x.RollingMean
				}
				return func() { must(f(opts)).Release() }
			})
		})
	}
	b.Run("WhenThenOtherwise", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			vals := wlInt64s(n, 42, 1<<20)
			x := must(series.FromInt64("a", vals, nil))
			y := must(series.FromInt64("b", vals, nil))
			df := must(dataframe.New(x, y))
			b.Cleanup(df.Release)
			e := expr.When(expr.Col("a").Gt(expr.Lit(int64(1 << 19)))).
				Then(expr.Col("a")).Otherwise(expr.Col("b")).Alias("r")
			return func() { must(lazy.FromDataFrame(df).Select(e).Collect(ctx)).Release() }
		})
	})
	b.Run("SumOverGroup", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			df := wlGroupFrame(n)
			b.Cleanup(df.Release)
			e := expr.Col("v").Sum().Over("k").Alias("vs")
			return func() { must(lazy.FromDataFrame(df).Select(e).Collect(ctx)).Release() }
		})
	})
	b.Run("CumSumOverGroup", func(b *testing.B) {
		runWorkload(b, 16, func(b *testing.B, n int) func() {
			df := wlGroupFrame(n)
			b.Cleanup(df.Release)
			e := expr.Col("v").CumSum().Over("k").Alias("cum")
			return func() { must(lazy.FromDataFrame(df).Select(e).Collect(ctx)).Release() }
		})
	})
	for _, kind := range []string{"Sum", "Max"} {
		b.Run(kind+"Horizontal", func(b *testing.B) {
			runWorkload(b, 24, func(b *testing.B, n int) func() {
				x := must(series.FromInt64("a", wlInt64s(n, 42, 1<<20), nil))
				y := must(series.FromInt64("b", wlInt64s(n, 43, 1<<20), nil))
				z := must(series.FromInt64("c", wlInt64s(n, 44, 1<<20), nil))
				df := must(dataframe.New(x, y, z))
				b.Cleanup(df.Release)
				if kind == "Sum" {
					return func() { must(df.SumHorizontal(ctx, dataframe.IgnoreNulls)).Release() }
				}
				return func() { must(df.MaxHorizontal(ctx, dataframe.IgnoreNulls)).Release() }
			})
		})
	}
}
