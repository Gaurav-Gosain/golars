package eval_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkOverCumSum exercises the generic over() path (non-scalar
// inner) with int64 keys.
func BenchmarkOverCumSum(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		r := rand.New(rand.NewPCG(42, 43))
		keys := make([]int64, n)
		vals := make([]int64, n)
		for i := range keys {
			keys[i] = r.Int64N(64)
			vals[i] = r.Int64N(1 << 20)
		}
		k, _ := series.FromInt64("k", keys, nil)
		v, _ := series.FromInt64("v", vals, nil)
		df, _ := dataframe.New(k, v)
		defer df.Release()

		name := fmt.Sprintf("n=%d", n)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				out, err := lazy.FromDataFrame(df).
					Select(expr.Col("v").CumSum().Over("k").Alias("cum")).
					Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

// BenchmarkOverSumMultiKey exercises scalar-agg over() with two int64
// keys (groupby + join-back path).
func BenchmarkOverSumMultiKey(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		r := rand.New(rand.NewPCG(42, 43))
		k1 := make([]int64, n)
		k2 := make([]int64, n)
		vals := make([]int64, n)
		for i := range k1 {
			k1[i] = r.Int64N(64)
			k2[i] = r.Int64N(32)
			vals[i] = r.Int64N(1 << 20)
		}
		a, _ := series.FromInt64("a", k1, nil)
		cc, _ := series.FromInt64("b", k2, nil)
		v, _ := series.FromInt64("v", vals, nil)
		df, _ := dataframe.New(a, cc, v)
		defer df.Release()

		name := fmt.Sprintf("n=%d", n)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				out, err := lazy.FromDataFrame(df).
					Select(expr.Col("v").Sum().Over("a", "b").Alias("s")).
					Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
