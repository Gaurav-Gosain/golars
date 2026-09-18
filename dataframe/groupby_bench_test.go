package dataframe_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

var regionNames = []string{"n", "s", "e", "w", "ne", "nw", "se", "sw"}

// BenchmarkTopK10 selects the 10 largest rows by an int64 key.
func BenchmarkTopK10(b *testing.B) {
	ctx := context.Background()
	n := 1 << 18
	vals := make([]int64, n)
	ids := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i * 2654435761 % (1 << 20))
		ids[i] = int64(i)
	}
	v, _ := series.FromInt64("v", vals, nil)
	id, _ := series.FromInt64("id", ids, nil)
	df, _ := dataframe.New(v, id)
	defer df.Release()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		out, err := df.TopK(ctx, 10, "v")
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

// BenchmarkPartitionByString partitions rows by a string key.
func BenchmarkPartitionByString(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		keys := make([]string, n)
		vals := make([]int64, n)
		for i := range keys {
			keys[i] = regionNames[i%len(regionNames)]
			vals[i] = int64(i)
		}
		k, _ := series.FromString("k", keys, nil)
		v, _ := series.FromInt64("v", vals, nil)
		df, _ := dataframe.New(k, v)
		defer df.Release()

		name := fmt.Sprintf("n=%d", n)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				parts, err := df.PartitionBy(ctx, "k")
				if err != nil {
					b.Fatal(err)
				}
				for _, p := range parts {
					p.Release()
				}
			}
		})
	}
}

// BenchmarkGroupBySumMultiKey exercises the multi-key path (string +
// int64 keys) that cannot use the single-key hash fast path.
func BenchmarkGroupBySumMultiKey(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		regions := make([]string, n)
		years := make([]int64, n)
		vals := make([]int64, n)
		for i := range regions {
			regions[i] = regionNames[i%len(regionNames)]
			years[i] = 2020 + int64(i%5)
			vals[i] = int64(i)
		}
		r, _ := series.FromString("region", regions, nil)
		y, _ := series.FromInt64("year", years, nil)
		v, _ := series.FromInt64("v", vals, nil)
		df, _ := dataframe.New(r, y, v)
		defer df.Release()

		name := fmt.Sprintf("n=%d", n)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 24)
			for b.Loop() {
				out, err := df.GroupBy("region", "year").Agg(ctx,
					[]expr.Expr{expr.Col("v").Sum().Alias("sum")})
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

// BenchmarkGroupBySumInt64 varies the row count and the number of distinct
// groups. Low group-count = many rows per group (cheap per-group aggs, more
// sort work); high group-count = fewer rows per group (more per-group
// overhead).
func BenchmarkGroupBySumInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		for _, groups := range []int{8, 1024} {
			name := fmt.Sprintf("n=%d,groups=%d", n, groups)
			b.Run(name, func(b *testing.B) {
				keys := make([]int64, n)
				vals := make([]int64, n)
				for i := range keys {
					keys[i] = int64(i % groups)
					vals[i] = int64(i)
				}
				k, _ := series.FromInt64("k", keys, nil)
				v, _ := series.FromInt64("v", vals, nil)
				df, _ := dataframe.New(k, v)
				defer df.Release()

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(n) * 16)
				for b.Loop() {
					out, err := df.GroupBy("k").Agg(ctx,
						[]expr.Expr{expr.Col("v").Sum().Alias("sum")})
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}
