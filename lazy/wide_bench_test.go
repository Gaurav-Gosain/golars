package lazy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func wideBenchFrame(ncols, nrows int) *dataframe.DataFrame {
	cols := make([]*series.Series, ncols)
	for i := range cols {
		vals := make([]int64, nrows)
		for j := range vals {
			vals[j] = int64(i*1000000 + j)
		}
		s, _ := series.FromInt64(fmt.Sprintf("c%d", i), vals, nil)
		cols[i] = s
	}
	df, _ := dataframe.New(cols...)
	return df
}

// BenchmarkWideFilter filters a wide frame on c0.
func BenchmarkWideFilter(b *testing.B) {
	ctx := context.Background()
	for _, tc := range [][2]int{{32, 1 << 14}, {32, 1 << 18}} {
		ncols, nrows := tc[0], tc[1]
		df := wideBenchFrame(ncols, nrows)
		defer df.Release()
		name := fmt.Sprintf("cols=%d,rows=%d", ncols, nrows)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				out, err := lazy.FromDataFrame(df).
					Filter(expr.Col("c0").GtLit(int64(nrows / 2))).
					Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

// BenchmarkWideSelect projects ncols c[i]+c[i+1] expressions.
func BenchmarkWideSelect(b *testing.B) {
	ctx := context.Background()
	for _, tc := range [][2]int{{4, 1 << 14}, {32, 1 << 14}, {32, 1 << 18}} {
		ncols, nrows := tc[0], tc[1]
		df := wideBenchFrame(ncols, nrows)
		defer df.Release()
		exprs := make([]expr.Expr, ncols)
		for i := range exprs {
			exprs[i] = expr.Col(fmt.Sprintf("c%d", i)).Add(expr.Col(fmt.Sprintf("c%d", (i+1)%ncols))).Alias(fmt.Sprintf("s%d", i))
		}
		name := fmt.Sprintf("cols=%d,rows=%d", ncols, nrows)
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				out, err := lazy.FromDataFrame(df).Select(exprs...).Collect(ctx)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
