package parquet_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// A scan over a file with many row groups runs as morsels. The result
// must match the same query over the file read eagerly.
func TestScanRowGroupMorsels(t *testing.T) {
	ctx := context.Background()
	const n = 10_000
	k := make([]int64, n)
	v := make([]float64, n)
	s := make([]string, n)
	for i := range n {
		k[i] = int64(i % 7)
		v[i] = float64(i) / 3
		s[i] = []string{"x", "y", "z"}[i%3]
	}
	ks, _ := series.FromInt64("k", k, nil)
	vs, _ := series.FromFloat64("v", v, nil)
	ss, _ := series.FromString("s", s, nil)
	df, _ := dataframe.New(ks, vs, ss)
	defer df.Release()
	path := filepath.Join(t.TempDir(), "rg.parquet")
	if err := parquet.WriteFile(ctx, path, df, parquet.WithChunkSize(999)); err != nil {
		t.Fatal(err)
	}
	r, err := parquet.OpenRowGroups(path, []string{"v"})
	if err != nil {
		t.Fatal(err)
	}
	if r.NumRowGroups() < 10 {
		t.Fatalf("row groups = %d, want many", r.NumRowGroups())
	}
	r.Close()

	queries := map[string]func(lazy.LazyFrame) lazy.LazyFrame{
		"filter": func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(expr.Col("s").Eq(expr.LitString("y"))).Select(expr.Col("k"), expr.Col("v"))
		},
		"group": func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(expr.Col("v").Gt(expr.LitFloat64(100))).GroupBy("k").
				Agg(expr.Col("v").Sum().Alias("s"), expr.Len().Alias("n")).Sort("k", false)
		},
		"total": func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Select(expr.Col("v").Max().Alias("m"), expr.Col("k").Sum().Alias("ks"))
		},
	}
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			got, err := q(parquet.Scan(path)).Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer got.Release()
			eager, err := parquet.ReadFile(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer eager.Release()
			want, err := q(lazy.FromDataFrame(eager)).Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()
			if got.String() != want.String() {
				t.Fatalf("got\n%s\nwant\n%s", got, want)
			}
		})
	}
}
