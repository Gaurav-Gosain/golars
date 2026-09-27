package lazy_test

import (
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// sliceBatches serves a frame as fixed-size row slices.
type sliceBatches struct {
	df   *dataframe.DataFrame
	size int
	cols []string
}

func (b sliceBatches) NumBatches() int { return (b.df.Height() + b.size - 1) / b.size }
func (b sliceBatches) ReadBatch(_ context.Context, i int) (*dataframe.DataFrame, error) {
	lo := i * b.size
	part, err := b.df.Slice(lo, min(b.size, b.df.Height()-lo))
	if err != nil || b.cols == nil {
		return part, err
	}
	defer part.Release()
	return part.Select(b.cols...)
}
func (b sliceBatches) Close() error { return nil }

// batchedAndPlain returns the same data as a batched source and as a
// plain source, so a plan can be run with and without morsels.
func batchedAndPlain(df *dataframe.DataFrame, size int) (lazy.LazyFrame, lazy.LazyFrame) {
	load := func(_ context.Context, cols []string) (*dataframe.DataFrame, error) {
		if cols == nil {
			return df.Clone(), nil
		}
		return df.Select(cols...)
	}
	open := func(_ context.Context, cols []string) (lazy.BatchSource, error) {
		return sliceBatches{df: df, size: size, cols: cols}, nil
	}
	return lazy.FromSourceBatched("batched", df.Schema(), load, open),
		lazy.FromSourceProjected("plain", df.Schema(), load)
}

func morselFrame(t *testing.T, o series.Option, n int) *dataframe.DataFrame {
	t.Helper()
	g := make([]string, n)
	gv := make([]bool, n)
	x := make([]float64, n)
	xv := make([]bool, n)
	k := make([]int32, n)
	for i := range n {
		// Group "d" appears only in the first rows, so later batches
		// lack it; group "e" has only null x values.
		switch {
		case i < 5:
			g[i] = "d"
		case i%11 == 0:
			g[i] = "e"
		default:
			g[i] = []string{"a", "b", "c"}[i%3]
		}
		gv[i] = i%17 != 4
		x[i] = float64(i%23) - 7.5
		if i%29 == 3 {
			x[i] = math.NaN()
		}
		xv[i] = i%7 != 2 && g[i] != "e"
		k[i] = int32(i % 5)
	}
	gs, _ := series.FromString("g", g, gv, o)
	xs, _ := series.FromFloat64("x", x, xv, o)
	ks, _ := series.FromInt32("k", k, nil, o)
	df, err := dataframe.New(gs, xs, ks)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestMorselExecutionMatchesEager(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	df := morselFrame(t, o, 1000)
	defer df.Release()

	x, k := expr.Col("x"), expr.Col("k")
	cases := []struct {
		name  string
		build func(lazy.LazyFrame) lazy.LazyFrame
	}{
		{"filter", func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Filter(x.Gt(expr.LitFloat64(2))) }},
		{"filter nothing kept", func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Filter(x.Gt(expr.LitFloat64(1e9))) }},
		{"with_columns and select", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.WithColumns(x.Mul(expr.LitFloat64(2)).Alias("y")).
				Filter(k.Ne(expr.LitInt64(3))).Select(expr.Col("g"), expr.Col("y"))
		}},
		{"rename and drop", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Rename("x", "z").Drop("k").Filter(expr.Col("z").IsNotNull())
		}},
		{"group by decomposable", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(k.Ne(expr.LitInt64(1))).GroupBy("g").Agg(
				x.Sum().Alias("s"), x.Min().Alias("lo"), x.Max().Alias("hi"),
				x.Count().Alias("n"), x.Mean().Alias("m"), expr.Len().Alias("len"),
				k.Sum().Alias("ks"), x.Mul(expr.LitFloat64(2)).Sum().Alias("s2"))
		}},
		{"group by two keys", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.GroupBy("g", "k").Agg(x.Sum().Alias("s"), k.Count().Alias("n"))
		}},
		{"group by not decomposable", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.GroupBy("g").Agg(x.Median().Alias("med"), x.First().Alias("f"))
		}},
		{"select aggregations", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(x.Lt(expr.LitFloat64(5))).Select(
				x.Sum().Round(2).Alias("s"),
				expr.LitFloat64(100).Mul(expr.When(k.Eq(expr.LitInt64(2))).Then(x).Otherwise(expr.LitFloat64(0)).Sum()).Div(x.Sum()).Alias("share"),
				x.Mean().Alias("m"), expr.Len().Alias("n"), k.Max().Alias("kmax"))
		}},
		{"select aggregation of empty", func(lf lazy.LazyFrame) lazy.LazyFrame {
			return lf.Filter(x.Gt(expr.LitFloat64(1e9))).Select(x.Sum().Alias("s"), x.Mean().Alias("m"), x.Min().Alias("lo"))
		}},
	}
	for _, size := range []int{1000, 64, 7} {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				batched, plain := batchedAndPlain(df, size)
				got, err := tc.build(batched).Collect(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				defer got.Release()
				want, err := tc.build(plain).Collect(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				defer want.Release()
				if got.Height() > 0 && tc.name[:5] == "group" {
					assertSameRows(t, got, want)
					return
				}
				if !sameSchema(got, want) {
					t.Fatalf("schema %s, want %s", got.Schema(), want.Schema())
				}
				assertClose(t, got, want)
			})
		}
	}
}

func sameSchema(a, b *dataframe.DataFrame) bool {
	return a.Schema().String() == b.Schema().String()
}

// assertClose compares frames row by row, allowing float sums to differ
// in the last bits (partial sums add in a different order).
func assertClose(t *testing.T, got, want *dataframe.DataFrame) {
	t.Helper()
	if got.Height() != want.Height() || got.Width() != want.Width() {
		t.Fatalf("shape %dx%d, want %dx%d\n%s\n%s", got.Height(), got.Width(), want.Height(), want.Width(), got, want)
	}
	for c := range got.Width() {
		gc, wc := got.ColumnAt(c), want.ColumnAt(c)
		for i := range got.Height() {
			gv, _ := gc.Get(i)
			wv, _ := wc.Get(i)
			gf, gok := gv.(float64)
			wf, wok := wv.(float64)
			if gok && wok {
				if math.IsNaN(gf) && math.IsNaN(wf) {
					continue
				}
				if math.Abs(gf-wf) <= 1e-9*math.Max(1, math.Abs(wf)) {
					continue
				}
			}
			if gv != wv {
				t.Fatalf("col %s row %d: got %v want %v\ngot\n%s\nwant\n%s", gc.Name(), i, gv, wv, got, want)
			}
		}
	}
}

var _ = compute.SortOptions{}
