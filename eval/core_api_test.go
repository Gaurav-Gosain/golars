package eval_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func collectCol(t *testing.T, df *dataframe.DataFrame, lf lazy.LazyFrame, col int, opts ...lazy.ExecOption) []any {
	t.Helper()
	out, err := lf.Collect(context.Background(), opts...)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	defer out.Release()
	return out.ColumnAt(col).ToList()
}

func TestCoreLenInLazySelect(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "main", series.WithAllocator(mem))
	defer df.Release()
	got := collectCol(t, df, lazy.FromDataFrame(df).Select(expr.Len()), 0, lazy.WithExecAllocator(mem))
	if !parityEqual(got, []any{int64(7)}) {
		t.Fatalf("len = %v", got)
	}
}

func TestCoreMapBatchesPerGroup(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "grp", series.WithAllocator(mem))
	defer df.Release()
	// The batch function sees one group at a time inside agg.
	count := expr.Col("y").MapBatches(func(s *series.Series) (*series.Series, error) {
		return series.ScalarUint32(s.Name(), s.Len(), true)
	}).Alias("n")
	lf := lazy.FromDataFrame(df).GroupBy("g").Agg(count.Implode().Explode().First()).Sort("g", false)
	got := collectCol(t, df, lf, 1, lazy.WithExecAllocator(mem))
	if !parityEqual(got, []any{int64(3), int64(2), int64(2)}) {
		t.Fatalf("per-group lengths = %v", got)
	}
	over := collectCol(t, df, lazy.FromDataFrame(df).Select(count.Over("g")), 0, lazy.WithExecAllocator(mem))
	if !parityEqual(over, []any{int64(3), int64(2), int64(3), int64(2), int64(3), int64(2), int64(2)}) {
		t.Fatalf("over lengths = %v", over)
	}
}

func TestCoreMapElementsTyped(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "main", series.WithAllocator(mem))
	defer df.Release()
	e := expr.MapElements(expr.Col("s"), func(v string) string { return strings.ToUpper(v) + "!" })
	got := collectCol(t, df, lazy.FromDataFrame(df).Select(e), 0, lazy.WithExecAllocator(mem))
	want := []any{"B!", nil, "A!", "B!", "C!", "A!", nil}
	if !parityEqual(got, want) {
		t.Fatalf("got %v", got)
	}
	sq := expr.MapElements(expr.Col("i"), func(v int64) float64 { return float64(v) / 2 })
	got = collectCol(t, df, lazy.FromDataFrame(df).Select(sq), 0, lazy.WithExecAllocator(mem))
	want = []any{1.5, nil, 0.5, 1.5, 1.0, nil, 2.5}
	if !parityEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestCoreRollingMap(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "main", series.WithAllocator(mem))
	defer df.Release()
	sum := func(w *series.Series) (*series.Series, error) {
		vals := w.ToList()
		var acc int64
		for _, v := range vals {
			if v != nil {
				acc += v.(int64)
			}
		}
		return series.FromInt64(w.Name(), []int64{acc}, nil)
	}
	e := expr.Col("u").RollingMap(sum, expr.RollingWindow{WindowSize: 3})
	got := collectCol(t, df, lazy.FromDataFrame(df).Select(e), 0, lazy.WithExecAllocator(mem))
	// polars: rolling_map(lambda s: s.sum(), 3)
	want := []any{nil, nil, int64(10), int64(6), int64(8), int64(13), int64(14)}
	if !parityEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestCoreSampleShuffle(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "main", series.WithAllocator(mem))
	defer df.Release()
	shuffled := collectCol(t, df, lazy.FromDataFrame(df).Select(expr.Col("u").Shuffle(7)), 0, lazy.WithExecAllocator(mem))
	orig := []int64{5, 1, 4, 1, 3, 9, 2}
	got := make([]int64, 0, len(shuffled))
	for _, v := range shuffled {
		got = append(got, v.(int64))
	}
	slices.Sort(got)
	want := slices.Clone(orig)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("shuffle is not a permutation: %v", got)
	}
	sample := collectCol(t, df, lazy.FromDataFrame(df).Select(expr.Col("u").Sample(3, false, false, 1)), 0, lazy.WithExecAllocator(mem))
	if len(sample) != 3 {
		t.Fatalf("sample len = %d", len(sample))
	}
}

func TestCoreEagerGroupByGeneric(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "grp", series.WithAllocator(mem))
	defer df.Release()
	out, err := df.GroupBy("g").Agg(context.Background(), []expr.Expr{
		expr.Col("x").Filter(expr.Col("y").Gt(expr.Lit(2))).Sum().Alias("fs"),
		expr.Col("y").Sort(false).Alias("ys"),
		expr.Col("x").Sum().Alias("plain"),
	}, dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	cols := make([][]any, out.Width())
	for j := range cols {
		cols[j] = out.ColumnAt(j).ToList()
	}
	rows := make([][]any, out.Height())
	for i := range rows {
		for j := range cols {
			rows[i] = append(rows[i], cols[j][i])
		}
	}
	// polars: df.group_by("g").agg(...).sort("g")
	want := [][]any{
		{"a", int64(4), []any{int64(2), int64(5), int64(7)}, int64(9)},
		{"b", int64(4), []any{int64(1), int64(3)}, int64(6)},
		{"c", int64(6), []any{int64(8), int64(9)}, int64(6)},
	}
	for i, r := range rows {
		for j := range r {
			if !parityEqual(normalizeCell(r[j]), want[i][j]) {
				t.Fatalf("row %d col %d = %v, want %v", i, j, r[j], want[i][j])
			}
		}
	}
}

func TestCorePipeAndScalarLiteral(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "main", series.WithAllocator(mem))
	defer df.Release()
	double := func(e expr.Expr) expr.Expr { return e.Mul(expr.Lit(2)) }
	got := collectCol(t, df, lazy.FromDataFrame(df).Select(expr.Col("u").Sum().Pipe(double).Add(expr.Lit(1))), 0, lazy.WithExecAllocator(mem))
	// A scalar aggregation combined with literals stays one row.
	if !parityEqual(got, []any{int64(51)}) {
		t.Fatalf("got %v", got)
	}
}

func TestCoreCumFoldIncludeInit(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := parityFrame(t, "h", series.WithAllocator(mem))
	defer df.Release()
	e := expr.CumFold(expr.Col("a").Mul(expr.Lit(0)), addFold, true, expr.Col("a"), expr.Col("b"))
	out, err := lazy.FromDataFrame(df).Select(e).Collect(context.Background(), lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col := out.ColumnAt(0)
	if got := col.DType().String(); got != "struct{a: i64, a: i64, b: i64}" {
		t.Fatalf("dtype = %s", got)
	}
	if col.Name() != "cum_fold" {
		t.Fatalf("name = %s", col.Name())
	}
}
