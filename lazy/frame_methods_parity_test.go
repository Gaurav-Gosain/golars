package lazy_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// lxCol builds a Series from Go values (nil is null); []any values build
// list elements.
func lxCol(t testing.TB, mem memory.Allocator, name string, dt arrow.DataType, vals ...any) *series.Series {
	t.Helper()
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for _, v := range vals {
		lxAppend(t, b, v)
	}
	arr := b.NewArray()
	s, err := series.New(name, arr)
	if err != nil {
		arr.Release()
		t.Fatal(err)
	}
	return s
}

func lxAppend(t testing.TB, b array.Builder, v any) {
	t.Helper()
	if v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.Int64Builder:
		bb.Append(int64(v.(int)))
	case *array.Float64Builder:
		bb.Append(v.(float64))
	case *array.StringBuilder:
		bb.Append(v.(string))
	case *array.ListBuilder:
		bb.Append(true)
		for _, x := range v.([]any) {
			lxAppend(t, bb.ValueBuilder(), x)
		}
	case *array.StructBuilder:
		bb.Append(true)
		for i, x := range v.([]any) {
			lxAppend(t, bb.FieldBuilder(i), x)
		}
	default:
		t.Fatalf("lxAppend: unsupported builder %T", b)
	}
}

func lxFrame(t testing.TB, cols ...*series.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

var (
	lxI64 = arrow.PrimitiveTypes.Int64
	lxF64 = arrow.PrimitiveTypes.Float64
	lxStr = arrow.BinaryTypes.String
)

// checkLazy collects lf three ways (optimized, unoptimized, streaming),
// compares each against polars' output and checks that the statically
// inferred schema equals the schema of the result.
func checkLazy(t *testing.T, mem memory.Allocator, lf lazy.LazyFrame, key string) {
	t.Helper()
	want, ok := lazyExpected[key]
	if !ok {
		t.Fatalf("no expected output for %q", key)
	}
	ctx := context.Background()
	runs := []struct {
		name string
		run  func() (*dataframe.DataFrame, error)
	}{
		{"optimized", func() (*dataframe.DataFrame, error) { return lf.Collect(ctx, lazy.WithExecAllocator(mem)) }},
		{"unoptimized", func() (*dataframe.DataFrame, error) { return lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem)) }},
		{"streaming", func() (*dataframe.DataFrame, error) {
			return lf.Collect(ctx, lazy.WithExecAllocator(mem), lazy.WithStreaming(), lazy.WithStreamingMorselRows(2))
		}},
	}
	for _, r := range runs {
		out, err := r.run()
		if err != nil {
			t.Fatalf("%s: %v", r.name, err)
		}
		if !framerepr.Match(framerepr.Frame(out), want) {
			got := framerepr.Frame(out)
			out.Release()
			t.Fatalf("%s mismatch\n got:\n%s\nwant:\n%s", r.name, got, want)
		}
		if r.name == "optimized" {
			sch, err := lf.CollectSchema()
			if err != nil {
				out.Release()
				t.Fatalf("CollectSchema: %v", err)
			}
			if !sch.Equal(out.Schema()) {
				t.Errorf("CollectSchema = %s, collected %s", sch, out.Schema())
			}
		}
		out.Release()
	}
}

func baseFixture(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	return lxFrame(t,
		lxCol(t, mem, "k", lxStr, "x", "y", "x", "z", "y", "x"),
		lxCol(t, mem, "a", lxI64, 1, 2, nil, 4, 5, 6),
		lxCol(t, mem, "f", lxF64, 1.5, nil, 2.5, math.NaN(), 0.5, 3.0),
		lxCol(t, mem, "l", arrow.ListOf(lxI64), []any{1, 2}, []any{3}, nil, []any{}, []any{4, 5}, []any{6}),
		lxCol(t, mem, "l2", arrow.ListOf(lxStr), []any{"a", "b"}, []any{"c"}, nil, []any{}, []any{"d", "e"}, []any{"f"}),
	)
}

func TestParityLazyFrameMethods(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := baseFixture(t, mem)
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	col := expr.Col
	num := lf.Select(col("a"), col("f"))
	cases := []struct {
		key string
		lf  lazy.LazyFrame
	}{
		{"explode l", lf.Explode("l").Drop("l2")},
		{"explode l l2", lf.Explode("l", "l2")},
		{"explode filter", lf.Explode("l").Drop("l2").Filter(col("l").Gt(expr.LitInt64(2)))},
		{"unpivot", lf.Select(col("k"), col("a"), col("f")).Unpivot(lazy.UnpivotOptions{On: []string{"a", "f"}, Index: []string{"k"}})},
		{"unpivot names", lf.Select(col("k"), col("a")).Unpivot(lazy.UnpivotOptions{
			On: []string{"a"}, Index: []string{"k"}, VariableName: "var", ValueName: "val",
		})},
		{"top_k", lf.Select(col("k"), col("a")).TopK(2, "a")},
		{"bottom_k", lf.Select(col("k"), col("a")).BottomK(2, "a")},
		{"shift", lf.Select(col("k"), col("a")).Shift(1, nil)},
		{"shift fill", lf.Select(col("a")).Shift(-2, 0)},
		{"gather_every", lf.Select(col("k")).GatherEvery(2, 1)},
		{"first", lf.Select(col("k"), col("a")).First()},
		{"last", lf.Select(col("k"), col("a")).Last()},
		{"agg sum", num.Sum()},
		{"agg mean", num.Mean()},
		{"agg median", num.Median()},
		{"agg min", num.Min()},
		{"agg max", num.Max()},
		{"agg std", num.Std()},
		{"agg var", num.Var()},
		{"agg count", num.Count()},
		{"agg null_count", num.NullCount()},
		{"agg quantile", num.Quantile(0.5, dataframe.QuantileLinear)},
		{"drop_nans", lf.Select(col("k"), col("f")).DropNans()},
		{"fill_null fwd", lf.Select(col("a")).FillNullWithStrategy(dataframe.FillForward, 0)},
		{"fill_null zero", lf.Select(col("a"), col("f")).FillNullWithStrategy(dataframe.FillZero, 0)},
		{"clear", lf.Select(col("k"), col("a")).Clear(2)},
		{"cast", lf.Select(col("a"), col("f")).CastColumns(map[string]dtype.DType{"a": dtype.Float64()}, true)},
		{"with_row_index", lf.Select(col("k")).WithRowIndex("index", 0)},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) { checkLazy(t, mem, c.lf, c.key) })
	}
}

func TestParityLazyUnnestAndPivot(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	stType := arrow.StructOf(
		arrow.Field{Name: "p", Type: lxI64, Nullable: true},
		arrow.Field{Name: "q", Type: lxStr, Nullable: true},
	)
	st := lxFrame(t,
		lxCol(t, mem, "id", lxI64, 1, 2),
		lxCol(t, mem, "s", stType, []any{1, "u"}, []any{2, "v"}),
	)
	defer st.Release()
	checkLazy(t, mem, lazy.FromDataFrame(st).Unnest("s"), "unnest")

	piv := lxFrame(t,
		lxCol(t, mem, "k", lxStr, "a", "a", "b", "b", "c"),
		lxCol(t, mem, "c", lxStr, "x", "y", "x", "z", "y"),
		lxCol(t, mem, "v", lxI64, 1, 2, 3, 4, 5),
	)
	defer piv.Release()
	plf := lazy.FromDataFrame(piv)
	checkLazy(t, mem, plf.Pivot(lazy.PivotOptions{
		On: "c", OnColumns: []string{"y", "x", "w"}, Index: []string{"k"}, Values: "v", Agg: dataframe.PivotSum,
	}).Sort("k", false), "pivot sum")
	checkLazy(t, mem, plf.Pivot(lazy.PivotOptions{
		On: "c", OnColumns: []string{"x", "z"}, Index: []string{"k"}, Values: "v", Agg: dataframe.PivotFirst,
	}).Sort("k", false), "pivot first")
}

func TestParityLazyGroupByMethods(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := baseFixture(t, mem)
	defer df.Release()
	col := expr.Col
	g := lazy.FromDataFrame(df).Select(col("k"), col("a"), col("f"))
	byK := g.GroupBy("k").MaintainOrder()
	two, three := expr.LitInt64(2), expr.LitInt64(3)
	cases := []struct {
		key string
		lf  lazy.LazyFrame
	}{
		{"gb len", byK.Len("")},
		{"gb sum", byK.Sum()},
		{"gb mean", byK.Mean()},
		{"gb median", byK.Median()},
		{"gb n_unique", byK.NUnique()},
		{"gb first", byK.First()},
		{"gb last", byK.Last()},
		{"gb head", byK.Head(1)},
		{"gb tail", byK.Tail(1)},
		{"gb all", byK.All()},
		{"gb quantile", byK.Quantile(0.5, "")},
		{"gb agg maintain", byK.Agg(col("a").Sum().Alias("s"), col("f").Max())},
		{"gb expr agg", g.GroupByExprs(col("a").Gt(three)).MaintainOrder().Agg(col("f").Sum())},
		{"gb expr alias agg", g.GroupByExprs(col("a").Gt(three).Alias("p")).MaintainOrder().Agg(col("a").Sum())},
		{"gb expr sum", g.Select(col("a"), col("f")).GroupByExprs(col("a").Gt(three)).MaintainOrder().Sum()},
		{"gb mixed keys", g.GroupByExprs(col("k"), col("a").Gt(two)).MaintainOrder().Agg(col("f").Sum())},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) { checkLazy(t, mem, c.lf, c.key) })
	}
}

func TestParityLazyBinaryOps(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	left := lxFrame(t, lxCol(t, mem, "t", lxI64, 1, 5, 10), lxCol(t, mem, "l", lxStr, "a", "b", "c"))
	defer left.Release()
	right := lxFrame(t, lxCol(t, mem, "t", lxI64, 2, 4, 9), lxCol(t, mem, "r", lxI64, 20, 40, 90))
	defer right.Release()
	l, r := lazy.FromDataFrame(left), lazy.FromDataFrame(right)
	checkLazy(t, mem, l.JoinAsof(r, dataframe.AsofOptions{On: "t"}), "join_asof")
	checkLazy(t, mem, l.JoinAsof(r, dataframe.AsofOptions{On: "t"}).Filter(expr.Col("r").Gt(expr.LitInt64(30))), "join_asof filter")
	jw := l.JoinWhere(r.Rename("t", "t2"), []expr.Expr{expr.Col("t").Gt(expr.Col("t2"))}).
		SortBy([]string{"t", "t2"}, []compute.SortOptions{{}, {}})
	checkLazy(t, mem, jw, "join_where")

	m1 := lxFrame(t, lxCol(t, mem, "k", lxI64, 1, 3, 5), lxCol(t, mem, "v", lxStr, "a", "b", "c"))
	defer m1.Release()
	m2 := lxFrame(t, lxCol(t, mem, "k", lxI64, 2, 3, 4), lxCol(t, mem, "v", lxStr, "x", "y", "z"))
	defer m2.Release()
	checkLazy(t, mem, lazy.FromDataFrame(m1).MergeSorted(lazy.FromDataFrame(m2), "k"), "merge_sorted")

	ua := lxFrame(t, lxCol(t, mem, "A", lxI64, 1, 2, 3, 4), lxCol(t, mem, "B", lxI64, 400, 500, 600, 700))
	defer ua.Release()
	ub := lxFrame(t, lxCol(t, mem, "B", lxI64, -66, nil, -99), lxCol(t, mem, "C", lxI64, 5, 3, 1))
	defer ub.Release()
	checkLazy(t, mem, lazy.FromDataFrame(ua).Update(lazy.FromDataFrame(ub), dataframe.UpdateOptions{
		LeftOn: []string{"A"}, RightOn: []string{"C"}, How: dataframe.UpdateFull,
	}), "update")
}
