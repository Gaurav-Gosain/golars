package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestWhenThenOtherwise(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(2)))).
			Then(expr.Lit(int64(99))).
			Otherwise(expr.Col("a")).
			Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Int64)
	want := []int64{1, 2, 99, 99, 99}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %d want %d", i, arr.Value(i), v)
		}
	}
}

func TestWhenThenPromoteMixedDtype(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	// Then branch is int literal, Otherwise is float literal; result
	// should be promoted to float64.
	out, err := lazy.FromDataFrame(df).
		Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(1)))).
			Then(expr.Lit(int64(1))).
			Otherwise(expr.Lit(0.5)).
			Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Float64)
	want := []float64{0.5, 1, 1}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

// A missing otherwise is a null literal; the result takes the then
// branch's dtype (polars: pl.when(a > 1).then(1) is i64 with nulls).
func TestWhenThenNullOtherwise(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(1)))).
			Then(expr.Lit(int64(7))).
			Otherwise(expr.LitNull(dtype.Null())).
			Alias("r")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("r")
	arr := col.Chunk(0).(*array.Int64)
	if arr.IsValid(0) || arr.Value(1) != 7 || arr.Value(2) != 7 {
		t.Fatalf("got %v, want [null 7 7]", arr)
	}
}

// fill_null with a column fills row by row (polars:
// pl.col("b").fill_null(pl.col("a"))).
func TestFillNullFromColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{0.5, 0, 0}, []bool{true, false, false}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("b").FillNullExpr(expr.Col("a"))).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("b")
	arr := col.Chunk(0).(*array.Float64)
	want := []float64{0.5, 2, 3}
	for i, v := range want {
		if !arr.IsValid(i) || arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr, want)
		}
	}
}
