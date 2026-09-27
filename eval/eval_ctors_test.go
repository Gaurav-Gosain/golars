package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestCoalesceExpr(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{10, 20, 0}, []bool{true, true, false}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Coalesce(expr.Col("a"), expr.Col("b")).Alias("c")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("c")
	arr := col.Chunk(0).(*array.Int64) // polars keeps i64
	// Row 0: a=1 -> 1. Row 1: a null, b=20 -> 20. Row 2: a=3 -> 3.
	want := []int64{1, 20, 3}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestConcatStrExpr(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromString("a", []string{"x", "y"}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{1, 2}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.ConcatStr("-", expr.Col("a"), expr.Col("b")).Alias("c")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("c")
	arr := col.Chunk(0).(*array.String)
	want := []string{"x-1", "y-2"}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %q want %q", i, arr.Value(i), v)
		}
	}
}
