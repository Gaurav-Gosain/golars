package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFramePivot(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	ids, _ := series.FromString("id", []string{"A", "A", "B", "B"}, nil, series.WithAllocator(alloc))
	cat, _ := series.FromString("cat", []string{"x", "y", "x", "y"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(ids, cat, v)
	defer df.Release()

	out, err := df.Pivot(context.Background(), []string{"id"}, "cat", "v", dataframe.PivotFirst)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height: got %d want 2", out.Height())
	}
	// Expect id, x, y columns.
	names := out.ColumnNames()
	if len(names) != 3 || names[0] != "id" {
		t.Fatalf("columns: got %v want [id,x,y]", names)
	}
	xCol, _ := out.Column("x")
	yCol, _ := out.Column("y")
	// polars keeps the value dtype for first.
	x := xCol.Chunk(0).(*array.Int64)
	y := yCol.Chunk(0).(*array.Int64)
	if x.Value(0) != 1 || x.Value(1) != 3 {
		t.Fatalf("x: got %v, %v", x.Value(0), x.Value(1))
	}
	if y.Value(0) != 2 || y.Value(1) != 4 {
		t.Fatalf("y: got %v, %v", y.Value(0), y.Value(1))
	}
}

func TestDataFramePivotSumAgg(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	ids, _ := series.FromString("id", []string{"A", "A", "A", "B"}, nil, series.WithAllocator(alloc))
	cat, _ := series.FromString("cat", []string{"x", "x", "y", "x"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 10}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(ids, cat, v)
	defer df.Release()

	out, err := df.Pivot(context.Background(), []string{"id"}, "cat", "v", dataframe.PivotSum)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	xCol, _ := out.Column("x")
	// polars keeps the value dtype for sum.
	x := xCol.Chunk(0).(*array.Int64)
	// A: 1+2=3, B: 10
	if x.Value(0) != 3 || x.Value(1) != 10 {
		t.Fatalf("x sums: got %v, %v", x.Value(0), x.Value(1))
	}
}
