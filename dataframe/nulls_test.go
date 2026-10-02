package dataframe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDataFrameDropNullsAllColumns(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3, 4},
		[]bool{true, false, true, true},
		series.WithAllocator(alloc))
	b, _ := series.FromString("b",
		[]string{"x", "y", "", "z"},
		[]bool{true, true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.DropNulls(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height = %d, want 2", out.Height())
	}
}

func TestDataFrameDropNullsSubset(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	b, _ := series.FromString("b",
		[]string{"x", "y", ""},
		[]bool{true, true, false},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.DropNulls(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height = %d, want 2 (only rows where a is null drop)", out.Height())
	}
}

func TestDataFrameFillNullPerDtype(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	b, _ := series.FromString("b",
		[]string{"x", "", "z"},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	// Int64 fill: int column filled, string column left with nulls (dtype mismatch).
	out, err := df.FillNull(int64(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	aCol, _ := out.Column("a")
	if aCol.NullCount() != 0 {
		t.Errorf("a null_count = %d, want 0", aCol.NullCount())
	}
	if v := aCol.Chunk(0).(*array.Int64).Value(1); v != -1 {
		t.Errorf("a[1] = %d, want -1", v)
	}
	bCol, _ := out.Column("b")
	if bCol.NullCount() != 1 {
		t.Errorf("b null_count = %d, want 1 (dtype skip)", bCol.NullCount())
	}
}

func TestDataFrameUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 1, 2, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"x", "x", "y", "y", "z"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.Unique(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Fatalf("height = %d, want 3", out.Height())
	}
}

func TestDataFrameWithRowIndex(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromString("name", []string{"x", "y", "z"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := df.WithRowIndex("idx", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 2 {
		t.Fatalf("width = %d, want 2", out.Width())
	}
	names := out.ColumnNames()
	if names[0] != "idx" {
		t.Errorf("first column = %q, want idx", names[0])
	}
	idxCol, _ := out.Column("idx")
	idxArr := idxCol.Chunk(0).(*array.Uint32)
	for i, w := range []uint32{10, 11, 12} {
		if idxArr.Value(i) != w {
			t.Errorf("idx[%d] = %d, want %d", i, idxArr.Value(i), w)
		}
	}

	// Duplicate name errors.
	if _, err := df.WithRowIndex("name", 0); !errors.Is(err, dataframe.ErrDuplicateColumn) {
		t.Errorf("expected ErrDuplicateColumn, got %v", err)
	}
}

func TestDataFrameHStack(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{3, 4}, nil, series.WithAllocator(alloc))
	left, _ := dataframe.New(a)
	right, _ := dataframe.New(b)
	defer left.Release()
	defer right.Release()

	out, err := left.HStack(right)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 2 || out.Height() != 2 {
		t.Fatalf("shape = (%d,%d)", out.Width(), out.Height())
	}

	// Duplicate column name errors.
	dup, _ := series.FromInt64("a", []int64{5, 6}, nil, series.WithAllocator(alloc))
	dupDF, _ := dataframe.New(dup)
	defer dupDF.Release()
	if _, err := left.HStack(dupDF); !errors.Is(err, dataframe.ErrDuplicateColumn) {
		t.Errorf("expected ErrDuplicateColumn, got %v", err)
	}

	// Height mismatch errors.
	c, _ := series.FromInt64("c", []int64{9}, nil, series.WithAllocator(alloc))
	shortDF, _ := dataframe.New(c)
	defer shortDF.Release()
	if _, err := left.HStack(shortDF); !errors.Is(err, dataframe.ErrHeightMismatch) {
		t.Errorf("expected ErrHeightMismatch, got %v", err)
	}
}

func TestDataFrameExtend(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a1, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	d1, _ := dataframe.New(a1)
	defer d1.Release()
	a2, _ := series.FromInt64("a", []int64{3, 4}, nil, series.WithAllocator(alloc))
	d2, _ := dataframe.New(a2)
	defer d2.Release()

	out, err := d1.Extend(d2)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 4 {
		t.Errorf("height = %d, want 4", out.Height())
	}

	// Schema mismatch errors.
	b, _ := series.FromInt64("b", []int64{5, 6}, nil, series.WithAllocator(alloc))
	bDF, _ := dataframe.New(b)
	defer bDF.Release()
	if _, err := d1.Extend(bDF); err == nil {
		t.Errorf("expected schema-mismatch error")
	}
}

func TestDataFrameAnyNullMask(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	b, _ := series.FromString("b",
		[]string{"x", "y", ""},
		[]bool{true, true, false},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	mask, err := df.AnyNullMask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer mask.Release()
	m := mask.Chunk(0).(*array.Boolean)
	for i, want := range []bool{false, true, true} {
		if m.Value(i) != want {
			t.Errorf("mask[%d] = %v, want %v", i, m.Value(i), want)
		}
	}
}

func TestDataFrameApply(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{-1, -2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{-4, 5, -6}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.Apply(func(s *series.Series) (*series.Series, error) {
		return s.Abs(series.WithAllocator(alloc))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	aCol, _ := out.Column("a")
	aArr := aCol.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 2, 3} {
		if aArr.Value(i) != w {
			t.Errorf("abs(a)[%d] = %d, want %d", i, aArr.Value(i), w)
		}
	}
}

func TestDataFrameSampleFrac(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := df.SampleFrac(context.Background(), 0.3, false, 42)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Errorf("height = %d, want 3 (floor(0.3 * 10))", out.Height())
	}

	if _, err := df.SampleFrac(context.Background(), 1.5, false, 42); err == nil {
		t.Errorf("fraction > 1 without replacement should error")
	}
	if _, err := df.SampleFrac(context.Background(), -0.1, false, 42); err == nil {
		t.Errorf("negative fraction should error")
	}
}
