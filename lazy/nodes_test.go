package lazy_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestLazyUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 1, 2, 3, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).Unique().
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Errorf("height = %d, want 3", out.Height())
	}
}

func TestLazyFillNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).FillNull(int64(-1)).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	col, _ := out.Column("a")
	if col.NullCount() != 0 {
		t.Errorf("post-fill null_count = %d", col.NullCount())
	}
	if col.Chunk(0).(*array.Int64).Value(1) != -1 {
		t.Errorf("filled value = %d, want -1", col.Chunk(0).(*array.Int64).Value(1))
	}
}

func TestLazyDropNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{10, 20, 30}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	// Default: drop any-null rows.
	out, err := lazy.FromDataFrame(df).DropNulls().
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Errorf("DropNulls height = %d, want 2", out.Height())
	}

	// Subset: only check column b (no nulls) - should keep all rows.
	out2, err := lazy.FromDataFrame(df).DropNulls("b").
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	if out2.Height() != 3 {
		t.Errorf("DropNulls(b) height = %d, want 3", out2.Height())
	}
}

func TestLazyCast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).Cast("a", dtype.Float64()).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("a")
	if col.DType().String() != "f64" {
		t.Errorf("dtype after cast = %s, want f64", col.DType())
	}
}

func TestLazyCache(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	// Cache uses runtime.SetFinalizer to release its held frame when the
	// cacheState is GC'd. AssertSize runs after our explicit GC below
	// so the finalizer fires before the leak check.
	defer func() {
		runtime.GC()
		runtime.GC()
		alloc.AssertSize(t, 0)
	}()

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	// Scope the LazyFrame so its cache state can be GC'd before
	// AssertSize.
	func() {
		lf := lazy.FromDataFrame(df).Cache()
		r1, err := lf.Collect(context.Background(), lazy.WithExecAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer r1.Release()
		r2, err := lf.Collect(context.Background(), lazy.WithExecAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		defer r2.Release()
		if !r1.Equals(r2) {
			t.Errorf("Cache second collect differs from first")
		}
	}()
}

func TestLazyWithRowIndex(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromString("n", []string{"x", "y", "z"}, nil,
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).WithRowIndex("row", 100).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.ColumnNames()[0] != "row" {
		t.Errorf("first col = %q, want row", out.ColumnNames()[0])
	}
	idx, _ := out.Column("row")
	arr := idx.Chunk(0).(*array.Uint32)
	if arr.Value(0) != 100 || arr.Value(2) != 102 {
		t.Errorf("row idx = [%d, %d, %d]", arr.Value(0), arr.Value(1), arr.Value(2))
	}
}

func TestLazyIterBatches(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	count := 0
	for batch, err := range lazy.FromDataFrame(df).IterBatches(context.Background(), lazy.WithExecAllocator(alloc)) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if batch.Height() != 3 {
			t.Errorf("batch height = %d", batch.Height())
		}
		batch.Release()
	}
	if count != 1 {
		t.Errorf("batch count = %d, want 1", count)
	}
}
