package eval_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestOverSumByGroup(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "a", "b", "b", "a"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 10, 20, 3}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").Sum().Over("k").Alias("vs")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("vs")
	arr := col.Chunk(0).(*array.Int64)
	// a: 1+2+3=6; b: 10+20=30.
	want := []int64{6, 6, 30, 30, 6}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestOverSumMultiKeyWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 1, 0, 1, 0},
		[]bool{true, true, false, true, false}, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{7, 0, 7, 7, 7},
		[]bool{true, false, true, true, true}, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{10, 20, 30, 40, 50}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").Sum().Over("a", "b").Alias("s")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("s")
	arr := col.Chunk(0).(*array.Int64)
	// Groups: (1,7)={0:10,3:40}=50; (1,null)={1:20}=20;
	// (null,7)={2:30,4:50}=80.
	want := []int64{50, 20, 80, 50, 80}
	for i, w := range want {
		if !arr.IsValid(i) || arr.Value(i) != w {
			t.Fatalf("idx %d: got valid=%v value=%v want %v",
				i, arr.IsValid(i), arr.Value(i), w)
		}
	}
}

func TestOverFloatKeysGroupSignedZeroAndNaN(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromFloat64("k",
		[]float64{0.0, math.NaN(), 1.0, 0.0, math.NaN()},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 4, 8, 16}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").Sum().Over("k").Alias("s")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("s")
	arr := col.Chunk(0).(*array.Int64)
	// +0/-0 share a group (9), NaNs share a group (18), 1.0 alone (4).
	// Every row must hit its group: no nulls in the output.
	want := []int64{9, 18, 4, 9, 18}
	for i, w := range want {
		if !arr.IsValid(i) || arr.Value(i) != w {
			t.Fatalf("idx %d: got valid=%v value=%v want %v",
				i, arr.IsValid(i), arr.Value(i), w)
		}
	}
}

func TestOverCumSumIntKeyWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromInt64("k", []int64{1, 2, 1, 0, 2, 1}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{10, 0, 20, 30, 40, 0},
		[]bool{true, false, true, true, true, false}, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").CumSum().Over("k").Alias("cum")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("cum")
	arr := col.Chunk(0).(*array.Float64)
	// Group 1: rows 0(10),2(20),5(null) -> 10,30,null.
	// Group 2: rows 1(null),4(40) -> null,40.
	// Group 0: row 3(30) -> 30.
	want := []float64{10, 0, 30, 30, 40, 0}
	wantValid := []bool{true, false, true, true, true, false}
	for i := range want {
		if arr.IsValid(i) != wantValid[i] {
			t.Fatalf("idx %d: valid=%v want %v", i, arr.IsValid(i), wantValid[i])
		}
		if wantValid[i] && arr.Value(i) != want[i] {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), want[i])
		}
	}
}

// TestOverGenericPath exercises the generic per-group loop with a
// non-scalar, non-cumsum inner that no fast path claims.
func TestOverGenericPath(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "a", "b", "a", "b"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, -2, 3, -4, 5}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").Abs().Over("k").Alias("av")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("av")
	arr := col.Chunk(0).(*array.Float64)
	want := []float64{1, 2, 3, 4, 5}
	for i, w := range want {
		if !arr.IsValid(i) || arr.Value(i) != w {
			t.Fatalf("idx %d: got valid=%v value=%v want %v",
				i, arr.IsValid(i), arr.Value(i), w)
		}
	}
}

func TestOverCumSumByGroup(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k", []string{"a", "a", "b", "a", "b"}, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", []int64{1, 2, 10, 3, 20}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Select(expr.Col("v").CumSum().Over("k").Alias("cum")).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("cum")
	arr := col.Chunk(0).(*array.Float64)
	// Per-group cumsum broadcast back to original row order:
	// a: 1, 3, ?, 6, ?  → rows 0,1,3 = 1,3,6
	// b: 10, ?, 30     → rows 2,4 = 10,30
	// Combined by position: [1, 3, 10, 6, 30]
	want := []float64{1, 3, 10, 6, 30}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}
