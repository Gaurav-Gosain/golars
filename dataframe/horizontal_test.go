package dataframe_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestSumHorizontalNoNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{10, 20, 30}, nil, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{100, 200, 300}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := df.SumHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	want := []float64{111, 222, 333}
	got := out.Chunk(0).(*array.Float64).Float64Values()
	for i, v := range want {
		if got[i] != v {
			t.Fatalf("row %d: got %v want %v", i, got[i], v)
		}
	}
}

func TestSumHorizontalIgnoreNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 6}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{1, 0, 0}, []bool{true, false, false}, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{4, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := df.SumHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// polars keeps the integer supertype.
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{6, 2, 9}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
	if arr.NullN() != 0 {
		t.Fatalf("ignore-nulls should yield no nulls when each row has at least one value, got %d", arr.NullN())
	}
}

func TestSumHorizontalPropagateNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 6}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{1, 0, 0}, []bool{true, false, true}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	out, err := df.SumHorizontal(context.Background(), dataframe.PropagateNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Int64)
	if !arr.IsValid(0) || arr.Value(0) != 2 {
		t.Fatalf("row 0: want valid 2, got valid=%v value=%v", arr.IsValid(0), arr.Value(0))
	}
	if arr.IsValid(1) {
		t.Fatalf("row 1: want null, got %v", arr.Value(1))
	}
	if !arr.IsValid(2) || arr.Value(2) != 6 {
		t.Fatalf("row 2: want valid 6, got valid=%v value=%v", arr.IsValid(2), arr.Value(2))
	}
}

func TestMeanHorizontalMatchesPolars(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 6}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{1, 0, 0}, []bool{true, false, false}, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{4, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := df.MeanHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	arr := out.Chunk(0).(*array.Float64)
	want := []float64{2.0, 2.0, 4.5}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestMinMaxHorizontal(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 6}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{1, 0, 0}, []bool{true, false, false}, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{4, 0, 3}, []bool{true, false, true}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	mn, err := df.MinHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Release()
	mx, err := df.MaxHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer mx.Release()

	mnArr := mn.Chunk(0).(*array.Int64)
	mxArr := mx.Chunk(0).(*array.Int64)
	mnWant := []int64{1, 2, 3}
	mxWant := []int64{4, 2, 6}
	for i := range mnWant {
		if mnArr.Value(i) != mnWant[i] {
			t.Fatalf("min row %d: got %v want %v", i, mnArr.Value(i), mnWant[i])
		}
		if mxArr.Value(i) != mxWant[i] {
			t.Fatalf("max row %d: got %v want %v", i, mxArr.Value(i), mxWant[i])
		}
	}
}

func TestHorizontalExplicitColumns(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromInt64("b", []int64{10, 20, 30}, nil, series.WithAllocator(alloc))
	c, _ := series.FromInt64("c", []int64{100, 200, 300}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, c)
	defer df.Release()

	out, err := df.SumHorizontal(context.Background(), dataframe.IgnoreNulls, "a", "c")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	// Fast path: same-dtype, no-null inputs preserve the input dtype.
	arr := out.Chunk(0).(*array.Int64)
	want := []int64{101, 202, 303}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("row %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestAllAnyHorizontal(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromBool("a", []bool{true, true, false}, nil, series.WithAllocator(alloc))
	b, _ := series.FromBool("b", []bool{true, false, false}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	all, err := df.AllHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer all.Release()
	any, err := df.AnyHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer any.Release()

	allArr := all.Chunk(0).(*array.Boolean)
	anyArr := any.Chunk(0).(*array.Boolean)
	wantAll := []bool{true, false, false}
	wantAny := []bool{true, true, false}
	for i := range wantAll {
		if allArr.Value(i) != wantAll[i] {
			t.Fatalf("all row %d: got %v want %v", i, allArr.Value(i), wantAll[i])
		}
		if anyArr.Value(i) != wantAny[i] {
			t.Fatalf("any row %d: got %v want %v", i, anyArr.Value(i), wantAny[i])
		}
	}
}

func TestHorizontalRejectsNonNumeric(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"x", "y", "z"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	// Default path (cols==nil) skips non-numeric columns silently so
	// the sum matches just column a.
	out, err := df.SumHorizontal(context.Background(), dataframe.IgnoreNulls)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// Explicit column list that includes a non-numeric must error.
	if _, err := df.SumHorizontal(context.Background(), dataframe.IgnoreNulls, "a", "b"); err == nil {
		t.Fatal("expected error for non-numeric column")
	}
}

func TestSumAllReturnsSingleRow(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	b, _ := series.FromFloat64("b", []float64{0.5, 1.5, 2.5, 3.5}, nil, series.WithAllocator(alloc))
	s, _ := series.FromString("s", []string{"x", "y", "z", "w"}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b, s)
	defer df.Release()

	out, err := df.SumAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 1 {
		t.Fatalf("height: got %d want 1", out.Height())
	}
	// polars keeps every column: a string sums to a null string.
	if out.Width() != 3 {
		t.Fatalf("width: got %d want 3", out.Width())
	}
	names := out.ColumnNames()
	if names[0] != "a" || names[1] != "b" || names[2] != "s" {
		t.Fatalf("names: got %v", names)
	}
	col, _ := out.Column("a")
	if col.Chunk(0).(*array.Int64).Value(0) != 10 {
		t.Fatalf("sum(a): got %v want 10", col.Chunk(0).(*array.Int64).Value(0))
	}
	if sc, _ := out.Column("s"); sc.NullCount() != 1 {
		t.Fatalf("sum(s): want null")
	}
}

func TestMeanAllAndStdAll(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a", []float64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	mean, err := df.MeanAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer mean.Release()

	col, _ := mean.Column("a")
	if col.Chunk(0).(*array.Float64).Value(0) != 3 {
		t.Fatalf("mean(a): got %v want 3", col.Chunk(0).(*array.Float64).Value(0))
	}

	std, err := df.StdAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer std.Release()
	sc, _ := std.Column("a")
	got := sc.Chunk(0).(*array.Float64).Value(0)
	want := math.Sqrt(2.5)
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("std(a): got %v want %v", got, want)
	}
}

func TestCountAllAndNullCountAll(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, []bool{true, false, true, true}, series.WithAllocator(alloc))
	b, _ := series.FromString("b", []string{"x", "y", "z", "w"}, []bool{true, true, false, true}, series.WithAllocator(alloc))
	df, _ := dataframe.New(a, b)
	defer df.Release()

	cnt, err := df.CountAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cnt.Release()

	ca, _ := cnt.Column("a")
	cb, _ := cnt.Column("b")
	if ca.Chunk(0).(*array.Uint32).Value(0) != 3 {
		t.Fatalf("count(a): got %v want 3", ca.Chunk(0).(*array.Uint32).Value(0))
	}
	if cb.Chunk(0).(*array.Uint32).Value(0) != 3 {
		t.Fatalf("count(b): got %v want 3", cb.Chunk(0).(*array.Uint32).Value(0))
	}

	nulls, err := df.NullCountAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer nulls.Release()
	na, _ := nulls.Column("a")
	nb, _ := nulls.Column("b")
	if na.Chunk(0).(*array.Uint32).Value(0) != 1 {
		t.Fatalf("nulls(a): got %v want 1", na.Chunk(0).(*array.Uint32).Value(0))
	}
	if nb.Chunk(0).(*array.Uint32).Value(0) != 1 {
		t.Fatalf("nulls(b): got %v want 1", nb.Chunk(0).(*array.Uint32).Value(0))
	}
}
