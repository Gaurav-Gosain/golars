package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Literal comparisons on dtypes outside the int64/float64 fast paths.
// Expected values come from polars 1.39.
func TestCompareLitOtherDTypes(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	opt := series.WithAllocator(mem)

	str, _ := series.FromString("s", []string{"us", "eu", ""}, []bool{true, true, false}, opt)
	defer str.Release()
	strB, _ := series.FromString("s", []string{"b", "a", "c"}, nil, opt)
	defer strB.Release()
	b, _ := series.FromBool("b", []bool{true, false, false}, []bool{true, true, false}, opt)
	defer b.Release()
	f32, _ := series.FromFloat32("f", []float32{1, 2.5}, nil, opt)
	defer f32.Release()
	i32, _ := series.FromInt32("i", []int32{2, 3}, nil, opt)
	defer i32.Release()
	i64, _ := series.FromInt64("i", []int64{2, 3}, nil, opt)
	defer i64.Release()

	cases := []struct {
		name string
		fn   func() (*series.Series, error)
		want string
	}{
		{"str == us", func() (*series.Series, error) { return compute.EqLit(ctx, str, "us", compute.WithAllocator(mem)) }, "[True, False, None]"},
		{"str >= b", func() (*series.Series, error) { return compute.GeLit(ctx, strB, "b", compute.WithAllocator(mem)) }, "[True, False, True]"},
		{"bool == true", func() (*series.Series, error) { return compute.EqLit(ctx, b, true, compute.WithAllocator(mem)) }, "[True, False, None]"},
		{"f32 > 2.0", func() (*series.Series, error) { return compute.GtLit(ctx, f32, 2.0, compute.WithAllocator(mem)) }, "[False, True]"},
		{"i32 == 2.5", func() (*series.Series, error) { return compute.EqLit(ctx, i32, 2.5, compute.WithAllocator(mem)) }, "[False, False]"},
		{"i64 < 2.5", func() (*series.Series, error) { return compute.LtLit(ctx, i64, 2.5, compute.WithAllocator(mem)) }, "[True, False]"},
	}
	for _, c := range cases {
		out, err := c.fn()
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
		out.Release()
	}
}

// polars total order with a literal: [NaN, 1, None] against 5 and NaN.
func TestCompareLitNaN(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	nan := math.NaN()
	withNull, _ := series.FromFloat64("x", []float64{nan, 1, 0}, []bool{true, true, false}, series.WithAllocator(mem))
	defer withNull.Release()
	noNull, _ := series.FromFloat64("x", []float64{nan, 1, 7, 5}, nil, series.WithAllocator(mem))
	defer noNull.Release()
	type fn func(context.Context, *series.Series, any, ...compute.Option) (*series.Series, error)
	cases := []struct {
		name string
		s    *series.Series
		op   fn
		lit  float64
		want string
	}{
		{"null == NaN", withNull, compute.EqLit, nan, "[True, False, None]"},
		{"null > 5", withNull, compute.GtLit, 5, "[True, False, None]"},
		{"null < NaN", withNull, compute.LtLit, nan, "[False, True, None]"},
		{"null != NaN", withNull, compute.NeLit, nan, "[False, True, None]"},
		{"> 5", noNull, compute.GtLit, 5, "[True, False, True, False]"},
		{">= 5", noNull, compute.GeLit, 5, "[True, False, True, True]"},
		{"< 5", noNull, compute.LtLit, 5, "[False, True, False, False]"},
		{"<= 5", noNull, compute.LeLit, 5, "[False, True, False, True]"},
		{"== 5", noNull, compute.EqLit, 5, "[False, False, False, True]"},
		{"== NaN", noNull, compute.EqLit, nan, "[True, False, False, False]"},
		{">= NaN", noNull, compute.GeLit, nan, "[True, False, False, False]"},
		{"<= NaN", noNull, compute.LeLit, nan, "[True, True, True, True]"},
	}
	for _, c := range cases {
		out, err := c.op(ctx, c.s, c.lit, compute.WithAllocator(mem))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
		out.Release()
	}
}

// The inverted Gt/Ge path must stay correct at SIMD and parallel sizes.
func TestCompareLitNaNLarge(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	n := 300_003
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = float64(i % 10)
	}
	for i := 0; i < n; i += 97 {
		vals[i] = math.NaN()
	}
	s, _ := series.FromFloat64("x", vals, nil, series.WithAllocator(mem))
	defer s.Release()
	out, err := compute.GtLit(context.Background(), s, 4.0, compute.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0)
	for i, v := range vals {
		want := v != v || v > 4
		got := arr.(interface{ Value(int) bool }).Value(i)
		if got != want {
			t.Fatalf("row %d (%v): got %v, want %v", i, v, got, want)
		}
	}
	if out.Len() != n || arr.NullN() != 0 {
		t.Fatalf("len=%d nulls=%d", out.Len(), arr.NullN())
	}
}
