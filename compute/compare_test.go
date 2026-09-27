package compute_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestCompareInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{5, 4, 3, 2, 1}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	cases := []struct {
		name string
		fn   func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error)
		want []bool
	}{
		{"Eq", compute.Eq, []bool{false, false, true, false, false}},
		{"Ne", compute.Ne, []bool{true, true, false, true, true}},
		{"Lt", compute.Lt, []bool{true, true, false, false, false}},
		{"Le", compute.Le, []bool{true, true, true, false, false}},
		{"Gt", compute.Gt, []bool{false, false, false, true, true}},
		{"Ge", compute.Ge, []bool{false, false, true, true, true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(ctx, a, b, compute.WithAllocator(mem))
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			defer got.Release()
			assertBoolValues(t, got, tc.want, nil)
		})
	}
}

func TestCompareFloat64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromFloat64("a", []float64{1.5, 2.5, 3.5}, nil, series.WithAllocator(mem))
	b, _ := series.FromFloat64("b", []float64{1.5, 2.0, 4.0}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	eq, _ := compute.Eq(ctx, a, b, compute.WithAllocator(mem))
	defer eq.Release()
	assertBoolValues(t, eq, []bool{true, false, false}, nil)

	gt, _ := compute.Gt(ctx, a, b, compute.WithAllocator(mem))
	defer gt.Release()
	assertBoolValues(t, gt, []bool{false, true, false}, nil)
}

func TestCompareString(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromString("a", []string{"apple", "banana", "cherry"}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"apple", "avocado", "cherry"}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	eq, _ := compute.Eq(ctx, a, b, compute.WithAllocator(mem))
	defer eq.Release()
	assertBoolValues(t, eq, []bool{true, false, true}, nil)

	lt, _ := compute.Lt(ctx, a, b, compute.WithAllocator(mem))
	defer lt.Release()
	assertBoolValues(t, lt, []bool{false, false, false}, nil)

	gt, _ := compute.Gt(ctx, a, b, compute.WithAllocator(mem))
	defer gt.Release()
	// "banana" > "avocado" lexically (b > a), "cherry" == "cherry" so false.
	assertBoolValues(t, gt, []bool{false, true, false}, nil)
}

func TestCompareBool(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromBool("a", []bool{true, false, true, false}, nil, series.WithAllocator(mem))
	b, _ := series.FromBool("b", []bool{true, true, false, false}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	eq, _ := compute.Eq(ctx, a, b, compute.WithAllocator(mem))
	defer eq.Release()
	assertBoolValues(t, eq, []bool{true, false, false, true}, nil)

	ne, _ := compute.Ne(ctx, a, b, compute.WithAllocator(mem))
	defer ne.Release()
	assertBoolValues(t, ne, []bool{false, true, true, false}, nil)

	// polars orders booleans false < true.
	lt, err := compute.Lt(ctx, a, b, compute.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer lt.Release()
	assertBoolValues(t, lt, []bool{false, true, false, false}, nil)
}

func TestCompareNullPropagation(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	a, _ := series.FromInt64("a",
		[]int64{1, 2, 3, 4},
		[]bool{true, false, true, true},
		series.WithAllocator(mem))
	b, _ := series.FromInt64("b",
		[]int64{1, 2, 0, 4},
		[]bool{true, true, false, true},
		series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	eq, _ := compute.Eq(ctx, a, b, compute.WithAllocator(mem))
	defer eq.Release()

	if eq.NullCount() != 2 {
		t.Errorf("NullCount = %d, want 2", eq.NullCount())
	}
	assertBoolValues(t, eq, []bool{true, false, false, true}, []bool{true, false, false, true})
}

func TestCompareUnsupportedDType(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	// A list column has no ordering.
	list := func(name string, v int64) *series.Series {
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
		defer lb.Release()
		lb.Append(true)
		lb.ValueBuilder().(*array.Int64Builder).Append(v)
		s, err := series.New(name, lb.NewArray())
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := list("a", 1), list("b", 2)
	defer a.Release()
	defer b.Release()

	_, err := compute.Lt(ctx, a, b, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrUnsupportedDType) {
		t.Errorf("Lt on list: expected ErrUnsupportedDType, got %v", err)
	}
}

func BenchmarkCompareInt64Gt(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			av := make([]int64, n)
			bv := make([]int64, n)
			for i := range av {
				av[i] = int64(i)
				bv[i] = int64(n - i)
			}
			sa, _ := series.FromInt64("a", av, nil)
			sb, _ := series.FromInt64("b", bv, nil)
			defer sa.Release()
			defer sb.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				out, err := compute.Gt(ctx, sa, sb)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
