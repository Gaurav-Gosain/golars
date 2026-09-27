package compute_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestSumInt64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, err := compute.SumInt64(context.Background(), s, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("SumInt64: %v", err)
	}
	if got != 15 {
		t.Errorf("SumInt64 = %d, want 15", got)
	}
}

func TestSumInt64SkipsNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x",
		[]int64{10, 0, 30, 0, 50},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumInt64(context.Background(), s, compute.WithAllocator(mem))
	if got != 90 {
		t.Errorf("SumInt64 = %d, want 90", got)
	}
}

func TestSumFloat64(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromFloat64("x", []float64{1.5, 2.5, 3.5}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumFloat64(context.Background(), s, compute.WithAllocator(mem))
	if got != 7.5 {
		t.Errorf("SumFloat64 = %v, want 7.5", got)
	}
}

func TestSumFloat64FromInt(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumFloat64(context.Background(), s, compute.WithAllocator(mem))
	if got != 6.0 {
		t.Errorf("SumFloat64 = %v, want 6.0", got)
	}
}

func TestSumInt64LargeParallel(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	const n = 1 << 18
	vals := make([]int64, n)
	var want int64
	for i := range vals {
		vals[i] = int64(i)
		want += int64(i)
	}
	s, _ := series.FromInt64("x", vals, nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.SumInt64(context.Background(), s, compute.WithAllocator(mem))
	if got != want {
		t.Errorf("SumInt64 = %d, want %d", got, want)
	}
}

func TestMean(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromFloat64("x", []float64{2, 4, 6, 8}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, ok, err := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("MeanFloat64: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != 5.0 {
		t.Errorf("MeanFloat64 = %v, want 5.0", got)
	}
}

func TestMeanEmptyReturnsNotOK(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x",
		[]int64{0, 0, 0},
		[]bool{false, false, false},
		series.WithAllocator(mem))
	defer s.Release()

	_, ok, err := compute.MeanFloat64(context.Background(), s, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("MeanFloat64: %v", err)
	}
	if ok {
		t.Error("expected ok=false on all-null input")
	}
}

func TestMinMaxInt(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x", []int64{5, 3, 9, 1, 7}, nil, series.WithAllocator(mem))
	defer s.Release()

	minV, ok, _ := compute.MinInt64(context.Background(), s, compute.WithAllocator(mem))
	if !ok || minV != 1 {
		t.Errorf("Min = (%d, %v), want (1, true)", minV, ok)
	}
	maxV, ok, _ := compute.MaxInt64(context.Background(), s, compute.WithAllocator(mem))
	if !ok || maxV != 9 {
		t.Errorf("Max = (%d, %v), want (9, true)", maxV, ok)
	}
}

func TestMinMaxIntSkipsNulls(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x",
		[]int64{5, 3, 9, 1, 7},
		[]bool{true, true, false, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	minV, _, _ := compute.MinInt64(context.Background(), s, compute.WithAllocator(mem))
	if minV != 3 {
		t.Errorf("Min = %d, want 3", minV)
	}
	maxV, _, _ := compute.MaxInt64(context.Background(), s, compute.WithAllocator(mem))
	if maxV != 7 {
		t.Errorf("Max = %d, want 7", maxV)
	}
}

func TestMinMaxFloat(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromFloat64("x", []float64{5.5, 3.3, 9.9, 1.1, 7.7}, nil, series.WithAllocator(mem))
	defer s.Release()

	minV, _, _ := compute.MinFloat64(context.Background(), s, compute.WithAllocator(mem))
	if minV != 1.1 {
		t.Errorf("Min = %v, want 1.1", minV)
	}
	maxV, _, _ := compute.MaxFloat64(context.Background(), s, compute.WithAllocator(mem))
	if maxV != 9.9 {
		t.Errorf("Max = %v, want 9.9", maxV)
	}
}

func TestMinMaxFloatNaN(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	nan := math.NaN()
	ctx := context.Background()
	big := make([]float64, 300_000)
	for i := range big {
		big[i] = float64(i % 1000)
	}
	big[0], big[7], big[len(big)-1] = nan, nan, nan
	allNaN := make([]float64, 300_000)
	for i := range allNaN {
		allNaN[i] = nan
	}
	// polars 1.39: NaN is ignored by min/max unless every value is NaN.
	cases := []struct {
		name     string
		vals     []float64
		valid    []bool
		min, max float64
	}{
		{"mixed", []float64{1.0, nan, 3.0, 2.0}, nil, 1, 3},
		{"leading nan", []float64{nan, 3.0, 2.0}, nil, 2, 3},
		{"all nan", []float64{nan, nan}, nil, nan, nan},
		{"nan and null", []float64{nan, 0, 5}, []bool{true, false, true}, 5, 5},
		{"nan only with null", []float64{nan, 0}, []bool{true, false}, nan, nan},
		{"large", big, nil, 0, 999},
		{"large all nan", allNaN, nil, nan, nan},
	}
	same := func(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }
	for _, c := range cases {
		s, _ := series.FromFloat64("x", c.vals, c.valid, series.WithAllocator(mem))
		maxV, _, _ := compute.MaxFloat64(ctx, s, compute.WithAllocator(mem))
		minV, _, _ := compute.MinFloat64(ctx, s, compute.WithAllocator(mem))
		s.Release()
		if !same(maxV, c.max) || !same(minV, c.min) {
			t.Errorf("%s: min, max = %v, %v; want %v, %v", c.name, minV, maxV, c.min, c.max)
		}
	}
}

func TestCountAndNullCount(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("x",
		[]int64{1, 0, 3, 0},
		[]bool{true, false, true, false},
		series.WithAllocator(mem))
	defer s.Release()

	if got := compute.Count(s); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}
	if got := compute.NullCount(s); got != 2 {
		t.Errorf("NullCount = %d, want 2", got)
	}
}

func TestSumUnsupportedDType(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("x", []string{"a"}, nil, series.WithAllocator(mem))
	defer s.Release()

	_, err := compute.SumInt64(context.Background(), s, compute.WithAllocator(mem))
	if !errors.Is(err, compute.ErrUnsupportedDType) {
		t.Errorf("expected ErrUnsupportedDType, got %v", err)
	}
}

// Benchmarks.

func BenchmarkSumInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			vals := make([]int64, n)
			for i := range vals {
				vals[i] = int64(i)
			}
			s, _ := series.FromInt64("x", vals, nil)
			defer s.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				_, err := compute.SumInt64(ctx, s)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMinFloat64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18, 1 << 22} {
		b.Run(benchSize(n), func(b *testing.B) {
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = float64(n - i)
			}
			s, _ := series.FromFloat64("x", vals, nil)
			defer s.Release()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				_, _, err := compute.MinFloat64(ctx, s)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
