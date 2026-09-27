package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestSortInt64Ascending(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, err := compute.Sort(context.Background(), s, compute.SortOptions{},
		compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	defer got.Release()

	assertInt64Values(t, got, []int64{1, 1, 2, 3, 4, 5, 6, 9}, nil)
}

func TestSortInt64Descending(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{Descending: true},
		compute.WithAllocator(mem))
	defer got.Release()

	assertInt64Values(t, got, []int64{9, 6, 5, 4, 3, 2, 1, 1}, nil)
}

func TestSortWithNullsLast(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x",
		[]int64{3, 0, 4, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{Nulls: compute.NullsLast},
		compute.WithAllocator(mem))
	defer got.Release()

	assertInt64Values(t, got,
		[]int64{3, 4, 5, 0, 0},
		[]bool{true, true, true, false, false})
}

// polars sorts with nulls_last=False by default, ascending and
// descending: pl.Series([3, None, 4, None, 5]).sort() == [None, None, 3, 4, 5].
func TestSortDefaultNullsFirst(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x",
		[]int64{3, 0, 4, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s, compute.SortOptions{}, compute.WithAllocator(mem))
	defer got.Release()
	assertInt64Values(t, got,
		[]int64{0, 0, 3, 4, 5},
		[]bool{false, false, true, true, true})

	desc, _ := compute.Sort(context.Background(), s, compute.SortOptions{Descending: true}, compute.WithAllocator(mem))
	defer desc.Release()
	assertInt64Values(t, desc,
		[]int64{0, 0, 5, 4, 3},
		[]bool{false, false, true, true, true})
}

func TestSortWithNullsFirst(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromInt64("x",
		[]int64{3, 0, 4, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{Nulls: compute.NullsFirst},
		compute.WithAllocator(mem))
	defer got.Release()

	assertInt64Values(t, got,
		[]int64{0, 0, 3, 4, 5},
		[]bool{false, false, true, true, true})
}

func TestSortFloat64WithNaN(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromFloat64("x",
		[]float64{3.0, math.NaN(), 1.5, 2.0, math.NaN()},
		nil,
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{},
		compute.WithAllocator(mem))
	defer got.Release()

	// Expected: [1.5, 2.0, 3.0, NaN, NaN] (NaN sorts last).
	vals := float64Values(got)
	if vals[0] != 1.5 || vals[1] != 2.0 || vals[2] != 3.0 {
		t.Errorf("front = %v, want [1.5 2.0 3.0 ...]", vals[:3])
	}
	if !math.IsNaN(vals[3]) || !math.IsNaN(vals[4]) {
		t.Errorf("tail = %v, want [NaN NaN]", vals[3:])
	}
}

func TestSortString(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromString("x",
		[]string{"banana", "apple", "cherry", "apricot"},
		nil,
		series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{},
		compute.WithAllocator(mem))
	defer got.Release()

	vals := stringValuesAt(got)
	want := []string{"apple", "apricot", "banana", "cherry"}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("[%d] = %q, want %q", i, vals[i], want[i])
		}
	}
}

func TestSortStability(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	// Two keys that tie on value. Stable sort preserves original order.
	keys, _ := series.FromInt64("k", []int64{1, 1, 1, 1}, nil, series.WithAllocator(mem))
	defer keys.Release()

	idx, _ := compute.SortIndices(context.Background(), keys,
		compute.SortOptions{},
		compute.WithAllocator(mem))
	for i, v := range idx {
		if v != i {
			t.Errorf("stable indices = %v, want identity", idx)
			return
		}
	}
}

func TestSortMultiKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	// Rows: (1, b), (1, a), (0, c), (0, b). Sorting by (a asc, b asc) gives:
	//   (0, b), (0, c), (1, a), (1, b). Indices: [3, 2, 1, 0].
	a, _ := series.FromInt64("a", []int64{1, 1, 0, 0}, nil, series.WithAllocator(mem))
	b, _ := series.FromString("b", []string{"b", "a", "c", "b"}, nil, series.WithAllocator(mem))
	defer a.Release()
	defer b.Release()

	idx, err := compute.SortIndicesMulti(context.Background(),
		[]*series.Series{a, b},
		[]compute.SortOptions{{}, {}},
		compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("SortIndicesMulti: %v", err)
	}
	want := []int{3, 2, 1, 0}
	for i := range want {
		if idx[i] != want[i] {
			t.Errorf("indices = %v, want %v", idx, want)
			return
		}
	}
}

func TestSortBool(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	s, _ := series.FromBool("x",
		[]bool{true, false, true, false, true},
		nil, series.WithAllocator(mem))
	defer s.Release()

	got, _ := compute.Sort(context.Background(), s,
		compute.SortOptions{}, compute.WithAllocator(mem))
	defer got.Release()

	vals := boolValuesAt(got)
	want := []bool{false, false, true, true, true}
	for i, w := range want {
		if vals[i] != w {
			t.Errorf("[%d] = %v, want %v", i, vals[i], w)
		}
	}
}

func BenchmarkSortInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1024, 1 << 14, 1 << 18} {
		b.Run(benchSize(n), func(b *testing.B) {
			base := make([]int64, n)
			for i := range base {
				base[i] = int64((i * 2654435761) % n) // pseudo-random
			}
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n) * 8)
			for b.Loop() {
				s, _ := series.FromInt64("x", base, nil)
				out, err := compute.Sort(ctx, s, compute.SortOptions{})
				if err != nil {
					b.Fatal(err)
				}
				s.Release()
				out.Release()
			}
		})
	}
}
