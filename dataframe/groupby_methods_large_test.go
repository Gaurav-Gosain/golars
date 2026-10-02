package dataframe_test

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// largeGroupFrame builds n rows with nGroups int64 keys in a scrambled
// first-appearance order and a float value column with some nulls.
func largeGroupFrame(t testing.TB, mem memory.Allocator, n, nGroups int, withNulls bool) *dataframe.DataFrame {
	keys := make([]int64, n)
	vals := make([]float64, n)
	var valid []bool
	if withNulls {
		valid = make([]bool, n)
	}
	for i := range n {
		keys[i] = int64((i * 7919) % nGroups)
		vals[i] = float64((i * 31) % 997)
		if withNulls {
			valid[i] = i%13 != 0
		}
	}
	k, err := series.FromInt64("k", keys, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	v, err := series.FromFloat64("v", vals, valid, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(k, v)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// TestGroupByMethodsLargeParallel exercises the parallel assign and
// per-group fan-out paths and checks them against a naive computation.
func TestGroupByMethodsLargeParallel(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	opt := dataframe.WithGroupByAllocator(mem)
	const n, groups = 300_000, 1_000
	df := largeGroupFrame(t, mem, n, groups, true)
	defer df.Release()

	byKey := map[int64][]float64{}
	nulls := map[int64]bool{}
	var order []int64
	kArr := df.ColumnAt(0).Chunk(0).(*array.Int64)
	vArr := df.ColumnAt(1).Chunk(0).(*array.Float64)
	for i := range n {
		k := kArr.Value(i)
		if _, ok := byKey[k]; !ok && !nulls[k] {
			order = append(order, k)
		}
		if vArr.IsNull(i) {
			nulls[k] = true
			if byKey[k] == nil {
				byKey[k] = []float64{}
			}
			continue
		}
		byKey[k] = append(byKey[k], vArr.Value(i))
	}

	g := df.GroupBy("k").MaintainOrder()
	med, err := g.Median(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer med.Release()
	nu, err := g.NUnique(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer nu.Release()
	sum, err := g.Sum(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer sum.Release()
	if med.Height() != groups || nu.Height() != groups || sum.Height() != groups {
		t.Fatalf("heights %d %d %d", med.Height(), nu.Height(), sum.Height())
	}
	mk := med.ColumnAt(0).Chunk(0).(*array.Int64)
	mv := med.ColumnAt(1).Chunk(0).(*array.Float64)
	nv := nu.ColumnAt(1).Chunk(0).(*array.Uint32)
	sv := sum.ColumnAt(1).Chunk(0).(*array.Float64)
	for gid, k := range order {
		if mk.Value(gid) != k {
			t.Fatalf("group %d key %d want %d", gid, mk.Value(gid), k)
		}
		vals := slices.Clone(byKey[k])
		slices.Sort(vals)
		var want float64
		if m := len(vals); m%2 == 1 {
			want = vals[m/2]
		} else {
			want = vals[m/2-1] + (vals[m/2]-vals[m/2-1])*0.5
		}
		if math.Abs(mv.Value(gid)-want) > 1e-9 {
			t.Fatalf("median group %d = %v want %v", k, mv.Value(gid), want)
		}
		distinct := len(slices.Compact(vals))
		if nulls[k] {
			distinct++
		}
		if int(nv.Value(gid)) != distinct {
			t.Fatalf("n_unique group %d = %d want %d", k, nv.Value(gid), distinct)
		}
		var s float64
		for _, x := range byKey[k] {
			s += x
		}
		if math.Abs(sv.Value(gid)-s) > 1e-6 {
			t.Fatalf("sum group %d = %v want %v", k, sv.Value(gid), s)
		}
	}
}

// TestGroupByMaintainOrderLargeNoNulls covers the parallel int64 assign
// path, which only runs without nulls.
func TestGroupByMaintainOrderLargeNoNulls(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	df := largeGroupFrame(t, mem, 300_000, 5_000, false)
	defer df.Release()
	out, err := df.GroupBy("k").MaintainOrder().Len(ctx, "", dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	keys := out.ColumnAt(0).Chunk(0).(*array.Int64)
	seen := map[int64]bool{}
	src := df.ColumnAt(0).Chunk(0).(*array.Int64)
	next := 0
	for i := range src.Len() {
		k := src.Value(i)
		if seen[k] {
			continue
		}
		seen[k] = true
		if keys.Value(next) != k {
			t.Fatalf("group %d key %d want %d", next, keys.Value(next), k)
		}
		next++
	}
}

func BenchmarkGroupByMedian1M(b *testing.B) {
	ctx := context.Background()
	df := largeGroupFrame(b, memory.DefaultAllocator, 1_000_000, 1_000, false)
	defer df.Release()
	b.ResetTimer()
	for range b.N {
		out, err := df.GroupBy("k").Median(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkGroupByNUnique1M(b *testing.B) {
	ctx := context.Background()
	df := largeGroupFrame(b, memory.DefaultAllocator, 1_000_000, 1_000, false)
	defer df.Release()
	b.ResetTimer()
	for range b.N {
		out, err := df.GroupBy("k").NUnique(ctx)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
