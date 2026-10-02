package compute_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Chunk layouts used by the property tests.
const (
	layoutSingle = iota
	layoutChunked
	layoutSliced
	numLayouts
)

// propSeries builds a Series holding vals in the given chunk layout.
// Sliced layouts carry non-zero array offsets, which is where bitmap
// and buffer offset bugs hide.
func propSeries(t testing.TB, mem memory.Allocator, r *rand.Rand, name string, dt arrow.DataType, vals []any, layout int) *series.Series {
	t.Helper()
	build := func(v []any) *series.Series {
		s, err := series.FromValues(name, dt, v, series.WithAllocator(mem))
		if err != nil {
			t.Fatalf("FromValues(%s): %v", dt, err)
		}
		return s
	}
	switch layout {
	case layoutChunked:
		if len(vals) < 2 {
			return build(vals)
		}
		cuts := []int{0}
		for range 1 + r.IntN(3) {
			cuts = append(cuts, r.IntN(len(vals)+1))
		}
		cuts = append(cuts, len(vals))
		slices.Sort(cuts)
		var chunks []arrow.Array
		for i := 1; i < len(cuts); i++ {
			part := build(vals[cuts[i-1]:cuts[i]])
			c := part.Chunks()[0]
			c.Retain()
			part.Release()
			chunks = append(chunks, c)
		}
		s, err := series.New(name, chunks...)
		if err != nil {
			t.Fatal(err)
		}
		return s
	case layoutSliced:
		pre := r.IntN(9)
		post := r.IntN(9)
		padded := make([]any, 0, pre+len(vals)+post)
		padded = append(padded, testutil.RandValues(r, dt, pre, 0.3)...)
		padded = append(padded, vals...)
		padded = append(padded, testutil.RandValues(r, dt, post, 0.3)...)
		full := build(padded)
		defer full.Release()
		s, err := full.Slice(pre, len(vals))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	return build(vals)
}

var sortPropTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Duration_ms,
}

// refCompareRows orders rows a and b of vals the way polars does:
// nulls first unless nullsLast (in both directions), descending flips
// only the non-null comparison.
func refCompareRows(vals []any, so compute.SortOptions, a, b int) int {
	va, vb := vals[a], vals[b]
	switch {
	case va == nil && vb == nil:
		return 0
	case va == nil:
		if so.Nulls == compute.NullsLast {
			return 1
		}
		return -1
	case vb == nil:
		if so.Nulls == compute.NullsLast {
			return -1
		}
		return 1
	}
	c := testutil.CompareValues(va, vb)
	if so.Descending {
		c = -c
	}
	return c
}

// refSortIndices is the reference stable sort permutation.
func refSortIndices(keys [][]any, opts []compute.SortOptions, n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		for k := range keys {
			if c := refCompareRows(keys[k], opts[k], a, b); c != 0 {
				return c
			}
		}
		return 0
	})
	return idx
}

func sameValue(a, b any) bool {
	// Sort output must keep NaN as NaN and keep the exact value, but
	// -0.0 and 0.0 are equal keys so either may land first.
	if fa, ok := a.(float64); ok {
		if fb, ok := b.(float64); ok {
			if math.IsNaN(fa) || math.IsNaN(fb) {
				return math.IsNaN(fa) && math.IsNaN(fb)
			}
			return fa == fb
		}
	}
	return testutil.ValuesEqual(a, b)
}

func checkSortOne(t *testing.T, mem memory.Allocator, r *rand.Rand, dt arrow.DataType, n int, layout int, so compute.SortOptions, nullFrac float64) {
	t.Helper()
	ctx := context.Background()
	vals := testutil.RandValues(r, dt, n, nullFrac)
	s := propSeries(t, mem, r, "x", dt, vals, layout)
	defer s.Release()
	vals = s.ToList()
	desc := fmt.Sprintf("%s n=%d layout=%d opts=%+v nulls=%.2f", dt, n, layout, so, nullFrac)

	want := refSortIndices([][]any{vals}, []compute.SortOptions{so}, n)

	got, err := compute.SortIndices(ctx, s, so, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("%s: SortIndices: %v", desc, err)
	}
	// Stable sort has exactly one valid permutation once -0.0 and 0.0
	// compare equal, so the indices must match the reference exactly.
	if !slices.Equal(got, want) {
		i := firstDiff(got, want)
		t.Fatalf("%s: SortIndices differs at %d: got idx %d (%v) want idx %d (%v)",
			desc, i, got[i], vals[got[i]], want[i], vals[want[i]])
	}

	sorted, err := compute.Sort(ctx, s, so, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("%s: Sort: %v", desc, err)
	}
	defer sorted.Release()
	if sorted.Len() != n {
		t.Fatalf("%s: Sort length %d want %d", desc, sorted.Len(), n)
	}
	gotVals := sorted.ToList()
	for i, w := range want {
		if !sameValue(gotVals[i], vals[w]) {
			t.Fatalf("%s: Sort value %d = %v want %v", desc, i, gotVals[i], vals[w])
		}
	}
}

func firstDiff(a, b []int) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func TestSortPropSingleKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(1, 2))
	allOpts := []compute.SortOptions{
		{},
		{Descending: true},
		{Nulls: compute.NullsLast},
		{Descending: true, Nulls: compute.NullsLast},
	}
	for _, dt := range sortPropTypes {
		for _, n := range testutil.PropSizes {
			for layout := range numLayouts {
				for _, so := range allOpts {
					for _, nf := range []float64{0, 0.2} {
						checkSortOne(t, mem, r, dt, n, layout, so, nf)
					}
				}
			}
		}
	}
}

// TestSortPropCutoffs sorts around the serial/parallel radix cutoffs,
// where a different kernel takes over.
func TestSortPropCutoffs(t *testing.T) {
	if testing.Short() {
		t.Skip("large inputs")
	}
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(3, 4))
	floatCut := 64 * 1024
	intCut := 256 * 1024
	cases := []struct {
		dt arrow.DataType
		n  int
	}{
		{arrow.PrimitiveTypes.Float64, floatCut - 1},
		{arrow.PrimitiveTypes.Float64, floatCut},
		{arrow.PrimitiveTypes.Float64, floatCut + 1},
		{arrow.PrimitiveTypes.Float32, floatCut},
		{arrow.PrimitiveTypes.Int64, intCut - 1},
		{arrow.PrimitiveTypes.Int64, intCut},
		{arrow.PrimitiveTypes.Int32, intCut + 1},
		{arrow.PrimitiveTypes.Uint64, intCut},
		{arrow.BinaryTypes.String, floatCut + 1},
	}
	for _, c := range cases {
		for _, so := range []compute.SortOptions{{}, {Descending: true, Nulls: compute.NullsLast}} {
			for _, nf := range []float64{0, 0.1} {
				checkSortOne(t, mem, r, c.dt, c.n, r.IntN(numLayouts), so, nf)
			}
		}
	}
}

func TestSortPropMultiKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(5, 6))
	sizes := append(slices.Clone(testutil.PropSizes), 1000, 5000)
	for iter := range 400 {
		n := sizes[r.IntN(len(sizes))]
		nk := 1 + r.IntN(3)
		keys := make([][]any, nk)
		cols := make([]*series.Series, nk)
		opts := make([]compute.SortOptions, nk)
		for k := range nk {
			dt := sortPropTypes[r.IntN(len(sortPropTypes))]
			nf := 0.0
			if r.IntN(2) == 0 {
				nf = 0.25
			}
			keys[k] = testutil.RandValues(r, dt, n, nf)
			cols[k] = propSeries(t, mem, r, fmt.Sprintf("k%d", k), dt, keys[k], r.IntN(numLayouts))
			keys[k] = cols[k].ToList()
			opts[k] = compute.SortOptions{Descending: r.IntN(2) == 0}
			if r.IntN(2) == 0 {
				opts[k].Nulls = compute.NullsLast
			}
		}
		want := refSortIndices(keys, opts, n)
		got, err := compute.SortIndicesMulti(ctx, cols, opts, compute.WithAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		if !slices.Equal(got, want) {
			i := firstDiff(got, want)
			var types []string
			for _, c := range cols {
				types = append(types, c.DType().String())
			}
			t.Fatalf("iter %d n=%d types=%v opts=%+v: differs at %d: got row %d want row %d",
				iter, n, types, opts, i, got[i], want[i])
		}
		for _, c := range cols {
			c.Release()
		}
	}
}

// FuzzSortFloat64 checks that SortIndices on arbitrary float64 bit
// patterns (with a null mask) matches the reference stable sort.
func FuzzSortFloat64(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0, 0, 0, 0, 0, 128}, uint8(0), uint8(0))
	f.Add(make([]byte, 64), uint8(3), uint8(5))
	f.Fuzz(func(t *testing.T, data []byte, flags uint8, nullSeed uint8) {
		n := len(data) / 8
		vals := make([]any, n)
		for i := range n {
			bits := uint64(0)
			for j := range 8 {
				bits |= uint64(data[i*8+j]) << (8 * j)
			}
			if nullSeed != 0 && (i*int(nullSeed))%7 == 0 {
				continue
			}
			vals[i] = math.Float64frombits(bits)
		}
		so := compute.SortOptions{Descending: flags&1 != 0}
		if flags&2 != 0 {
			so.Nulls = compute.NullsLast
		}
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		r := rand.New(rand.NewPCG(uint64(flags), uint64(nullSeed)))
		s := propSeries(t, mem, r, "x", arrow.PrimitiveTypes.Float64, vals, int(flags>>2)%numLayouts)
		defer s.Release()
		got, err := compute.SortIndices(context.Background(), s, so, compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		want := refSortIndices([][]any{vals}, []compute.SortOptions{so}, n)
		if !slices.Equal(got, want) {
			i := firstDiff(got, want)
			t.Fatalf("opts=%+v differs at %d: got %v want %v", so, i, vals[got[i]], vals[want[i]])
		}
		sorted, err := compute.Sort(context.Background(), s, so, compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		defer sorted.Release()
		gv := sorted.ToList()
		for i, w := range want {
			if !sameValue(gv[i], vals[w]) {
				t.Fatalf("opts=%+v Sort value %d = %v want %v", so, i, gv[i], vals[w])
			}
		}
	})
}

// FuzzSortInt64 does the same for int64 keys, covering the radix paths.
func FuzzSortInt64(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 255, 255, 255, 255, 255, 255, 255}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, flags uint8) {
		n := len(data) / 8
		vals := make([]any, n)
		for i := range n {
			bits := uint64(0)
			for j := range 8 {
				bits |= uint64(data[i*8+j]) << (8 * j)
			}
			if flags&8 != 0 && data[i*8]%5 == 0 {
				continue
			}
			vals[i] = int64(bits)
		}
		so := compute.SortOptions{Descending: flags&1 != 0}
		if flags&2 != 0 {
			so.Nulls = compute.NullsLast
		}
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		r := rand.New(rand.NewPCG(uint64(flags), 1))
		s := propSeries(t, mem, r, "x", arrow.PrimitiveTypes.Int64, vals, int(flags>>4)%numLayouts)
		defer s.Release()
		got, err := compute.SortIndices(context.Background(), s, so, compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		want := refSortIndices([][]any{vals}, []compute.SortOptions{so}, n)
		if !slices.Equal(got, want) {
			t.Fatalf("opts=%+v differs at %d", so, firstDiff(got, want))
		}
	})
}
