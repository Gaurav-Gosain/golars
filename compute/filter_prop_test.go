package compute_test

import (
	"context"

	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

var filterPropTypes = append(sortPropTypes,
	arrow.FixedWidthTypes.Date32,
	arrow.FixedWidthTypes.Timestamp_us,
	arrow.FixedWidthTypes.Duration_ns,
)

func randMask(r *rand.Rand, n int) []any {
	m := make([]any, n)
	mode := r.IntN(4)
	for i := range m {
		switch mode {
		case 0: // all true
			m[i] = true
		case 1: // all false
			m[i] = false
		default:
			if r.IntN(6) == 0 {
				continue // null mask entry
			}
			m[i] = r.IntN(2) == 0
		}
	}
	return m
}

func checkFilterOne(t *testing.T, mem memory.Allocator, r *rand.Rand, dt arrow.DataType, n int) {
	t.Helper()
	vals := testutil.RandValues(r, dt, n, 0.2*float64(r.IntN(2)))
	maskVals := randMask(r, n)
	s := propSeries(t, mem, r, "x", dt, vals, r.IntN(numLayouts))
	defer s.Release()
	vals = s.ToList()
	mask := propSeries(t, mem, r, "m", arrow.FixedWidthTypes.Boolean, maskVals, r.IntN(numLayouts))
	defer mask.Release()

	var want []any
	for i, m := range maskVals {
		if m == true {
			want = append(want, vals[i])
		}
	}
	got, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(mem))
	if err != nil {
		t.Fatalf("%s n=%d: %v", dt, n, err)
	}
	defer got.Release()
	gv := got.ToList()
	if len(gv) != len(want) {
		t.Fatalf("%s n=%d: filter kept %d rows want %d", dt, n, len(gv), len(want))
	}
	for i := range want {
		if !sameValue(gv[i], want[i]) {
			t.Fatalf("%s n=%d: row %d = %v want %v", dt, n, i, gv[i], want[i])
		}
	}
	if got.DType().ID() != dt.ID() {
		t.Fatalf("%s: filter changed dtype to %s", dt, got.DType())
	}
}

func TestFilterProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(7, 8))
	sizes := append(testutil.PropSizes, 127, 128, 129, 1000, 4097)
	for _, dt := range filterPropTypes {
		for _, n := range sizes {
			for range 4 {
				checkFilterOne(t, mem, r, dt, n)
			}
		}
	}
}

// FuzzFilterInt64 derives values and mask bits from the fuzzer input.
func FuzzFilterInt64(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{0xff, 0x0f}, uint8(0))
	f.Fuzz(func(t *testing.T, data, maskBits []byte, layout uint8) {
		n := len(data)
		if len(maskBits) == 0 {
			return
		}
		vals := make([]any, n)
		maskVals := make([]any, n)
		for i := range n {
			if data[i]%11 != 0 {
				vals[i] = int64(int8(data[i]))
			}
			b := maskBits[(i/8)%len(maskBits)]>>(i%8)&1 == 1
			if int(layout)&4 != 0 && data[i]%13 == 0 {
				continue
			}
			maskVals[i] = b
		}
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		r := rand.New(rand.NewPCG(uint64(layout), 9))
		s := propSeries(t, mem, r, "x", arrow.PrimitiveTypes.Int64, vals, int(layout)%numLayouts)
		defer s.Release()
		mask := propSeries(t, mem, r, "m", arrow.FixedWidthTypes.Boolean, maskVals, int(layout/8)%numLayouts)
		defer mask.Release()
		got, err := compute.Filter(context.Background(), s, mask, compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		gv := got.ToList()
		j := 0
		for i, m := range maskVals {
			if m != true {
				continue
			}
			if j >= len(gv) || !testutil.ValuesEqual(gv[j], vals[i]) {
				t.Fatalf("mismatch at output row %d (input row %d)", j, i)
			}
			j++
		}
		if j != len(gv) {
			t.Fatalf("filter kept %d rows want %d", len(gv), j)
		}
	})
}

func TestSliceHeadTailProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(9, 10))
	for iter := range 300 {
		dt := filterPropTypes[r.IntN(len(filterPropTypes))]
		n := testutil.PropSizes[r.IntN(len(testutil.PropSizes))]
		vals := testutil.RandValues(r, dt, n, 0.2)
		s := propSeries(t, mem, r, "x", dt, vals, r.IntN(numLayouts))
		vals = s.ToList()
		off := r.IntN(2*n+3) - n - 1
		length := r.IntN(n+3) - 1
		start, l := clampRef(off, length, n)
		cs, cl := series.ClampSlice(off, length, n)
		if cs != start || cl != l {
			t.Fatalf("iter %d: ClampSlice(%d,%d,%d) = %d,%d want %d,%d", iter, off, length, n, cs, cl, start, l)
		}
		sl, err := s.Slice(start, l)
		if err != nil {
			t.Fatal(err)
		}
		got := sl.ToList()
		for i := range l {
			if !testutil.ValuesEqual(got[i], vals[start+i]) {
				t.Fatalf("iter %d %s: slice(%d,%d)[%d] = %v want %v", iter, dt, start, l, i, got[i], vals[start+i])
			}
		}
		sl.Release()
		s.Release()
	}
}

// clampRef is polars' slice semantics written out directly: a negative
// offset counts from the end, a negative length (None in polars) runs
// to the end, and the window is clipped to the data.
func clampRef(offset, length, n int) (int, int) {
	start := offset
	if start < 0 {
		start = n + start
	}
	end := n
	if length >= 0 {
		end = start + length
	}
	start = max(0, min(start, n))
	end = max(start, min(end, n))
	return start, end - start
}
