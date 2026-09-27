package dataframe_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

var frameColTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us, arrow.FixedWidthTypes.Duration_ms,
}

// randFrame builds a frame with an int64 "id" column (0..n-1) plus
// ncols random columns; it returns the frame and every column's values.
func randFrame(t *testing.T, mem memory.Allocator, r *rand.Rand, n, ncols int, types []arrow.DataType) (*dataframe.DataFrame, [][]any) {
	t.Helper()
	ids := make([]any, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	cols := []*series.Series{propSeries(t, mem, r, "id", arrow.PrimitiveTypes.Int64, ids)}
	vals := [][]any{ids}
	for c := range ncols {
		dt := types[r.IntN(len(types))]
		s := propSeries(t, mem, r, fmt.Sprintf("c%d", c), dt, testutil.RandValues(r, dt, n, 0.2))
		cols = append(cols, s)
		vals = append(vals, s.ToList())
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df, vals
}

func frameValues(df *dataframe.DataFrame) [][]any {
	out := make([][]any, df.Width())
	for i := range df.Width() {
		out[i] = df.ColumnAt(i).ToList()
	}
	return out
}

// checkRows asserts that got holds exactly the source rows listed in
// rows, in that order.
func checkRows(t *testing.T, desc string, got *dataframe.DataFrame, src [][]any, rows []int) {
	t.Helper()
	if got.Height() != len(rows) {
		t.Fatalf("%s: height %d want %d", desc, got.Height(), len(rows))
	}
	gv := frameValues(got)
	for c := range src {
		for i, row := range rows {
			if !testutil.ValuesEqual(gv[c][i], src[c][row]) {
				t.Fatalf("%s: column %s row %d = %v want %v (source row %d)",
					desc, got.ColumnAt(c).Name(), i, gv[c][i], src[c][row], row)
			}
		}
	}
}

func TestFrameSortProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(41, 42))
	sortable := frameColTypes
	sizes := append(slices.Clone(testutil.PropSizes), 1000)
	for iter := range 300 {
		n := sizes[r.IntN(len(sizes))]
		ncols := 1 + r.IntN(3)
		df, vals := randFrame(t, mem, r, n, ncols, sortable)
		nk := 1 + r.IntN(ncols)
		perm := r.Perm(ncols)[:nk]
		keys := make([]string, nk)
		keyVals := make([][]any, nk)
		opts := make([]compute.SortOptions, nk)
		for k, c := range perm {
			keys[k] = fmt.Sprintf("c%d", c)
			keyVals[k] = vals[c+1]
			opts[k].Descending = r.IntN(2) == 0
			if r.IntN(2) == 0 {
				opts[k].Nulls = compute.NullsLast
			}
		}
		want := refStableOrder(keyVals, opts, n)
		out, err := df.SortBy(ctx, keys, opts, dataframe.WithSortAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		if !slices.Equal(idsOf(out), want) {
			t.Fatalf("iter %d n=%d sort by %v %+v types %v:\n got %v\nwant %v\nkeys %v", iter, n, keys, opts, typesOf(df, keys), idsOf(out), want, keyVals)
		}
		checkRows(t, fmt.Sprintf("iter %d sort by %v %+v", iter, keys, opts), out, vals, want)
		out.Release()
		df.Release()
	}
}

func refStableOrder(keys [][]any, opts []compute.SortOptions, n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		for k, col := range keys {
			va, vb := col[a], col[b]
			var c int
			switch {
			case va == nil && vb == nil:
			case va == nil:
				c = -1
				if opts[k].Nulls == compute.NullsLast {
					c = 1
				}
			case vb == nil:
				c = 1
				if opts[k].Nulls == compute.NullsLast {
					c = -1
				}
			default:
				c = testutil.CompareValues(va, vb)
				if opts[k].Descending {
					c = -c
				}
			}
			if c != 0 {
				return c
			}
		}
		return 0
	})
	return idx
}

func TestFrameFilterProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(43, 44))
	sizes := append(slices.Clone(testutil.PropSizes), 1000, 5000)
	for iter := range 200 {
		n := sizes[r.IntN(len(sizes))]
		df, vals := randFrame(t, mem, r, n, r.IntN(12), frameColTypes)
		maskVals := make([]any, n)
		var keep []int
		for i := range maskVals {
			if r.IntN(7) == 0 {
				continue
			}
			b := r.IntN(2) == 0
			maskVals[i] = b
			if b {
				keep = append(keep, i)
			}
		}
		mask := propSeries(t, mem, r, "m", arrow.FixedWidthTypes.Boolean, maskVals)
		out, err := df.Filter(ctx, mask, dataframe.WithFilterAllocator(mem))
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		checkRows(t, fmt.Sprintf("iter %d filter", iter), out, vals, keep)
		out.Release()
		mask.Release()
		df.Release()
	}
}

func TestFrameSliceHeadTailProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(45, 46))
	for iter := range 300 {
		n := testutil.PropSizes[r.IntN(len(testutil.PropSizes))]
		df, vals := randFrame(t, mem, r, n, 1+r.IntN(3), frameColTypes)
		k := r.IntN(n+3) - 1
		kk := max(0, min(k, n))
		head := df.Head(k)
		checkRows(t, fmt.Sprintf("iter %d head(%d)", iter, k), head, vals, seq(0, kk))
		tail := df.Tail(k)
		checkRows(t, fmt.Sprintf("iter %d tail(%d)", iter, k), tail, vals, seq(n-kk, n))
		head.Release()
		tail.Release()
		off := r.IntN(n + 1)
		length := r.IntN(n - off + 1)
		sl, err := df.Slice(off, length)
		if err != nil {
			t.Fatalf("iter %d: slice(%d,%d) of %d: %v", iter, off, length, n, err)
		}
		checkRows(t, fmt.Sprintf("iter %d slice(%d,%d)", iter, off, length), sl, vals, seq(off, off+length))
		// Slicing a slice composes.
		if length > 0 {
			o2 := r.IntN(length)
			l2 := r.IntN(length - o2 + 1)
			sl2, err := sl.Slice(o2, l2)
			if err != nil {
				t.Fatal(err)
			}
			checkRows(t, fmt.Sprintf("iter %d slice(%d,%d).slice(%d,%d)", iter, off, length, o2, l2), sl2, vals, seq(off+o2, off+o2+l2))
			sl2.Release()
		}
		sl.Release()
		df.Release()
	}
}

func idsOf(df *dataframe.DataFrame) []int {
	c, _ := df.Column("id")
	var out []int
	for _, v := range c.ToList() {
		out = append(out, int(v.(int64)))
	}
	return out
}

func typesOf(df *dataframe.DataFrame, names []string) []string {
	var out []string
	for _, n := range names {
		c, _ := df.Column(n)
		out = append(out, c.DType().String())
	}
	return out
}

func seq(lo, hi int) []int {
	out := make([]int, 0, max(hi-lo, 0))
	for i := lo; i < hi; i++ {
		out = append(out, i)
	}
	return out
}
