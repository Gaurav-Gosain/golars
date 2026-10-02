package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// selectCol runs a lazy Select pipeline on a single column fixture and
// returns the resulting Series. Centralises the setup for every
// eval-path test below.
func selectCol(t *testing.T, alloc memory.Allocator, data []int64, e expr.Expr) *series.Series {
	t.Helper()
	s, err := series.FromInt64("a", data, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := lazy.FromDataFrame(df).Select(e).
		Collect(context.Background(), lazy.WithExecAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, err := out.Column(expr.OutputName(e))
	if err != nil {
		t.Fatal(err)
	}
	return col.Clone()
}

func TestExprSortAndUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	got := selectCol(t, alloc, []int64{3, 1, 2, 1, 4}, expr.Col("a").Sort(false).Alias("sorted"))
	defer got.Release()
	arr := got.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 1, 2, 3, 4} {
		if arr.Value(i) != w {
			t.Errorf("Sort[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}

	u := selectCol(t, alloc, []int64{3, 1, 3, 2, 1}, expr.Col("a").Unique().Alias("u"))
	defer u.Release()
	if u.Len() != 3 {
		t.Errorf("Unique len = %d, want 3", u.Len())
	}
}

func TestExprCumFamily(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	cs := selectCol(t, alloc, []int64{1, 2, 3, 4}, expr.Col("a").CumSum().Alias("cs"))
	defer cs.Release()
	cArr := cs.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 3, 6, 10} {
		if cArr.Value(i) != w {
			t.Errorf("CumSum[%d] = %d, want %d", i, cArr.Value(i), w)
		}
	}

	cmin := selectCol(t, alloc, []int64{5, 2, 8, 1, 3}, expr.Col("a").CumMin().Alias("cmin"))
	defer cmin.Release()
	mArr := cmin.Chunk(0).(*array.Int64)
	for i, w := range []int64{5, 2, 2, 1, 1} {
		if mArr.Value(i) != w {
			t.Errorf("CumMin[%d] = %d, want %d", i, mArr.Value(i), w)
		}
	}

	cmax := selectCol(t, alloc, []int64{1, 5, 3, 8, 2}, expr.Col("a").CumMax().Alias("cmax"))
	defer cmax.Release()
	xArr := cmax.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 5, 5, 8, 8} {
		if xArr.Value(i) != w {
			t.Errorf("CumMax[%d] = %d, want %d", i, xArr.Value(i), w)
		}
	}

	cc := selectCol(t, alloc, []int64{1, 2, 3}, expr.Col("a").CumCount().Alias("cc"))
	defer cc.Release()
	ccArr := cc.Chunk(0).(*array.Uint32)
	for i, w := range []uint32{1, 2, 3} {
		if ccArr.Value(i) != w {
			t.Errorf("CumCount[%d] = %d, want %d", i, ccArr.Value(i), w)
		}
	}
}

func TestExprDiffAndPctChange(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	d := selectCol(t, alloc, []int64{5, 8, 2, 10}, expr.Col("a").Diff(1).Alias("d"))
	defer d.Release()
	dArr := d.Chunk(0).(*array.Int64)
	if dArr.IsValid(0) {
		t.Errorf("Diff[0] should be null")
	}
	for i, w := range []int64{0, 3, -6, 8} {
		if i == 0 {
			continue
		}
		if dArr.Value(i) != w {
			t.Errorf("Diff[%d] = %d, want %d", i, dArr.Value(i), w)
		}
	}

	pct := selectCol(t, alloc, []int64{100, 150, 75}, expr.Col("a").PctChange(1).Alias("pct"))
	defer pct.Release()
	pArr := pct.Chunk(0).(*array.Float64)
	// 150/100 - 1 = 0.5; 75/150 - 1 = -0.5
	if pArr.Value(1) != 0.5 {
		t.Errorf("PctChange[1] = %v, want 0.5", pArr.Value(1))
	}
	if pArr.Value(2) != -0.5 {
		t.Errorf("PctChange[2] = %v, want -0.5", pArr.Value(2))
	}
}

func TestExprRankAndUniqueDuplicated(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	r := selectCol(t, alloc, []int64{3, 1, 2, 2, 5}, expr.Col("a").Rank("min").Alias("r"))
	defer r.Release()
	// polars returns u32 ranks for every method except average.
	rArr := r.Chunk(0).(*array.Uint32)
	for i, w := range []uint32{4, 1, 2, 2, 5} {
		if rArr.Value(i) != w {
			t.Errorf("Rank.min[%d] = %v, want %v", i, rArr.Value(i), w)
		}
	}

	d := selectCol(t, alloc, []int64{1, 2, 1, 3}, expr.Col("a").IsDuplicated().Alias("d"))
	defer d.Release()
	dArr := d.Chunk(0).(*array.Boolean)
	for i, w := range []bool{true, false, true, false} {
		if dArr.Value(i) != w {
			t.Errorf("IsDuplicated[%d] = %v, want %v", i, dArr.Value(i), w)
		}
	}
}

func TestExprHashValues(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	h := selectCol(t, alloc, []int64{1, 2, 1}, expr.Col("a").HashValues().Alias("h"))
	defer h.Release()
	if h.DType().String() != "u64" {
		t.Errorf("Hash dtype = %s, want u64", h.DType())
	}
	hArr := h.Chunk(0).(*array.Uint64)
	if hArr.Value(0) != hArr.Value(2) {
		t.Errorf("equal inputs should hash equally")
	}
	if hArr.Value(0) == hArr.Value(1) {
		t.Errorf("different inputs should (almost always) hash differently")
	}
}

func TestExprNUnique(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	n := selectCol(t, alloc, []int64{1, 2, 1, 3, 2}, expr.Col("a").NUnique().Alias("n"))
	defer n.Release()
	// polars returns n_unique as u32.
	arr := n.Chunk(0).(*array.Uint32)
	if arr.Value(0) != 3 {
		t.Errorf("NUnique = %d, want 3", arr.Value(0))
	}
}
