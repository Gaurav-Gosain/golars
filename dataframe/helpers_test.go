package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func fixtureDF(t *testing.T, alloc memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("id", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	b, _ := series.FromString("name", []string{"ada", "brian", "carl"}, nil,
		series.WithAllocator(alloc))
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestDataFrameIsEmptyAndClear(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()
	if df.IsEmpty() {
		t.Errorf("IsEmpty = true, want false")
	}

	cleared := df.Clear()
	defer cleared.Release()
	if !cleared.IsEmpty() {
		t.Errorf("Clear() should produce an empty frame")
	}
	if cleared.Width() != df.Width() {
		t.Errorf("Clear should preserve schema width")
	}
}

func TestDataFrameColumnNamesAndDTypes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()
	names := df.ColumnNames()
	if len(names) != 2 || names[0] != "id" || names[1] != "name" {
		t.Errorf("ColumnNames = %v", names)
	}
	ts := df.DTypes()
	if len(ts) != 2 {
		t.Fatalf("DTypes len = %d", len(ts))
	}
	if ts[0].String() != "i64" {
		t.Errorf("DTypes[0] = %s", ts[0])
	}
	if ts[1].String() != "str" {
		t.Errorf("DTypes[1] = %s", ts[1])
	}
}

func TestDataFrameEqualsAndHasColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a := fixtureDF(t, alloc)
	defer a.Release()
	b := fixtureDF(t, alloc)
	defer b.Release()

	if !a.Equals(b) {
		t.Errorf("identical frames should compare equal")
	}
	if !a.HasColumn("id") || a.HasColumn("missing") {
		t.Errorf("HasColumn mismatch")
	}
}

func TestDataFrameWithColumns(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()

	extra, _ := series.FromInt64("age", []int64{27, 34, 19}, nil,
		series.WithAllocator(alloc))
	out, err := df.WithColumns(extra)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 3 {
		t.Fatalf("width = %d, want 3", out.Width())
	}
	if !out.HasColumn("age") {
		t.Errorf("missing age column")
	}
}

func TestDataFrameNullCountAndEstimatedSize(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a",
		[]int64{1, 0, 3},
		[]bool{true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(a)
	defer df.Release()

	nc := df.NullCount()
	defer nc.Release()
	if nc.Height() != 1 || nc.Width() != 1 {
		t.Fatalf("null_count shape = (%d,%d)", nc.Height(), nc.Width())
	}
	row, _ := nc.Row(0)
	if row[0].(uint32) != 1 {
		t.Errorf("null_count = %v, want 1", row[0])
	}

	if df.EstimatedSize() <= 0 {
		t.Errorf("EstimatedSize = %d, want > 0", df.EstimatedSize())
	}
}

func TestDataFrameRowAndRows(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()

	r, err := df.Row(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r) != 2 || r[0].(int64) != 2 || r[1].(string) != "brian" {
		t.Errorf("Row(1) = %v", r)
	}
	if _, err := df.Row(99); err == nil {
		t.Errorf("out-of-bounds Row should error")
	}

	rows, _ := df.Rows()
	if len(rows) != 3 {
		t.Errorf("Rows len = %d", len(rows))
	}

	m, _ := df.ToMap()
	if len(m["id"]) != 3 {
		t.Errorf("ToMap id len = %d", len(m["id"]))
	}
}

func TestDataFrameGatherAndReverse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()
	ctx := context.Background()

	g, err := df.Gather(ctx, []int{2, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Release()
	if g.Height() != 2 {
		t.Fatalf("Gather height = %d", g.Height())
	}
	r0, _ := g.Row(0)
	if r0[0].(int64) != 3 {
		t.Errorf("Gather[0] = %v, want id=3", r0)
	}

	// Negative index (count-from-end).
	g2, err := df.Gather(ctx, []int{-1})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Release()
	r, _ := g2.Row(0)
	if r[0].(int64) != 3 {
		t.Errorf("Gather[-1] = %v, want id=3", r)
	}

	// Out-of-range error.
	if _, err := df.Gather(ctx, []int{99}); err == nil {
		t.Errorf("Gather out-of-range should error")
	}

	rev, err := df.Reverse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Release()
	r0, _ = rev.Row(0)
	if r0[0].(int64) != 3 {
		t.Errorf("Reverse[0] id = %v, want 3", r0[0])
	}
}

func TestDataFrameShuffle(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()
	ctx := context.Background()

	out, err := df.Shuffle(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Errorf("Shuffle height = %d", out.Height())
	}
}

func TestDataFrameLimitAndGlimpse(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := fixtureDF(t, alloc)
	defer df.Release()

	lim := df.Limit(2)
	defer lim.Release()
	if lim.Height() != 2 {
		t.Errorf("Limit(2) height = %d", lim.Height())
	}

	g := df.Glimpse(1)
	if g == "" {
		t.Errorf("Glimpse should return non-empty")
	}
}
