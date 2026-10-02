// Behavioural parity with polars' tests/unit/operations/test_join.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Polars' join test suite is enormous (~30 files worth of edge
// cases); this file ports a hand-written subset. The option matrix
// (every join type with coalesce, nulls_equal, maintain_order,
// validate, suffixes and multi-key or mixed-dtype keys) is covered by
// join_general_parity_test.go, generated from polars.
//
// Scenarios NOT ported:
//   - join on expressions       (no expression-keyed join)

package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// smallJoinFrames builds two sources commonly used across polars' join
// tests: left with keys 1..4, right with keys matching a subset.
func smallJoinFrames(t *testing.T, alloc memory.Allocator) (left, right *dataframe.DataFrame) {
	t.Helper()
	lk, _ := series.FromInt64("k", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", []int64{10, 20, 30, 40}, nil, series.WithAllocator(alloc))
	left, err := dataframe.New(lk, lv)
	if err != nil {
		t.Fatal(err)
	}
	rk, _ := series.FromInt64("k", []int64{2, 3, 5}, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", []int64{200, 300, 500}, nil, series.WithAllocator(alloc))
	right, err = dataframe.New(rk, rv)
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}

// Polars: test_join: inner join keeps only the intersection of keys,
// in left-row order, with right's columns appended.
func TestParityJoinInnerKeepsIntersection(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, r := smallJoinFrames(t, alloc)
	defer l.Release()
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height=%d want 2 (keys 2 and 3 match)", out.Height())
	}
	// Column order: left columns first (k, lv), then right's non-key (rv).
	if got := out.Schema().Names(); !strSliceEq(got, []string{"k", "lv", "rv"}) {
		t.Fatalf("column order = %v", got)
	}
	k := out.ColumnAt(0).Chunk(0).(*array.Int64)
	lv := out.ColumnAt(1).Chunk(0).(*array.Int64)
	rv := out.ColumnAt(2).Chunk(0).(*array.Int64)
	// Expected rows (left-order): (2, 20, 200), (3, 30, 300).
	if k.Value(0) != 2 || lv.Value(0) != 20 || rv.Value(0) != 200 {
		t.Fatalf("row 0 mismatch: %d %d %d", k.Value(0), lv.Value(0), rv.Value(0))
	}
	if k.Value(1) != 3 || lv.Value(1) != 30 || rv.Value(1) != 300 {
		t.Fatalf("row 1 mismatch: %d %d %d", k.Value(1), lv.Value(1), rv.Value(1))
	}
}

// Polars: left join keeps every left row; non-matching right columns
// are null. The left side's column order is preserved and row order
// follows the left input.
func TestParityJoinLeftKeepsAllLeftRows(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, r := smallJoinFrames(t, alloc)
	defer l.Release()
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.LeftJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 4 {
		t.Fatalf("height=%d want 4", out.Height())
	}
	rv := out.ColumnAt(2).Chunk(0).(*array.Int64)
	// Row 0 (key 1): no match → rv null.
	// Row 1 (key 2): match → 200.
	// Row 2 (key 3): match → 300.
	// Row 3 (key 4): no match → null.
	if rv.IsValid(0) || rv.IsValid(3) {
		t.Fatal("rows 0 and 3 should have null rv")
	}
	if !rv.IsValid(1) || rv.Value(1) != 200 {
		t.Fatal("row 1 rv mismatch")
	}
	if !rv.IsValid(2) || rv.Value(2) != 300 {
		t.Fatal("row 2 rv mismatch")
	}
}

// Polars: cross join is the Cartesian product. Shape is (|L| × |R|, |L|+|R|);
// the join key appears once per side (left's "k" is kept, right's is
// suffixed or dropped: golars drops).
func TestParityJoinCrossShape(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	lk, _ := series.FromInt64("k", []int64{1, 2}, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", []int64{10, 20}, nil, series.WithAllocator(alloc))
	l, _ := dataframe.New(lk, lv)
	defer l.Release()
	rk, _ := series.FromInt64("k", []int64{3, 4, 5}, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", []int64{30, 40, 50}, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out, err := l.Join(context.Background(), r, nil, dataframe.CrossJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 6 {
		t.Fatalf("cross height = %d, want 6 (2×3)", out.Height())
	}
}

// Polars: joining on a non-existent key errors.
func TestParityJoinMissingKeyErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, r := smallJoinFrames(t, alloc)
	defer l.Release()
	defer r.Release()

	if _, err := l.Join(context.Background(), r, []string{"nope"}, dataframe.InnerJoin); err == nil {
		t.Fatal("missing key must error")
	}
}

// Polars: empty-left inner join yields empty.
func TestParityJoinEmptyLeftInnerIsEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	lk, _ := series.FromInt64("k", nil, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", nil, nil, series.WithAllocator(alloc))
	l, _ := dataframe.New(lk, lv)
	defer l.Release()

	unusedL, r := smallJoinFrames(t, alloc)
	unusedL.Release()
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 0 {
		t.Fatalf("empty-left inner height = %d, want 0", out.Height())
	}
}

// Polars: empty-right inner join yields empty (no key can match).
func TestParityJoinEmptyRightInnerIsEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, unusedR := smallJoinFrames(t, alloc)
	defer l.Release()
	unusedR.Release()

	rk, _ := series.FromInt64("k", nil, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", nil, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 0 {
		t.Fatalf("empty-right inner height = %d, want 0", out.Height())
	}
}

// Polars: empty-right LEFT join preserves all left rows with null on
// the right's value columns.
func TestParityJoinEmptyRightLeftPreservesLeft(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, unusedR := smallJoinFrames(t, alloc)
	defer l.Release()
	unusedR.Release() // drop the factory-default right; we replace it below

	rk, _ := series.FromInt64("k", nil, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", nil, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out2, err := l.Join(context.Background(), r, []string{"k"}, dataframe.LeftJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	if out2.Height() != 4 {
		t.Fatalf("height=%d want 4", out2.Height())
	}
	rvCol := out2.ColumnAt(2)
	if rvCol.NullCount() != 4 {
		t.Fatalf("expected all rv null, got %d nulls", rvCol.NullCount())
	}
}

// Polars: duplicate keys on the right side of an inner join produce
// one output row per matching pair (cross-product within the key).
func TestParityJoinInnerDuplicateRightKeys(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	lk, _ := series.FromInt64("k", []int64{1, 2}, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", []int64{10, 20}, nil, series.WithAllocator(alloc))
	l, _ := dataframe.New(lk, lv)
	defer l.Release()

	// Right has two rows with key 2.
	rk, _ := series.FromInt64("k", []int64{2, 2, 3}, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", []int64{201, 202, 301}, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height=%d want 2 (left key 2 matches right×2)", out.Height())
	}
}

// Polars: joining on a string key works the same way as on int.
func TestParityJoinStringKey(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	lk, _ := series.FromString("k", []string{"a", "b", "c"}, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	l, _ := dataframe.New(lk, lv)
	defer l.Release()

	rk, _ := series.FromString("k", []string{"b", "c", "d"}, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", []int64{20, 30, 40}, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height=%d want 2 (b, c)", out.Height())
	}
	k := out.ColumnAt(0).Chunk(0).(*array.String)
	if k.Value(0) != "b" || k.Value(1) != "c" {
		t.Fatalf("keys wrong: %q %q", k.Value(0), k.Value(1))
	}
}

// Polars: int64 keys containing negative values are not special-cased -
// they join the same way as positive keys.
func TestParityJoinNegativeIntegerKeys(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	lk, _ := series.FromInt64("k", []int64{-3, -1, 0, 2}, nil, series.WithAllocator(alloc))
	lv, _ := series.FromInt64("lv", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	l, _ := dataframe.New(lk, lv)
	defer l.Release()

	rk, _ := series.FromInt64("k", []int64{-1, 2, 5}, nil, series.WithAllocator(alloc))
	rv, _ := series.FromInt64("rv", []int64{100, 200, 500}, nil, series.WithAllocator(alloc))
	r, _ := dataframe.New(rk, rv)
	defer r.Release()

	out, err := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 2 {
		t.Fatalf("height=%d want 2 (-1 and 2)", out.Height())
	}
}

// Polars: inner join preserves the source's non-key column names.
// Collisions on non-key column names error (or get suffixed in
// polars' left-outer, depending on config). golars simply drops the
// right's key column; the right's non-key columns keep their names
// unless the user handles collisions manually.
func TestParityJoinPreservesColumnNames(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	l, r := smallJoinFrames(t, alloc)
	defer l.Release()
	defer r.Release()

	out, _ := l.Join(context.Background(), r, []string{"k"}, dataframe.InnerJoin)
	defer out.Release()
	names := out.Schema().Names()
	// Expected: [k, lv, rv]: left columns first, right's non-key appended.
	if !strSliceEq(names, []string{"k", "lv", "rv"}) {
		t.Fatalf("column names = %v", names)
	}
}
