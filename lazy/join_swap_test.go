package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// An inner join whose right input is much larger runs with the inputs
// swapped. Columns must keep the left-then-right layout and the rows
// must match the eager join (in any order).
func TestInnerJoinBuildsOnSmallerSide(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	lk, _ := series.FromInt64("k", []int64{1, 2, 2, 5, 0}, []bool{true, true, true, true, false}, o)
	lv, _ := series.FromString("lv", []string{"a", "b", "c", "d", "e"}, nil, o)
	left, _ := dataframe.New(lk, lv)
	defer left.Release()
	n := 40
	rk := make([]int64, n)
	rv := make([]int64, n)
	valid := make([]bool, n)
	for i := range n {
		rk[i] = int64(i % 7)
		rv[i] = int64(i)
		valid[i] = i%9 != 0
	}
	rks, _ := series.FromInt64("k", rk, valid, o)
	rvs, _ := series.FromInt64("rv", rv, nil, o)
	right, _ := dataframe.New(rks, rvs)
	defer right.Release()

	got, err := lazy.FromDataFrame(left).Join(lazy.FromDataFrame(right), []string{"k"}, dataframe.InnerJoin).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	want, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin, dataframe.WithJoinAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer want.Release()
	assertSameRows(t, got, want)

	// A colliding non-key name keeps the unswapped path and its suffix.
	rx, _ := series.FromString("lv", make([]string, n), nil, o)
	rks2, _ := series.FromInt64("k", rk, valid, o)
	right2, _ := dataframe.New(rks2, rx)
	defer right2.Release()
	got2, err := lazy.FromDataFrame(left).Join(lazy.FromDataFrame(right2), []string{"k"}, dataframe.InnerJoin).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer got2.Release()
	if names := got2.ColumnNames(); len(names) != 3 || names[2] != "lv_right" {
		t.Fatalf("columns = %v", names)
	}
}
