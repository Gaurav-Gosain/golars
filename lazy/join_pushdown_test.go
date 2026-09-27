package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func pushdownFrames(t *testing.T, o series.Option) (*dataframe.DataFrame, *dataframe.DataFrame) {
	t.Helper()
	lk, _ := series.FromInt64("k", []int64{1, 2, 3, 4, 5, 6}, []bool{true, true, true, true, true, false}, o)
	la, _ := series.FromInt64("a", []int64{10, 20, 30, 40, 50, 60}, nil, o)
	lx, _ := series.FromString("x", []string{"p", "q", "p", "q", "p", "q"}, nil, o)
	left, err := dataframe.New(lk, la, lx)
	if err != nil {
		t.Fatal(err)
	}
	rk, _ := series.FromInt64("k", []int64{2, 3, 4, 4, 7}, nil, o)
	rb, _ := series.FromInt64("b", []int64{1, 2, 3, 4, 5}, []bool{true, false, true, true, true}, o)
	rx, _ := series.FromString("x", []string{"r", "s", "r", "s", "r"}, nil, o)
	right, err := dataframe.New(rk, rb, rx)
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}

// Filters and column pruning pushed through joins must not change the
// answer: every plan is checked against the unoptimized execution.
func TestJoinPushdownMatchesUnoptimized(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	left, right := pushdownFrames(t, o)
	defer left.Release()
	defer right.Release()
	L, R := lazy.FromDataFrame(left), lazy.FromDataFrame(right)

	cases := []struct {
		name string
		lf   lazy.LazyFrame
		// wantPushed lists fragments the optimized plan must contain.
		wantPushed []string
	}{
		{"inner left term", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Filter(expr.Col("a").Gt(expr.LitInt64(15))), []string{"predicate=(col(\"a\") > 15)"}},
		{"inner right term", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Filter(expr.Col("b").Gt(expr.LitInt64(2))), []string{"predicate=(col(\"b\") > 2)"}},
		{"inner key term goes both ways", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Filter(expr.Col("k").Lt(expr.LitInt64(4))), nil},
		{"inner mixed and suffixed terms", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Filter(expr.Col("a").Gt(expr.Col("b")).And(expr.Col("x_right").Eq(expr.LitString("r"))).And(expr.Col("x").Eq(expr.LitString("q")))), nil},
		{"left join keeps right term above", L.Join(R, []string{"k"}, dataframe.LeftJoin).
			Filter(expr.Col("b").IsNull().Or(expr.Col("b").Gt(expr.LitInt64(3)))), nil},
		{"left join pushes left term", L.Join(R, []string{"k"}, dataframe.LeftJoin).
			Filter(expr.Col("a").Lt(expr.LitInt64(45))), []string{"predicate=(col(\"a\") < 45)"}},
		{"stacked filters", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Filter(expr.Col("a").Gt(expr.LitInt64(15))).Filter(expr.Col("b").IsNotNull()), nil},
		{"prune with suffix", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Select(expr.Col("k"), expr.Col("x_right")), nil},
		{"prune left only", L.Join(R, []string{"k"}, dataframe.InnerJoin).
			Select(expr.Col("a").Sum().Alias("s")), nil},
		{"rename key then join", L.JoinOn(R.Rename("k", "rk"), "k", "rk", dataframe.InnerJoin).
			Filter(expr.Col("b").Gt(expr.LitInt64(1))).Select(expr.Col("k"), expr.Col("b")), nil},
		{"cross join", L.Select(expr.Col("a")).Join(R.Select(expr.Col("b")), nil, dataframe.CrossJoin).
			Filter(expr.Col("a").Gt(expr.LitInt64(40)).And(expr.Col("b").Lt(expr.LitInt64(3)))), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.lf.Collect(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer got.Release()
			want, err := tc.lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()
			assertSameRows(t, got, want)
			plan := tc.lf.ExplainString()
			for _, frag := range tc.wantPushed {
				opt := plan[strings.Index(plan, "Optimized plan"):]
				if !strings.Contains(opt, frag) {
					t.Errorf("optimized plan lacks %s:\n%s", frag, opt)
				}
			}
		})
	}
}

// assertSameRows compares two frames after sorting on every column,
// since join output order is not part of the contract.
func assertSameRows(t *testing.T, got, want *dataframe.DataFrame) {
	t.Helper()
	if strings.Join(got.ColumnNames(), ",") != strings.Join(want.ColumnNames(), ",") {
		t.Fatalf("columns = %v, want %v", got.ColumnNames(), want.ColumnNames())
	}
	names := got.ColumnNames()
	opts := make([]compute.SortOptions, len(names))
	gs, err := got.SortBy(context.Background(), names, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Release()
	ws, err := want.SortBy(context.Background(), names, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Release()
	if gs.String() != ws.String() {
		t.Fatalf("got\n%s\nwant\n%s", gs, ws)
	}
}
