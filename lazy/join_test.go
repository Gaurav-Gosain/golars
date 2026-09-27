package lazy_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func joinTestFrames(t *testing.T) (*dataframe.DataFrame, *dataframe.DataFrame) {
	t.Helper()
	lk, _ := series.FromInt64("k", []int64{1, 2, 3, 0, 5, 2}, []bool{true, true, true, false, true, true})
	la, _ := series.FromInt64("a", []int64{10, 20, 30, 40, 50, 60}, nil)
	lv, _ := series.FromString("v", []string{"p", "q", "r", "s", "t", "u"}, nil)
	left, err := dataframe.New(lk, la, lv)
	if err != nil {
		t.Fatal(err)
	}
	rk, _ := series.FromInt64("k", []int64{2, 3, 0, 7, 2}, []bool{true, true, false, true, true})
	rb, _ := series.FromInt64("b", []int64{100, 0, 300, 400, 500}, []bool{true, false, true, true, true})
	rv, _ := series.FromString("v", []string{"w", "x", "y", "z", "q"}, nil)
	right, err := dataframe.New(rk, rb, rv)
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}

func sortedFrame(df *dataframe.DataFrame) string {
	lines := strings.Split(framerepr.Frame(df), "\n")
	slices.Sort(lines[1:])
	return strings.Join(lines, "\n")
}

// TestJoinPredicatePushdownEquivalence runs filters over every join type
// with and without the optimizer. Filters that read null-extended
// columns (the right side of a left join, the left side of a right
// join, either side of a full join) must stay above the join; the
// results must match either way.
func TestJoinPredicatePushdownEquivalence(t *testing.T) {
	left, right := joinTestFrames(t)
	defer left.Release()
	defer right.Release()
	hows := []dataframe.JoinType{dataframe.InnerJoin, dataframe.LeftJoin, dataframe.RightJoin,
		dataframe.FullJoin, dataframe.SemiJoin, dataframe.AntiJoin, dataframe.CrossJoin}
	k, a, b := expr.Col("k"), expr.Col("a"), expr.Col("b")
	preds := []expr.Expr{
		k.GtLit(int64(1)),
		k.IsNull(),
		a.GtLit(int64(15)),
		a.IsNull(),
		b.IsNull(),
		b.GtLit(int64(150)),
		expr.Col("v").EqLit("q"),
		expr.Col("v_right").EqLit("q"),
		expr.Col("v_right").IsNull(),
		expr.Col("k_right").IsNull(),
		a.GtLit(int64(15)).And(b.IsNull()),
		a.GtLit(int64(15)).And(k.LtLit(int64(3))),
		a.GtLit(int64(15)).Or(b.IsNull()),
		a.Gt(a.Mean()),
	}
	for _, how := range hows {
		for _, co := range []int{-1, 0, 1} {
			for _, ne := range []bool{false, true} {
				opts := []dataframe.JoinOption{dataframe.WithJoinNullsEqual(ne)}
				if co >= 0 {
					opts = append(opts, dataframe.WithJoinCoalesce(co == 1))
				}
				joined := lazy.FromDataFrame(left).Join(lazy.FromDataFrame(right), []string{"k"}, how, opts...)
				schema, err := joined.CollectSchema()
				if err != nil {
					t.Fatal(err)
				}
				for pi, p := range preds {
					ok := true
					for _, c := range expr.Columns(p) {
						ok = ok && schema.Contains(c)
					}
					if !ok {
						continue
					}
					name := fmt.Sprintf("%s_co%d_ne%v_%d", how, co, ne, pi)
					t.Run(name, func(t *testing.T) {
						// Plain, and with a selection of the last output
						// column so projection pruning through the join
						// runs as well.
						last := expr.Col(schema.Names()[schema.Len()-1])
						for _, plan := range []lazy.LazyFrame{joined.Filter(p), joined.Filter(p).Select(last)} {
							want, err := plan.CollectUnoptimized(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							got, err := plan.Collect(context.Background())
							if err != nil {
								want.Release()
								t.Fatal(err)
							}
							g, w := sortedFrame(got), sortedFrame(want)
							got.Release()
							want.Release()
							if g != w {
								opt, _, _ := lazy.DefaultOptimizer().Optimize(plan.Plan())
								t.Fatalf("optimized result differs for %s\nplan:\n%s\n got:\n%s\nwant:\n%s", p, opt.String(), g, w)
							}
						}
					})
				}
			}
		}
	}
}

// joinFilterSides reports whether the optimized plan still has a filter
// above the join and whether each join input received a filter.
func joinFilterSides(t *testing.T, plan lazy.LazyFrame) (above, left, right bool) {
	t.Helper()
	opt, _, err := lazy.DefaultOptimizer().Optimize(plan.Plan())
	if err != nil {
		t.Fatal(err)
	}
	n := opt
	if f, ok := n.(lazy.Filter); ok {
		above = true
		n = f.Input
	}
	j, ok := n.(lazy.Join)
	if !ok {
		t.Fatalf("expected a join below the filter, got %T", n)
	}
	hasPred := func(n lazy.Node) bool {
		switch x := n.(type) {
		case lazy.Filter:
			return true
		case lazy.DataFrameScan:
			return x.Predicate != nil
		}
		return false
	}
	return above, hasPred(j.Left), hasPred(j.Right)
}

func TestJoinPredicatePushdownSides(t *testing.T) {
	left, right := joinTestFrames(t)
	defer left.Release()
	defer right.Release()
	lf, rf := lazy.FromDataFrame(left), lazy.FromDataFrame(right)
	a, b, k := expr.Col("a"), expr.Col("b"), expr.Col("k")
	cases := []struct {
		name               string
		plan               lazy.LazyFrame
		above, left, right bool
	}{
		{"inner left col", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(a.GtLit(int64(1))), false, true, false},
		{"inner right col", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(b.GtLit(int64(1))), false, false, true},
		{"inner key", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(k.GtLit(int64(1))), false, true, true},
		{"inner split", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(a.GtLit(int64(1)).And(b.GtLit(int64(1)))), false, true, true},
		{"inner mixed", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(a.Gt(b)), true, false, false},
		{"inner suffixed", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(expr.Col("v_right").EqLit("q")), true, false, false},
		{"left left col", lf.Join(rf, []string{"k"}, dataframe.LeftJoin).Filter(a.GtLit(int64(1))), false, true, false},
		{"left right col", lf.Join(rf, []string{"k"}, dataframe.LeftJoin).Filter(b.IsNull()), true, false, false},
		{"right left col", lf.Join(rf, []string{"k"}, dataframe.RightJoin).Filter(a.IsNull()), true, false, false},
		{"right right col", lf.Join(rf, []string{"k"}, dataframe.RightJoin).Filter(b.GtLit(int64(1))), false, false, true},
		{"full left col", lf.Join(rf, []string{"k"}, dataframe.FullJoin).Filter(a.GtLit(int64(1))), true, false, false},
		{"full coalesced key", lf.Join(rf, []string{"k"}, dataframe.FullJoin, dataframe.WithJoinCoalesce(true)).Filter(k.GtLit(int64(1))), false, true, true},
		{"semi left col", lf.Join(rf, []string{"k"}, dataframe.SemiJoin).Filter(a.GtLit(int64(1))), false, true, false},
		{"anti key", lf.Join(rf, []string{"k"}, dataframe.AntiJoin).Filter(k.GtLit(int64(1))), false, true, false},
		{"validated", lf.Join(rf, []string{"k"}, dataframe.InnerJoin, dataframe.WithJoinValidate(dataframe.ValidateManyToOne)).Filter(a.GtLit(int64(1))), true, false, false},
		{"not elementwise", lf.Join(rf, []string{"k"}, dataframe.InnerJoin).Filter(a.Gt(a.Mean())), true, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			above, l, r := joinFilterSides(t, c.plan)
			if above != c.above || l != c.left || r != c.right {
				t.Fatalf("above=%v left=%v right=%v, want %v %v %v", above, l, r, c.above, c.left, c.right)
			}
		})
	}
}

// The lazy schema must match what the eager join produces.
func TestJoinLazySchemaMatchesResult(t *testing.T) {
	left, right := joinTestFrames(t)
	defer left.Release()
	defer right.Release()
	for _, how := range []dataframe.JoinType{dataframe.InnerJoin, dataframe.LeftJoin, dataframe.RightJoin,
		dataframe.FullJoin, dataframe.SemiJoin, dataframe.AntiJoin, dataframe.CrossJoin} {
		for _, co := range []bool{false, true} {
			plan := lazy.FromDataFrame(left).Join(lazy.FromDataFrame(right), []string{"k"}, how,
				dataframe.WithJoinCoalesce(co), dataframe.WithJoinSuffix("_r"))
			s, err := plan.CollectSchema()
			if err != nil {
				t.Fatal(err)
			}
			out, err := plan.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !s.Equal(out.Schema()) {
				t.Fatalf("%s coalesce=%v: schema %v, result %v", how, co, s.Names(), out.Schema().Names())
			}
			out.Release()
		}
	}
}
