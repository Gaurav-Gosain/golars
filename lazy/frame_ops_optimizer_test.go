package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// TestFrameOpsOptimizerEquivalence wraps every new plan node in the
// operators the optimizer rewrites (filter, projection, slice) and checks
// that optimized and unoptimized plans agree. Pushing any of these
// through a row-changing node (explode, shift, gather_every, top_k,
// group-by, joins) would change the result.
func TestFrameOpsOptimizerEquivalence(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := baseFixture(t, mem)
	defer df.Release()
	right := lxFrame(t,
		lxCol(t, mem, "a", lxI64, 1, 3, 5),
		lxCol(t, mem, "r", lxI64, 10, 30, 50),
	)
	defer right.Release()

	col := expr.Col
	lf := lazy.FromDataFrame(df)
	sorted := lf.Select(col("k"), col("a"), col("f")).DropNulls("a").Sort("a", false)
	nodes := map[string]lazy.LazyFrame{
		"explode":        lf.Explode("l").Drop("l2"),
		"shift":          lf.Select(col("k"), col("a"), col("f")).Shift(2, nil),
		"gather_every":   lf.Select(col("k"), col("a"), col("f")).GatherEvery(2, 1),
		"top_k":          lf.Select(col("k"), col("a"), col("f")).TopK(3, "a"),
		"with_row_index": lf.Select(col("k"), col("a"), col("f")).WithRowIndex("i", 0),
		"unpivot": lf.Select(col("k"), col("a"), col("f")).
			Unpivot(lazy.UnpivotOptions{On: []string{"a", "f"}, Index: []string{"k"}}).Rename("value", "a"),
		"group_by_sum":  lf.Select(col("k"), col("a"), col("f")).GroupBy("k").MaintainOrder().Sum(),
		"group_by_head": lf.Select(col("k"), col("a"), col("f")).GroupBy("k").MaintainOrder().Head(1),
		"group_by_expr": lf.Select(col("k"), col("a"), col("f")).
			GroupByExprs(col("a").Gt(expr.LitInt64(2)).Alias("p")).MaintainOrder().Agg(col("f").Sum(), col("a").Max()),
		"drop_nans":    lf.Select(col("k"), col("a"), col("f")).DropNans(),
		"join_asof":    sorted.JoinAsof(lazy.FromDataFrame(right).Sort("a", false), dataframe.AsofOptions{On: "a"}),
		"merge_sorted": sorted.MergeSorted(sorted, "a"),
		"fill_strategy": lf.Select(col("k"), col("a"), col("f")).
			FillNullWithStrategy(dataframe.FillForward, 1),
	}
	wrappers := map[string]func(lazy.LazyFrame) lazy.LazyFrame{
		"filter": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.Filter(col("a").Gt(expr.LitInt64(1)))
		},
		"filter_after_projection": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.Select(col("a")).Filter(col("a").Lt(expr.LitInt64(6)))
		},
		"projection": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.Select(col("a"))
		},
		"slice": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.Slice(1, 2)
		},
		"slice_projection": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.Select(col("a")).Head(2)
		},
		"with_columns_filter": func(x lazy.LazyFrame) lazy.LazyFrame {
			return x.WithColumns(col("a").Alias("a2")).Filter(col("a2").IsNotNull())
		},
	}
	ctx := context.Background()
	for nn, node := range nodes {
		for wn, wrap := range wrappers {
			t.Run(nn+"/"+wn, func(t *testing.T) {
				plan := wrap(node)
				opt, err := plan.Collect(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatalf("optimized: %v", err)
				}
				defer opt.Release()
				raw, err := plan.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatalf("unoptimized: %v", err)
				}
				defer raw.Release()
				if got, want := framerepr.Frame(opt), framerepr.Frame(raw); got != want {
					t.Fatalf("optimized plan changed the result\noptimized:\n%s\nunoptimized:\n%s\nplan:\n%s",
						got, want, plan.ExplainString())
				}
			})
		}
	}
}

// TestFrameOpsBlockPushdown checks the optimized plan keeps a filter
// above a row-changing frame operation.
func TestFrameOpsBlockPushdown(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := baseFixture(t, mem)
	defer df.Release()
	plan := lazy.FromDataFrame(df).Select(expr.Col("a")).Shift(1, nil).
		Filter(expr.Col("a").Gt(expr.LitInt64(1)))
	out, _, err := lazy.DefaultOptimizer().Optimize(plan.Plan())
	if err != nil {
		t.Fatal(err)
	}
	f, ok := out.(lazy.Filter)
	if !ok {
		t.Fatalf("root is %T, want Filter kept above SHIFT:\n%s", out, lazy.Explain(out))
	}
	if op, ok := f.Input.(lazy.FrameOpNode); !ok || !strings.HasPrefix(op.String(), "SHIFT") {
		t.Fatalf("filter input is %T, want the SHIFT node", f.Input)
	}
}
