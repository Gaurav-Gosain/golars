package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// A filter on a column that a projection or with_columns introduces as an
// alias of another column must not be pushed below that node: the alias
// does not exist there.
func TestPredicatePushdownKeepsAliasFilters(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := lxFrame(t, lxCol(t, mem, "a", lxI64, 1, nil, 3))
	defer df.Release()
	col := expr.Col
	plans := map[string]lazy.LazyFrame{
		"with_columns": lazy.FromDataFrame(df).WithColumns(col("a").Alias("a2")).Filter(col("a2").IsNotNull()),
		"select":       lazy.FromDataFrame(df).Select(col("a").Alias("b")).Filter(col("b").Gt(expr.LitInt64(1))),
	}
	for name, plan := range plans {
		t.Run(name, func(t *testing.T) {
			opt, err := plan.Collect(context.Background(), lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatalf("optimized: %v", err)
			}
			defer opt.Release()
			raw, err := plan.CollectUnoptimized(context.Background(), lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatalf("unoptimized: %v", err)
			}
			defer raw.Release()
			if framerepr.Frame(opt) != framerepr.Frame(raw) {
				t.Fatalf("optimized %s != unoptimized %s", framerepr.Frame(opt), framerepr.Frame(raw))
			}
		})
	}
}
