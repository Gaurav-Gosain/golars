package lazy

import (
	"context"
	"fmt"
	"slices"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// groupByExt carries the LazyGroupBy options that are implemented by
// rewriting the plan instead of by the Aggregate operator.
type groupByExt struct {
	// outNames holds the final name of every key; keys that were
	// computed from expressions are grouped under a synthetic column
	// and renamed at the end. Empty means the keys are plain columns.
	outNames []string
	// maintainOrder emits groups in order of first appearance.
	maintainOrder bool
}

func (e groupByExt) active() bool { return e.maintainOrder || len(e.outNames) > 0 }

const groupOrderCol = "__gb_order"

// GroupByExprs starts a group-by whose keys are expressions, for example
// GroupByExprs(expr.Col("a").Mod(expr.LitInt64(2))). Each key column is
// named by its expression's output name, like polars; plain column
// expressions behave exactly like GroupBy. Mirrors polars'
// LazyFrame.group_by with expression arguments.
func (lf LazyFrame) GroupByExprs(keys ...expr.Expr) LazyGroupBy {
	var staged []expr.Expr
	names := make([]string, len(keys))
	outNames := make([]string, len(keys))
	computed := false
	for i, k := range keys {
		outNames[i] = expr.OutputName(k)
		if c, ok := k.Node().(expr.ColNode); ok {
			names[i] = c.Name
			continue
		}
		computed = true
		names[i] = fmt.Sprintf("__gb_key_%d", i)
		staged = append(staged, k.Alias(names[i]))
	}
	input := lf.plan
	if len(staged) > 0 {
		input = WithColumns{Input: input, Exprs: staged}
	}
	g := LazyGroupBy{input: input, keys: names}
	if computed {
		g.ext.outNames = outNames
	}
	return g
}

// MaintainOrder makes the group-by emit groups in the order their keys
// first appear, like polars' group_by(..., maintain_order=True).
func (g LazyGroupBy) MaintainOrder() LazyGroupBy {
	g.ext.maintainOrder = true
	return g
}

// aggExt is Agg for group-bys with expression keys or maintain_order.
// maintain_order adds a row number, aggregates its minimum per group,
// sorts by it and drops it; expression keys are renamed at the end.
func (g LazyGroupBy) aggExt(exprs []expr.Expr) LazyFrame {
	input := g.input
	aggs := exprs
	if g.ext.maintainOrder {
		// The order column is widened to i64 because the hash
		// aggregation's min kernel covers i64 but not u32.
		input = CastNode{Input: WithRowIndexNode{Input: input, Name: groupOrderCol}, Col: groupOrderCol, To: dtype.Int64()}
		aggs = append(slices.Clone(exprs), expr.Col(groupOrderCol).Min().Alias(groupOrderCol))
	}
	out := LazyGroupBy{input: input, keys: g.keys}.Agg(aggs...)
	if g.ext.maintainOrder {
		out = out.Sort(groupOrderCol, false).Drop(groupOrderCol)
	}
	return g.renameKeys(out)
}

// renameKeys renames synthetic expression-key columns to their output
// names.
func (g LazyGroupBy) renameKeys(lf LazyFrame) LazyFrame {
	for i, final := range g.ext.outNames {
		if g.keys[i] != final {
			lf = lf.Rename(g.keys[i], final)
		}
	}
	return lf
}

// convenience runs an eager GroupBy convenience method over all non-key
// columns. Original columns that share a name with a computed key are
// excluded, as polars' all() excludes key names.
func (g LazyGroupBy) convenience(op string, fn func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error)) LazyFrame {
	keys := g.keys
	var shadowed []string
	for i, final := range g.ext.outNames {
		if keys[i] != final {
			shadowed = append(shadowed, final)
		}
	}
	maintain := g.ext.maintainOrder
	lf := LazyFrame{plan: FrameOpNode{
		Input:  g.input,
		Op:     "GROUP_BY_" + op,
		Detail: fmt.Sprint(keys),
		Fn: func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
			src := df
			if len(shadowed) > 0 {
				src = df.Drop(shadowed...)
				defer src.Release()
			}
			gb := src.GroupBy(keys...)
			if maintain {
				gb = gb.MaintainOrder()
			}
			return fn(ctx, gb)
		},
	}}
	return g.renameKeys(lf)
}

// Len counts the rows of every group into a u32 column named name ("len"
// when empty). Mirrors polars' GroupBy.len.
func (g LazyGroupBy) Len(name string) LazyFrame {
	if name == "" {
		name = "len"
	}
	return g.convenience("LEN", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Len(ctx, name)
	})
}

// Count is Len with polars' column name "count". Mirrors polars'
// GroupBy.count.
func (g LazyGroupBy) Count() LazyFrame {
	return g.convenience("COUNT", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Count(ctx)
	})
}

// First takes the first row of every group. Mirrors polars'
// GroupBy.first.
func (g LazyGroupBy) First() LazyFrame {
	return g.convenience("FIRST", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.First(ctx)
	})
}

// Last takes the last row of every group. Mirrors polars' GroupBy.last.
func (g LazyGroupBy) Last() LazyFrame {
	return g.convenience("LAST", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Last(ctx)
	})
}

// Sum sums every non-key column per group. Mirrors polars' GroupBy.sum.
func (g LazyGroupBy) Sum() LazyFrame {
	return g.convenience("SUM", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Sum(ctx)
	})
}

// Mean averages every non-key column per group. Mirrors polars'
// GroupBy.mean.
func (g LazyGroupBy) Mean() LazyFrame {
	return g.convenience("MEAN", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Mean(ctx)
	})
}

// Median takes the median of every non-key column per group. Mirrors
// polars' GroupBy.median.
func (g LazyGroupBy) Median() LazyFrame {
	return g.convenience("MEDIAN", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Median(ctx)
	})
}

// Min takes the minimum of every non-key column per group. Mirrors
// polars' GroupBy.min.
func (g LazyGroupBy) Min() LazyFrame {
	return g.convenience("MIN", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Min(ctx)
	})
}

// Max takes the maximum of every non-key column per group. Mirrors
// polars' GroupBy.max.
func (g LazyGroupBy) Max() LazyFrame {
	return g.convenience("MAX", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Max(ctx)
	})
}

// NUnique counts distinct values of every non-key column per group.
// Mirrors polars' GroupBy.n_unique.
func (g LazyGroupBy) NUnique() LazyFrame {
	return g.convenience("N_UNIQUE", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.NUnique(ctx)
	})
}

// Quantile takes the q-quantile of every non-key column per group.
// Mirrors polars' GroupBy.quantile.
func (g LazyGroupBy) Quantile(q float64, method dataframe.QuantileMethod) LazyFrame {
	return g.convenience("QUANTILE", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Quantile(ctx, q, method)
	})
}

// All collects every non-key column into a list per group. Mirrors
// polars' GroupBy.all.
func (g LazyGroupBy) All() LazyFrame {
	return g.convenience("ALL", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.All(ctx)
	})
}

// Head keeps the first n rows of every group. Mirrors polars'
// GroupBy.head.
func (g LazyGroupBy) Head(n int) LazyFrame {
	return g.convenience("HEAD", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Head(ctx, n)
	})
}

// Tail keeps the last n rows of every group. Mirrors polars'
// GroupBy.tail.
func (g LazyGroupBy) Tail(n int) LazyFrame {
	return g.convenience("TAIL", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.Tail(ctx, n)
	})
}

// MapGroups calls fn once per group (keys included) and concatenates the
// results in group order. sch is the output schema; nil means the input
// schema. Mirrors polars' LazyGroupBy.map_groups.
func (g LazyGroupBy) MapGroups(fn func(*dataframe.DataFrame) (*dataframe.DataFrame, error), sch *schema.Schema) LazyFrame {
	lf := g.convenience("MAP_GROUPS", func(ctx context.Context, gb *dataframe.GroupBy) (*dataframe.DataFrame, error) {
		return gb.MapGroups(ctx, fn)
	})
	if op, ok := lf.plan.(FrameOpNode); ok {
		if sch != nil {
			op.SchemaFn = func(*schema.Schema) (*schema.Schema, error) { return sch, nil }
		} else {
			op.SchemaFn = sameSchema
		}
		lf.plan = op
	}
	return lf
}
