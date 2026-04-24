package lazy

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// LazyFrame is a lazily-evaluated DataFrame. Every operation appends a node
// to its logical plan; Collect runs the optimizer and executor.
type LazyFrame struct {
	plan Node
}

// FromDataFrame wraps an in-memory [dataframe.DataFrame] as the
// source of a [LazyFrame] pipeline.
//
// The wrapped frame is held by pointer; all downstream ops defer
// until [LazyFrame.Collect] runs, so building the pipeline does no
// work and allocates only plan nodes.
//
// # Parameters
//
//   - df: source DataFrame.
//
// # Returns
//
// A [LazyFrame] whose logical plan is a single DataFrameScan over df.
//
// # Examples
//
//	df, _ := golars.ReadCSV("data.csv")
//	defer df.Release()
//
//	lf := lazy.FromDataFrame(df).
//	    Filter(expr.Col("age").Gt(expr.LitInt64(18))).
//	    GroupBy("dept").
//	    Agg(expr.Col("salary").Mean().Alias("avg"))
//
//	out, err := lf.Collect(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer out.Release()
func FromDataFrame(df *dataframe.DataFrame) LazyFrame {
	return LazyFrame{plan: DataFrameScan{Source: df, Length: -1}}
}

// Plan returns the current (unoptimised) logical plan as a [Node]
// tree. Useful for introspection; optimiser passes run inside
// [LazyFrame.Collect] and leave the stored plan untouched.
func (lf LazyFrame) Plan() Node { return lf.plan }

// Schema returns the best-effort output schema of lf without
// executing the plan. Errors if the plan references columns that
// can't be resolved statically.
func (lf LazyFrame) Schema() (*schema.Schema, error) { return lf.plan.Schema() }

// Select projects lf down to exactly the listed expressions,
// dropping every column not named. Use [LazyFrame.WithColumns] to
// append instead of replace.
//
// # Examples
//
//	lf.Select(expr.Col("name"), expr.Col("salary").Alias("pay"))
func (lf LazyFrame) Select(exprs ...expr.Expr) LazyFrame {
	return LazyFrame{plan: Projection{Input: lf.plan, Exprs: exprs}}
}

// WithColumns appends computed columns to lf.
//
// Each expression runs against the input schema; to reference a
// column added in the same call, split into two WithColumns calls.
// Existing columns of the same name are overwritten.
//
// # Parameters
//
//   - exprs: one or more [expr.Expr] values; use [expr.Expr.Alias]
//     to set the output column name.
//
// # Examples
//
//	lf.WithColumns(
//	    expr.Col("price").Mul(expr.Col("qty")).Alias("revenue"),
//	    expr.Col("name").Str().ToUpper().Alias("name_upper"),
//	)
func (lf LazyFrame) WithColumns(exprs ...expr.Expr) LazyFrame {
	return LazyFrame{plan: WithColumns{Input: lf.plan, Exprs: exprs}}
}

// Filter returns a [LazyFrame] restricted to rows where pred
// evaluates to true. Null-valued predicates drop the row (mirrors
// polars).
//
// # Parameters
//
//   - pred: boolean [expr.Expr].
//
// # Examples
//
//	// Adults in engineering:
//	lf.Filter(
//	    expr.Col("age").Ge(expr.LitInt64(18)).And(
//	        expr.Col("dept").Eq(expr.LitString("eng")),
//	    ),
//	)
//
// The optimiser pushes compatible filters down to scan nodes
// automatically.
func (lf LazyFrame) Filter(pred expr.Expr) LazyFrame {
	return LazyFrame{plan: Filter{Input: lf.plan, Predicate: pred}}
}

// Sort sorts lf by one column.
//
// # Parameters
//
//   - by: column name.
//   - desc: true for descending order.
//
// For multi-key sorts or per-column options (nulls_first, stable)
// use [LazyFrame.SortBy].
//
// # Examples
//
//	lf.Sort("salary", true)   // biggest salary first
func (lf LazyFrame) Sort(by string, desc bool) LazyFrame {
	return LazyFrame{plan: Sort{
		Input:   lf.plan,
		Keys:    []string{by},
		Options: []compute.SortOptions{{Descending: desc}},
	}}
}

// SortBy sorts by many columns with per-column options.
func (lf LazyFrame) SortBy(keys []string, opts []compute.SortOptions) LazyFrame {
	return LazyFrame{plan: Sort{Input: lf.plan, Keys: keys, Options: opts}}
}

// Slice restricts to a row range.
func (lf LazyFrame) Slice(offset, length int) LazyFrame {
	return LazyFrame{plan: SliceNode{Input: lf.plan, Offset: offset, Length: length}}
}

// Head is a shortcut for Slice(0, n).
func (lf LazyFrame) Head(n int) LazyFrame { return lf.Slice(0, n) }

// Limit is an alias for Head.
func (lf LazyFrame) Limit(n int) LazyFrame { return lf.Slice(0, n) }

// Rename renames a single column.
func (lf LazyFrame) Rename(oldName, newName string) LazyFrame {
	return LazyFrame{plan: Rename{Input: lf.plan, Old: oldName, New: newName}}
}

// Drop removes columns from the output.
func (lf LazyFrame) Drop(cols ...string) LazyFrame {
	return LazyFrame{plan: Drop{Input: lf.plan, Columns: cols}}
}

// LazyGroupBy is a pending group-by on a LazyFrame. Call Agg to turn it into
// a LazyFrame.
type LazyGroupBy struct {
	input Node
	keys  []string
}

// GroupBy starts a group-by on lf. Close it with
// [LazyGroupBy.Agg] to produce a [LazyFrame].
//
// # Parameters
//
//   - keys: one or more column names to group by; order matters
//     only for the output row order of unsorted group-bys.
//
// # Examples
//
//	// Per-department salary stats:
//	lf.GroupBy("dept").Agg(
//	    expr.Col("salary").Sum().Alias("total"),
//	    expr.Col("salary").Mean().Alias("avg"),
//	    expr.Col("salary").Count().Alias("headcount"),
//	)
//
//	// Multi-key group:
//	lf.GroupBy("region", "product").Agg(
//	    expr.Col("qty").Sum().Alias("units"),
//	)
func (lf LazyFrame) GroupBy(keys ...string) LazyGroupBy {
	return LazyGroupBy{input: lf.plan, keys: keys}
}

// Agg closes the group-by.
//
// Polars lets `col(a).Mul(col(b)).Sum()` run directly inside an agg
// block because its planner auto-hoists the arithmetic into an
// implicit WithColumns. Our executor wants the agg input to be a
// bare column ref, so we rewrite the plan here: for each complex
// agg input we stage it as a synthetic column (`__agg_i`), insert a
// WithColumns above the Aggregate, and rewrite the agg to reference
// the staged column. Aliases are preserved so the user-visible
// output name is unchanged.
//
// Bare-column inputs (the fast path) pass through unchanged.
func (g LazyGroupBy) Agg(exprs ...expr.Expr) LazyFrame {
	hoisted := make([]expr.Expr, 0, len(exprs))
	rewritten := make([]expr.Expr, len(exprs))
	nextID := 0
	input := g.input
	for i, e := range exprs {
		re, hoist, ok := rewriteAggInput(e, &nextID)
		rewritten[i] = re
		if ok {
			hoisted = append(hoisted, hoist)
		}
	}
	if len(hoisted) > 0 {
		input = WithColumns{Input: input, Exprs: hoisted}
	}
	return LazyFrame{plan: Aggregate{Input: input, Keys: g.keys, Aggs: rewritten}}
}

// rewriteAggInput returns (rewritten agg expr, hoisted WithColumns
// expr, needs_hoist). When the agg's input is already a bare column
// reference, we return the original expr unchanged and
// needs_hoist=false. Otherwise we synthesise a column name, build a
// WithColumns expression that materialises the complex input under
// that name, and rewrite the agg to read from it.
//
// Supports `col.op()` and `col.op().alias("x")`. Filtered aggs
// (`col.filter(...).op()`) and other shapes pass through untouched;
// the downstream validator will reject them if the executor can't
// handle them.
func rewriteAggInput(e expr.Expr, nextID *int) (expr.Expr, expr.Expr, bool) {
	node := e.Node()
	var outerAlias string
	if alias, ok := node.(expr.AliasNode); ok {
		outerAlias = alias.Name
		node = alias.Inner.Node()
	}
	agg, ok := node.(expr.AggNode)
	if !ok {
		// Not an aggregation; pass through. dataframe.parseAggs will
		// surface a clear error if this is unsupported.
		return e, expr.Expr{}, false
	}
	// Already a bare column: nothing to hoist.
	if _, isCol := agg.Inner.Node().(expr.ColNode); isCol {
		return e, expr.Expr{}, false
	}
	// Hoist the inner expression.
	synth := fmt.Sprintf("__agg_%d", *nextID)
	*nextID++
	hoistExpr := agg.Inner.Alias(synth)
	// Rebuild the Agg via the public methods so we don't reach into
	// the unexported expr.Expr shape.
	newAgg, ok := rebuildAgg(agg.Op, expr.Col(synth))
	if !ok {
		// Unknown AggOp: can't hoist safely. Fall back to the
		// original shape; parseAggs will report the real error.
		return e, expr.Expr{}, false
	}
	if outerAlias != "" {
		newAgg = newAgg.Alias(outerAlias)
	} else {
		// Preserve the visible output name of the original agg. Polars
		// uses the inner expression's root name; a grouped sum of
		// `price*(1-disc)` shows up as `price` without this alias.
		newAgg = newAgg.Alias(expr.OutputName(e))
	}
	return newAgg, hoistExpr, true
}

// rebuildAgg reconstructs an aggregation of the given op applied to
// inner, using the public expr constructors so we don't touch Expr's
// unexported node field. Returns (expr, true) on success or
// (zero-value, false) for ops we don't know how to build.
func rebuildAgg(op expr.AggOp, inner expr.Expr) (expr.Expr, bool) {
	switch op {
	case expr.AggSum:
		return inner.Sum(), true
	case expr.AggMean:
		return inner.Mean(), true
	case expr.AggMin:
		return inner.Min(), true
	case expr.AggMax:
		return inner.Max(), true
	case expr.AggCount:
		return inner.Count(), true
	case expr.AggNullCount:
		return inner.NullCount(), true
	case expr.AggFirst:
		return inner.First(), true
	case expr.AggLast:
		return inner.Last(), true
	}
	return expr.Expr{}, false
}

// Join merges lf with other on a set of key columns.
//
// # Parameters
//
//   - other: right-hand side [LazyFrame].
//   - on: shared key column names (must exist in both frames).
//   - how: one of [dataframe.InnerJoin], [dataframe.LeftJoin],
//     [dataframe.CrossJoin]. Inner drops rows with no match on
//     either side; left keeps all rows from lf; cross produces the
//     Cartesian product (ignores `on`).
//
// # Returns
//
// A [LazyFrame] whose output has the union of both schemas; key
// columns appear once, collisions on non-key columns surface as an
// error at execute time.
//
// # Examples
//
//	// Merge salaries onto people by name:
//	people := lazy.FromDataFrame(peopleDF)
//	salaries := lazy.FromDataFrame(salariesDF)
//	out, _ := people.Join(salaries, []string{"name"}, dataframe.InnerJoin).
//	    Filter(expr.Col("salary").Gt(expr.LitFloat64(50_000))).
//	    Collect(ctx)
func (lf LazyFrame) Join(other LazyFrame, on []string, how dataframe.JoinType) LazyFrame {
	return LazyFrame{plan: Join{Left: lf.plan, Right: other.plan, On: on, How: how}}
}

// keep dataframe import referenced via JoinType.
var _ dataframe.JoinType

// Collect materialises lf into an in-memory [dataframe.DataFrame].
//
// Runs the optimiser (predicate pushdown, projection pushdown, CSE,
// constant folding, ...) then the executor. The returned DataFrame
// owns its Arrow buffers: callers must call
// [dataframe.DataFrame.Release] when done.
//
// # Parameters
//
//   - ctx: cancellation / deadline; the executor checks between
//     pipeline stages and between morsels in streaming mode.
//   - opts: optional [ExecOption] values. [WithStreaming] pushes
//     scan+filter+projection+with_columns through the morsel-driven
//     executor; pipeline breakers ([Sort], [Aggregate], [Join]) still
//     evaluate their upstream via streaming and then fall back to
//     eager kernels.
//
// # Returns
//
// A materialised [dataframe.DataFrame] and any execution error.
//
// # Examples
//
// Eager execution:
//
//	out, err := lf.Collect(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer out.Release()
//
// Streaming execution:
//
//	out, err := lf.Collect(ctx, lazy.WithStreaming())
//
// Use [LazyFrame.CollectUnoptimized] to bypass the optimiser when
// debugging a plan.
func (lf LazyFrame) Collect(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, error) {
	optimized, _, err := DefaultOptimizer().Optimize(lf.plan)
	if err != nil {
		return nil, err
	}
	cfg := resolveExec(opts)
	return executeMaybeStreaming(ctx, cfg, optimized)
}

// CollectUnoptimized bypasses the optimizer. Useful for testing that the
// plan is semantically correct before optimization.
func (lf LazyFrame) CollectUnoptimized(ctx context.Context, opts ...ExecOption) (*dataframe.DataFrame, error) {
	return Execute(ctx, lf.plan, opts...)
}

// Explain returns a three-section report: logical plan, optimizer pass log,
// optimized plan. The plans are rendered with the indented form used by
// polars' .explain(). For the box-drawn tree form, use ExplainTree.
func (lf LazyFrame) Explain() (string, error) { return ExplainFull(lf.plan) }

// ExplainString is Explain but panics on error, suitable for printing in
// examples and tests.
func (lf LazyFrame) ExplainString() string {
	out, err := lf.Explain()
	if err != nil {
		return "explain error: " + err.Error()
	}
	return out
}

// ExplainTree is Explain rendered as a box-drawing tree. Each child
// sits under its parent with ├── / └── connectors so nesting is
// easier to follow at a glance.
func (lf LazyFrame) ExplainTree() (string, error) { return ExplainTreeFull(lf.plan) }

// ExplainTreeString is ExplainTree but returns the error text inline
// rather than as a second value. Intended for printing from examples.
func (lf LazyFrame) ExplainTreeString() string {
	out, err := lf.ExplainTree()
	if err != nil {
		return "explain error: " + err.Error()
	}
	return out
}
