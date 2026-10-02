package lazy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Partial aggregation over morsels. Every batch is reduced to one row
// per group of partial aggregates (a sum, a count, a min, ...), the
// partials of all batches are combined with a second aggregation, and
// a final projection rebuilds each requested output from the combined
// columns. Only decomposable aggregations qualify:
//
//	sum, min, max    partial op, combined with the same op
//	count, len       partial count, combined with sum
//	mean             partial sum and count, combined sums divided
//
// Aggregations may sit inside elementwise expressions (round(sum(x)),
// 100 * sum(a) / sum(b)); those are evaluated in the final projection.
// Anything else (first, last, median, n_unique, std, ...) keeps the
// eager path.

// aggSplit is the three-stage form of a list of aggregations.
type aggSplit struct {
	partial []expr.Expr // per batch
	combine []expr.Expr // over the stacked partials
	final   []expr.Expr // outputs, from the combined columns
}

// splitAggs rewrites exprs into partial, combine and final stages.
// keys are the group-by keys (nil for a projection of aggregations);
// the final stage passes them through first.
func splitAggs(exprs []expr.Expr, keys []string) (aggSplit, bool) {
	var sp aggSplit
	next := 0
	name := func(kind string) string {
		next++
		return fmt.Sprintf("__morsel_%s_%d", kind, next)
	}
	ok := true
	var rewrite func(e expr.Expr) expr.Expr
	rewrite = func(e expr.Expr) expr.Expr {
		if !ok {
			return e
		}
		switch n := e.Node().(type) {
		case expr.AggNode:
			if !expr.IsElementwise(n.Inner) {
				ok = false
				return e
			}
			p, c := name("p"), name("c")
			switch n.Op {
			case expr.AggSum, expr.AggMin, expr.AggMax:
				op := map[expr.AggOp]func(expr.Expr) expr.Expr{
					expr.AggSum: expr.Expr.Sum, expr.AggMin: expr.Expr.Min, expr.AggMax: expr.Expr.Max,
				}[n.Op]
				sp.partial = append(sp.partial, op(n.Inner).Alias(p))
				sp.combine = append(sp.combine, op(expr.Col(p)).Alias(c))
				return expr.Col(c)
			case expr.AggCount:
				sp.partial = append(sp.partial, n.Inner.Count().Alias(p))
				sp.combine = append(sp.combine, expr.Col(p).Sum().Cast(dtype.Uint32()).Alias(c))
				return expr.Col(c)
			case expr.AggMean:
				pn, cn := name("p"), name("c")
				sp.partial = append(sp.partial, n.Inner.Sum().Alias(p), n.Inner.Count().Alias(pn))
				sp.combine = append(sp.combine, expr.Col(p).Sum().Alias(c), expr.Col(pn).Sum().Alias(cn))
				// A group with no non-null values has mean null, not 0/0.
				return expr.When(expr.Col(cn).Eq(expr.LitInt64(0))).
					Then(expr.LitNull(dtype.Float64())).
					Otherwise(expr.Col(c).Cast(dtype.Float64()).Div(expr.Col(cn).Cast(dtype.Float64())))
			}
			ok = false
			return e
		case expr.FunctionNode:
			if n.Name == "frame_len" && len(n.Args) == 0 {
				p, c := name("p"), name("c")
				sp.partial = append(sp.partial, expr.Len().Alias(p))
				sp.combine = append(sp.combine, expr.Col(p).Sum().Cast(dtype.Uint32()).Alias(c))
				return expr.Col(c)
			}
			// Judge the function itself, not its arguments (which may be
			// aggregations): swap the arguments for literals.
			lits := make([]expr.Expr, len(n.Args))
			for i := range lits {
				lits[i] = expr.LitInt64(0)
			}
			if !expr.IsElementwise(expr.WithChildren(e, lits)) {
				ok = false
				return e
			}
		case expr.ColNode:
			// A bare column outside any aggregation has one value per row,
			// not per group.
			ok = false
			return e
		case expr.LitNode:
			return e
		case expr.BinaryNode, expr.UnaryNode, expr.AliasNode, expr.CastNode, expr.WhenThenNode, expr.IsNullNode:
		default:
			ok = false
			return e
		}
		kids := expr.Children(e)
		nk := make([]expr.Expr, len(kids))
		for i, k := range kids {
			nk[i] = rewrite(k)
		}
		return expr.WithChildren(e, nk)
	}
	for _, k := range keys {
		sp.final = append(sp.final, expr.Col(k))
	}
	for _, e := range exprs {
		before := len(sp.partial)
		out := rewrite(e)
		if !ok || len(sp.partial) == before {
			// Not decomposable, or no aggregation at all (a literal).
			return aggSplit{}, false
		}
		sp.final = append(sp.final, out.Alias(expr.OutputName(e)))
	}
	return sp, true
}

// tryMorselAggregate runs an Aggregate or a Projection of aggregations
// with partial aggregation over a morsel fragment. ok is false when the
// eager path should run instead.
func tryMorselAggregate(ctx context.Context, cfg execConfig, input Node, keys []string, aggs []expr.Expr) (*dataframe.DataFrame, bool, error) {
	leaf, _, ok := morselFragment(input)
	if !ok {
		return nil, false, nil
	}
	sp, ok := splitAggs(aggs, keys)
	if !ok {
		return nil, false, nil
	}
	aggregate := func(ctx context.Context, df *dataframe.DataFrame, exprs []expr.Expr) (*dataframe.DataFrame, error) {
		if len(keys) == 0 {
			return executeNode(ctx, cfg, Projection{Input: DataFrameScan{Source: df, Length: -1}, Exprs: exprs})
		}
		return df.GroupBy(keys...).Agg(ctx, exprs, dataframe.WithGroupByAllocator(cfg.alloc))
	}
	// The output dtypes must match the eager path, which some combined
	// forms change (a sum of an i32 column is i64 either way, but a
	// count summed is cast back to u32 only by name). Run the original
	// aggregation eagerly on one row of the first batch to learn them.
	var (
		want     []dtype.DType
		wantOnce sync.Once
		wantErr  error
	)
	reduce := func(ctx context.Context, df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		wantOnce.Do(func() {
			one, err := df.Slice(0, min(df.Height(), 1))
			if err != nil {
				wantErr = err
				return
			}
			defer one.Release()
			ref, err := aggregate(ctx, one, aggs)
			if err != nil {
				wantErr = err
				return
			}
			defer ref.Release()
			for _, c := range ref.Columns() {
				want = append(want, c.DType())
			}
		})
		return aggregate(ctx, df, sp.partial)
	}
	parts, err := runMorsels(ctx, cfg, input, leaf, reduce)
	if errors.Is(err, errMorselNotApplicable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if wantErr != nil {
		// Without a reference the combined dtypes are used as is.
		want = nil
	}
	stacked, err := concatMorsels(ctx, parts, true)
	if err != nil {
		return nil, true, err
	}
	combined, err := aggregate(ctx, stacked, sp.combine)
	stacked.Release()
	if err != nil {
		return nil, true, err
	}
	out, err := executeNode(ctx, cfg, Projection{Input: DataFrameScan{Source: combined, Length: -1}, Exprs: sp.final})
	combined.Release()
	if err != nil {
		return nil, true, err
	}
	out, err = castColumns(ctx, out, want, cfg)
	return out, true, err
}

// castColumns casts the columns of df to want where they differ,
// consuming df.
func castColumns(ctx context.Context, df *dataframe.DataFrame, want []dtype.DType, cfg execConfig) (*dataframe.DataFrame, error) {
	if len(want) != df.Width() {
		return df, nil
	}
	cols := df.Columns()
	changed := false
	out := make([]*series.Series, len(cols))
	for i, c := range cols {
		if c.DType().Equal(want[i]) {
			out[i] = c.Clone()
			continue
		}
		cast, err := compute.Cast(ctx, c, want[i], compute.WithAllocator(cfg.alloc))
		if err != nil {
			releaseSeries(out[:i])
			df.Release()
			return nil, err
		}
		out[i] = cast
		changed = true
	}
	if !changed {
		releaseSeries(out)
		return df, nil
	}
	df.Release()
	return dataframe.New(out...)
}
