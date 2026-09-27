package lazy

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/expr"
)

// liftAggWrappers handles group-by outputs that wrap aggregations in
// elementwise expressions, such as sum(x).round(2) or sum(a) / sum(b).
// The group-by kernels take bare aggregations only, so these used to go
// through the generic per-group evaluator, which evaluates the whole
// expression once per group (TPC-H Q10 spent most of its time there).
// Each aggregation inside a wrapper becomes its own output of the
// Aggregate, and a Projection above applies the wrapper to the
// aggregated columns.
//
// It returns the aggregations for the Aggregate node and, when anything
// was lifted, the projection to put above it (nil otherwise).
func liftAggWrappers(exprs []expr.Expr, keys []string) ([]expr.Expr, []expr.Expr) {
	aggs := make([]expr.Expr, 0, len(exprs))
	post := make([]expr.Expr, 0, len(keys)+len(exprs))
	for _, k := range keys {
		post = append(post, expr.Col(k))
	}
	lifted := false
	next := 0
	for _, e := range exprs {
		name := expr.OutputName(e)
		inner := e
		if a, ok := e.Node().(expr.AliasNode); ok {
			inner = a.Inner
		}
		if _, isAgg := inner.Node().(expr.AggNode); isAgg || !liftable(inner) {
			aggs = append(aggs, e)
			post = append(post, expr.Col(name))
			continue
		}
		var pulled []expr.Expr
		rewritten := replaceAggs(inner, func(agg expr.Expr) expr.Expr {
			tmp := fmt.Sprintf("__aggw_%d", next)
			next++
			pulled = append(pulled, agg.Alias(tmp))
			return expr.Col(tmp)
		})
		aggs = append(aggs, pulled...)
		post = append(post, rewritten.Alias(name))
		lifted = true
	}
	if !lifted {
		return exprs, nil
	}
	return aggs, post
}

// liftable reports whether e is elementwise apart from aggregation
// subtrees and contains at least one of them. A bare column outside an
// aggregation disqualifies it: it has one value per row, not per group.
func liftable(e expr.Expr) bool {
	sawAgg := false
	ok := true
	var walk func(x expr.Expr)
	walk = func(x expr.Expr) {
		if !ok {
			return
		}
		switch n := x.Node().(type) {
		case expr.AggNode:
			sawAgg = true
			return
		case expr.ColNode:
			ok = false
			return
		case expr.LitNode:
			return
		case expr.FunctionNode:
			lits := make([]expr.Expr, len(n.Args))
			for i := range lits {
				lits[i] = expr.LitInt64(0)
			}
			if !expr.IsElementwise(expr.WithChildren(x, lits)) {
				ok = false
				return
			}
		case expr.BinaryNode, expr.UnaryNode, expr.AliasNode, expr.CastNode, expr.WhenThenNode, expr.IsNullNode:
		default:
			ok = false
			return
		}
		for _, c := range expr.Children(x) {
			walk(c)
		}
	}
	walk(e)
	return ok && sawAgg
}

// replaceAggs returns e with every aggregation subtree replaced by
// fn(subtree).
func replaceAggs(e expr.Expr, fn func(expr.Expr) expr.Expr) expr.Expr {
	if _, ok := e.Node().(expr.AggNode); ok {
		return fn(e)
	}
	kids := expr.Children(e)
	if len(kids) == 0 {
		return e
	}
	nk := make([]expr.Expr, len(kids))
	for i, k := range kids {
		nk[i] = replaceAggs(k, fn)
	}
	return expr.WithChildren(e, nk)
}
