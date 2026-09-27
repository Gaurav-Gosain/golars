package lazy

import "github.com/Gaurav-Gosain/golars/expr"

// rewriteIsBetween turns col.is_between(lit, lit) into two comparisons
// joined by AND. The comparison kernels take a scalar right-hand side
// directly, while is_between broadcasts both bounds to full columns and
// compares row by row through a generic path; on TPC-H Q6 the rewrite
// removes most of the filter cost. The results agree: a null input
// gives null either way, and floats compare with the same total order
// (NaN above every number) in both.
//
// Only a bare column with non-null literal bounds is rewritten, so no
// subexpression is evaluated twice.
func rewriteIsBetween(n expr.FunctionNode) (expr.Expr, bool) {
	if len(n.Args) != 3 {
		return expr.Expr{}, false
	}
	x, lo, hi := n.Args[0], n.Args[1], n.Args[2]
	if _, ok := x.Node().(expr.ColNode); !ok {
		return expr.Expr{}, false
	}
	for _, b := range []expr.Expr{lo, hi} {
		l, ok := b.Node().(expr.LitNode)
		if !ok || l.Value == nil {
			return expr.Expr{}, false
		}
	}
	closed := "both"
	if len(n.Params) > 0 {
		if s, ok := n.Params[0].(string); ok && s != "" {
			closed = s
		}
	}
	var lower, upper expr.Expr
	switch closed {
	case "both":
		lower, upper = x.Ge(lo), x.Le(hi)
	case "left":
		lower, upper = x.Ge(lo), x.Lt(hi)
	case "right":
		lower, upper = x.Gt(lo), x.Le(hi)
	case "none":
		lower, upper = x.Gt(lo), x.Lt(hi)
	default:
		return expr.Expr{}, false
	}
	return lower.And(upper), true
}
