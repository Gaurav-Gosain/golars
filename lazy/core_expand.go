package lazy

import (
	"slices"

	"github.com/Gaurav-Gosain/golars/expr"
)

// expandSelectors expands column-set selectors (expr.AllCols, expr.Nth,
// expr.Exclude) against the input schema. When the schema cannot be
// resolved the expressions are returned unchanged and evaluation
// reports the unexpanded selector.
func expandSelectors(input Node, exprs []expr.Expr, skip []string) []expr.Expr {
	if !slices.ContainsFunc(exprs, expr.HasColumnSelector) {
		return exprs
	}
	sch, err := input.Schema()
	if err != nil {
		return exprs
	}
	out, err := expr.ExpandColumns(exprs, sch.Names(), skip)
	if err != nil {
		return exprs
	}
	return out
}
