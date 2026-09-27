package lazy

import (
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
)

// Optimizer rules that move filters and column pruning through joins.
// Without them every join input is read in full (all columns, all
// rows) and filters on either side run on the joined output, which for
// TPC-H style plans is most of the work.

// splitAnd flattens a conjunction into its terms.
func splitAnd(e expr.Expr) []expr.Expr {
	if b, ok := e.Node().(expr.BinaryNode); ok && b.Op == expr.OpAnd {
		return append(splitAnd(b.Left), splitAnd(b.Right)...)
	}
	return []expr.Expr{e}
}

// joinAnd rebuilds a conjunction; terms must be non-empty.
func joinAnd(terms []expr.Expr) expr.Expr {
	out := terms[0]
	for _, t := range terms[1:] {
		out = out.And(t)
	}
	return out
}

// mergeFilters folds Filter(Filter(x, a), b) into Filter(x, a AND b).
// Filtering twice keeps exactly the rows where both predicates are
// true, which is what the Kleene AND keeps, so this holds for nulls
// too. Only elementwise predicates merge: b = (x > x.mean()) must see
// just the rows a kept.
func mergeFilters(outer Filter, inner Filter) (Node, bool) {
	if !expr.IsElementwise(outer.Predicate) || !expr.IsElementwise(inner.Predicate) {
		return outer, false
	}
	return Filter{Input: inner.Input, Predicate: inner.Predicate.And(outer.Predicate)}, true
}

// joinChildNeeds splits the columns needed above a join into the
// columns each input must produce, using the join's output layout (so
// left_on/right_on, suffixes and coalescing are honoured). Keys are
// always needed. A needed right column that was renamed by the suffix
// also keeps the left column it collided with, so the rename still
// happens. ok is false when a schema is unknown, in which case both
// inputs keep every column.
func joinChildNeeds(j Join, needed map[string]struct{}) (left, right map[string]struct{}, ok bool) {
	ls, err := j.Left.Schema()
	if err != nil {
		return nil, nil, false
	}
	rs, err := j.Right.Schema()
	if err != nil {
		return nil, nil, false
	}
	spec := j.Spec()
	srcs, err := dataframe.JoinOutputSources(ls, rs, spec)
	if err != nil {
		return nil, nil, false
	}
	left, right = map[string]struct{}{}, map[string]struct{}{}
	for _, k := range spec.LeftOn {
		left[k] = struct{}{}
	}
	for _, k := range spec.RightOn {
		right[k] = struct{}{}
	}
	for _, c := range srcs {
		if _, want := needed[c.Name]; !want {
			continue
		}
		if c.Left != "" {
			left[c.Left] = struct{}{}
		}
		if c.Right != "" {
			right[c.Right] = struct{}{}
			if c.Name != c.Right && ls.Contains(c.Right) {
				left[c.Right] = struct{}{}
			}
		}
	}
	// A child reduced to zero columns would lose its row count (a
	// cross join with nothing read from one side).
	if len(left) == 0 {
		left = nil
	}
	if len(right) == 0 {
		right = nil
	}
	return left, right, true
}

// pruneFilterOnlyColumns drops, right after f, the columns only its
// predicate reads. Without it a column such as o_orderdate, needed to
// filter orders, rode along through every join above. needed is the
// set the surrounding plan reads (nil means all, and nothing is
// pruned).
func pruneFilterOnlyColumns(f Filter, needed map[string]struct{}) (Node, bool) {
	if needed == nil || referencesAllColumns(f.Predicate) {
		return nil, false
	}
	predOnly := false
	for _, c := range expr.Columns(f.Predicate) {
		if _, ok := needed[c]; !ok {
			predOnly = true
			break
		}
	}
	if !predOnly {
		return nil, false
	}
	sch, err := f.Input.Schema()
	if err != nil {
		return nil, false
	}
	keep := orderedIntersect(sch.Names(), needed)
	if len(keep) == 0 {
		return nil, false
	}
	exprs := make([]expr.Expr, len(keep))
	for i, c := range keep {
		exprs[i] = expr.Col(c)
	}
	return Projection{Input: f, Exprs: exprs}, true
}

// fuseWithColumns merges with_columns(with_columns(x, A), B) into
// with_columns(x, A ++ B) when no expression in B reads a column A
// produces. B then sees the same input either way, the outputs are
// applied in the same order (a later same-named output still wins),
// and all expressions evaluate in one parallel batch instead of two.
func fuseWithColumns(outer WithColumns) (Node, bool) {
	inner, ok := outer.Input.(WithColumns)
	if !ok {
		return nil, false
	}
	produced := map[string]struct{}{}
	for _, e := range inner.Exprs {
		produced[expr.OutputName(e)] = struct{}{}
	}
	for _, e := range outer.Exprs {
		if referencesAllColumns(e) {
			return nil, false
		}
		for _, c := range expr.Columns(e) {
			if _, hit := produced[c]; hit {
				return nil, false
			}
		}
	}
	merged := append(append(make([]expr.Expr, 0, len(inner.Exprs)+len(outer.Exprs)), inner.Exprs...), outer.Exprs...)
	return WithColumns{Input: inner.Input, Exprs: merged}, true
}
