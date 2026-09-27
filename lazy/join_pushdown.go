package lazy

import (
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
)

// Optimizer rules that move filters and column pruning through joins.
// Without them every join input is read in full (all columns, all
// rows) and filters on either side run on the joined output, which for
// TPC-H style plans is most of the work.

// joinRightSuffix is the suffix Join gives right-side columns whose
// name collides with a left-side column. The lazy Join node always
// uses the dataframe default.
const joinRightSuffix = "_right"

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

// pushFilterIntoJoin moves the terms of an elementwise conjunction that
// read only one join input below the join. For inner and cross joins
// either side qualifies, and a term on key columns only goes to both
// sides. For a left join only left-side terms move: filtering the right
// input would turn dropped matches into null-filled rows instead of
// removing them.
func pushFilterIntoJoin(f Filter, j Join) (Node, bool) {
	if j.How != dataframe.InnerJoin && j.How != dataframe.LeftJoin && j.How != dataframe.CrossJoin {
		return f, false
	}
	if !expr.IsElementwise(f.Predicate) || referencesAllColumns(f.Predicate) {
		return f, false
	}
	ls, err := j.Left.Schema()
	if err != nil {
		return f, false
	}
	rs, err := j.Right.Schema()
	if err != nil {
		return f, false
	}
	keys := stringSet(j.On)
	var toLeft, toRight, keep []expr.Expr
	for _, term := range splitAnd(f.Predicate) {
		refs := expr.Columns(term)
		if len(refs) == 0 {
			keep = append(keep, term)
			continue
		}
		onLeft, onRight := true, true
		for _, r := range refs {
			if !ls.Contains(r) {
				onLeft = false
			}
			// A right column keeps its name in the output only when it
			// is a key or does not collide with a left column.
			_, isKey := keys[r]
			if !rs.Contains(r) || (!isKey && ls.Contains(r)) {
				onRight = false
			}
		}
		switch {
		case onLeft && onRight && j.How != dataframe.LeftJoin:
			toLeft = append(toLeft, term)
			toRight = append(toRight, term)
		case onLeft:
			toLeft = append(toLeft, term)
		case onRight && j.How != dataframe.LeftJoin:
			toRight = append(toRight, term)
		default:
			keep = append(keep, term)
		}
	}
	if len(toLeft) == 0 && len(toRight) == 0 {
		return f, false
	}
	nj := j
	if len(toLeft) > 0 {
		nj.Left = Filter{Input: j.Left, Predicate: joinAnd(toLeft)}
	}
	if len(toRight) > 0 {
		nj.Right = Filter{Input: j.Right, Predicate: joinAnd(toRight)}
	}
	if len(keep) == 0 {
		return nj, true
	}
	return Filter{Input: nj, Predicate: joinAnd(keep)}, true
}

// joinChildNeeds splits the columns needed above a join into the
// columns each input must produce. Keys are always needed. A needed
// "x_right" maps to right column x and also keeps left column x, so the
// collision that produced the suffix still happens. ok is false when a
// schema is unknown, in which case both inputs keep every column.
func joinChildNeeds(j Join, needed map[string]struct{}) (left, right map[string]struct{}, ok bool) {
	ls, err := j.Left.Schema()
	if err != nil {
		return nil, nil, false
	}
	rs, err := j.Right.Schema()
	if err != nil {
		return nil, nil, false
	}
	left, right = map[string]struct{}{}, map[string]struct{}{}
	keys := stringSet(j.On)
	for k := range keys {
		left[k] = struct{}{}
		right[k] = struct{}{}
	}
	for name := range needed {
		if ls.Contains(name) {
			left[name] = struct{}{}
		} else if rs.Contains(name) {
			right[name] = struct{}{}
		}
		if base, cut := strings.CutSuffix(name, joinRightSuffix); cut && !ls.Contains(name) {
			if _, isKey := keys[base]; !isKey && ls.Contains(base) && rs.Contains(base) {
				left[base] = struct{}{}
				right[base] = struct{}{}
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
