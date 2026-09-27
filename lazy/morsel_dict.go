package lazy

import (
	"github.com/Gaurav-Gosain/golars/expr"
)

// dictColumns returns the string columns of src that fragment only
// compares with string literals in filters and that do not reach its
// output. Such columns can be read dictionary-encoded: the comparison
// then runs once per distinct value instead of once per row and the
// strings are never materialised (TPC-H Q19 filters six million rows on
// l_shipmode and l_shipinstruct this way). Because the columns never
// leave the fragment, the dtype change is invisible outside it.
func dictColumns(fragment Node, src SourceFunc) []string {
	if src.KnownSchema == nil {
		return nil
	}
	out, err := fragment.Schema()
	if err != nil {
		return nil
	}
	var cands []string
	for _, f := range src.KnownSchema.Fields() {
		if !f.DType.IsString() || out.Contains(f.Name) {
			continue
		}
		if len(src.Projection) > 0 && !contains(src.Projection, f.Name) {
			continue
		}
		cands = append(cands, f.Name)
	}
	if len(cands) == 0 {
		return nil
	}
	ok := map[string]bool{}
	for _, c := range cands {
		ok[c] = true
	}
	// Every use inside the fragment must be a literal comparison in a
	// filter; any other use (projection, with_columns, rename, drop by
	// name is fine) disqualifies the column.
	for n := fragment; ; {
		switch node := n.(type) {
		case SourceFunc:
			var res []string
			for _, c := range cands {
				if ok[c] {
					res = append(res, c)
				}
			}
			return res
		case Filter:
			markNonLiteralUses(node.Predicate, ok)
			n = node.Input
		case WithColumns:
			for _, e := range node.Exprs {
				markAllUses(e, ok)
			}
			n = node.Input
		case Projection:
			for _, e := range node.Exprs {
				markAllUses(e, ok)
			}
			n = node.Input
		case Rename:
			ok[node.Old] = false
			ok[node.New] = false
			n = node.Input
		case Drop:
			n = node.Input
		default:
			return nil
		}
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// markAllUses clears every column e reads.
func markAllUses(e expr.Expr, ok map[string]bool) {
	if referencesAllColumns(e) {
		for k := range ok {
			ok[k] = false
		}
		return
	}
	for _, c := range expr.Columns(e) {
		ok[c] = false
	}
}

// markNonLiteralUses clears columns e reads other than as one side of
// col == "lit" or col != "lit".
func markNonLiteralUses(e expr.Expr, ok map[string]bool) {
	if b, isBin := e.Node().(expr.BinaryNode); isBin && (b.Op == expr.OpEq || b.Op == expr.OpNe) {
		if isColVsStringLit(b.Left, b.Right) || isColVsStringLit(b.Right, b.Left) {
			return
		}
	}
	switch e.Node().(type) {
	case expr.ColNode:
		markAllUses(e, ok)
		return
	case expr.BinaryNode, expr.UnaryNode, expr.LitNode:
	default:
		// Functions and other nodes may read the column in ways a
		// categorical does not support.
		markAllUses(e, ok)
		return
	}
	for _, c := range expr.Children(e) {
		markNonLiteralUses(c, ok)
	}
}

func isColVsStringLit(c, l expr.Expr) bool {
	_, isCol := c.Node().(expr.ColNode)
	lit, isLit := l.Node().(expr.LitNode)
	if !isCol || !isLit {
		return false
	}
	_, isStr := lit.Value.(string)
	return isStr
}
