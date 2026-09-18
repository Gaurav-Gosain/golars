package expr

// ReferencedColumns collects the column names an expression reads.
// It reports complete=false on any unrecognised node so callers fall
// back to a wider frame instead of silently dropping input.
func ReferencedColumns(e Expr) (cols []string, complete bool) {
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	var walk func(n Node) bool
	walk = func(n Node) bool {
		switch t := n.(type) {
		case ColNode:
			add(t.Name)
		case LitNode:
		case BinaryNode:
			if !walk(t.Left.node) || !walk(t.Right.node) {
				return false
			}
		case UnaryNode:
			if !walk(t.Arg.node) {
				return false
			}
		case AliasNode:
			if !walk(t.Inner.node) {
				return false
			}
		case CastNode:
			if !walk(t.Inner.node) {
				return false
			}
		case AggNode:
			if !walk(t.Inner.node) {
				return false
			}
		case IsNullNode:
			if !walk(t.Inner.node) {
				return false
			}
		case WhenThenNode:
			if !walk(t.Pred.node) || !walk(t.Then.node) || !walk(t.Otherwise.node) {
				return false
			}
		case FunctionNode:
			for _, a := range t.Args {
				if !walk(a.node) {
					return false
				}
			}
		case OverNode:
			for _, k := range t.Keys {
				add(k)
			}
			if !walk(t.Inner.node) {
				return false
			}
		default:
			return false
		}
		return true
	}
	if e.node == nil {
		return nil, false
	}
	if !walk(e.node) {
		return nil, false
	}
	return out, true
}
