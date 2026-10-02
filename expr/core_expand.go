package expr

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// HasColumnSelector reports whether e contains a multi-column selector
// (AllCols, Nth, Exclude) that must be expanded against a schema.
func HasColumnSelector(e Expr) bool {
	found := false
	Walk(e, func(x Expr) bool {
		if f, ok := x.node.(FunctionNode); ok && strings.HasPrefix(f.Name, "cols_") {
			found = true
			return false
		}
		return !found
	})
	return found
}

// ExpandColumns rewrites every expression that contains a column-set
// selector into one expression per selected column, in schema order.
// names is the input schema; skip lists columns a selector never picks
// (the group-by keys inside an aggregation). Expressions without a
// selector pass through unchanged.
func ExpandColumns(exprs []Expr, names, skip []string) ([]Expr, error) {
	out := make([]Expr, 0, len(exprs))
	for _, e := range exprs {
		if !HasColumnSelector(e) {
			out = append(out, e)
			continue
		}
		sel, cols, err := findSelector(e, names, skip)
		if err != nil {
			return nil, err
		}
		for _, c := range cols {
			out = append(out, replaceNode(e, sel, Col(c)))
		}
	}
	return out, nil
}

// findSelector returns the outermost selector node in e and the columns
// it resolves to.
func findSelector(e Expr, names, skip []string) (Expr, []string, error) {
	var sel Expr
	Walk(e, func(x Expr) bool {
		if sel.node != nil {
			return false
		}
		if f, ok := x.node.(FunctionNode); ok && strings.HasPrefix(f.Name, "cols_") {
			sel = x
			return false
		}
		return true
	})
	cols, err := resolveSelector(sel, names, skip)
	return sel, cols, err
}

func resolveSelector(sel Expr, names, skip []string) ([]string, error) {
	available := make([]string, 0, len(names))
	for _, n := range names {
		if !slices.Contains(skip, n) {
			available = append(available, n)
		}
	}
	switch n := sel.node.(type) {
	case ColNode:
		switch {
		case n.Name == "*":
			return available, nil
		case IsMultiColumn(n.Name):
			re, err := regexp.Compile(n.Name)
			if err != nil {
				return nil, fmt.Errorf("expr: invalid column pattern %q: %w", n.Name, err)
			}
			out := make([]string, 0, len(available))
			for _, c := range available {
				if re.MatchString(c) {
					out = append(out, c)
				}
			}
			return out, nil
		}
		return []string{n.Name}, nil
	case FunctionNode:
		switch n.Name {
		case "cols_all":
			return available, nil
		case "cols_nth":
			idx, _ := paramOf(n, 0).([]int)
			out := make([]string, 0, len(idx))
			for _, i := range idx {
				j := i
				if j < 0 {
					j += len(names)
				}
				if j < 0 || j >= len(names) {
					return nil, fmt.Errorf("expr: nth index %d out of bounds for %d columns", i, len(names))
				}
				out = append(out, names[j])
			}
			return out, nil
		case "cols_exclude":
			drop, _ := paramOf(n, 0).([]string)
			return without(available, drop), nil
		case "cols_exclude_from":
			drop, _ := paramOf(n, 0).([]string)
			if len(n.Args) != 1 {
				return nil, fmt.Errorf("expr: exclude needs an input selector")
			}
			base, err := resolveSelector(n.Args[0], names, skip)
			if err != nil {
				return nil, err
			}
			return without(base, drop), nil
		}
	}
	return nil, fmt.Errorf("expr: %s is not a column selector", sel)
}

func without(names, drop []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !slices.Contains(drop, n) {
			out = append(out, n)
		}
	}
	return out
}

func paramOf(f FunctionNode, i int) any {
	if i < len(f.Params) {
		return f.Params[i]
	}
	return nil
}

// replaceNode returns a copy of e with the subtree target (compared by
// identity of its string form and position) replaced by with.
func replaceNode(e, target, with Expr) Expr {
	if sameNode(e, target) {
		return with
	}
	kids := Children(e)
	if len(kids) == 0 {
		return e
	}
	changed := false
	next := make([]Expr, len(kids))
	for i, k := range kids {
		next[i] = replaceNode(k, target, with)
		if !sameNode(next[i], k) {
			changed = true
		}
	}
	if !changed {
		return e
	}
	return WithChildren(e, next)
}

func sameNode(a, b Expr) bool {
	fa, ok1 := a.node.(FunctionNode)
	fb, ok2 := b.node.(FunctionNode)
	if ok1 && ok2 {
		return fa.Name == fb.Name && fa.String() == fb.String()
	}
	if ok1 != ok2 {
		return false
	}
	return a.String() == b.String()
}
