package main

import (
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// checkColumns reports the first column a statement reads that the
// focused frame does not have, with the statement and a caret under
// the name. Lazy pipelines would otherwise fail only at the next
// collect, far from the typo.
func (s *state) checkColumns(st *syntax.Stmt) error {
	switch st.Spec.Category {
	case "pipeline", "reshape", "inspect", "aggregate":
	default:
		return nil
	}
	if !s.hasFocus() {
		return nil
	}
	sch, err := s.schema()
	if err != nil {
		return nil
	}
	check := func(name string, off, end int) error {
		if sch.Contains(name) {
			return nil
		}
		d := syntax.Diag{Severity: syntax.SevError, Msg: "unknown column \"" + name + "\""}
		if best := script.Closest(name, sch.Names()); best != "" {
			d.Hint = "did you mean \"" + best + "\"?"
		} else {
			d.Hint = "columns: " + strings.Join(sch.Names(), ", ")
		}
		return syntax.NewStmtError(st, d, off, end)
	}
	for _, p := range st.Parts {
		switch p.Kind {
		case syntax.PartColumn:
			if err := check(p.Value, p.Off, p.End); err != nil {
				return err
			}
		case syntax.PartList, syntax.PartAgg:
			for _, it := range p.Items {
				if it.Kind != syntax.PartColumn {
					continue
				}
				if err := check(it.Value, it.Off, it.End); err != nil {
					return err
				}
			}
		case syntax.PartExpr:
			if p.Expr == nil {
				continue
			}
			var first error
			p.Expr.Walk(func(n *exprparse.Node) bool {
				if first == nil && n.Kind == exprparse.KindCol {
					first = check(n.Name, p.Off+n.NamePos, p.Off+n.NameEnd())
				}
				return first == nil
			})
			if first != nil {
				return first
			}
		}
	}
	return nil
}
