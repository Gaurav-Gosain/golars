package analysis

import (
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
	"github.com/Gaurav-Gosain/golars/script/syntax"
)

// Target is what sits under a position: a statement part and, inside
// an expression, the node.
type Target struct {
	Stmt *syntax.Stmt
	Part *syntax.Part
	// Node is the expression node under the position, if any.
	Node *exprparse.Node
	// Off and End span the word in Stmt.Text.
	Off, End int
}

// At finds the part (and expression node) at a 0-based line and byte
// column.
func (r *Result) At(line, col int) (Target, bool) {
	st := r.File.StmtAt(line)
	if st == nil {
		return Target{}, false
	}
	off := st.Offset(line, col)
	if off < 0 {
		return Target{}, false
	}
	p := st.PartAt(off)
	if p == nil {
		return Target{}, false
	}
	t := Target{Stmt: st, Part: p, Off: p.Off, End: p.End}
	if p.Kind == syntax.PartExpr && p.Expr != nil {
		rel := off - p.Off
		p.Expr.Walk(func(n *exprparse.Node) bool {
			switch n.Kind {
			case exprparse.KindCol, exprparse.KindCall:
				if rel >= n.NamePos && rel <= n.NameEnd() {
					t.Node = n
					t.Off, t.End = p.Off+n.NamePos, p.Off+n.NameEnd()
				}
			case exprparse.KindLit:
				if rel >= n.Pos && rel < n.End && t.Node == nil {
					t.Node = n
				}
			}
			for _, k := range n.Kw {
				if rel >= k.NamePos && rel <= k.NamePos+len(k.Name) {
					t.Node = &exprparse.Node{Kind: exprparse.KindCall, Name: "=" + k.Name, NS: n.NS,
						Recv: n, NamePos: k.NamePos}
					t.Off, t.End = p.Off+k.NamePos, p.Off+k.NamePos+len(k.Name)
				}
			}
			return true
		})
	}
	return t, true
}

// Hover returns markdown for the position, or "".
func (r *Result) Hover(line, col int) string {
	t, found := r.At(line, col)
	if !found {
		return ""
	}
	st, p := t.Stmt, t.Part
	step := r.StepAt(st.Line)
	switch {
	case p.Kind == syntax.PartCommand:
		if st.Spec == nil {
			return ""
		}
		md := st.Spec.Markdown()
		if step != nil && step.Changes {
			md += "\n\n---\n\nAfter this statement: " + frameSummary(step.After)
		}
		return md
	case t.Node != nil:
		return r.hoverNode(t, step)
	case p.Kind == syntax.PartColumn || p.Kind == syntax.PartList:
		if step == nil {
			return ""
		}
		return columnHover(p.Value, step.Before)
	case p.Kind == syntax.PartNewColumn:
		if step == nil {
			return ""
		}
		dt := step.Items[p.Value]
		if dt == "" {
			dt = step.After.DType(p.Value)
		}
		return fmt.Sprintf("**%s** `%s`\n\nNew column defined here.", p.Value, orUnknown(dt))
	case p.Kind == syntax.PartFrame || p.Kind == syntax.PartTarget || p.Kind == syntax.PartNewFrame:
		if step == nil {
			return ""
		}
		frames := step.Frames
		if p.Kind == syntax.PartNewFrame {
			frames = step.FramesAfter
		}
		if f, found := frames[p.Value]; found {
			return fmt.Sprintf("**frame %s**\n\n%s", p.Value, frameSummary(f))
		}
	case p.Kind == syntax.PartPath:
		if step == nil {
			return ""
		}
		f := step.After
		if nf := stagedName(st); nf != "" {
			f = step.FramesAfter[nf]
		}
		return fmt.Sprintf("**%s**\n\n%s", p.Value, frameSummary(f))
	case p.Kind == syntax.PartAgg:
		return "`col:op[:alias]` aggregation: `" + p.Text + "`"
	case p.Kind == syntax.PartDType:
		return "dtype `" + p.Value + "`"
	}
	return ""
}

func stagedName(st *syntax.Stmt) string {
	for _, p := range st.Parts {
		if p.Kind == syntax.PartNewFrame {
			return p.Value
		}
	}
	return ""
}

func (r *Result) hoverNode(t Target, step *Step) string {
	n := t.Node
	switch n.Kind {
	case exprparse.KindCol:
		if step == nil {
			return ""
		}
		return columnHover(n.Name, step.Before)
	case exprparse.KindCall:
		if strings.HasPrefix(n.Name, "=") {
			return fmt.Sprintf("keyword argument `%s` of `%s`", n.Name[1:], n.Recv.Name)
		}
		if n.Infix != "" {
			return infixDoc(n.Infix)
		}
		ns := n.NS
		info, found := exprparse.Lookup(ns, n.Name)
		if !found && n.Recv == nil && ns != "" {
			info, found = exprparse.Lookup("", n.Name)
		}
		if found {
			return info.Markdown()
		}
	case exprparse.KindLit:
		return ""
	}
	return ""
}

func infixDoc(op string) string {
	switch op {
	case "in":
		return "`x in [a, b]`: true where x is one of the listed values (`is_in`)."
	case "is_null", "is_not_null":
		return "`x " + op + "`: test for nulls."
	}
	return "`s " + op + " pattern`: string test (`str." + op + "`)."
}

func orUnknown(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// columnHover describes column name in frame f.
func columnHover(name string, f Frame) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**%s** `%s`", name, orUnknown(f.DType(name)))
	if f.Known() && !f.Schema.Contains(name) {
		b.WriteString("\n\nNot a column at this point.")
		return b.String()
	}
	fmt.Fprintf(&b, "\n\nFrame here: %s", f.Shape())
	if f.Source != "" {
		fmt.Fprintf(&b, " from `%s`", f.Source)
	}
	if o, found := f.Origins[name]; found {
		fmt.Fprintf(&b, "\n\nDefined on line %d.", o.Line+1)
	}
	return b.String()
}

// frameSummary renders a frame's shape and schema as markdown.
func frameSummary(f Frame) string {
	var b strings.Builder
	b.WriteString(f.Shape())
	if !f.Known() {
		b.WriteString(" (columns unknown until it runs)")
		return b.String()
	}
	b.WriteString("\n\n| column | dtype |\n|---|---|\n")
	for i, fl := range f.Schema.Fields() {
		if i == 40 {
			fmt.Fprintf(&b, "| ... %d more | |\n", f.Schema.Len()-40)
			break
		}
		fmt.Fprintf(&b, "| %s | %s |\n", fl.Name, fl.DType)
	}
	return strings.TrimRight(b.String(), "\n")
}

// Signature is the call around a position, for signature help.
type Signature struct {
	Info exprparse.FuncInfo
	// Active is the index of the parameter under the cursor in the
	// label's parameter list (receiver included for method forms), or
	// -1.
	Active int
	// Params are the parameter labels in order, as they appear in
	// Info.Label().
	Params []string
}

// SignatureAt returns the innermost function call whose argument list
// contains the position.
func (r *Result) SignatureAt(line, col int) (Signature, bool) {
	text := ""
	if line >= 0 && line < len(r.File.Lines) {
		text = r.File.Lines[line]
	}
	col = min(max(col, 0), len(text))
	prefix := text[:col]
	for l := line; l > 0; l-- {
		prev, _ := splitCommentIdx(strings.TrimRight(r.File.Lines[l-1], " \t"))
		prev = strings.TrimRight(prev, " \t")
		if !strings.HasSuffix(prev, "\\") {
			break
		}
		prefix = strings.TrimSuffix(prev, "\\") + " " + prefix
	}
	probe := prefix + cursorMark + closers(prefix)
	f := syntax.Parse(probe)
	if len(f.Stmts) == 0 {
		return Signature{}, false
	}
	st := f.Stmts[0]
	off := strings.Index(st.Text, cursorMark)
	var p *syntax.Part
	for i := range st.Parts {
		if pp := &st.Parts[i]; pp.Kind == syntax.PartExpr && off >= pp.Off && off <= pp.End {
			p = pp
		}
	}
	if p == nil || p.Expr == nil {
		return Signature{}, false
	}
	rel := off - p.Off
	var best *exprparse.Node
	active := -1
	p.Expr.Walk(func(n *exprparse.Node) bool {
		if n.Kind != exprparse.KindCall || !n.CallParens || n.Infix != "" {
			return true
		}
		open := n.NameEnd()
		if rel <= open || rel >= n.End {
			return true
		}
		best = n
		active = -1
		for i, a := range n.Args {
			if rel >= a.Pos && rel <= a.End {
				active = i
			}
		}
		for _, k := range n.Kw {
			if rel >= k.NamePos && rel <= k.Val.End {
				active = -2
				best = n
				break
			}
		}
		return true
	})
	if best == nil {
		return Signature{}, false
	}
	ns := best.NS
	info, found := exprparse.Lookup(ns, best.Name)
	if !found && best.Recv == nil && ns != "" {
		info, found = exprparse.Lookup("", best.Name)
		ns = ""
	}
	if !found {
		return Signature{}, false
	}
	sig := Signature{Info: info, Active: -1}
	if !info.Free {
		sig.Params = append(sig.Params, "x")
	}
	for _, prm := range info.Params {
		sig.Params = append(sig.Params, prm.String())
	}
	for _, k := range info.Keywords {
		sig.Params = append(sig.Params, k+"=...")
	}
	shift := 0
	if best.Recv != nil {
		// Method form: the receiver is outside the parentheses.
		shift = 1
	}
	switch {
	case active >= 0:
		sig.Active = min(active+shift, len(sig.Params)-1)
		if last := len(info.Params) - 1; last >= 0 && info.Params[last].Variadic {
			base := 0
			if !info.Free {
				base = 1
			}
			sig.Active = min(sig.Active, base+last)
		}
	case active == -2:
		for _, k := range best.Kw {
			if rel >= k.NamePos && rel <= k.Val.End {
				for i, prm := range sig.Params {
					if strings.HasPrefix(prm, k.Name+":") || strings.HasPrefix(prm, k.Name+"=") {
						sig.Active = i
					}
				}
			}
		}
	default:
		if len(best.Args) == 0 && len(best.Kw) == 0 {
			sig.Active = min(shift, len(sig.Params)-1)
		}
	}
	return sig, true
}
