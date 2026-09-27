package syntax

import (
	"strings"
	"unicode/utf8"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// Format returns the canonical form of a script:
//
//   - commands lower case, without the leading '.'
//   - one space between arguments, `, ` inside lists
//   - expressions printed by exprparse.Format
//   - consecutive `with NAME = ...` and `load PATH as NAME` lines
//     aligned on `=` and `as`, and trailing comments of consecutive
//     lines aligned, as gofmt aligns them
//   - continuation lines kept where they were, indented two spaces
//   - comments and single blank lines kept; trailing whitespace and
//     runs of blank lines trimmed
//
// A statement with a syntax error is kept as written (only trimmed)
// so formatting never changes what a broken line means.
func Format(src string) string {
	f := Parse(src)
	type outLine struct {
		code    string // formatted code, "" for comment-only or blank
		comment string
		stmt    *Stmt
		cont    bool // a continuation line of a multi-line statement
		// align fields: key is the column to align at, head the text
		// before it.
		alignKey  string
		alignHead string
		alignTail string
	}
	var out []outLine
	next := 0
	for i := 0; i < len(f.Lines); i++ {
		var st *Stmt
		if next < len(f.Stmts) && f.Stmts[next].Line == i {
			st = f.Stmts[next]
			next++
		}
		if st == nil {
			c := f.CommentAt(i)
			if c != nil {
				out = append(out, outLine{comment: strings.TrimRight(c.Text, " \t")})
				continue
			}
			if strings.TrimSpace(f.Lines[i]) != "" {
				// A stray continuation remainder; keep it.
				out = append(out, outLine{code: strings.TrimSpace(f.Lines[i])})
				continue
			}
			if len(out) > 0 && out[len(out)-1].code == "" && out[len(out)-1].comment == "" {
				continue // collapse blank runs
			}
			out = append(out, outLine{})
			continue
		}
		pieces := formatStmt(st)
		comment := ""
		if c := f.CommentAt(st.EndLine); c != nil {
			comment = strings.TrimRight(c.Text, " \t")
		}
		for k, piece := range pieces {
			ol := outLine{code: piece, stmt: st, cont: k > 0}
			if k == len(pieces)-1 {
				ol.comment = comment
			}
			if len(pieces) == 1 {
				ol.alignKey, ol.alignHead, ol.alignTail = alignable(st, piece)
			}
			out = append(out, ol)
		}
		i = st.EndLine
	}
	// Drop leading and trailing blank lines.
	for len(out) > 0 && out[0].code == "" && out[0].comment == "" {
		out = out[1:]
	}
	for len(out) > 0 && out[len(out)-1].code == "" && out[len(out)-1].comment == "" {
		out = out[:len(out)-1]
	}
	// Align `=` / `as` in runs of consecutive statements of one kind.
	for i := 0; i < len(out); {
		key := out[i].alignKey
		j := i + 1
		for key != "" && j < len(out) && out[j].alignKey == key {
			j++
		}
		if key != "" && j-i > 1 {
			width := 0
			for k := i; k < j; k++ {
				width = max(width, utf8.RuneCountInString(out[k].alignHead))
			}
			for k := i; k < j; k++ {
				h := out[k].alignHead
				out[k].code = h + strings.Repeat(" ", width-utf8.RuneCountInString(h)) + out[k].alignTail
			}
		}
		i = j
	}
	// Align trailing comments in runs of consecutive code lines.
	for i := 0; i < len(out); {
		if out[i].code == "" || out[i].comment == "" {
			i++
			continue
		}
		j := i + 1
		for j < len(out) && out[j].code != "" && out[j].comment != "" {
			j++
		}
		width := 0
		for k := i; k < j; k++ {
			width = max(width, utf8.RuneCountInString(out[k].code))
		}
		for k := i; k < j; k++ {
			out[k].code += strings.Repeat(" ", width-utf8.RuneCountInString(out[k].code))
		}
		i = j
	}
	var b strings.Builder
	for _, l := range out {
		line := l.code
		if l.cont {
			line = "  " + line
		}
		if l.comment != "" {
			if line != "" {
				line += " "
			}
			line += l.comment
		}
		b.WriteString(strings.TrimRight(line, " \t"))
		b.WriteByte('\n')
	}
	return b.String()
}

// alignable returns the alignment key and the text before and from
// the aligned token of a single-line statement, or "" when the line
// does not take part in alignment.
func alignable(st *Stmt, line string) (key, head, tail string) {
	if len(st.Diags) > 0 || st.Spec == nil {
		return "", "", ""
	}
	switch st.Spec.Name {
	case "with":
		if strings.Count(line, " = ") >= 1 && len(st.Parts) == 3 {
			i := strings.Index(line, " = ")
			return "with", line[:i], line[i:]
		}
	case "load", "scan_csv", "scan_parquet", "scan_ipc", "scan_json", "scan_ndjson", "scan_auto":
		if i := strings.LastIndex(line, " as "); i > 0 && len(st.Parts) == 3 {
			return "load", line[:i], line[i:]
		}
	}
	return "", "", ""
}

// formatStmt returns the formatted statement, split into physical
// lines where the source had continuations.
func formatStmt(st *Stmt) []string {
	if len(st.Diags) > 0 {
		return keepAsWritten(st)
	}
	var b strings.Builder
	b.WriteString(st.Cmd.Value)
	body := formatArgs(st)
	if body != "" {
		b.WriteByte(' ')
		b.WriteString(body)
	}
	line := b.String()
	if st.EndLine == st.Line {
		return []string{line}
	}
	return rebreak(st, line)
}

// keepAsWritten returns the statement's physical lines trimmed.
func keepAsWritten(st *Stmt) []string {
	var out []string
	for k, sg := range st.segs {
		text := st.Text[sg.off : sg.off+sg.n]
		text = strings.TrimSpace(text)
		if k < len(st.segs)-1 {
			text += " \\"
		}
		out = append(out, text)
	}
	if len(out) > 0 {
		out[0] = strings.TrimPrefix(out[0], ".")
	}
	return out
}

// rebreak splits a formatted multi-line statement before the same
// tokens that started each continuation line in the source, and
// appends the `\` markers. When the token streams do not line up the
// statement is joined onto one line.
func rebreak(st *Stmt, line string) []string {
	var breaks []int // token index where each continuation line starts
	for _, sg := range st.segs[1:] {
		breaks = append(breaks, countTokens(st.Text[:sg.off]))
	}
	starts := tokenStarts(line)
	if countTokens(st.Text) != len(starts) {
		return []string{line}
	}
	var out []string
	prev := 0
	for _, k := range breaks {
		if k <= 0 || k >= len(starts) || starts[k] <= prev {
			continue
		}
		out = append(out, strings.TrimRight(line[prev:starts[k]], " ")+" \\")
		prev = starts[k]
	}
	return append(out, line[prev:])
}

// tokenStarts returns the byte offsets where lexical tokens start in
// s. Quoted strings are one token; commas, parentheses and brackets
// are tokens of their own; the leading '.' of a command is ignored.
func tokenStarts(s string) []int {
	var out []int
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case isSpace(c):
			i++
		case c == '"' || c == '\'':
			out = append(out, i)
			j := i + 1
			for j < len(s) && s[j] != c {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			i = min(j+1, len(s))
		case strings.IndexByte("(),[]=", c) >= 0:
			out = append(out, i)
			if c == '=' && i+1 < len(s) && s[i+1] == '=' {
				i++
			}
			i++
		default:
			out = append(out, i)
			for i < len(s) && !isSpace(s[i]) && strings.IndexByte("(),[]\"'=", s[i]) < 0 {
				i++
			}
		}
	}
	if len(out) > 0 && s[out[0]] == '.' {
		// `.load` and `load` are the same token.
		out[0]++
	}
	return out
}

func countTokens(s string) int { return len(tokenStarts(s)) }

// formatArgs renders the arguments of a well-formed statement.
func formatArgs(st *Stmt) string {
	if st.Spec == nil {
		return joinParts(st.Parts, " ")
	}
	switch st.Spec.Grammar {
	case "@filter":
		return formatExpr(st.Parts[0])
	case "@with":
		var items []string
		for i := 0; i+2 < len(st.Parts); i++ {
			if st.Parts[i].Kind == PartNewColumn {
				items = append(items, st.Parts[i].Text+" = "+formatExpr(st.Parts[i+2]))
				i += 2
			}
		}
		return strings.Join(items, ", ")
	case "@select":
		var items []string
		for i := 0; i < len(st.Parts); i++ {
			p := st.Parts[i]
			switch p.Kind {
			case PartList:
				for _, it := range p.Items {
					items = append(items, it.Text)
				}
			case PartNewColumn:
				items = append(items, p.Text+" = "+formatExpr(st.Parts[i+2]))
				i += 2
			case PartExpr:
				items = append(items, formatExpr(p))
			}
		}
		return strings.Join(items, ", ")
	case "@sort":
		var items []string
		for _, p := range st.Parts {
			if p.Kind == PartKeyword {
				items = append(items, p.Value)
			} else {
				items = append(items, p.Text)
			}
		}
		return strings.Join(items, " ")
	case "@groupby", "@dynamic":
		var items []string
		for i := 0; i < len(st.Parts); i++ {
			p := st.Parts[i]
			if p.Kind == PartNewColumn && i+2 < len(st.Parts) && st.Parts[i+2].Kind == PartExpr {
				e := formatExpr(st.Parts[i+2])
				if strings.ContainsAny(e, " \t") {
					items = append(items, "("+p.Text+" = "+e+")")
				} else {
					items = append(items, p.Text+"="+e)
				}
				i += 2
				continue
			}
			items = append(items, formatPart(p))
		}
		return strings.Join(items, " ")
	}
	return joinParts(st.Parts, " ")
}

func joinParts(parts []Part, sep string) string {
	items := make([]string, 0, len(parts))
	for _, p := range parts {
		items = append(items, formatPart(p))
	}
	return strings.Join(items, sep)
}

func formatPart(p Part) string {
	switch p.Kind {
	case PartKeyword, PartOption:
		return p.Value
	case PartList:
		items := make([]string, len(p.Items))
		for i, it := range p.Items {
			items[i] = it.Text
		}
		return strings.Join(items, ", ")
	case PartAgg:
		var b strings.Builder
		for i, it := range p.Items {
			if i > 0 {
				b.WriteByte(':')
			}
			if it.Kind == PartOption {
				b.WriteString(it.Value)
			} else {
				b.WriteString(it.Text)
			}
		}
		return b.String()
	case PartExpr:
		return formatExpr(p)
	}
	return p.Text
}

func formatExpr(p Part) string {
	if p.Expr == nil {
		return p.Text
	}
	return exprparse.Format(p.Expr)
}
