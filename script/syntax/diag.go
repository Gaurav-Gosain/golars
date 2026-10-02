package syntax

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Severity ranks a diagnostic.
type Severity int

// Severities, in LSP order.
const (
	SevError Severity = iota + 1
	SevWarning
	SevInfo
	SevHint
)

func (s Severity) String() string {
	switch s {
	case SevError:
		return "error"
	case SevWarning:
		return "warning"
	case SevInfo:
		return "info"
	}
	return "hint"
}

// Diag is a problem at a span of a script. Lines are 0-based and
// columns are byte offsets into the physical line.
type Diag struct {
	Line, Col, EndLine, EndCol int
	Severity                   Severity
	// Code names the check, as in "unknown-column".
	Code string
	Msg  string
	Hint string
	// Fix, when non-nil, replaces the span with Fix.Text.
	Fix *Fix
}

// Fix is an edit that resolves a diagnostic.
type Fix struct {
	// Title describes the edit for code-action menus.
	Title string
	// Line, Col, EndLine, EndCol give the replaced range; a zero
	// range means the diagnostic's own span.
	Line, Col, EndLine, EndCol int
	Text                       string
	// Insert, when true, inserts Text as a new line before Line.
	Insert bool
}

// diagAt builds a diagnostic for the byte span [off, end) of s.Text.
func (s *Stmt) diagAt(off, end int, sev Severity, code, format string, args ...any) Diag {
	d := Diag{Severity: sev, Code: code, Msg: fmt.Sprintf(format, args...)}
	d.Line, d.Col = s.Position(off)
	d.EndLine, d.EndCol = s.Position(max(end, off))
	return d
}

// DiagAt is diagAt for other packages.
func (s *Stmt) DiagAt(off, end int, sev Severity, code, format string, args ...any) Diag {
	return s.diagAt(off, end, sev, code, format, args...)
}

// Render formats a diagnostic the way compilers do: the location,
// the message, the source line and a caret under the span.
//
//	pipeline.glr:3:8: error: unknown column "salry"
//	  filter salry > 100
//	         ^^^^^
//	  hint: did you mean "salary"?
func Render(name string, lines []string, d Diag) string {
	var b strings.Builder
	col := d.Col
	src := ""
	if d.Line >= 0 && d.Line < len(lines) {
		src = lines[d.Line]
	}
	col = min(col, len(src))
	fmt.Fprintf(&b, "%s:%d:%d: %s: %s\n", name, d.Line+1, utf8.RuneCountInString(src[:col])+1, d.Severity, d.Msg)
	if src != "" {
		end := d.EndCol
		if d.EndLine != d.Line || end > len(src) {
			end = len(src)
		}
		end = max(end, col)
		b.WriteString("  ")
		b.WriteString(strings.ReplaceAll(src, "\t", " "))
		b.WriteString("\n  ")
		b.WriteString(strings.Repeat(" ", utf8.RuneCountInString(src[:col])))
		width := max(utf8.RuneCountInString(src[col:end]), 1)
		b.WriteString(strings.Repeat("^", width))
		b.WriteByte('\n')
	}
	if d.Hint != "" {
		b.WriteString("  hint: ")
		b.WriteString(d.Hint)
		b.WriteByte('\n')
	}
	return b.String()
}
