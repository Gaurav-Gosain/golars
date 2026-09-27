package syntax

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// StmtError is a diagnostic for a single statement, as a REPL or a
// script runner reports it: the message, then the statement with a
// caret under the span, then the hint.
type StmtError struct {
	// Text is the statement as written, without a leading '.'.
	Text string
	// Diag holds the message; Col and EndCol are byte offsets into
	// Text.
	Diag Diag
}

// NewStmtError builds a StmtError for the span [off, end) of st.Text.
// A leading '.' and indentation are dropped from the display.
func NewStmtError(st *Stmt, d Diag, off, end int) *StmtError {
	text := st.Text
	trim := len(text) - len(strings.TrimLeft(text, " \t"))
	if st.Dot {
		trim++
	}
	text = strings.TrimRight(text[trim:], " \t")
	off = min(max(off-trim, 0), len(text))
	end = min(max(end-trim, off), len(text))
	d.Line, d.EndLine, d.Col, d.EndCol = 0, 0, off, end
	return &StmtError{Text: text, Diag: d}
}

// FirstError returns the first syntax diagnostic of st as a
// StmtError, or nil when the statement is well formed.
func FirstError(st *Stmt) error {
	for _, d := range st.Diags {
		if d.Severity != SevError {
			continue
		}
		off := st.Offset(d.Line, d.Col)
		end := st.Offset(d.EndLine, d.EndCol)
		return NewStmtError(st, d, off, end)
	}
	return nil
}

// Locate turns an error raised while running st into a StmtError
// when it carries an expression position: the expression text is
// found in the statement to place the caret. Other errors pass
// through unchanged.
func Locate(st *Stmt, err error) error {
	var pe *exprparse.Error
	if err == nil || !errors.As(err, &pe) || pe.Src == "" {
		return err
	}
	base := strings.Index(st.Text, pe.Src)
	if base < 0 {
		return err
	}
	d := Diag{Severity: SevError, Msg: pe.Msg, Hint: pe.Hint}
	return NewStmtError(st, d, base+pe.Pos, base+pe.End)
}

func (e *StmtError) Error() string {
	var b strings.Builder
	b.WriteString(e.Diag.Msg)
	col := min(e.Diag.Col, len(e.Text))
	end := min(max(e.Diag.EndCol, col), len(e.Text))
	b.WriteString("\n    ")
	b.WriteString(e.Text)
	b.WriteString("\n    ")
	b.WriteString(strings.Repeat(" ", utf8.RuneCountInString(e.Text[:col])))
	b.WriteString(strings.Repeat("^", max(utf8.RuneCountInString(e.Text[col:end]), 1)))
	if e.Diag.Hint != "" {
		b.WriteString("\n  hint: ")
		b.WriteString(e.Diag.Hint)
	}
	return b.String()
}
