package exprparse

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// Error is a parse or compile error with the byte span of the
// offending text. Src is the expression the offsets refer to, so a
// caller that embedded it in a longer line can locate it and draw a
// caret under the exact column.
type Error struct {
	// Pos and End are byte offsets into Src. End is exclusive and at
	// least Pos; a zero-width span points between two characters (the
	// end of input, for example).
	Pos, End int
	// Msg says what is wrong, Hint (optional) what to do about it.
	Msg, Hint string
	// Fix, when set, is a replacement for Src[Pos:End] that resolves
	// the error (the target of a "did you mean" hint).
	Fix string
	// Src is the full expression text the offsets index into.
	Src string
}

// Error renders the message with the 1-based column and the hint.
func (e *Error) Error() string {
	msg := e.Msg
	if e.Src != "" {
		msg = fmt.Sprintf("%s at column %d", msg, e.Column())
	}
	if e.Hint != "" {
		msg += "; " + e.Hint
	}
	return msg
}

// Column returns the 1-based character column of Pos within Src.
func (e *Error) Column() int {
	pos := min(max(e.Pos, 0), len(e.Src))
	return utf8.RuneCountInString(e.Src[:pos]) + 1
}

// errAt builds an Error spanning n.
func errAt(n *Node, format string, args ...any) *Error {
	e := &Error{Pos: -1, End: -1, Msg: fmt.Sprintf(format, args...)}
	if n != nil {
		e.Pos, e.End = n.Pos, n.End
	}
	return e
}

// errSpan builds an Error spanning [pos, end).
func errSpan(pos, end int, format string, args ...any) *Error {
	return &Error{Pos: pos, End: end, Msg: fmt.Sprintf(format, args...)}
}

// withHint sets the hint and returns e for chaining.
func (e *Error) withHint(format string, args ...any) *Error {
	e.Hint = fmt.Sprintf(format, args...)
	return e
}

// locate turns err into an *Error that spans n unless it already
// carries a position. Compile helpers return plain errors; the node
// being compiled when one surfaces is the best place to point at.
func locate(err error, n *Node) error {
	if err == nil {
		return nil
	}
	var pe *Error
	if errors.As(err, &pe) {
		if pe.Pos < 0 && n != nil {
			pe.Pos, pe.End = n.Pos, n.End
		}
		return err
	}
	out := &Error{Msg: err.Error()}
	if n != nil {
		out.Pos, out.End = n.Pos, n.End
	}
	return out
}

// attachSource records src on every positioned error so Error() can
// report a column.
func attachSource(err error, src string) error {
	var pe *Error
	if errors.As(err, &pe) && pe.Src == "" {
		pe.Src = src
		if pe.End < pe.Pos {
			pe.End = pe.Pos
		}
		pe.Pos = min(max(pe.Pos, 0), len(src))
		pe.End = min(max(pe.End, pe.Pos), len(src))
	}
	return err
}
