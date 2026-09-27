// Package syntax parses .glr scripts into statements with source
// positions, checks each statement against its command grammar in
// script.Commands, and formats scripts canonically.
//
// It is the shared front end of `golars fmt`, `golars lint`, the
// language server, the REPL and the Jupyter kernel: they all see the
// same statements, argument roles and error spans.
package syntax

import (
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
)

// File is a parsed script.
type File struct {
	// Src is the text the file was parsed from.
	Src string
	// Lines are the physical lines of Src without line terminators.
	Lines []string
	// Stmts are the statements in source order.
	Stmts []*Stmt
	// Comments are the comments in source order, one per line at most.
	Comments []Comment
}

// Comment is a `#` comment.
type Comment struct {
	Line int // 0-based physical line
	Col  int // byte column of the '#'
	Text string
	// Own is true when the comment is the only thing on its line.
	Own bool
}

// Stmt is one logical statement. Continuation lines (a trailing `\`)
// are joined with a single space, and offsets into Text map back to
// physical positions through Position.
type Stmt struct {
	// Text is the statement with comments removed and continuation
	// lines joined. It keeps the original leading indentation.
	Text string
	// Line and EndLine are the first and last physical lines (0-based).
	Line, EndLine int
	// Dot is true when the command was written with a leading '.'.
	Dot bool
	// Cmd is the command word; Spec is its command, nil when unknown.
	Cmd  Part
	Spec *script.CommandSpec
	// Parts are the classified arguments after the command.
	Parts []Part
	// Diags are syntax problems found in this statement.
	Diags []Diag
	// segs map offsets in Text to physical lines.
	segs []segment
}

type segment struct {
	off  int // offset in Stmt.Text where this physical line starts
	line int
	n    int    // bytes of the physical line copied into Text
	raw  string // the physical line without its comment
}

// Position maps a byte offset in s.Text to a 0-based physical line
// and byte column.
func (s *Stmt) Position(off int) (line, col int) {
	seg := s.segs[0]
	for _, sg := range s.segs {
		if off >= sg.off {
			seg = sg
		}
	}
	col = off - seg.off
	// The joining space after a continued line maps to its end.
	col = min(max(col, 0), seg.n)
	return seg.line, col
}

// Offset maps a physical position back to an offset in s.Text, or -1
// when the position is outside the statement.
func (s *Stmt) Offset(line, col int) int {
	for _, sg := range s.segs {
		if sg.line == line {
			if col > sg.n {
				col = sg.n
			}
			return sg.off + max(col, 0)
		}
	}
	return -1
}

// Rest returns the text after the command word and its offset.
func (s *Stmt) Rest() (string, int) {
	off := s.Cmd.End
	for off < len(s.Text) && isSpace(s.Text[off]) {
		off++
	}
	return strings.TrimRight(s.Text[off:], " \t"), off
}

// Name returns the canonical command name, or the typed word when
// the command is unknown.
func (s *Stmt) Name() string {
	if s.Spec != nil {
		return s.Spec.Name
	}
	return s.Cmd.Value
}

// PartAt returns the part (or list item) covering offset off in
// s.Text, preferring the innermost. The end offset is inclusive so a
// cursor right after a word still finds it.
func (s *Stmt) PartAt(off int) *Part {
	if off >= s.Cmd.Off && off <= s.Cmd.End {
		return &s.Cmd
	}
	for i := range s.Parts {
		p := &s.Parts[i]
		if off < p.Off || off > p.End {
			continue
		}
		for j := range p.Items {
			if it := &p.Items[j]; off >= it.Off && off <= it.End {
				return it
			}
		}
		return p
	}
	return nil
}

// Parse splits src into statements and parses each against its
// command grammar. It never fails: problems are reported as Diags on
// the statements.
func Parse(src string) *File {
	f := &File{Src: src, Lines: splitLines(src)}
	var cur *Stmt
	var text strings.Builder
	flush := func() {
		if cur == nil {
			return
		}
		cur.Text = text.String()
		text.Reset()
		if strings.TrimSpace(cur.Text) != "" {
			parseStmt(cur)
			f.Stmts = append(f.Stmts, cur)
		}
		cur = nil
	}
	for i, raw := range f.Lines {
		body, commentCol := splitComment(raw)
		if commentCol >= 0 {
			f.Comments = append(f.Comments, Comment{
				Line: i, Col: commentCol, Text: raw[commentCol:],
				Own: cur == nil && strings.TrimSpace(body) == "",
			})
		}
		trimmed := strings.TrimRight(body, " \t")
		cont := strings.HasSuffix(trimmed, "\\")
		if cont {
			trimmed = trimmed[:len(trimmed)-1]
		} else {
			trimmed = body
		}
		if cur == nil {
			if !cont && strings.TrimSpace(trimmed) == "" {
				continue
			}
			cur = &Stmt{Line: i}
		}
		cur.segs = append(cur.segs, segment{off: text.Len(), line: i, n: len(trimmed), raw: body})
		cur.EndLine = i
		text.WriteString(trimmed)
		if cont {
			text.WriteByte(' ')
			continue
		}
		flush()
	}
	flush()
	return f
}

// splitLines splits src into physical lines, accepting \n and \r\n.
func splitLines(src string) []string {
	lines := strings.Split(src, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	return lines
}

// splitComment returns the line without its comment and the byte
// column of the comment's '#', or -1. It follows script.Normalize: a
// '#' inside quotes or after a backslash is text.
func splitComment(s string) (string, int) {
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			i++
		case c == inQuote:
			inQuote = 0
		case (c == '"' || c == '\'') && inQuote == 0:
			inQuote = c
		case c == '#' && inQuote == 0:
			return s[:i], i
		}
	}
	return s, -1
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// StmtAt returns the statement that covers physical line, or nil.
func (f *File) StmtAt(line int) *Stmt {
	for _, s := range f.Stmts {
		if line >= s.Line && line <= s.EndLine {
			return s
		}
		if s.Line > line {
			break
		}
	}
	return nil
}

// CommentAt returns the comment on physical line, or nil.
func (f *File) CommentAt(line int) *Comment {
	for i := range f.Comments {
		if f.Comments[i].Line == line {
			return &f.Comments[i]
		}
	}
	return nil
}
