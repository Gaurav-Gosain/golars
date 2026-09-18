package script

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Executor runs a single statement. The line passed in is pre-cleaned
// (leading/trailing whitespace trimmed, comments stripped, leading
// "." added if missing, multi-line continuations joined). Returning
// an error stops script execution; Runner lets callers opt into
// continue-on-error.
type Executor interface {
	Run(line string) error
}

// ExecutorFunc adapts a plain function as an Executor.
type ExecutorFunc func(line string) error

// Run calls f.
func (f ExecutorFunc) Run(line string) error { return f(line) }

// Runner runs scripts against an Executor.
type Runner struct {
	Exec Executor

	// ContinueOnErr, when true, keeps running subsequent statements
	// after a failed one (errors are written to ErrOut if set).
	ContinueOnErr bool

	// ErrOut receives non-fatal errors when ContinueOnErr is true.
	ErrOut io.Writer

	// Trace, if set, is called with each normalised statement just
	// before execution. Useful for showing which line ran before its
	// output, mirroring a REPL transcript.
	Trace func(line string)
}

// RunFile reads the file at path and runs each statement through the
// executor. Directory paths, missing files, and I/O errors surface as
// wrapped errors.
func (r Runner) RunFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("script: %w", err)
	}
	defer f.Close()
	return r.Run(f, path)
}

// Run drains the reader and runs each statement. `name` is surfaced
// in error messages (e.g. path or "<stdin>").
//
// Lines ending in `\` are joined with the next line before execution.
// Comments and blank lines are skipped. The enclosing statement
// retains the line number of its first physical line.
func (r Runner) Run(rd io.Reader, name string) error {
	if r.Exec == nil {
		return errors.New("script: nil Executor")
	}
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 1<<20), 1<<20)

	physLine := 0
	startLine := 0
	var cont strings.Builder

	for sc.Scan() {
		physLine++
		raw := stripComment(sc.Text())
		// Line continuation: a `\` at the VERY end of a physical line
		// (after trimming trailing whitespace) joins with the next.
		trimmed := strings.TrimRight(raw, " \t")
		if body, ok := strings.CutSuffix(trimmed, "\\"); ok {
			if cont.Len() == 0 {
				startLine = physLine
			}
			cont.WriteString(body)
			cont.WriteByte(' ')
			continue
		}
		if cont.Len() > 0 {
			cont.WriteString(raw)
			raw = cont.String()
			cont.Reset()
		} else {
			startLine = physLine
		}

		stmt := Normalize(raw)
		if stmt == "" {
			continue
		}
		if r.Trace != nil {
			r.Trace(stmt)
		}
		if err := r.Exec.Run(stmt); err != nil {
			werr := fmt.Errorf("%s:%d: %w", name, startLine, annotateError(stmt, err))
			if !r.ContinueOnErr {
				return werr
			}
			if r.ErrOut != nil {
				fmt.Fprintln(r.ErrOut, werr)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	// Trailing backslash continuation without a following line: run
	// what we have so the user isn't surprised by silent dropping.
	if cont.Len() > 0 {
		stmt := Normalize(cont.String())
		if stmt != "" {
			if r.Trace != nil {
				r.Trace(stmt)
			}
			if err := r.Exec.Run(stmt); err != nil {
				werr := fmt.Errorf("%s:%d: %w", name, startLine, annotateError(stmt, err))
				if !r.ContinueOnErr {
					return werr
				}
				if r.ErrOut != nil {
					fmt.Fprintln(r.ErrOut, werr)
				}
			}
		}
	}
	return nil
}

// Normalize strips comments, trims whitespace, and ensures the line
// is addressed to a REPL-style dot command. Returns "" for comment
// or blank lines.
//
// Quoted strings (double-quote bounded) are preserved verbatim so
// callers can pass values containing `#` by wrapping in quotes.
func Normalize(raw string) string {
	line := stripComment(raw)
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	if line[0] != '.' {
		line = "." + line
	}
	return line
}

// stripComment drops everything from the first unquoted `#` onward.
// A backslash before `#` also escapes it.
func stripComment(s string) string {
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			i++ // skip next char
		case c == inQuote:
			inQuote = 0
		case (c == '"' || c == '\'') && inQuote == 0:
			inQuote = c
		case c == '#' && inQuote == 0:
			return s[:i]
		}
	}
	return s
}

// annotateError adds a "did you mean?" suggestion when the executor
// returns "unknown command" for a verb close to a known one.
func annotateError(stmt string, err error) error {
	msg := err.Error()
	// Rough heuristic: if the error mentions "unknown" and the first
	// token is a dot-command, look up the closest Commands entry.
	if !strings.Contains(msg, "unknown") {
		return err
	}
	verb := firstToken(stmt)
	if verb == "" {
		return err
	}
	if suggest := SuggestCommand(verb); suggest != "" {
		return fmt.Errorf("%w (did you mean `.%s`?)", err, suggest)
	}
	return err
}

// firstToken returns the first dot-stripped identifier in stmt.
func firstToken(stmt string) string {
	s := strings.TrimLeft(stmt, ".")
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		s = s[:i]
	}
	return s
}
