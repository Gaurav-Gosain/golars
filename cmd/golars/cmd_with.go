package main

import (
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// cmdWith parses a `with NEW = EXPR` directive and appends the
// derived column to the focused lazy pipeline. The expression
// grammar lives in script/exprparse and covers arithmetic,
// comparisons, logical ops, string methods (upper / lower / trim /
// replace / like / regex), aggregates, rolling windows, casts, and
// a handful of top-level constructors (col, lit, sum, coalesce).
//
// Syntax (from inside a .glr script or the REPL):
//
//	with bulk = amount > 1000
//	with name_upper = name.str.upper()
//	with revenue = price * qty
//	with rolling7 = amount.rolling_mean(7, 1)
//	with score = coalesce(primary, backup).str.trim()
//
// The NEW column is appended via LazyFrame.WithColumns so the
// optimiser can fuse subsequent projections and filters.
func (s *state) cmdWith(rest string) error {
	name, exprText, err := splitAssignment(rest)
	if err != nil {
		return err
	}
	e, err := exprparse.Parse(exprText)
	if err != nil {
		return fmt.Errorf(".with %q: %w", name, err)
	}
	// Alias the expression so the output column matches the user's
	// requested name. expr.Alias collapses chained aliases so this
	// stays a single AST node even on re-entry.
	e = e.Alias(name)
	next := s.currentLazy().WithColumns(e)
	s.lf = &next
	fmt.Printf("%s with %s = %s\n", successStyle.Render("✓"),
		cmdStyle.Render(name), dimStyle.Render(exprText))
	return nil
}

// splitAssignment carves `NAME = EXPR` out of the raw argument
// string. Both sides are trimmed; the equals sign must appear
// outside any quoted segment.
func splitAssignment(rest string) (name, expr string, err error) {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", "", fmt.Errorf("usage: .with NAME = EXPR")
	}
	inQuote := byte(0)
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if inQuote != 0 {
			if c == '\\' && i+1 < len(rest) {
				i++
				continue
			}
			if c == inQuote {
				inQuote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			inQuote = c
		case '=':
			if i+1 < len(rest) && rest[i+1] == '=' {
				// Part of `==`, not the assignment.
				i++
				continue
			}
			name = strings.TrimSpace(rest[:i])
			expr = strings.TrimSpace(rest[i+1:])
			if name == "" || expr == "" {
				return "", "", fmt.Errorf("usage: .with NAME = EXPR")
			}
			return name, expr, nil
		}
	}
	return "", "", fmt.Errorf("usage: .with NAME = EXPR (missing `=`)")
}
