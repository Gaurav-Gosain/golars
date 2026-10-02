// Package predparse parses the `filter` predicate used by the golars
// REPL, the script runner and the glr-to-Go transpiler into an
// [expr.Expr] tree.
//
// A predicate is any glr expression (see [exprparse]) that yields a
// boolean column. The expression grammar carries the classic predicate
// forms, so all of these parse:
//
//	age >= 30 and dept == "eng"
//	x is_null
//	name contains "foo"
//	brand like "%COPPER"
//	status in ["open", "held"]
//	dt.year(ts) == 2024 or str.len_chars(name) > 10
//	active
//
// Keywords (and, or, not, in, is_null, contains, ...) are
// case-insensitive; `and` binds tighter than `or` and parentheses
// group.
package predparse

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// Parse builds an expr.Expr from a filter predicate string.
func Parse(input string) (expr.Expr, error) {
	e, err := exprparse.Parse(input)
	if err != nil {
		return expr.Expr{}, err
	}
	if _, isLit := e.Node().(expr.LitNode); isLit {
		return expr.Expr{}, fmt.Errorf("predicate %q is a constant; compare a column, as in `x > 5`", input)
	}
	return e, nil
}

// GoSource returns Go source that rebuilds the predicate with the
// golars expr package.
func GoSource(input string) (string, error) {
	if _, err := Parse(input); err != nil {
		return "", err
	}
	return exprparse.GoSource(input)
}
