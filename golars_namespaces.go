package golars

import (
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/selector"
)

// Expression-namespace helpers: the polars top-level constructors that
// pair with the str, list, arr, struct and name namespaces.

// Element refers to the current list element inside
// Expr.List().Eval / Filter. Mirrors polars `pl.element()`.
func Element() Expr { return expr.Element() }

// Field refers to a struct field inside Expr.Struct().WithFields.
// Mirrors polars `pl.field(name)`.
func Field(name string) Expr { return expr.Field(name) }

// AllColumns selects every column (polars `pl.all()` / `pl.col("*")`).
// Combine with the name namespace, for example
// AllColumns().Name().Suffix("_x").
func AllColumns() Expr { return expr.AllColumns() }

// ColSelector expands to every column the selector matches, for
// example ColSelector(selector.Numeric()).
func ColSelector(sel selector.Selector) Expr { return expr.ColSelector(sel) }
