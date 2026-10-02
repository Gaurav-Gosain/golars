package expr

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Gaurav-Gosain/golars/selector"
)

// NameOps is the polars `name` namespace: it renames the output of an
// expression without changing its values. Reach it via Expr.Name().
//
// The renames resolve through OutputName, so they apply wherever an
// output name is used (select, with_columns, group_by aggregations)
// and compose with multi-column inputs such as Col("*"), regex columns
// and ColSelector.
type NameOps struct{ e Expr }

// Name opens the name namespace.
func (e Expr) Name() NameOps { return NameOps{e: e} }

// Keep restores the root column name, undoing any alias.
func (o NameOps) Keep() Expr { return fn1("name.keep", o.e) }

// Prefix prepends prefix to the output name.
func (o NameOps) Prefix(prefix string) Expr { return fn1p("name.prefix", o.e, prefix) }

// Suffix appends suffix to the output name.
func (o NameOps) Suffix(suffix string) Expr { return fn1p("name.suffix", o.e, suffix) }

// Map renames the output with fn applied to the current output name.
func (o NameOps) Map(fn func(string) string) Expr { return fn1p("name.map", o.e, NameMapper(fn)) }

// ToLowercase lowercases the output name.
func (o NameOps) ToLowercase() Expr { return fn1("name.to_lowercase", o.e) }

// ToUppercase uppercases the output name.
func (o NameOps) ToUppercase() Expr { return fn1("name.to_uppercase", o.e) }

// Replace replaces matches of pattern in the output name with value.
// With literal false (the polars default) pattern is a regular
// expression and value may use $1-style group references.
func (o NameOps) Replace(pattern, value string, literal bool) Expr {
	return fn1p("name.replace", o.e, pattern, value, literal)
}

// PrefixFields prepends prefix to every field name of a struct output.
func (o NameOps) PrefixFields(prefix string) Expr {
	return fn1p("name.prefix_fields", o.e, prefix)
}

// SuffixFields appends suffix to every field name of a struct output.
func (o NameOps) SuffixFields(suffix string) Expr {
	return fn1p("name.suffix_fields", o.e, suffix)
}

// MapFields renames every field of a struct output with fn.
func (o NameOps) MapFields(fn func(string) string) Expr {
	return fn1p("name.map_fields", o.e, NameMapper(fn))
}

// NameMapper is the function type carried by name.map and
// name.map_fields. Its String form keeps expression reprs readable.
type NameMapper func(string) string

// String renders a placeholder: Go functions have no useful repr.
func (NameMapper) String() string { return "<fn>" }

// ColSelector expands to one column reference per column of the input
// schema that sel matches, in schema order. It is resolved when the
// expression enters a lazy plan (see ExpandColumns).
func ColSelector(sel selector.Selector) Expr {
	return Expr{FunctionNode{Name: "selector", Params: []any{sel}}}
}

// AllColumns is polars `pl.all()`: every column, i.e. Col("*").
// (expr.All already names the AND-fold of predicates.)
func AllColumns() Expr { return Col("*") }

// IsMultiColumn reports whether c refers to more than one column: the
// wildcard "*" or a regex name wrapped in ^...$ (polars' convention).
func IsMultiColumn(name string) bool {
	return name == "*" || (len(name) >= 2 && strings.HasPrefix(name, "^") && strings.HasSuffix(name, "$"))
}

// ApplyNameOp computes the output name produced by a name.* function
// node over an inner name. root is the root column name (for keep).
func applyNameOp(f FunctionNode, inner, root string) (string, error) {
	str := func(i int) string {
		if i < len(f.Params) {
			if s, ok := f.Params[i].(string); ok {
				return s
			}
		}
		return ""
	}
	switch f.Name {
	case "name.keep":
		return root, nil
	case "name.prefix":
		return str(0) + inner, nil
	case "name.suffix":
		return inner + str(0), nil
	case "name.to_lowercase":
		return strings.ToLower(inner), nil
	case "name.to_uppercase":
		return strings.ToUpper(inner), nil
	case "name.map":
		if len(f.Params) > 0 {
			if fn, ok := f.Params[0].(NameMapper); ok && fn != nil {
				return fn(inner), nil
			}
		}
		return inner, nil
	case "name.replace":
		literal := false
		if len(f.Params) > 2 {
			literal, _ = f.Params[2].(bool)
		}
		if literal {
			return strings.ReplaceAll(inner, str(0), str(1)), nil
		}
		re, err := regexp.Compile(str(0))
		if err != nil {
			return "", fmt.Errorf("name.replace: %w", err)
		}
		return re.ReplaceAllString(inner, str(1)), nil
	}
	// name.*_fields rename struct fields, not the column itself.
	return inner, nil
}

// outputNameOf resolves the output name of e the way polars does:
// an alias or name.* rename wins, struct field access yields the field
// name, and otherwise the name comes from the left-most input that
// carries one. ok is false when no input carries a name (for example a
// bare literal).
func outputNameOf(e Expr) (string, bool) {
	switch n := e.node.(type) {
	case nil:
		return "", false
	case AliasNode:
		return n.Name, true
	case ColNode:
		return n.Name, true
	case FunctionNode:
		if strings.HasPrefix(n.Name, "name.") && len(n.Args) > 0 {
			inner, ok := outputNameOf(n.Args[0])
			if !ok {
				inner = OutputName(n.Args[0])
			}
			root := inner
			if cols := Columns(n.Args[0]); len(cols) > 0 {
				root = cols[0]
			}
			name, err := applyNameOp(n, inner, root)
			if err != nil {
				return inner, true
			}
			return name, true
		}
		switch n.Name {
		case "struct.field", "struct.field_ref":
			if len(n.Params) > 0 {
				if s, ok := n.Params[0].(string); ok {
					return s, true
				}
			}
		}
	}
	for _, c := range Children(e) {
		if carriesName(c) {
			return outputNameOf(c)
		}
	}
	return "", false
}

// carriesName reports whether e contains a column reference, a field
// reference or an alias, i.e. something that can name an output.
func carriesName(e Expr) bool {
	found := false
	Walk(e, func(x Expr) bool {
		if found {
			return false
		}
		switch n := x.node.(type) {
		case ColNode, AliasNode:
			found = true
		case FunctionNode:
			if n.Name == "struct.field_ref" {
				found = true
			}
		}
		return !found
	})
	return found
}
