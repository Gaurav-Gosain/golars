package lazy

import (
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/selector"
)

// expand.go resolves polars' multi-output expressions when they enter a
// plan node, using the input schema:
//
//   - Col("*"), Col("^regex$") and expr.ColSelector(...) become one
//     expression per matching column;
//   - struct.field with several names becomes one expression per name;
//   - struct.unnest becomes one struct.field per struct field.
//
// The name namespace composes with all of these because renames are
// resolved through expr.OutputName on each expanded expression.

// expandExprs returns exprs with every multi-output expression
// expanded against input's schema. exclude lists columns a wildcard
// must skip (the group keys in an aggregation). When the schema cannot
// be computed the expressions are returned unchanged.
func expandExprs(input Node, exprs []expr.Expr, exclude []string) []expr.Expr {
	if !slices.ContainsFunc(exprs, needsExpansion) {
		return exprs
	}
	in, err := input.Schema()
	if err != nil {
		return exprs
	}
	out := make([]expr.Expr, 0, len(exprs))
	for _, e := range exprs {
		out = append(out, expandOne(e, in, exclude)...)
	}
	return out
}

func needsExpansion(e expr.Expr) bool {
	found := false
	expr.Walk(e, func(x expr.Expr) bool {
		switch n := x.Node().(type) {
		case expr.ColNode:
			found = found || expr.IsMultiColumn(n.Name)
		case expr.FunctionNode:
			switch n.Name {
			case "selector", "struct.unnest":
				found = true
			case "struct.field":
				found = found || len(n.Params) > 1
			}
		}
		return !found
	})
	return found
}

func expandOne(e expr.Expr, in *schema.Schema, exclude []string) []expr.Expr {
	// Column wildcards first: every other rule then sees plain columns.
	if names, ok := wildcardNames(e, in, exclude); ok {
		out := make([]expr.Expr, 0, len(names))
		for _, name := range names {
			sub := rewriteExpr(e, func(x expr.Expr) (expr.Expr, bool) {
				switch n := x.Node().(type) {
				case expr.ColNode:
					if expr.IsMultiColumn(n.Name) {
						return expr.Col(name), true
					}
				case expr.FunctionNode:
					if n.Name == "selector" {
						return expr.Col(name), true
					}
				}
				return x, false
			})
			out = append(out, expandOne(sub, in, exclude)...)
		}
		return out
	}
	// struct.field("a", "b") and struct.unnest().
	var target expr.FunctionNode
	var names []string
	found := false
	expr.Walk(e, func(x expr.Expr) bool {
		if found {
			return false
		}
		n, ok := x.Node().(expr.FunctionNode)
		if !ok || len(n.Args) == 0 {
			return true
		}
		switch {
		case n.Name == "struct.field" && len(n.Params) > 1:
			for _, p := range n.Params {
				if s, ok := p.(string); ok {
					names = append(names, s)
				}
			}
			target, found = n, true
		case n.Name == "struct.unnest":
			if fields, ok := structFieldNames(n.Args[0], in); ok {
				names, target, found = fields, n, true
			}
		}
		return !found
	})
	if !found {
		return []expr.Expr{e}
	}
	out := make([]expr.Expr, 0, len(names))
	for _, name := range names {
		sub := rewriteExpr(e, func(x expr.Expr) (expr.Expr, bool) {
			if n, ok := x.Node().(expr.FunctionNode); ok && sameFunction(n, target) {
				return n.Args[0].Struct().Field(name), true
			}
			return x, false
		})
		out = append(out, expandOne(sub, in, exclude)...)
	}
	return out
}

func sameFunction(a, b expr.FunctionNode) bool {
	return a.Name == b.Name && a.String() == b.String()
}

// wildcardNames returns the columns the first multi-column reference in
// e matches, in schema order.
func wildcardNames(e expr.Expr, in *schema.Schema, exclude []string) ([]string, bool) {
	var match func(string) bool
	var sel selector.Selector
	expr.Walk(e, func(x expr.Expr) bool {
		if match != nil || sel != nil {
			return false
		}
		switch n := x.Node().(type) {
		case expr.ColNode:
			switch {
			case n.Name == "*":
				match = func(string) bool { return true }
			case expr.IsMultiColumn(n.Name):
				re, err := regexp.Compile(n.Name)
				if err != nil {
					return true
				}
				match = re.MatchString
			}
		case expr.FunctionNode:
			if n.Name == "selector" && len(n.Params) > 0 {
				sel, _ = n.Params[0].(selector.Selector)
			}
		}
		return match == nil && sel == nil
	})
	var names []string
	switch {
	case sel != nil:
		names = sel.Apply(in)
	case match != nil:
		for _, name := range in.Names() {
			if match(name) {
				names = append(names, name)
			}
		}
	default:
		return nil, false
	}
	names = slices.DeleteFunc(names, func(n string) bool { return slices.Contains(exclude, n) })
	return names, true
}

// rewriteExpr rebuilds e bottom-up, replacing every sub-expression for
// which fn returns true. Replaced nodes are not descended into.
func rewriteExpr(e expr.Expr, fn func(expr.Expr) (expr.Expr, bool)) expr.Expr {
	if r, ok := fn(e); ok {
		return r
	}
	children := expr.Children(e)
	if len(children) == 0 {
		return e
	}
	changed := false
	next := make([]expr.Expr, len(children))
	for i, c := range children {
		next[i] = rewriteExpr(c, fn)
		if !expr.Equal(next[i], c) {
			changed = true
		}
	}
	if !changed {
		return e
	}
	return expr.WithChildren(e, next)
}

// structFieldNames resolves the field names of a struct-valued
// expression at plan time. Only the names matter for unnest, so struct
// producers whose dtypes are unknown still resolve when their field
// names follow from the parameters.
func structFieldNames(e expr.Expr, in *schema.Schema) ([]string, bool) {
	switch n := e.Node().(type) {
	case expr.ColNode:
		if f, ok := in.FieldByName(n.Name); ok {
			return dtypeFieldNames(f.DType)
		}
		return nil, false
	case expr.AliasNode:
		return structFieldNames(n.Inner, in)
	case expr.CastNode:
		return dtypeFieldNames(n.To)
	case expr.FunctionNode:
		return functionFieldNames(n, in)
	}
	if dt, err := inferExprDType(e, in); err == nil {
		return dtypeFieldNames(dt)
	}
	return nil, false
}

func dtypeFieldNames(dt dtype.DType) ([]string, bool) {
	st, ok := dt.Arrow().(*arrow.StructType)
	if !ok {
		return nil, false
	}
	names := make([]string, st.NumFields())
	for i, f := range st.Fields() {
		names[i] = f.Name
	}
	return names, true
}

func fieldSeq(n int) []string {
	out := make([]string, max(n, 0))
	for i := range out {
		out[i] = "field_" + strconv.Itoa(i)
	}
	return out
}

func functionFieldNames(n expr.FunctionNode, in *schema.Schema) ([]string, bool) {
	param := func(i int) any {
		if i < len(n.Params) {
			return n.Params[i]
		}
		return nil
	}
	intParam := func(i int) (int, bool) {
		switch v := param(i).(type) {
		case int:
			return v, true
		case int64:
			return int(v), true
		}
		return 0, false
	}
	stringsParam := func(i int) []string {
		s, _ := param(i).([]string)
		return s
	}
	inner := func() ([]string, bool) {
		if len(n.Args) == 0 {
			return nil, false
		}
		return structFieldNames(n.Args[0], in)
	}
	switch n.Name {
	case "struct.rename_fields":
		names := stringsParam(0)
		base, ok := inner()
		if !ok {
			return nil, false
		}
		return names[:min(len(names), len(base))], true
	case "name.prefix_fields", "name.suffix_fields", "name.map_fields":
		base, ok := inner()
		if !ok {
			return nil, false
		}
		out := make([]string, len(base))
		for i, b := range base {
			switch n.Name {
			case "name.prefix_fields":
				s, _ := param(0).(string)
				out[i] = s + b
			case "name.suffix_fields":
				s, _ := param(0).(string)
				out[i] = b + s
			default:
				fn, _ := param(0).(expr.NameMapper)
				if fn == nil {
					return nil, false
				}
				out[i] = fn(b)
			}
		}
		return out, true
	case "struct.with_fields":
		base, ok := inner()
		if !ok {
			return nil, false
		}
		out := append([]string(nil), base...)
		for _, a := range n.Args[1:] {
			name := expr.OutputName(a)
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
		return out, true
	case "str.splitn":
		k, ok := intParam(1)
		return fieldSeq(k), ok
	case "str.split_exact_n":
		k, ok := intParam(1)
		return fieldSeq(k + 1), ok
	case "str.extract_groups":
		pat, _ := param(0).(string)
		re, err := regexp.Compile(pat)
		if err != nil {
			return nil, false
		}
		names := re.SubexpNames()[1:]
		out := make([]string, len(names))
		for i, nm := range names {
			if nm == "" {
				nm = strconv.Itoa(i + 1)
			}
			out[i] = nm
		}
		return out, true
	case "list.to_struct", "arr.to_struct":
		if names := stringsParam(0); names != nil {
			return names, true
		}
		if n.Name == "arr.to_struct" && len(n.Args) > 0 {
			if dt, err := inferExprDType(n.Args[0], in); err == nil {
				if fl, ok := dt.Arrow().(*arrow.FixedSizeListType); ok {
					return fieldSeq(int(fl.Len())), true
				}
			}
		}
		return nil, false
	case "str.json_decode":
		if dt, ok := param(0).(dtype.DType); ok {
			return dtypeFieldNames(dt)
		}
		return nil, false
	}
	if strings.HasPrefix(n.Name, "name.") {
		return inner()
	}
	return nil, false
}

// resolveNameOps turns a top-level name.* rename into an Alias so that
// consumers that only understand aliases (the group-by aggregation
// rewrite) see the final output name.
func resolveNameOps(exprs []expr.Expr) []expr.Expr {
	out := append([]expr.Expr(nil), exprs...)
	for i, e := range exprs {
		n, ok := e.Node().(expr.FunctionNode)
		if !ok || !isPlainRename(n.Name) || len(n.Args) == 0 {
			continue
		}
		inner := n.Args[0]
		for {
			m, ok := inner.Node().(expr.FunctionNode)
			if !ok || !isPlainRename(m.Name) || len(m.Args) == 0 {
				break
			}
			inner = m.Args[0]
		}
		if a, ok := inner.Node().(expr.AliasNode); ok {
			inner = a.Inner
		}
		out[i] = inner.Alias(expr.OutputName(e))
	}
	return out
}

func isPlainRename(name string) bool {
	switch name {
	case "name.keep", "name.prefix", "name.suffix", "name.map",
		"name.to_lowercase", "name.to_uppercase", "name.replace":
		return true
	}
	return false
}
