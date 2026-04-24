package transpile

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/expr"
)

// renderExpr turns an expr.Node back into Go source that reconstructs
// the same expression using the public expr package builders. Only
// node shapes produced by script/exprparse need to be handled; anything
// else returns a commented placeholder so the generated program stays
// syntactically valid and easy to edit.
func renderExpr(n expr.Node) string {
	switch v := n.(type) {
	case expr.ColNode:
		return fmt.Sprintf("expr.Col(%q)", v.Name)
	case expr.LitNode:
		return renderLit(v)
	case expr.BinaryNode:
		return renderBinary(v)
	case expr.UnaryNode:
		return renderUnary(v)
	case expr.AggNode:
		return fmt.Sprintf("%s.%s()", renderExpr(v.Inner.Node()), aggName(v.Op))
	case expr.AliasNode:
		return fmt.Sprintf("%s.Alias(%q)", renderExpr(v.Inner.Node()), v.Name)
	case expr.CastNode:
		return fmt.Sprintf("%s.Cast(%s)", renderExpr(v.Inner.Node()), dtypeCtor(v.To.String()))
	case expr.IsNullNode:
		method := "IsNull"
		if v.Negate {
			method = "IsNotNull"
		}
		return fmt.Sprintf("%s.%s()", renderExpr(v.Inner.Node()), method)
	case expr.FunctionNode:
		return renderFunction(v)
	}
	return fmt.Sprintf("/* unsupported node %T */ expr.Lit(nil)", n)
}

func renderLit(n expr.LitNode) string {
	switch v := n.Value.(type) {
	case int64:
		return fmt.Sprintf("expr.LitInt64(%d)", v)
	case int:
		return fmt.Sprintf("expr.LitInt64(%d)", v)
	case float64:
		return fmt.Sprintf("expr.LitFloat64(%s)", strconv.FormatFloat(v, 'g', -1, 64))
	case bool:
		return fmt.Sprintf("expr.LitBool(%t)", v)
	case string:
		return fmt.Sprintf("expr.LitString(%q)", v)
	case nil:
		return "expr.LitNull(dtype.Null())"
	}
	return fmt.Sprintf("/* lit %T */ expr.Lit(nil)", n.Value)
}

func renderBinary(b expr.BinaryNode) string {
	left := renderExpr(b.Left.Node())
	right := renderExpr(b.Right.Node())
	method := binaryMethod(b.Op)
	return fmt.Sprintf("%s.%s(%s)", left, method, right)
}

func renderUnary(u expr.UnaryNode) string {
	inner := renderExpr(u.Arg.Node())
	switch u.Op {
	case expr.OpNot:
		return fmt.Sprintf("%s.Not()", inner)
	case expr.OpNeg:
		return fmt.Sprintf("%s.Neg()", inner)
	}
	return fmt.Sprintf("/* unary %v */ %s", u.Op, inner)
}

func binaryMethod(op expr.BinaryOp) string {
	switch op {
	case expr.OpAdd:
		return "Add"
	case expr.OpSub:
		return "Sub"
	case expr.OpMul:
		return "Mul"
	case expr.OpDiv:
		return "Div"
	case expr.OpEq:
		return "Eq"
	case expr.OpNe:
		return "Ne"
	case expr.OpLt:
		return "Lt"
	case expr.OpLe:
		return "Le"
	case expr.OpGt:
		return "Gt"
	case expr.OpGe:
		return "Ge"
	case expr.OpAnd:
		return "And"
	case expr.OpOr:
		return "Or"
	}
	return "Add"
}

func aggName(op expr.AggOp) string {
	switch op {
	case expr.AggSum:
		return "Sum"
	case expr.AggMean:
		return "Mean"
	case expr.AggMin:
		return "Min"
	case expr.AggMax:
		return "Max"
	case expr.AggCount:
		return "Count"
	case expr.AggNullCount:
		return "NullCount"
	case expr.AggFirst:
		return "First"
	case expr.AggLast:
		return "Last"
	}
	return "Sum"
}

func renderFunction(f expr.FunctionNode) string {
	// Str namespace helpers: `str.contains`, `str.upper`, etc.
	if strings.HasPrefix(f.Name, "str.") {
		return renderStrCall(f)
	}
	// No-arg scalar helpers.
	switch f.Name {
	case "abs", "sqrt", "exp", "log", "log10", "log2", "sign", "floor",
		"ceil", "skew", "kurtosis", "approx_n_unique", "n_unique",
		"median", "std", "var", "product", "peak_max", "peak_min",
		"reverse", "cum_sum", "cum_min", "cum_max", "cum_prod",
		"cum_count":
		if len(f.Args) != 1 {
			break
		}
		return fmt.Sprintf("%s.%s()", renderExpr(f.Args[0].Node()), pascal(f.Name))
	case "round", "shift", "diff", "head", "tail":
		if len(f.Args) != 1 {
			break
		}
		p := "0"
		if len(f.Params) >= 1 {
			p = renderParam(f.Params[0])
		}
		return fmt.Sprintf("%s.%s(%s)", renderExpr(f.Args[0].Node()), pascal(f.Name), p)
	case "quantile":
		if len(f.Args) != 1 || len(f.Params) < 1 {
			break
		}
		return fmt.Sprintf("%s.Quantile(%s)", renderExpr(f.Args[0].Node()), renderParam(f.Params[0]))
	case "rolling_sum", "rolling_mean", "rolling_min", "rolling_max",
		"rolling_std", "rolling_var":
		if len(f.Args) != 1 || len(f.Params) < 2 {
			break
		}
		return fmt.Sprintf("%s.%s(%s, %s)",
			renderExpr(f.Args[0].Node()), pascal(f.Name),
			renderParam(f.Params[0]), renderParam(f.Params[1]),
		)
	case "ewm_mean", "ewm_var", "ewm_std":
		if len(f.Args) != 1 || len(f.Params) < 1 {
			break
		}
		method := map[string]string{
			"ewm_mean": "EWMMean",
			"ewm_var":  "EWMVar",
			"ewm_std":  "EWMStd",
		}[f.Name]
		return fmt.Sprintf("%s.%s(%s)",
			renderExpr(f.Args[0].Node()), method, renderParam(f.Params[0]),
		)
	case "forward_fill", "backward_fill":
		if len(f.Args) != 1 {
			break
		}
		p := "0"
		if len(f.Params) >= 1 {
			p = renderParam(f.Params[0])
		}
		return fmt.Sprintf("%s.%s(%s)", renderExpr(f.Args[0].Node()), pascal(f.Name), p)
	case "fill_null":
		if len(f.Args) < 1 {
			break
		}
		// `fill_null(x)` stores the fill value as Args[1] when it's an
		// expression and as Params[0] when it was a numeric/string
		// literal in the grammar. Handle both.
		if len(f.Args) >= 2 {
			return fmt.Sprintf("%s.FillNullExpr(%s)",
				renderExpr(f.Args[0].Node()), renderExpr(f.Args[1].Node()))
		}
		if len(f.Params) >= 1 {
			return fmt.Sprintf("%s.FillNull(%s)",
				renderExpr(f.Args[0].Node()), renderParam(f.Params[0]))
		}
	case "between":
		if len(f.Args) != 1 || len(f.Params) < 2 {
			break
		}
		return fmt.Sprintf("%s.Between(%s, %s)",
			renderExpr(f.Args[0].Node()), renderParam(f.Params[0]), renderParam(f.Params[1]),
		)
	case "coalesce":
		parts := make([]string, len(f.Args))
		for i, a := range f.Args {
			parts[i] = renderExpr(a.Node())
		}
		return fmt.Sprintf("expr.Coalesce(%s)", strings.Join(parts, ", "))
	}
	return fmt.Sprintf("/* TODO fn %q */ %s", f.Name, renderExpr(f.Args[0].Node()))
}

func renderStrCall(f expr.FunctionNode) string {
	method := strings.TrimPrefix(f.Name, "str.")
	if len(f.Args) == 0 {
		return fmt.Sprintf("/* str.%s missing arg */", method)
	}
	base := renderExpr(f.Args[0].Node())
	params := make([]string, 0, len(f.Params))
	for _, p := range f.Params {
		params = append(params, renderParam(p))
	}
	// Map the serialised method name onto the Str() facade. Keep the
	// table small; anything unmapped falls through to a TODO comment.
	mapping := map[string]string{
		"to_upper": "ToUpper", "to_lower": "ToLower",
		"trim":                "Trim",
		"contains":            "Contains",
		"contains_regex":      "ContainsRegex",
		"starts_with":         "StartsWith",
		"ends_with":           "EndsWith",
		"like":                "Like",
		"not_like":            "NotLike",
		"replace":             "Replace",
		"replace_all":         "ReplaceAll",
		"strip_prefix":        "StripPrefix",
		"strip_suffix":        "StripSuffix",
		"len_bytes":           "LenBytes",
		"len_chars":           "LenChars",
		"slice":               "Slice",
		"head":                "Head",
		"tail":                "Tail",
		"find":                "Find",
		"count_matches":       "CountMatches",
		"split_exact":         "SplitExact",
	}
	mapped, ok := mapping[method]
	if !ok {
		return fmt.Sprintf("/* str.%s TODO */ %s", method, base)
	}
	return fmt.Sprintf("%s.Str().%s(%s)", base, mapped, strings.Join(params, ", "))
}

func renderParam(p any) string {
	switch v := p.(type) {
	case string:
		return strconv.Quote(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return fmt.Sprintf("%v", p)
}

func dtypeCtor(name string) string {
	switch name {
	case "i64":
		return "dtype.Int64()"
	case "i32":
		return "dtype.Int32()"
	case "f64":
		return "dtype.Float64()"
	case "f32":
		return "dtype.Float32()"
	case "bool":
		return "dtype.Bool()"
	case "str":
		return "dtype.String()"
	}
	return fmt.Sprintf("/* dtype %q */ dtype.Null()", name)
}

// pascal converts snake_case to PascalCase for Go method names.
func pascal(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		b.WriteString(p[1:])
	}
	return b.String()
}
