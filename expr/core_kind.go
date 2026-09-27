package expr

import "strings"

// scalarFuncs names FunctionNodes that reduce their input to a single
// value (one row per group inside group_by().agg()).
var scalarFuncs = map[string]bool{
	"median": true, "std": true, "var": true, "any": true, "all": true,
	"product": true, "quantile": true, "n_unique": true, "skew": true,
	"kurtosis": true, "entropy": true, "approx_n_unique": true,
	"arg_max": true, "arg_min": true, "null_count": true, "len": true,
	"first": true, "last": true, "max_by": true, "min_by": true,
	"nan_max": true, "nan_min": true, "item": true, "has_nulls": true,
	"index_of": true, "implode": true, "lower_bound": true,
	"upper_bound": true, "dot": true, "corr": true, "cov": true,
	"get": true, "count": true, "sum": true, "mean": true, "min": true,
	"max": true, "frame_len": true, "bitwise_and": true,
	"bitwise_or": true, "bitwise_xor": true,
}

// elementwiseFuncs names FunctionNodes whose output row i depends only on
// input row i of each argument. They are safe to hoist out of a group-by
// and to evaluate on any row subset.
var elementwiseFuncs = map[string]bool{
	"abs": true, "sqrt": true, "exp": true, "log": true, "log2": true,
	"log10": true, "sin": true, "cos": true, "tan": true, "sign": true,
	"floor": true, "ceil": true, "round": true, "pow": true, "clip": true,
	"fill_nan": true, "cbrt": true, "log1p": true,
	"expm1": true, "radians": true, "degrees": true, "arccos": true,
	"arcsin": true, "arctan": true, "cot": true, "sinh": true, "cosh": true,
	"tanh": true, "arcsinh": true, "arccosh": true, "arctanh": true,
	"arctan2": true, "hash": true, "is_nan": true, "is_not_nan": true,
	"is_finite": true, "is_infinite": true, "is_between": true,
	"is_close": true, "eq_missing": true, "ne_missing": true,
	"floordiv": true, "mod": true, "truediv": true, "xor": true,
	"bitwise_count_ones": true, "bitwise_count_zeros": true,
	"bitwise_leading_ones": true, "bitwise_leading_zeros": true,
	"bitwise_trailing_ones": true, "bitwise_trailing_zeros": true,
	"replace": true, "replace_strict": true, "round_sig_figs": true,
	"cut": true, "reinterpret": true, "to_physical": true, "coalesce": true,
	"concat_str": true, "log_base": true, "is_in": true, "pow_expr": true,
	"sum_horizontal": true, "min_horizontal": true, "max_horizontal": true,
	"mean_horizontal": true, "all_horizontal": true, "any_horizontal": true,
	"format": true, "struct": true, "concat_list": true, "concat_arr": true,
	"map_elements": true, "set_sorted": true, "rechunk": true,
	"fill_null": true,
}

// ReturnsScalar reports whether e reduces to a single value, which is
// how polars decides between a scalar column and a list column in
// group_by().agg(). Literals count as scalar. Unknown functions are
// treated as non-scalar.
func ReturnsScalar(e Expr) bool {
	switch n := e.node.(type) {
	case AggNode:
		return true
	case LitNode:
		return true
	case AliasNode:
		return ReturnsScalar(n.Inner)
	case CastNode:
		return ReturnsScalar(n.Inner)
	case UnaryNode:
		return ReturnsScalar(n.Arg)
	case IsNullNode:
		return ReturnsScalar(n.Inner)
	case BinaryNode:
		return ReturnsScalar(n.Left) && ReturnsScalar(n.Right)
	case WhenThenNode:
		return ReturnsScalar(n.Pred) && ReturnsScalar(n.Then) && ReturnsScalar(n.Otherwise)
	case FunctionNode:
		if scalarFuncs[n.Name] {
			return true
		}
		if isElementwiseName(n.Name) {
			for _, a := range n.Args {
				if !ReturnsScalar(a) {
					return false
				}
			}
			return len(n.Args) > 0
		}
	}
	return false
}

// IsLiteralOnly reports whether every leaf of e is a literal, so its
// value does not depend on the frame: it reads no column and no frame
// height (len(), int_range and selectors are function nodes without
// arguments and do not count). polars evaluates such an expression
// once, as a unit-length value, in every context.
func IsLiteralOnly(e Expr) bool {
	ok := true
	Walk(e, func(x Expr) bool {
		switch n := x.node.(type) {
		case LitNode, AliasNode, CastNode, UnaryNode, IsNullNode, BinaryNode,
			WhenThenNode, AggNode:
			return true
		case FunctionNode:
			if len(n.Args) == 0 {
				ok = false
				return false
			}
			for _, p := range n.Params {
				if pe, isExpr := p.(Expr); isExpr && pe.node != nil && !IsLiteralOnly(pe) {
					ok = false
					return false
				}
			}
			return true
		}
		ok = false
		return false
	})
	return ok
}

// IsElementwise reports whether every node in e maps input rows to
// output rows one to one, independent of the other rows. Such
// expressions can be computed on the whole frame before grouping, and
// the lazy optimizer only moves a filter or a slice below a projection
// whose expressions are all elementwise. Unknown functions are treated
// as not elementwise, which is always safe.
func IsElementwise(e Expr) bool {
	ok := true
	Walk(e, func(x Expr) bool {
		switch n := x.node.(type) {
		case ColNode, LitNode, AliasNode, CastNode, UnaryNode, IsNullNode,
			BinaryNode, WhenThenNode:
			return true
		case FunctionNode:
			if isElementwiseName(n.Name) && paramsElementwise(n.Params) {
				return true
			}
		}
		ok = false
		return false
	})
	return ok
}

// paramsElementwise checks expressions carried in Params, which Walk
// does not visit.
func paramsElementwise(params []any) bool {
	for _, p := range params {
		if pe, ok := p.(Expr); ok && pe.node != nil && !IsElementwise(pe) {
			return false
		}
	}
	return true
}

// nonElementwiseNS names namespace members that change the row count or
// read the whole column. Every other member of the namespaces listed in
// elementwiseNS works row by row.
var nonElementwiseNS = map[string]bool{
	"str.join": true, "str.concat": true, "str.explode": true,
	"list.explode": true, "arr.explode": true,
	"cat.get_categories": true, "struct.unnest": true,
}

var elementwiseNS = []string{"str.", "dt.", "list.", "arr.", "bin.", "cat.", "struct.", "name."}

// isElementwiseName reports whether a function name is row-wise.
func isElementwiseName(name string) bool {
	if elementwiseFuncs[name] {
		return true
	}
	if nonElementwiseNS[name] {
		return false
	}
	for _, p := range elementwiseNS {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// fixedOutputNames lists functions whose output column name does not
// follow the first referenced column, matching polars.
var fixedOutputNames = map[string]string{
	"cum_sum_horizontal": "cum_sum",
	"cum_fold":           "cum_fold",
	"cum_reduce":         "cum_reduce",
	"repeat":             "repeat",
	"frame_len":          "len",
	"linear_space":       "literal",
	"lit_values":         "literal",
}

// functionOutputName reports the polars output name of a top-level
// function node when it is not derived from the first column.
func functionOutputName(f FunctionNode) (string, bool) {
	if name, ok := fixedOutputNames[f.Name]; ok {
		return name, true
	}
	if f.Name == "fold" && len(f.Args) > 0 {
		return OutputName(f.Args[0]), true
	}
	return "", false
}
