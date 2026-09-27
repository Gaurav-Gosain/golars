package expr

import (
	"fmt"
	"strings"
)

// FunctionNode applies a named unary/multi-arg function to one or
// more inner expressions. Keeps the AST small while letting us add
// new scalar kernels (abs, sqrt, log, round, floor, ceil, …) without
// a new Node type each time.
//
// The name is the canonical lower-case function name (polars-style).
// The evaluator and optimiser switch on Name to dispatch.
type FunctionNode struct {
	Name string
	Args []Expr
	// Params carries non-Expr scalar arguments (e.g. the decimals arg
	// of round, or the low/high bounds of clip). Encoded as opaque any
	// so the evaluator can type-assert per function.
	Params []any
}

func (FunctionNode) isNode() {}

func (f FunctionNode) String() string {
	args := make([]string, 0, len(f.Args)+len(f.Params))
	for _, a := range f.Args {
		args = append(args, a.String())
	}
	for _, p := range f.Params {
		args = append(args, fmt.Sprintf("%v", p))
	}
	// Render the first arg as the receiver (polars style).
	if len(f.Args) >= 1 {
		rest := append([]string{}, args[1:]...)
		return fmt.Sprintf("%s.%s(%s)", f.Args[0], f.Name, strings.Join(rest, ", "))
	}
	return fmt.Sprintf("%s(%s)", f.Name, strings.Join(args, ", "))
}

// fn1 is shorthand for a unary function expression.
func fn1(name string, e Expr) Expr {
	return Expr{FunctionNode{Name: name, Args: []Expr{e}}}
}

// fn2 is shorthand for a two-arg function expression where arg2 is a
// non-Expr scalar (carried in Params).
func fn1p(name string, e Expr, params ...any) Expr {
	return Expr{FunctionNode{Name: name, Args: []Expr{e}, Params: params}}
}

// ---- unary math ----

// Abs returns |e|, preserving integer dtypes and promoting nothing.
func (e Expr) Abs() Expr { return fn1("abs", e) }

// Sqrt returns √e as a float64 column.
func (e Expr) Sqrt() Expr { return fn1("sqrt", e) }

// Exp returns e**x.
func (e Expr) Exp() Expr { return fn1("exp", e) }

// Log returns natural log.
func (e Expr) Log() Expr { return fn1("log", e) }

// Log2 returns log₂.
func (e Expr) Log2() Expr { return fn1("log2", e) }

// Log10 returns log₁₀.
func (e Expr) Log10() Expr { return fn1("log10", e) }

// Sin / Cos / Tan are trig wrappers.
func (e Expr) Sin() Expr { return fn1("sin", e) }
func (e Expr) Cos() Expr { return fn1("cos", e) }
func (e Expr) Tan() Expr { return fn1("tan", e) }

// Sign returns -1/0/+1 per element, preserving integer dtype.
func (e Expr) Sign() Expr { return fn1("sign", e) }

// Round rounds to the given number of decimal places (0 = integer).
// Integers pass through unchanged.
func (e Expr) Round(decimals int) Expr { return fn1p("round", e, decimals) }

// Floor returns the greatest integer ≤ e (float output).
func (e Expr) Floor() Expr { return fn1("floor", e) }

// Ceil returns the least integer ≥ e (float output).
func (e Expr) Ceil() Expr { return fn1("ceil", e) }

// Clip bounds e to [lo, hi]. Use +/-math.Inf for one-sided.
func (e Expr) Clip(lo, hi float64) Expr { return fn1p("clip", e, lo, hi) }

// Pow raises e to the given power.
func (e Expr) Pow(exponent float64) Expr { return fn1p("pow", e, exponent) }

// ---- null handling ----

// FillNull replaces nulls with the given scalar value. The value is
// wrapped in a literal Expr internally so the planner can fold.
func (e Expr) FillNull(v any) Expr {
	return Expr{FunctionNode{Name: "fill_null", Args: []Expr{e, scalarLit(v)}}}
}

// FillNullExpr replaces nulls with the result of another expression
// (polars allows a full expression here).
func (e Expr) FillNullExpr(other Expr) Expr {
	return Expr{FunctionNode{Name: "fill_null", Args: []Expr{e, other}}}
}

// FillNan replaces NaN values in float columns with v. Integer and
// string columns are returned unchanged (mirrors polars).
func (e Expr) FillNan(v float64) Expr { return fn1p("fill_nan", e, v) }

// Cbrt returns cube root element-wise.
func (e Expr) Cbrt() Expr { return fn1("cbrt", e) }

// Log1p returns log(1+x) element-wise.
func (e Expr) Log1p() Expr { return fn1("log1p", e) }

// Expm1 returns exp(x)-1 element-wise.
func (e Expr) Expm1() Expr { return fn1("expm1", e) }

// Radians converts degrees to radians.
func (e Expr) Radians() Expr { return fn1("radians", e) }

// Degrees converts radians to degrees.
func (e Expr) Degrees() Expr { return fn1("degrees", e) }

// Arccos / Arcsin / Arctan: inverse trig.
func (e Expr) Arccos() Expr { return fn1("arccos", e) }
func (e Expr) Arcsin() Expr { return fn1("arcsin", e) }
func (e Expr) Arctan() Expr { return fn1("arctan", e) }

// Cot returns 1/tan element-wise.
func (e Expr) Cot() Expr { return fn1("cot", e) }

// Sinh / Cosh / Tanh: hyperbolics.
func (e Expr) Sinh() Expr { return fn1("sinh", e) }
func (e Expr) Cosh() Expr { return fn1("cosh", e) }
func (e Expr) Tanh() Expr { return fn1("tanh", e) }

// Arcsinh / Arccosh / Arctanh: inverse hyperbolics.
func (e Expr) Arcsinh() Expr { return fn1("arcsinh", e) }
func (e Expr) Arccosh() Expr { return fn1("arccosh", e) }
func (e Expr) Arctanh() Expr { return fn1("arctanh", e) }

// Arctan2 returns atan2(e, other) element-wise.
func (e Expr) Arctan2(other Expr) Expr {
	return Expr{FunctionNode{Name: "arctan2", Args: []Expr{e, other}}}
}

// Skew returns the sample skewness as a scalar aggregation.
func (e Expr) Skew() Expr { return fn1("skew", e) }

// Kurtosis returns the excess kurtosis as a scalar aggregation.
func (e Expr) Kurtosis() Expr { return fn1("kurtosis", e) }

// Entropy returns Shannon entropy in the given base (natural log by
// default when base=0 or omitted in callers).
func (e Expr) Entropy(base float64) Expr { return fn1p("entropy", e, base) }

// PeakMax / PeakMin return boolean Series marking local extrema.
func (e Expr) PeakMax() Expr { return fn1("peak_max", e) }
func (e Expr) PeakMin() Expr { return fn1("peak_min", e) }

// ApproxNUnique estimates the number of distinct values using HLL.
func (e Expr) ApproxNUnique() Expr { return fn1("approx_n_unique", e) }

// RollingSum / Mean / Min / Max / Std / Var apply a fixed-size window
// reduction. minPeriods=0 defaults to windowSize. Mirrors polars'
// rolling_* Expr methods.
func (e Expr) RollingSum(windowSize, minPeriods int) Expr {
	return fn1p("rolling_sum", e, windowSize, minPeriods)
}
func (e Expr) RollingMean(windowSize, minPeriods int) Expr {
	return fn1p("rolling_mean", e, windowSize, minPeriods)
}
func (e Expr) RollingMin(windowSize, minPeriods int) Expr {
	return fn1p("rolling_min", e, windowSize, minPeriods)
}
func (e Expr) RollingMax(windowSize, minPeriods int) Expr {
	return fn1p("rolling_max", e, windowSize, minPeriods)
}
func (e Expr) RollingStd(windowSize, minPeriods int) Expr {
	return fn1p("rolling_std", e, windowSize, minPeriods)
}
func (e Expr) RollingVar(windowSize, minPeriods int) Expr {
	return fn1p("rolling_var", e, windowSize, minPeriods)
}

// EWMMean / EWMVar / EWMStd return the adjusted exponentially-
// weighted moving statistic with the given alpha. Alpha must be in
// (0, 1]; values outside trigger a runtime error at eval time.
func (e Expr) EWMMean(alpha float64) Expr { return fn1p("ewm_mean", e, alpha) }
func (e Expr) EWMVar(alpha float64) Expr  { return fn1p("ewm_var", e, alpha) }
func (e Expr) EWMStd(alpha float64) Expr  { return fn1p("ewm_std", e, alpha) }

// ForwardFill propagates the last non-null value forward into nulls.
// limit=0 means unlimited; otherwise at most limit consecutive nulls
// are filled before a run is left untouched.
func (e Expr) ForwardFill(limit int) Expr { return fn1p("forward_fill", e, limit) }

// BackwardFill propagates the next non-null value backward into nulls.
func (e Expr) BackwardFill(limit int) Expr { return fn1p("backward_fill", e, limit) }

// ---- transforms ----

// Reverse reverses the order of the column.
func (e Expr) Reverse() Expr { return fn1("reverse", e) }

// Head takes the first n rows of the column (per group when inside
// groupby).
func (e Expr) Head(n int) Expr { return fn1p("head", e, n) }

// Tail takes the last n rows.
func (e Expr) Tail(n int) Expr { return fn1p("tail", e, n) }

// Slice returns [offset, offset+length) per column.
func (e Expr) Slice(offset, length int) Expr {
	return fn1p("slice", e, offset, length)
}

// Shift shifts values by periods positions.
func (e Expr) Shift(periods int) Expr { return fn1p("shift", e, periods) }

// ---- predicates ----

// Between is sugar for (e >= lo) and (e <= hi).
func (e Expr) Between(lo, hi any) Expr {
	return e.Ge(Lit(lo)).And(e.Le(Lit(hi)))
}

// IsIn returns a boolean predicate: true iff e is one of `values`.
// Implemented via an OR-chain of equality; for a handful of values
// this is as fast as a hash lookup, and avoids a new evaluator node.
func (e Expr) IsIn(values ...any) Expr {
	if len(values) == 0 {
		return LitBool(false)
	}
	out := e.Eq(Lit(values[0]))
	for _, v := range values[1:] {
		out = out.Or(e.Eq(scalarLit(v)))
	}
	return out
}

// ---- extra aggs ----

// Median is sugar for Quantile(0.5).
func (e Expr) Median() Expr { return fn1("median", e) }

// Std (sample) standard deviation.
func (e Expr) Std() Expr { return fn1("std", e) }

// Var (sample) variance.
func (e Expr) Var() Expr { return fn1("var", e) }

// Any returns true if any non-null value is truthy.
func (e Expr) Any() Expr { return fn1("any", e) }

// All returns true if every non-null value is truthy (vacuous true
// on empty).
func (e Expr) All() Expr { return fn1("all", e) }

// Product is the multiplicative aggregate.
func (e Expr) Product() Expr { return fn1("product", e) }

// Quantile returns the q-th quantile (linear interpolation).
func (e Expr) Quantile(q float64) Expr { return fn1p("quantile", e, q) }
