package expr

import (
	"github.com/Gaurav-Gosain/golars/dtype"
)

// Core Expr methods that mirror polars' Expr API. Every method builds a
// FunctionNode; the evaluator dispatches on the name. Scalar arguments
// travel in Params, expression arguments in Args.

// ---- positional aggregations ----

// ArgMax returns the index of the largest value as a u32 scalar. Nulls
// are skipped and NaN is ignored unless every value is NaN. An empty or
// all-null input gives null. Mirrors polars' Expr.arg_max.
func (e Expr) ArgMax() Expr { return fn1("arg_max", e) }

// ArgMin returns the index of the smallest value as a u32 scalar.
// Mirrors polars' Expr.arg_min.
func (e Expr) ArgMin() Expr { return fn1("arg_min", e) }

// ArgSort returns the u32 permutation that sorts the column. Nulls go
// first unless nullsLast is set, matching polars' defaults.
func (e Expr) ArgSort(descending, nullsLast bool) Expr {
	return fn1p("arg_sort", e, descending, nullsLast)
}

// ArgTrue returns the u32 indices where a boolean column is true.
func (e Expr) ArgTrue() Expr { return fn1("arg_true", e) }

// ArgUnique returns the u32 index of the first occurrence of every
// distinct value (null counts as a value).
func (e Expr) ArgUnique() Expr { return fn1("arg_unique", e) }

// ---- bitwise ----

// BitwiseAnd reduces the column with bitwise AND (logical AND for
// booleans), ignoring nulls. Mirrors polars' Expr.bitwise_and.
func (e Expr) BitwiseAnd() Expr { return fn1("bitwise_and", e) }

// BitwiseOr reduces the column with bitwise OR.
func (e Expr) BitwiseOr() Expr { return fn1("bitwise_or", e) }

// BitwiseXor reduces the column with bitwise XOR.
func (e Expr) BitwiseXor() Expr { return fn1("bitwise_xor", e) }

// BitwiseCountOnes counts set bits per value (u32 output).
func (e Expr) BitwiseCountOnes() Expr { return fn1("bitwise_count_ones", e) }

// BitwiseCountZeros counts unset bits per value (u32 output).
func (e Expr) BitwiseCountZeros() Expr { return fn1("bitwise_count_zeros", e) }

// BitwiseLeadingOnes counts the leading one bits per value.
func (e Expr) BitwiseLeadingOnes() Expr { return fn1("bitwise_leading_ones", e) }

// BitwiseLeadingZeros counts the leading zero bits per value.
func (e Expr) BitwiseLeadingZeros() Expr { return fn1("bitwise_leading_zeros", e) }

// BitwiseTrailingOnes counts the trailing one bits per value.
func (e Expr) BitwiseTrailingOnes() Expr { return fn1("bitwise_trailing_ones", e) }

// BitwiseTrailingZeros counts the trailing zero bits per value.
func (e Expr) BitwiseTrailingZeros() Expr { return fn1("bitwise_trailing_zeros", e) }

// Xor returns the element-wise exclusive or: logical for booleans,
// bitwise for integers. Mirrors polars' Expr.xor.
func (e Expr) Xor(other Expr) Expr { return fn2("xor", e, other) }

// AndExpr is polars' Expr.and_: logical AND for booleans and bitwise
// AND for integers. The plain And method keeps its boolean contract.
func (e Expr) AndExpr(others ...Expr) Expr {
	out := e
	for _, o := range others {
		out = fn2("and", out, o)
	}
	return out
}

// OrExpr is polars' Expr.or_: logical OR for booleans and bitwise OR
// for integers.
func (e Expr) OrExpr(others ...Expr) Expr {
	out := e
	for _, o := range others {
		out = fn2("or", out, o)
	}
	return out
}

// ---- arithmetic ----

// FloorDiv returns floor(e / other). Integer inputs stay integer (a
// zero divisor gives null); floats floor the float quotient.
func (e Expr) FloorDiv(other Expr) Expr { return fn2("floordiv", e, other) }

// Mod returns the remainder with the sign of the divisor, as Python and
// polars do. Integer division by zero gives null.
func (e Expr) Mod(other Expr) Expr { return fn2("mod", e, other) }

// TrueDiv always divides as float64 (float32 when both sides are f32).
func (e Expr) TrueDiv(other Expr) Expr { return fn2("truediv", e, other) }

// PowExpr raises e to a per-row exponent. Integer base and exponent stay
// integer; otherwise the result is floating point.
func (e Expr) PowExpr(exponent Expr) Expr { return fn2("pow_expr", e, exponent) }

// LogBase returns the logarithm in the given base. Mirrors polars'
// Expr.log(base).
func (e Expr) LogBase(base float64) Expr { return fn1p("log_base", e, base) }

// RoundSigFigs rounds to the given number of significant digits.
// Integer columns pass through unchanged.
func (e Expr) RoundSigFigs(digits int) Expr { return fn1p("round_sig_figs", e, digits) }

// Dot returns the dot product of e and other (sum of products, nulls
// skipped). Mirrors polars' Expr.dot.
func (e Expr) Dot(other Expr) Expr { return fn2("dot", e, other) }

// ---- predicates ----

// IsNaN is true where a float value is NaN. Integers give false, nulls
// stay null. Mirrors polars' Expr.is_nan.
func (e Expr) IsNaN() Expr { return fn1("is_nan", e) }

// IsNotNaN is the negation of IsNaN.
func (e Expr) IsNotNaN() Expr { return fn1("is_not_nan", e) }

// IsFinite is true where a value is neither infinite nor NaN.
func (e Expr) IsFinite() Expr { return fn1("is_finite", e) }

// IsInfinite is true where a float value is +Inf or -Inf.
func (e Expr) IsInfinite() Expr { return fn1("is_infinite", e) }

// IntervalClosed names which bounds of an interval are inclusive.
type IntervalClosed string

// Closed interval options, matching polars' closed= strings.
const (
	ClosedBoth  IntervalClosed = "both"
	ClosedLeft  IntervalClosed = "left"
	ClosedRight IntervalClosed = "right"
	ClosedNone  IntervalClosed = "none"
)

// IsBetween tests lower <op> e <op> upper where closed picks which
// bounds are inclusive. Mirrors polars' Expr.is_between.
func (e Expr) IsBetween(lower, upper Expr, closed IntervalClosed) Expr {
	if closed == "" {
		closed = ClosedBoth
	}
	return Expr{FunctionNode{Name: "is_between", Args: []Expr{e, lower, upper}, Params: []any{string(closed)}}}
}

// IsClose tests |e - other| <= max(relTol * max(|e|, |other|), absTol).
// With nansEqual two NaNs compare close. Mirrors polars' Expr.is_close
// (polars defaults: absTol 0, relTol 1e-9).
func (e Expr) IsClose(other Expr, absTol, relTol float64, nansEqual bool) Expr {
	return Expr{FunctionNode{Name: "is_close", Args: []Expr{e, other}, Params: []any{absTol, relTol, nansEqual}}}
}

// EqMissing is equality where null == null is true and null == value is
// false. Mirrors polars' Expr.eq_missing.
func (e Expr) EqMissing(other Expr) Expr { return fn2("eq_missing", e, other) }

// NeMissing is the negation of EqMissing.
func (e Expr) NeMissing(other Expr) Expr { return fn2("ne_missing", e, other) }

// HasNulls is a boolean scalar: true when the input holds any null.
func (e Expr) HasNulls() Expr { return fn1("has_nulls", e) }

// ---- scalar reductions ----

// Len counts rows including nulls (u32). Mirrors polars' Expr.len.
func (e Expr) Len() Expr { return fn1("len", e) }

// NanMax is max where any NaN makes the result NaN.
func (e Expr) NanMax() Expr { return fn1("nan_max", e) }

// NanMin is min where any NaN makes the result NaN.
func (e Expr) NanMin() Expr { return fn1("nan_min", e) }

// Item returns the single value of a length-1 input and errors on any
// other length. Mirrors polars' Expr.item.
func (e Expr) Item() Expr { return fn1("item", e) }

// Get returns the value at index (negative counts from the end) as a
// scalar. Mirrors polars' Expr.get.
func (e Expr) Get(index int) Expr { return fn1p("get", e, index) }

// IndexOf returns the first index of value (u32), or null when absent.
// A nil value searches for the first null.
func (e Expr) IndexOf(value any) Expr { return fn1p("index_of", e, value) }

// LowerBound returns the minimum representable value of the dtype.
func (e Expr) LowerBound() Expr { return fn1("lower_bound", e) }

// UpperBound returns the maximum representable value of the dtype.
func (e Expr) UpperBound() Expr { return fn1("upper_bound", e) }

// MaxBy returns the value of e at the row where by is largest (first
// such row on ties). Mirrors polars' Expr.max_by.
func (e Expr) MaxBy(by Expr) Expr { return fn2("max_by", e, by) }

// MinBy returns the value of e at the row where by is smallest.
func (e Expr) MinBy(by Expr) Expr { return fn2("min_by", e, by) }

// Implode aggregates the whole column into a single list value.
func (e Expr) Implode() Expr { return fn1("implode", e) }

// QuantileInterpolation names a quantile interpolation strategy.
type QuantileInterpolation string

// Interpolation methods, matching polars' interpolation= strings.
const (
	QuantileNearest  QuantileInterpolation = "nearest"
	QuantileLinear   QuantileInterpolation = "linear"
	QuantileLower    QuantileInterpolation = "lower"
	QuantileHigher   QuantileInterpolation = "higher"
	QuantileMidpoint QuantileInterpolation = "midpoint"
	QuantileEquiprob QuantileInterpolation = "equiprobable"
)

// QuantileWith is Quantile with an explicit interpolation method.
// polars' default for Expr.quantile is nearest.
func (e Expr) QuantileWith(q float64, method QuantileInterpolation) Expr {
	return fn1p("quantile_with", e, q, string(method))
}

// StdDdof is the standard deviation with the given delta degrees of
// freedom (polars' std(ddof)).
func (e Expr) StdDdof(ddof int) Expr { return fn1p("std_ddof", e, ddof) }

// VarDdof is the variance with the given delta degrees of freedom.
func (e Expr) VarDdof(ddof int) Expr { return fn1p("var_ddof", e, ddof) }

// EntropyWith computes the entropy with an explicit normalize flag.
// polars' defaults are base e and normalize true.
func (e Expr) EntropyWith(base float64, normalize bool) Expr {
	return fn1p("entropy_with", e, base, normalize)
}

// ---- length-changing transforms ----

// DropNulls removes null rows.
func (e Expr) DropNulls() Expr { return fn1("drop_nulls", e) }

// DropNans removes NaN rows (float columns only; others pass through).
func (e Expr) DropNans() Expr { return fn1("drop_nans", e) }

// Filter keeps the rows where predicate is true. Inside group_by().agg()
// and over() the predicate is evaluated per group. Mirrors polars'
// Expr.filter.
func (e Expr) Filter(predicate Expr) Expr { return fn2("filter", e, predicate) }

// Gather takes the rows at the given indices (negative indices count
// from the end). Mirrors polars' Expr.gather.
func (e Expr) Gather(indices Expr) Expr { return fn2("gather", e, indices) }

// GatherIdx is Gather with literal indices.
func (e Expr) GatherIdx(indices ...int) Expr {
	vals := make([]int64, len(indices))
	for i, v := range indices {
		vals[i] = int64(v)
	}
	return fn1p("gather_idx", e, vals)
}

// GatherEvery takes every n-th row starting at offset.
func (e Expr) GatherEvery(n, offset int) Expr { return fn1p("gather_every", e, n, offset) }

// Limit is an alias for Head.
func (e Expr) Limit(n int) Expr { return e.Head(n) }

// ExtendConstant appends n copies of value (nil appends nulls).
func (e Expr) ExtendConstant(value any, n int) Expr {
	return fn1p("extend_constant", e, value, n)
}

// Explode flattens a list column into its elements. Non-list inputs
// pass through. Mirrors polars' Expr.explode.
func (e Expr) Explode() Expr { return fn1("explode", e) }

// Flatten is the polars alias of Explode.
func (e Expr) Flatten() Expr { return fn1("explode", e) }

// TopK returns the k largest values, sorted descending (NaN largest,
// nulls excluded unless needed to reach k).
func (e Expr) TopK(k int) Expr { return fn1p("top_k", e, k) }

// BottomK returns the k smallest values, sorted ascending.
func (e Expr) BottomK(k int) Expr { return fn1p("bottom_k", e, k) }

// TopKBy returns the rows of e matching the k largest rows of by.
// reverse flips individual keys (nil means none). Mirrors polars'
// Expr.top_k_by.
func (e Expr) TopKBy(by []Expr, k int, reverse []bool) Expr {
	args := append([]Expr{e}, by...)
	return Expr{FunctionNode{Name: "top_k_by", Args: args, Params: []any{k, reverse}}}
}

// BottomKBy returns the rows of e matching the k smallest rows of by.
func (e Expr) BottomKBy(by []Expr, k int, reverse []bool) Expr {
	args := append([]Expr{e}, by...)
	return Expr{FunctionNode{Name: "bottom_k_by", Args: args, Params: []any{k, reverse}}}
}

// SortByOptions tunes SortWith and SortBy. The zero value is ascending
// with nulls first, which is polars' default.
type SortByOptions struct {
	Descending    []bool
	NullsLast     bool
	MaintainOrder bool
}

// SortWith sorts the column with explicit null placement.
func (e Expr) SortWith(descending, nullsLast bool) Expr {
	return fn1p("sort_with", e, descending, nullsLast)
}

// SortBy sorts e by one or more key expressions. descending may hold
// one flag for all keys or one per key. The sort is stable. Mirrors
// polars' Expr.sort_by.
func (e Expr) SortBy(by []Expr, opts SortByOptions) Expr {
	args := append([]Expr{e}, by...)
	return Expr{FunctionNode{Name: "sort_by", Args: args, Params: []any{opts.Descending, opts.NullsLast}}}
}

// Mode returns the most frequent value(s). With ties the order is
// unspecified, as in polars.
func (e Expr) Mode() Expr { return fn1("mode", e) }

// UniqueCounts returns the count (u32) of every distinct value in order
// of first appearance.
func (e Expr) UniqueCounts() Expr { return fn1("unique_counts", e) }

// ValueCounts returns a struct column {value, count} with one row per
// distinct value. sort orders by descending count; normalize replaces
// the count with a proportion field. Mirrors polars' Expr.value_counts.
func (e Expr) ValueCounts(sort, normalize bool) Expr {
	return fn1p("value_counts", e, sort, normalize)
}

// Rle run-length encodes the column into a struct {len u32, value}.
func (e Expr) Rle() Expr { return fn1("rle", e) }

// RleID assigns a u32 run id that increments whenever the value changes.
func (e Expr) RleID() Expr { return fn1("rle_id", e) }

// RepeatBy repeats each value by the matching count in by, producing a
// list column. A null count gives a null list.
func (e Expr) RepeatBy(by Expr) Expr { return fn2("repeat_by", e, by) }

// Reshape reshapes the column. One dimension flattens; two dimensions
// produce a fixed-size list (polars' Array). A -1 dimension is inferred.
func (e Expr) Reshape(dims ...int) Expr { return fn1p("reshape", e, dims) }

// InterpolateMethod picks linear or nearest interpolation.
type InterpolateMethod string

// Interpolation methods, matching polars' method= strings.
const (
	InterpolateLinear  InterpolateMethod = "linear"
	InterpolateNearest InterpolateMethod = "nearest"
)

// Interpolate fills interior nulls. Linear interpolation outputs
// float64 (float32 stays float32); nearest keeps the dtype. Leading and
// trailing nulls stay null.
func (e Expr) Interpolate(method InterpolateMethod) Expr {
	if method == "" {
		method = InterpolateLinear
	}
	return fn1p("interpolate", e, string(method))
}

// InterpolateBy fills nulls by linear interpolation against another
// column (typically a timestamp or position).
func (e Expr) InterpolateBy(by Expr) Expr { return fn2("interpolate_by", e, by) }

// SearchSortedSide selects which insertion point SearchSorted returns.
type SearchSortedSide string

// SearchSorted sides, matching polars' side= strings.
const (
	SideAny   SearchSortedSide = "any"
	SideLeft  SearchSortedSide = "left"
	SideRight SearchSortedSide = "right"
)

// SearchSorted returns the insertion index (u32) of each value of
// element into the sorted column e.
func (e Expr) SearchSorted(element Expr, side SearchSortedSide) Expr {
	if side == "" {
		side = SideAny
	}
	return Expr{FunctionNode{Name: "search_sorted", Args: []Expr{e, element}, Params: []any{string(side)}}}
}

// Sample draws n rows (fraction is ignored when n >= 0). seed makes the
// draw reproducible; polars' RNG cannot be reproduced, only the
// distribution.
func (e Expr) Sample(n int, withReplacement, shuffle bool, seed uint64) Expr {
	return fn1p("sample", e, n, withReplacement, shuffle, seed)
}

// SampleFrac draws a fraction of the rows.
func (e Expr) SampleFrac(fraction float64, withReplacement, shuffle bool, seed uint64) Expr {
	return fn1p("sample_frac", e, fraction, withReplacement, shuffle, seed)
}

// Shuffle permutes the rows with the given seed.
func (e Expr) Shuffle(seed uint64) Expr { return fn1p("shuffle", e, seed) }

// CumulativeEval evaluates inner (written against Element()) on every
// growing prefix of the column: row i sees rows 0..i. Rows with fewer
// than minSamples values give null. Mirrors polars' cumulative_eval.
func (e Expr) CumulativeEval(inner Expr, minSamples int) Expr {
	return fn1p("cumulative_eval", e, inner, minSamples)
}

// ---- replace / bin ----

// Replace maps old[i] to new[i] and keeps every other value. The output
// keeps the input dtype. A nil in old matches nulls. Mirrors polars'
// Expr.replace.
func (e Expr) Replace(old, new []any) Expr { return fn1p("replace", e, old, new) }

// ReplaceStrictOptions tunes ReplaceStrict.
type ReplaceStrictOptions struct {
	// Default supplies values for unmapped rows. When nil, any unmapped
	// non-null value is an error.
	Default *Expr
	// ReturnDType casts the result. Zero value infers from new.
	ReturnDType dtype.DType
}

// ReplaceStrict maps old[i] to new[i]; the output dtype follows new
// (or ReturnDType). Unmapped values take Default, or error when no
// default is given. Mirrors polars' Expr.replace_strict.
func (e Expr) ReplaceStrict(old, new []any, opts ReplaceStrictOptions) Expr {
	args := []Expr{e}
	hasDefault := opts.Default != nil
	if hasDefault {
		args = append(args, *opts.Default)
	}
	return Expr{FunctionNode{Name: "replace_strict", Args: args, Params: []any{old, new, hasDefault, opts.ReturnDType}}}
}

// CutOptions tunes Cut and QCut.
type CutOptions struct {
	// Labels names the bins; len must be breaks+1.
	Labels []string
	// LeftClosed makes intervals [a, b) instead of (a, b].
	LeftClosed bool
	// IncludeBreaks returns a struct {breakpoint, category}.
	IncludeBreaks bool
	// AllowDuplicates drops duplicate quantile breaks instead of erroring
	// (QCut only).
	AllowDuplicates bool
}

// Cut bins values into intervals defined by breaks. The category is a
// string column (polars returns Categorical with the same labels).
func (e Expr) Cut(breaks []float64, opts CutOptions) Expr { return fn1p("cut", e, breaks, opts) }

// QCut bins values into quantile intervals at the given probabilities.
func (e Expr) QCut(quantiles []float64, opts CutOptions) Expr {
	return fn1p("qcut", e, quantiles, opts)
}

// QCutN bins values into n equal-probability intervals.
func (e Expr) QCutN(n int, opts CutOptions) Expr { return fn1p("qcut_n", e, n, opts) }

// HistOptions tunes Hist.
type HistOptions struct {
	// Bins are explicit bin edges; when empty BinCount equal-width bins
	// span the data range (polars' default is 10).
	Bins     []float64
	BinCount int
	// IncludeBreakpoint and IncludeCategory switch the output to a struct.
	IncludeBreakpoint bool
	IncludeCategory   bool
}

// Hist counts values per bin (u32). Mirrors polars' Expr.hist.
func (e Expr) Hist(opts HistOptions) Expr { return fn1p("hist", e, opts) }

// ---- dtype plumbing ----

// Reinterpret reinterprets 64-bit integers as signed or unsigned.
func (e Expr) Reinterpret(signed bool) Expr { return fn1p("reinterpret", e, signed) }

// ToPhysical returns the physical representation (dates as i32,
// datetimes and durations as i64, other types unchanged).
func (e Expr) ToPhysical() Expr { return fn1("to_physical", e) }

// ShrinkDtype is a no-op on expressions in polars 1.39 (deprecated);
// kept for API parity. Use series.Series.ShrinkDtype for the real
// shrinking.
func (e Expr) ShrinkDtype() Expr { return e }

// SetSorted marks the column as sorted. golars does not track sortedness
// flags, so this is an identity.
func (e Expr) SetSorted(descending bool) Expr { return e }

// Rechunk is an identity kept for API parity (evaluation always yields a
// contiguous column).
func (e Expr) Rechunk() Expr { return e }

// Pipe applies fn to e, for chaining helper functions fluently.
func (e Expr) Pipe(fn func(Expr) Expr) Expr { return fn(e) }

// ---- rolling ----

// RollingWindow tunes the rolling window methods below.
type RollingWindow struct {
	WindowSize int
	// MinSamples is the minimum number of non-null values per window;
	// 0 means WindowSize.
	MinSamples int
	// Center aligns the window on the row instead of ending at it.
	Center bool
}

// RollingMedian is the rolling median of each window.
func (e Expr) RollingMedian(opts RollingWindow) Expr { return fn1p("rolling_median", e, opts) }

// RollingQuantile is the rolling quantile with the given interpolation
// (nearest by default).
func (e Expr) RollingQuantile(q float64, method QuantileInterpolation, opts RollingWindow) Expr {
	if method == "" {
		method = QuantileNearest
	}
	return fn1p("rolling_quantile", e, opts, q, string(method))
}

// RollingSkew is the rolling skewness (biased unless bias is false).
func (e Expr) RollingSkew(bias bool, opts RollingWindow) Expr {
	return fn1p("rolling_skew", e, opts, bias)
}

// RollingKurtosis is the rolling kurtosis. fisher subtracts 3.
func (e Expr) RollingKurtosis(fisher, bias bool, opts RollingWindow) Expr {
	return fn1p("rolling_kurtosis", e, opts, fisher, bias)
}

// RollingRank ranks the last value of each window within the window.
// method follows Rank ("average" gives f64, others u32).
func (e Expr) RollingRank(method string, opts RollingWindow) Expr {
	if method == "" {
		method = "average"
	}
	return fn1p("rolling_rank", e, opts, method)
}

// RollingMap applies fn to every window (a Series of the window's rows)
// and collects the returned scalar. fn must return a length-1 Series.
func (e Expr) RollingMap(fn RollingMapFunc, opts RollingWindow) Expr {
	return fn1p("rolling_map", e, opts, fn)
}

// ---- helpers ----

// RankWith ranks with an explicit direction. method is "average" (f64
// output), "min", "max", "dense" or "ordinal" (u32 output).
func (e Expr) RankWith(method string, descending bool) Expr {
	return fn1p("rank", e, method, descending)
}

// fn2 is shorthand for a two-expression function.
func fn2(name string, a, b Expr) Expr {
	return Expr{FunctionNode{Name: name, Args: []Expr{a, b}}}
}
