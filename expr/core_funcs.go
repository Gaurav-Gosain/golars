package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// Top-level expression constructors mirroring polars' pl.* functions.

func fnN(name string, args []Expr, params ...any) Expr {
	return Expr{FunctionNode{Name: name, Args: append([]Expr(nil), args...), Params: params}}
}

// ---- horizontal ----

// SumHorizontal sums across expressions row-wise; nulls are skipped
// (all-null rows give 0). Mirrors pl.sum_horizontal.
func SumHorizontal(exprs ...Expr) Expr { return fnN("sum_horizontal", exprs) }

// MeanHorizontal averages across expressions row-wise, skipping nulls
// (all-null rows give null).
func MeanHorizontal(exprs ...Expr) Expr { return fnN("mean_horizontal", exprs) }

// MinHorizontal is the row-wise minimum, skipping nulls.
func MinHorizontal(exprs ...Expr) Expr { return fnN("min_horizontal", exprs) }

// MaxHorizontal is the row-wise maximum, skipping nulls.
func MaxHorizontal(exprs ...Expr) Expr { return fnN("max_horizontal", exprs) }

// AllHorizontal is the row-wise logical AND with Kleene null logic.
func AllHorizontal(exprs ...Expr) Expr { return fnN("all_horizontal", exprs) }

// AnyHorizontal is the row-wise logical OR with Kleene null logic.
func AnyHorizontal(exprs ...Expr) Expr { return fnN("any_horizontal", exprs) }

// CumSumHorizontal returns a struct whose i-th field is the row-wise
// running sum of the first i+1 inputs. Mirrors pl.cum_sum_horizontal.
func CumSumHorizontal(exprs ...Expr) Expr { return fnN("cum_sum_horizontal", exprs) }

// ---- folds ----

// Fold folds fn over exprs starting from acc, row-wise. Mirrors pl.fold.
func Fold(acc Expr, fn FoldFunc, exprs ...Expr) Expr {
	return fnN("fold", append([]Expr{acc}, exprs...), fn)
}

// Reduce is Fold where the first expression is the initial accumulator.
func Reduce(fn FoldFunc, exprs ...Expr) Expr { return fnN("reduce", exprs, fn) }

// CumFold is Fold that keeps every intermediate accumulator, returned as
// a struct column. includeInit adds the initial accumulator as the first
// field. Mirrors pl.cum_fold.
func CumFold(acc Expr, fn FoldFunc, includeInit bool, exprs ...Expr) Expr {
	return fnN("cum_fold", append([]Expr{acc}, exprs...), fn, includeInit)
}

// CumReduce is CumFold where the first expression is the initial
// accumulator. Mirrors pl.cum_reduce.
func CumReduce(fn FoldFunc, exprs ...Expr) Expr { return fnN("cum_reduce", exprs, fn) }

// ---- index helpers ----

// ArgWhere returns the u32 indices where cond is true. Mirrors
// pl.arg_where.
func ArgWhere(cond Expr) Expr { return fnN("arg_true", []Expr{cond}) }

// ArgSortBy returns the u32 permutation that sorts by the given keys.
// descending may hold one flag for all keys or one per key.
func ArgSortBy(by []Expr, descending []bool, nullsLast bool) Expr {
	return fnN("arg_sort_by", by, descending, nullsLast)
}

// ---- constructors ----

// Repeat returns value repeated n times. Mirrors pl.repeat.
func Repeat(value any, n int) Expr { return fnN("repeat", []Expr{Lit(value)}, n) }

// RepeatExpr repeats the (scalar) result of value n times.
func RepeatExpr(value Expr, n int) Expr { return fnN("repeat", []Expr{value}, n) }

// LinearSpace returns num evenly spaced float64 values between start and
// end. closed controls endpoint inclusion ("both" by default). Mirrors
// pl.linear_space.
func LinearSpace(start, end float64, num int, closed IntervalClosed) Expr {
	if closed == "" {
		closed = ClosedBoth
	}
	return fnN("linear_space", nil, start, end, num, string(closed))
}

// Arctan2 returns atan2(y, x) element-wise. Mirrors pl.arctan2.
func Arctan2(y, x Expr) Expr { return fnN("arctan2", []Expr{y, x}) }

// Format builds a string column from a template with "{}" placeholders
// filled by args in order. Nulls propagate. Mirrors pl.format.
func Format(template string, args ...Expr) Expr { return fnN("format", args, template) }

// Len counts the rows of the frame (u32). Mirrors pl.len().
func Len() Expr { return fnN("frame_len", nil) }

// FirstOf returns the first value of the named column (pl.first("a")).
func FirstOf(name string) Expr { return Col(name).First() }

// LastOf returns the last value of the named column (pl.last("a")).
func LastOf(name string) Expr { return Col(name).Last() }

// HeadOf returns the first n values of the named column (pl.head).
func HeadOf(name string, n int) Expr { return Col(name).Head(n) }

// TailOf returns the last n values of the named column (pl.tail).
func TailOf(name string, n int) Expr { return Col(name).Tail(n) }

// Struct combines expressions into one struct column whose fields are
// named after each expression's output name. Mirrors pl.struct.
func Struct(exprs ...Expr) Expr { return fnN("struct", exprs) }

// ConcatList concatenates expressions row-wise into a list column. List
// inputs contribute their elements; scalars contribute one element.
func ConcatList(exprs ...Expr) Expr { return fnN("concat_list", exprs) }

// ConcatArr is ConcatList producing a fixed-size list (polars' Array).
func ConcatArr(exprs ...Expr) Expr { return fnN("concat_arr", exprs) }

// IntRanges returns a list column where row i is range(start[i],
// end[i], step[i]). Mirrors pl.int_ranges.
func IntRanges(start, end, step Expr) Expr {
	return fnN("int_ranges", []Expr{start, end, step})
}

// ---- column sets ----

// AllCols selects every column (pl.all() without arguments). The
// package-level All is the predicate AND-fold; aggregating uses such as
// pl.all("a") are spelled Col("a").All(). Expansion happens when a lazy
// query is built.
func AllCols() Expr { return fnN("cols_all", nil) }

// Nth selects columns by position (negative counts from the end).
func Nth(indices ...int) Expr { return fnN("cols_nth", nil, indices) }

// Exclude selects every column except the named ones.
func Exclude(names ...string) Expr { return fnN("cols_exclude", nil, names) }

// FirstCol selects the first column (pl.first() without arguments).
func FirstCol() Expr { return Nth(0) }

// LastCol selects the last column (pl.last() without arguments).
func LastCol() Expr { return Nth(-1) }

// Exclude drops the named columns from a column-set expression such as
// AllCols(). On a single column it is an identity unless the column is
// excluded, in which case the expression expands to nothing.
func (e Expr) Exclude(names ...string) Expr {
	return fnN("cols_exclude_from", []Expr{e}, names)
}

// ---- statistics ----

// Corr is the correlation of a and b. method is "pearson" (default) or
// "spearman". Rows where either side is null are skipped. Mirrors
// pl.corr.
func Corr(a, b Expr, method string) Expr {
	if method == "" {
		method = "pearson"
	}
	return fnN("corr", []Expr{a, b}, method)
}

// Cov is the covariance of a and b with the given ddof (polars default
// 1). Mirrors pl.cov.
func Cov(a, b Expr, ddof int) Expr { return fnN("cov", []Expr{a, b}, ddof) }

// RollingCorr is the rolling Pearson correlation of a and b.
func RollingCorr(a, b Expr, opts RollingWindow, ddof int) Expr {
	return fnN("rolling_corr", []Expr{a, b}, opts, ddof)
}

// RollingCov is the rolling covariance of a and b.
func RollingCov(a, b Expr, opts RollingWindow, ddof int) Expr {
	return fnN("rolling_cov", []Expr{a, b}, opts, ddof)
}

// LitValues is a literal column holding the given values, cast to dt.
// Nil values become nulls.
func LitValues(dt dtype.DType, values ...any) Expr {
	return fnN("lit_values", nil, dt, values)
}
