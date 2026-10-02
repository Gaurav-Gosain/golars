package golars

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Top-level expression helpers mirroring polars' pl.* functions. The
// frame-level SumHorizontal & co. in golars.go operate on a DataFrame;
// the *Expr variants here build expressions for select/with_columns.

// SumHorizontalExpr is pl.sum_horizontal.
func SumHorizontalExpr(exprs ...Expr) Expr { return expr.SumHorizontal(exprs...) }

// MeanHorizontalExpr is pl.mean_horizontal.
func MeanHorizontalExpr(exprs ...Expr) Expr { return expr.MeanHorizontal(exprs...) }

// MinHorizontalExpr is pl.min_horizontal.
func MinHorizontalExpr(exprs ...Expr) Expr { return expr.MinHorizontal(exprs...) }

// MaxHorizontalExpr is pl.max_horizontal.
func MaxHorizontalExpr(exprs ...Expr) Expr { return expr.MaxHorizontal(exprs...) }

// AllHorizontalExpr is pl.all_horizontal.
func AllHorizontalExpr(exprs ...Expr) Expr { return expr.AllHorizontal(exprs...) }

// AnyHorizontalExpr is pl.any_horizontal.
func AnyHorizontalExpr(exprs ...Expr) Expr { return expr.AnyHorizontal(exprs...) }

// CumSumHorizontal is pl.cum_sum_horizontal.
func CumSumHorizontal(exprs ...Expr) Expr { return expr.CumSumHorizontal(exprs...) }

// Fold is pl.fold.
func Fold(acc Expr, fn expr.FoldFunc, exprs ...Expr) Expr { return expr.Fold(acc, fn, exprs...) }

// Reduce is pl.reduce.
func Reduce(fn expr.FoldFunc, exprs ...Expr) Expr { return expr.Reduce(fn, exprs...) }

// CumFold is pl.cum_fold.
func CumFold(acc Expr, fn expr.FoldFunc, includeInit bool, exprs ...Expr) Expr {
	return expr.CumFold(acc, fn, includeInit, exprs...)
}

// CumReduce is pl.cum_reduce.
func CumReduce(fn expr.FoldFunc, exprs ...Expr) Expr { return expr.CumReduce(fn, exprs...) }

// ArgWhere is pl.arg_where.
func ArgWhere(cond Expr) Expr { return expr.ArgWhere(cond) }

// ArgSortBy is pl.arg_sort_by.
func ArgSortBy(by []Expr, descending []bool, nullsLast bool) Expr {
	return expr.ArgSortBy(by, descending, nullsLast)
}

// Repeat is pl.repeat. Go ints become i64 (polars' Python ints are i32
// here).
func Repeat(value any, n int) Expr { return expr.Repeat(value, n) }

// LinearSpace is pl.linear_space.
func LinearSpace(start, end float64, num int, closed expr.IntervalClosed) Expr {
	return expr.LinearSpace(start, end, num, closed)
}

// Arctan2 is pl.arctan2.
func Arctan2(y, x Expr) Expr { return expr.Arctan2(y, x) }

// Format is pl.format.
func Format(template string, args ...Expr) Expr { return expr.Format(template, args...) }

// Len is pl.len(): the number of rows.
func Len() Expr { return expr.Len() }

// Nth is pl.nth: select columns by position.
func Nth(indices ...int) Expr { return expr.Nth(indices...) }

// Exclude is pl.exclude: every column except the named ones.
func Exclude(names ...string) Expr { return expr.Exclude(names...) }

// AllCols is pl.all() without arguments: every column.
func AllCols() Expr { return expr.AllCols() }

// All is pl.all(name): the boolean AND aggregation of one column. With
// no name it selects every column like pl.all().
func All(name ...string) Expr {
	if len(name) == 0 {
		return expr.AllCols()
	}
	return expr.Col(name[0]).All()
}

// Any is pl.any(name): the boolean OR aggregation of one column.
func Any(name string) Expr { return expr.Col(name).Any() }

// FirstCol is pl.first() without arguments: the first column.
func FirstCol() Expr { return expr.FirstCol() }

// LastCol is pl.last() without arguments: the last column.
func LastCol() Expr { return expr.LastCol() }

// Head is pl.head(name, n).
func Head(name string, n int) Expr { return expr.HeadOf(name, n) }

// Tail is pl.tail(name, n).
func Tail(name string, n int) Expr { return expr.TailOf(name, n) }

// Struct is pl.struct.
func Struct(exprs ...Expr) Expr { return expr.Struct(exprs...) }

// ConcatList is pl.concat_list.
func ConcatList(exprs ...Expr) Expr { return expr.ConcatList(exprs...) }

// ConcatArr is pl.concat_arr.
func ConcatArr(exprs ...Expr) Expr { return expr.ConcatArr(exprs...) }

// IntRanges is pl.int_ranges.
func IntRanges(start, end, step Expr) Expr { return expr.IntRanges(start, end, step) }

// Corr is pl.corr ("pearson" or "spearman").
func Corr(a, b Expr, method string) Expr { return expr.Corr(a, b, method) }

// Cov is pl.cov.
func Cov(a, b Expr, ddof int) Expr { return expr.Cov(a, b, ddof) }

// RollingCorr is pl.rolling_corr.
func RollingCorr(a, b Expr, opts expr.RollingWindow, ddof int) Expr {
	return expr.RollingCorr(a, b, opts, ddof)
}

// RollingCov is pl.rolling_cov.
func RollingCov(a, b Expr, opts expr.RollingWindow, ddof int) Expr {
	return expr.RollingCov(a, b, opts, ddof)
}

// ApproxNUnique is pl.approx_n_unique(name).
func ApproxNUnique(name string) Expr { return expr.Col(name).ApproxNUnique() }

// NUnique is pl.n_unique(name).
func NUnique(name string) Expr { return expr.Col(name).NUnique() }

// Quantile is pl.quantile(name, q) with polars' default (nearest)
// interpolation.
func Quantile(name string, q float64) Expr { return expr.Col(name).Quantile(q) }

// StdDdof is pl.std(name, ddof).
func StdDdof(name string, ddof int) Expr { return expr.Col(name).StdDdof(ddof) }

// VarDdof is pl.var(name, ddof).
func VarDdof(name string, ddof int) Expr { return expr.Col(name).VarDdof(ddof) }

// MapElements is the typed per-value map (polars' map_elements).
func MapElements[In, Out series.MapValue](e Expr, fn func(In) Out) Expr {
	return expr.MapElements(e, fn)
}

// Select evaluates expressions without an input frame, like pl.select.
// Length-1 results broadcast to the longest result.
func Select(ctx context.Context, exprs ...Expr) (*DataFrame, error) {
	dummy, err := series.FromBool("__golars_select__", []bool{false}, nil)
	if err != nil {
		return nil, err
	}
	frame, err := dataframe.New(dummy)
	if err != nil {
		dummy.Release()
		return nil, err
	}
	defer frame.Release()
	cols := make([]*Series, 0, len(exprs))
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}
	height := 0
	for _, e := range exprs {
		s, err := eval.Eval(ctx, eval.Default(), e, frame)
		if err != nil {
			release()
			return nil, err
		}
		name := expr.OutputName(e)
		if s.Name() != name {
			r := s.Rename(name)
			s.Release()
			s = r
		}
		cols = append(cols, s)
		height = max(height, s.Len())
	}
	for i, c := range cols {
		if c.Len() == height {
			continue
		}
		if c.Len() != 1 {
			release()
			return nil, fmt.Errorf("golars: select results have lengths %d and %d", c.Len(), height)
		}
		b, err := c.Broadcast(height)
		if err != nil {
			release()
			return nil, err
		}
		c.Release()
		cols[i] = b
	}
	return dataframe.New(cols...)
}

// ToFrame wraps a Series as a one-column DataFrame (polars'
// Series.to_frame). The Series is shared, not copied.
func ToFrame(s *Series) (*DataFrame, error) { return dataframe.New(s.Clone()) }

// DescribeSeries returns the summary statistics of one Series, like
// polars' Series.describe.
func DescribeSeries(ctx context.Context, s *Series) (*DataFrame, error) {
	df, err := ToFrame(s)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	return df.Describe(ctx)
}
