package expr

import (
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// BatchFunc maps a whole column to a new column. It must not retain or
// release its input; it returns a fresh Series the caller owns.
type BatchFunc func(s *series.Series) (*series.Series, error)

// RollingMapFunc reduces one window (a Series holding the window's
// rows) to a length-1 Series.
type RollingMapFunc func(window *series.Series) (*series.Series, error)

// FoldFunc combines an accumulator column with the next input column.
type FoldFunc func(acc, x *series.Series) (*series.Series, error)

// MapBatches applies fn to the evaluated column. Inside group_by().agg()
// and over() fn sees one group at a time. Mirrors polars'
// Expr.map_batches.
func (e Expr) MapBatches(fn BatchFunc) Expr { return fn1p("map_batches", e, fn) }

// MapElements applies fn to every non-null value of a column whose Go
// value type is In, producing a column of Out. Nulls stay null. It is
// the typed form of polars' Expr.map_elements; see series.MapValues for
// the supported In/Out types.
func MapElements[In, Out series.MapValue](e Expr, fn func(In) Out) Expr {
	batch := func(s *series.Series) (*series.Series, error) {
		return series.MapValues(s, fn)
	}
	return fn1p("map_elements", e, BatchFunc(batch))
}

// MapElementsTo is MapElements with an explicit output dtype for Out
// types that map to more than one dtype (for example int64 into a
// Datetime column).
func MapElementsTo[In, Out series.MapValue](e Expr, fn func(In) Out, out dtype.DType) Expr {
	return MapElements(e, fn).Cast(out)
}
