package eval

import (
	"context"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalWhenThen materialises when(pred).then(a).otherwise(b) by
// evaluating every branch then delegating to compute.Where.
func evalWhenThen(ctx context.Context, ec EvalContext, n expr.WhenThenNode, df *dataframe.DataFrame) (*series.Series, error) {
	// Literal and aggregated branches are unit length; they broadcast
	// against the others as in polars.
	ss, err := evalBroadcast(ctx, ec, []expr.Expr{n.Pred, n.Then, n.Otherwise}, df)
	if err != nil {
		return nil, err
	}
	defer releaseAll(ss)
	if err := adoptDynLiterals(ctx, ec, []expr.Expr{n.Then, n.Otherwise}, ss[1:]); err != nil {
		return nil, err
	}
	if err := stringSupertype(ctx, ec, ss[1:]); err != nil {
		return nil, err
	}
	if err := temporalSupertype(ctx, ec, ss[1:]); err != nil {
		return nil, err
	}
	pred, ifTrue, ifFalse := ss[0], ss[1], ss[2]
	// A Null-typed branch (when(...).then(1) with no otherwise) takes
	// the other branch's dtype, as in polars.
	if ifFalse.DType().IsNull() && !ifTrue.DType().IsNull() {
		typed, err := series.FullNull(ifFalse.Name(), ifTrue.DType().Arrow(), ifFalse.Len(), seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		defer typed.Release()
		ifFalse = typed
	} else if ifTrue.DType().IsNull() && !ifFalse.DType().IsNull() {
		typed, err := series.FullNull(ifTrue.Name(), ifFalse.DType().Arrow(), ifTrue.Len(), seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		defer typed.Release()
		ifTrue = typed
	}
	// Promote branches to a common dtype (e.g. int then + float
	// otherwise becomes float). Reuse the same helper the binary
	// evaluator uses.
	lhs, rhs, err := promoteBinary(ctx, ec, ifTrue, ifFalse)
	if err != nil {
		return nil, err
	}
	if lhs != ifTrue {
		defer lhs.Release()
	}
	if rhs != ifFalse {
		defer rhs.Release()
	}
	return compute.Where(ctx, pred, lhs, rhs, kernelOpts(ec)...)
}

// fillNullFrom replaces the nulls of s with the value of fill in the
// same row. Both sides are promoted to a common dtype first.
func fillNullFrom(ctx context.Context, ec EvalContext, s, fill *series.Series) (*series.Series, error) {
	pair := []*series.Series{s.Clone(), fill.Clone()}
	defer releaseAll(pair)
	if err := stringSupertype(ctx, ec, pair); err != nil {
		return nil, err
	}
	if err := temporalSupertype(ctx, ec, pair); err != nil {
		return nil, err
	}
	s, fill = pair[0], pair[1]
	valid, err := s.IsNotNull(seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer valid.Release()
	lhs, rhs, err := promoteBinary(ctx, ec, s, fill)
	if err != nil {
		return nil, err
	}
	if lhs != s {
		defer lhs.Release()
	}
	if rhs != fill {
		defer rhs.Release()
	}
	out, err := compute.Where(ctx, valid, lhs, rhs, kernelOpts(ec)...)
	if err != nil {
		return nil, err
	}
	defer out.Release()
	return out.Rename(s.Name()), nil
}

// stringSupertype casts numeric and bool branches to strings when any
// branch is a string: polars' supertype of str and a number is str
// (when(c).then("a").otherwise(1.5) gives "a" and "1.5"). It replaces
// the entries of ss it casts.
func stringSupertype(ctx context.Context, ec EvalContext, ss []*series.Series) error {
	hasStr := false
	for _, s := range ss {
		if s.DType().IsString() {
			hasStr = true
		}
	}
	if !hasStr {
		return nil
	}
	for i, s := range ss {
		if d := s.DType(); d.IsNumeric() || d.IsBool() {
			c, err := compute.Cast(ctx, s, dtype.String(), kernelOpts(ec)...)
			if err != nil {
				return err
			}
			s.Release()
			ss[i] = c
		}
	}
	return nil
}

// temporalSupertype casts two temporal branches of different dtypes to
// polars' supertype: the coarser unit for two durations or datetimes,
// and the datetime for a date and a datetime. It replaces the entries
// of ss it casts.
func temporalSupertype(ctx context.Context, ec EvalContext, ss []*series.Series) error {
	if len(ss) != 2 {
		return nil
	}
	a, b := ss[0].DType(), ss[1].DType()
	if !a.IsTemporal() || !b.IsTemporal() || a.Equal(b) {
		return nil
	}
	var target dtype.DType
	switch {
	case a.IsDuration() && b.IsDuration():
		target = dtype.Duration(coarserUnit(unitOf(a), unitOf(b)))
	case a.IsDatetime() && b.IsDatetime() && a.TimeZone() == b.TimeZone():
		target = dtype.Datetime(coarserUnit(unitOf(a), unitOf(b)), a.TimeZone())
	case a.IsDatetime() && b.IsDate():
		target = a
	case a.IsDate() && b.IsDatetime():
		target = b
	default:
		return nil
	}
	for i, s := range ss {
		if s.DType().Equal(target) {
			continue
		}
		c, err := compute.Cast(ctx, s, target, kernelOpts(ec)...)
		if err != nil {
			return err
		}
		s.Release()
		ss[i] = c
	}
	return nil
}

func coarserUnit(a, b dtype.TimeUnit) dtype.TimeUnit {
	return min(a, b)
}

func unitOf(d dtype.DType) dtype.TimeUnit {
	u, _ := d.TimeUnit()
	return u
}
