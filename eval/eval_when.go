package eval

import (
	"context"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
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
