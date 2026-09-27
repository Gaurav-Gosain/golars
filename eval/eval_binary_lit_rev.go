package eval

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// revArithLit computes lit - col or lit / col without broadcasting the
// literal. The compute scalar kernels only take the literal on the
// right, so `1 - l_discount` (TPC-H Q1, Q3, Q5, Q10, ...) used to build
// a full column of ones first. Covered: f64 columns with a numeric
// literal for both operators, and i64 columns with an i64 literal for
// subtraction (wrapping, like the column kernel). ok is false for any
// other shape.
// litName is the name the generic path gives these results (the left
// operand, a literal, names the output).
const litName = "literal"

func revArithLit(col *series.Series, lit any, op compareOp, ec EvalContext) (*series.Series, bool, error) {
	if col.NumChunks() != 1 || (op != opSub && op != opDiv) {
		return nil, false, nil
	}
	mem := ec.Alloc
	switch a := col.Chunk(0).(type) {
	case *array.Float64:
		var l float64
		switch v := lit.(type) {
		case float64:
			l = v
		case int64:
			l = float64(v)
		default:
			return nil, false, nil
		}
		vals := a.Float64Values()
		fill := func(out []float64) {
			if op == opSub {
				for i, x := range vals {
					out[i] = l - x
				}
				return
			}
			for i, x := range vals {
				out[i] = l / x
			}
		}
		s, err := series.BuildFloat64DirectWithValidity(litName, len(vals), mem, fill,
			series.CopyValidityBitmap(a, mem), a.NullN())
		return s, true, err
	case *array.Int64:
		l, ok := lit.(int64)
		if !ok || op != opSub {
			return nil, false, nil
		}
		vals := a.Int64Values()
		s, err := series.BuildInt64DirectWithValidity(litName, len(vals), mem, func(out []int64) {
			for i, x := range vals {
				out[i] = l - x
			}
		}, series.CopyValidityBitmap(a, mem), a.NullN())
		return s, true, err
	}
	return nil, false, nil
}

// litLeftArith evaluates lit - col or lit / col for an already evaluated
// column: the direct loop when revArithLit covers the shape, otherwise
// the generic operand path with the literal as a one-row operand, so the
// column is not evaluated a second time.
func litLeftArith(ctx context.Context, ec EvalContext, n expr.BinaryNode, col *series.Series, lit any, op compareOp, df *dataframe.DataFrame) (*series.Series, error) {
	if out, ok, err := revArithLit(col, lit, op, ec); ok {
		return out, err
	}
	left, err := evalScalar(ctx, ec, n.Left, df)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	return evalBinaryOperands(ctx, ec, n, left, col)
}
