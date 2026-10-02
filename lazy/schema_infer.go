package lazy

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/schema"
)

// inferByEmptyEval returns the dtype e produces by evaluating it against
// a zero-row frame of in. It covers every expression the evaluator
// supports (string, list, temporal and other function nodes) without a
// per-function inference table. It returns the Null dtype when the
// expression cannot be evaluated statically.
func inferByEmptyEval(e expr.Expr, in *schema.Schema) (dt dtype.DType) {
	dt = dtype.Null()
	if in == nil {
		return dt
	}
	df, err := emptyFrame(in)
	if err != nil {
		return dt
	}
	defer df.Release()
	defer func() {
		if r := recover(); r != nil {
			dt = dtype.Null()
		}
	}()
	s, err := eval.Eval(context.Background(), eval.Default(), e, df)
	if err != nil {
		return dt
	}
	defer s.Release()
	return s.DType()
}

// CollectSchema returns the output schema of lf without executing the
// plan. Mirrors polars' LazyFrame.collect_schema.
func (lf LazyFrame) CollectSchema() (*schema.Schema, error) { return lf.plan.Schema() }

// Columns returns the output column names without executing the plan.
// Mirrors polars' LazyFrame.columns.
func (lf LazyFrame) Columns() ([]string, error) {
	sch, err := lf.plan.Schema()
	if err != nil {
		return nil, err
	}
	return sch.Names(), nil
}

// DTypes returns the output dtypes without executing the plan. Mirrors
// polars' LazyFrame.dtypes.
func (lf LazyFrame) DTypes() ([]dtype.DType, error) {
	sch, err := lf.plan.Schema()
	if err != nil {
		return nil, err
	}
	return sch.DTypes(), nil
}

// Width returns the number of output columns without executing the plan.
// Mirrors polars' LazyFrame.width.
func (lf LazyFrame) Width() (int, error) {
	sch, err := lf.plan.Schema()
	if err != nil {
		return 0, err
	}
	return sch.Len(), nil
}

// fillNullSchema is the output schema of DataFrame.FillNull(value): a
// float value widens integer columns to f64, everything else keeps its
// dtype.
func fillNullSchema(in *schema.Schema, value any) (*schema.Schema, error) {
	switch value.(type) {
	case float64, float32:
	default:
		return in, nil
	}
	fields := in.Fields()
	out := make([]schema.Field, len(fields))
	for i, f := range fields {
		out[i] = f
		if f.DType.IsInteger() {
			out[i].DType = dtype.Float64()
		}
	}
	return schema.New(out...)
}
