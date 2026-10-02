package eval

import (
	"context"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// widenedCol names the temporary column a widened first argument is
// evaluated from.
const widenedCol = "__golars_widened"

// evalFunction evaluates n. Many series kernels only cover the 32 and
// 64 bit numeric dtypes; when one rejects a narrower input (i8, i16,
// u8, u16, u32) or f32, the function is evaluated again on the first
// argument widened to i64 or f64, and the result is cast back to the
// dtype polars gives it.
func evalFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	out, err := evalFunctionRaw(ctx, ec, n, df)
	if err == nil || len(n.Args) == 0 || !isUnsupportedDTypeErr(err) {
		return out, err
	}
	if res, ok := retryWidened(ctx, ec, n, df); ok {
		return res, nil
	}
	return nil, err
}

// isUnsupportedDTypeErr reports a golars kernel gap ("unsupported for
// dtype", "does not accept"), not an error polars raises too (those
// read "`op` operation not supported for dtype").
func isUnsupportedDTypeErr(err error) bool {
	msg := err.Error()
	if strings.Contains(msg, "operation not supported") {
		return false
	}
	return strings.Contains(msg, "unsupported") || strings.Contains(msg, "does not accept")
}

func retryWidened(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, bool) {
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, false
	}
	defer arg0.Release()
	orig := arg0.DType()
	var wide dtype.DType
	switch orig.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.BOOL:
		wide = dtype.Int64()
	case arrow.FLOAT32:
		wide = dtype.Float64()
	default:
		if !orig.IsTemporal() || n.Name == "diff" {
			return nil, false
		}
		// Temporal values work on their physical ticks.
		wide = dtype.Int64()
	}
	w, err := compute.Cast(ctx, arg0, wide, kernelOpts(ec)...)
	if err != nil {
		return nil, false
	}
	wr := w.Rename(widenedCol)
	w.Release()
	var frame *dataframe.DataFrame
	if df.Width() == 0 || wr.Len() == df.Height() {
		// WithColumn does not add to df's own references.
		frame, err = df.WithColumn(wr)
	} else {
		frame, err = dataframe.New(wr)
	}
	if err != nil {
		wr.Release()
		return nil, false
	}
	defer frame.Release()
	m := n
	m.Args = append([]expr.Expr{expr.Col(widenedCol)}, n.Args[1:]...)
	out, err := evalFunctionRaw(ctx, ec, m, frame)
	if err != nil {
		return nil, false
	}
	out = renamed(out, arg0.Name())
	want, ok := widenedResultDType(n.Name, orig, out.DType())
	if !ok || out.DType().Equal(want) {
		return out, true
	}
	cast, err := compute.Cast(ctx, out, want, kernelOpts(ec)...)
	out.Release()
	if err != nil {
		return nil, false
	}
	return cast, true
}

// widenedResultDType maps the dtype a kernel returned for a widened
// input back to the polars dtype for the original input orig.
func widenedResultDType(name string, orig, got dtype.DType) (dtype.DType, bool) {
	switch {
	case orig.ID() == arrow.FLOAT32 && got.ID() == arrow.FLOAT64:
		if name == "rank" {
			return got, false
		}
		return orig, true
	case orig.IsTemporal() && got.ID() == arrow.INT64:
		return orig, true
	case orig.IsBool() && got.ID() == arrow.INT64:
		return got, false
	case orig.IsInteger() && got.ID() == arrow.INT64:
		if name == "diff" {
			switch orig.ID() {
			case arrow.UINT8:
				return dtype.Int16(), true
			case arrow.UINT16:
				return dtype.Int32(), true
			case arrow.UINT32:
				return dtype.Int64(), true
			}
		}
		if name == "cum_prod" || name == "product" {
			return got, false
		}
		return orig, true
	}
	return got, false
}
