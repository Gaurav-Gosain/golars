package eval

import (
	"context"
	"fmt"
	"math/big"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

// temporalAgg evaluates min, max, sum and mean of temporal columns the
// way polars does: min/max keep the dtype, sum is only defined for
// durations, and the mean of dates and datetimes is a datetime (the
// mean of a date column is a microsecond datetime). ok is false for
// non-temporal inputs and for ops the generic path already handles.
func temporalAgg(n expr.AggNode, inner *series.Series, ec EvalContext) (*series.Series, bool, error) {
	dt := inner.DType()
	if !dt.IsTemporal() {
		return nil, false, nil
	}
	switch n.Op {
	case expr.AggMin, expr.AggMax, expr.AggSum, expr.AggMean:
	default:
		return nil, false, nil
	}
	name := expr.OutputName(n.Inner)
	arr := inner.Chunk(0)
	var vals []int64
	switch a := arr.(type) {
	case *array.Date32:
		for i := range a.Len() {
			vals = append(vals, int64(a.Value(i)))
		}
	case *array.Timestamp:
		for i := range a.Len() {
			vals = append(vals, int64(a.Value(i)))
		}
	case *array.Duration:
		for i := range a.Len() {
			vals = append(vals, int64(a.Value(i)))
		}
	case *array.Time64:
		for i := range a.Len() {
			vals = append(vals, int64(a.Value(i)))
		}
	case *array.Time32:
		for i := range a.Len() {
			vals = append(vals, int64(a.Value(i)))
		}
	default:
		return nil, true, fmt.Errorf("eval: aggregation on %s not supported", dt)
	}
	var sum big.Int
	var sumF float64
	var best int64
	count := 0
	for i, v := range vals {
		if arr.IsNull(i) {
			continue
		}
		if count == 0 || (n.Op == expr.AggMin && v < best) || (n.Op == expr.AggMax && v > best) {
			best = v
		}
		sum.Add(&sum, big.NewInt(v))
		sumF += float64(v)
		count++
	}
	out := series.TemporalScalar{DType: dt, Null: count == 0}
	switch n.Op {
	case expr.AggMin, expr.AggMax:
		out.Value = best
	case expr.AggSum:
		if !dt.IsDuration() {
			return nil, true, fmt.Errorf("`sum` operation not supported for dtype `%s`", dt)
		}
		out.Value, out.Null = sum.Int64(), false
	case expr.AggMean:
		if count > 0 {
			// polars averages in f64 and truncates back to ticks.
			out.Value = int64(sumF / float64(count))
		}
		if dt.IsDate() {
			out.DType = dtype.Datetime(dtype.Microsecond, "")
			if count > 0 {
				// Mean in microseconds, truncated like polars.
				us := new(big.Int).Mul(&sum, big.NewInt(temporal.UnitsPerDay(dtype.Microsecond)))
				out.Value = new(big.Int).Quo(us, big.NewInt(int64(count))).Int64()
			}
		}
	}
	s, err := out.Series(name, 1, series.WithAllocator(ec.Alloc))
	return s, true, err
}

// negateTemporal implements unary minus for durations.
func negateTemporal(ctx context.Context, ec EvalContext, inner *series.Series) (*series.Series, bool, error) {
	if !inner.DType().IsDuration() {
		return nil, false, nil
	}
	out, err := compute.MulLit(ctx, inner, int64(-1), kernelOpts(ec)...)
	return out, true, err
}
