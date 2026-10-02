package eval

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// rollingOptsFrom unpacks the two-int Params (window, min_periods)
// attached by expr.Rolling*.
func rollingOptsFrom(n expr.FunctionNode) series.RollingOptions {
	var opts series.RollingOptions
	if len(n.Params) >= 1 {
		if w, ok := n.Params[0].(int); ok {
			opts.WindowSize = w
		}
	}
	if len(n.Params) >= 2 {
		if mp, ok := n.Params[1].(int); ok {
			opts.MinPeriods = mp
		}
	}
	return opts
}

// rollingAggOp names the aggregation whose polars result dtype a
// rolling kernel follows.
var rollingAggOp = map[string]string{
	"rolling_sum": "sum", "rolling_min": "min", "rolling_max": "max",
	"rolling_mean": "mean", "rolling_std": "std", "rolling_var": "var",
}

// dispatchRolling routes by kernel name. The kernels compute in f64 on
// i32, i64 and f64 inputs; other numeric and bool inputs are widened
// first, and the result takes the polars dtype (rolling_sum of i32 is
// i32, rolling_min of u8 is u8, rolling_mean of f32 is f32).
func dispatchRolling(name string, s *series.Series, opts series.RollingOptions) (*series.Series, error) {
	if opts.MinPeriods > 0 && opts.WindowSize > 0 && opts.MinPeriods > opts.WindowSize {
		return nil, fmt.Errorf("`min_periods` should be <= `window_size`")
	}
	in := s.DType()
	switch in.ID() {
	case arrow.INT32, arrow.INT64, arrow.FLOAT64:
	default:
		if in.IsNumeric() || in.IsBool() {
			wide, err := compute.Cast(context.Background(), s, dtype.Float64())
			if err != nil {
				return nil, err
			}
			defer wide.Release()
			s = wide
		}
	}
	out, err := dispatchRollingRaw(name, s, opts)
	if err != nil {
		return nil, err
	}
	want, ok := dtype.AggResultDType(rollingAggOp[name], in)
	if !ok || out.DType().Equal(want) {
		return out, nil
	}
	defer out.Release()
	return compute.Cast(context.Background(), out, want)
}

func dispatchRollingRaw(name string, s *series.Series, opts series.RollingOptions) (*series.Series, error) {
	switch name {
	case "rolling_sum":
		return s.RollingSum(opts)
	case "rolling_mean":
		return s.RollingMean(opts)
	case "rolling_min":
		return s.RollingMin(opts)
	case "rolling_max":
		return s.RollingMax(opts)
	case "rolling_std":
		return s.RollingStd(opts)
	case "rolling_var":
		return s.RollingVar(opts)
	}
	return nil, fmt.Errorf("eval: unknown rolling op %q", name)
}
