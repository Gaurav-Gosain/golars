package eval

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func rollingWindowParam(n expr.FunctionNode, i int) series.RollingWindow {
	w, _ := paramAny(n, i).(expr.RollingWindow)
	return series.RollingWindow{WindowSize: w.WindowSize, MinSamples: w.MinSamples, Center: w.Center}
}

func registerRollingCore() {
	registerCore("rolling_median", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RollingMedianWindow(rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_quantile", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RollingQuantile(paramFloat(n, 1, 0.5), paramString(n, 2, series.QuantileNearest), rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_skew", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RollingSkew(paramBool(n, 1, true), rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_kurtosis", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RollingKurtosis(paramBool(n, 1, true), paramBool(n, 2, true), rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_rank", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RollingRank(paramString(n, 1, "average"), rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_map", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		fn, ok := paramAny(n, 1).(expr.RollingMapFunc)
		if !ok || fn == nil {
			return nil, fmt.Errorf("eval: rolling_map needs a function")
		}
		return s.RollingMap(fn, rollingWindowParam(n, 0), seriesAlloc(ec))
	}))
	registerCore("rolling_corr", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return series.RollingCorr(a, b, rollingWindowParam(n, 0), paramInt(n, 1, 1), seriesAlloc(ec))
	}))
	registerCore("rolling_cov", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return series.RollingCov(a, b, rollingWindowParam(n, 0), paramInt(n, 1, 1), seriesAlloc(ec))
	}))
}
