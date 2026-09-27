package eval

import (
	"math"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Overrides of legacy evalFunction cases whose results differed from
// polars (dropped nulls, f64 instead of f32 or u32, unsupported dtypes).

const (
	radToDeg = 180 / math.Pi
	degToRad = math.Pi / 180
)

var floatMath = map[string]func(float64) float64{
	"sqrt":    math.Sqrt,
	"exp":     math.Exp,
	"log":     math.Log,
	"log2":    math.Log2,
	"log10":   math.Log10,
	"sin":     math.Sin,
	"cos":     math.Cos,
	"tan":     math.Tan,
	"cbrt":    math.Cbrt,
	"log1p":   math.Log1p,
	"expm1":   math.Expm1,
	"radians": func(x float64) float64 { return x * degToRad },
	"degrees": func(x float64) float64 { return x * radToDeg },
	"arccos":  math.Acos,
	"arcsin":  math.Asin,
	"arctan":  math.Atan,
	"cot":     func(x float64) float64 { return 1 / math.Tan(x) },
	"sinh":    math.Sinh,
	"cosh":    math.Cosh,
	"tanh":    math.Tanh,
	"arcsinh": math.Asinh,
	"arccosh": math.Acosh,
	"arctanh": math.Atanh,
}

func init() {
	for name, fn := range floatMath {
		registerCore(name, unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
			return s.FloatUnary(name, fn, seriesAlloc(ec))
		}))
	}
	registerCore("arctan2", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.Arctan2With(b, seriesAlloc(ec))
	}))
	registerCore("pct_change", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.PctChangeAll(paramInt(n, 0, 1), seriesAlloc(ec))
	}))
	registerCore("is_first_distinct", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.DistinctMask(true, seriesAlloc(ec))
	}))
	registerCore("is_last_distinct", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.DistinctMask(false, seriesAlloc(ec))
	}))
	registerCore("is_unique", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.OccurrenceMask(true, seriesAlloc(ec))
	}))
	registerCore("is_duplicated", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.OccurrenceMask(false, seriesAlloc(ec))
	}))
	registerCore("rank", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RankWith(paramString(n, 0, "average"), paramBool(n, 1, false), seriesAlloc(ec))
	}))
	registerCore("hist", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		o, _ := paramAny(n, 0).(expr.HistOptions)
		return s.Hist(series.HistOptions{
			Bins:              o.Bins,
			BinCount:          o.BinCount,
			IncludeBreakpoint: o.IncludeBreakpoint,
			IncludeCategory:   o.IncludeCategory,
		}, seriesAlloc(ec))
	}))
}
