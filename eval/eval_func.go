package eval

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func isNaN(v float64) bool { return v != v }

// evalFunction dispatches FunctionNode by name. Each case materialises
// the first Arg via recursive evalNode, then calls the corresponding
// series kernel. Multi-arg functions (fill_null with another Expr)
// evaluate each arg first.
func evalFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if isTemporalFunction(n.Name) {
		return evalTemporalFunction(ctx, ec, n, df)
	}
	if fn, ok := coreFuncs[n.Name]; ok {
		return fn(ctx, ec, n, df)
	}
	// Argless constructors: int_range, ones, zeros don't reference a
	// column. Handle before the len==0 guard below.
	switch n.Name {
	case "int_range":
		return evalIntRange(n)
	case "ones":
		return evalOnesZeros(n, 1.0)
	case "zeros":
		return evalOnesZeros(n, 0.0)
	case "coalesce":
		return evalCoalesce(ctx, ec, n, df)
	case "concat_str":
		return evalConcatStr(ctx, ec, n, df)
	case "struct.field_ref":
		return evalFieldRef(ctx, n)
	}
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: function %q requires at least one argument", n.Name)
	}
	// All str.* kernels go through a dedicated dispatcher to keep the
	// main switch below small. Prefix match is cheap.
	if len(n.Name) > 4 && n.Name[:4] == "str." {
		return evalStrFunction(ctx, ec, n, df)
	}
	if len(n.Name) > 5 && n.Name[:5] == "list." {
		return evalListFunction(ctx, ec, n, df)
	}
	if len(n.Name) > 7 && n.Name[:7] == "struct." {
		return evalStructFunction(ctx, ec, n, df)
	}
	if out, ok, err := evalNamespaceFunction(ctx, ec, n, df); ok {
		return out, err
	}
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	// All branches below either consume arg0 or return it; ensure we
	// release on error to avoid leaks.
	releaseOnErr := arg0
	defer func() {
		if releaseOnErr != nil {
			releaseOnErr.Release()
		}
	}()

	opt := seriesAlloc(ec)
	switch n.Name {
	case "abs":
		out, err := arg0.Abs(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "sqrt":
		out, err := arg0.Sqrt(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "exp":
		out, err := arg0.Exp(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "log":
		out, err := arg0.Log(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "log2":
		out, err := arg0.Log2(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "log10":
		out, err := arg0.Log10(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "sin":
		out, err := arg0.Sin(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cos":
		out, err := arg0.Cos(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "tan":
		out, err := arg0.Tan(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "sign":
		out, err := arg0.Sign(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "floor":
		out, err := arg0.Floor(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "ceil":
		out, err := arg0.Ceil(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "reverse":
		out, err := arg0.Reverse(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "round":
		dec := 0
		if len(n.Params) >= 1 {
			if d, ok := n.Params[0].(int); ok {
				dec = d
			}
		}
		out, err := arg0.Round(dec, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "pow":
		exp := 1.0
		if len(n.Params) >= 1 {
			if f, ok := n.Params[0].(float64); ok {
				exp = f
			}
		}
		out, err := arg0.Pow(exp, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "clip":
		lo, hi := -inf, inf
		if len(n.Params) >= 2 {
			if f, ok := n.Params[0].(float64); ok {
				lo = f
			}
			if f, ok := n.Params[1].(float64); ok {
				hi = f
			}
		}
		out, err := arg0.Clip(lo, hi, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "head":
		nn := 5
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				nn = x
			}
		}
		out, err := arg0.Head(nn, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "tail":
		nn := 5
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				nn = x
			}
		}
		out, err := arg0.Tail(nn, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "slice":
		off, length := 0, arg0.Len()
		if len(n.Params) >= 2 {
			if x, ok := n.Params[0].(int); ok {
				off = x
			}
			if x, ok := n.Params[1].(int); ok {
				length = x
			}
		}
		off, length = series.ClampSlice(off, length, arg0.Len())
		out, err := arg0.Slice(off, length)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "shift":
		periods := 0
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				periods = x
			}
		}
		out, err := arg0.Shift(periods, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "fill_null":
		// Args[0] is the input; Args[1] (if present) carries the fill value.
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: fill_null requires a value arg")
		}
		fillArg, err := evalNode(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer fillArg.Release()
		// A column-valued fill (fill_null(col("b"))) fills row by row. On
		// an empty or one-row frame the column has no usable first value
		// (it is empty or null), so it also takes the row-wise path.
		if fillArg.Len() == arg0.Len() && (fillArg.Len() != 1 || fillArg.NullCount() == 1) {
			releaseOnErr = nil
			defer arg0.Release()
			return fillNullFrom(ctx, ec, arg0, fillArg)
		}
		// Extract the first element of fillArg as the fill scalar.
		v := scalarOf(fillArg)
		if v == nil {
			// A null fill value, or a literal broadcast over a zero-row
			// frame, leaves the input unchanged.
			releaseOnErr = nil
			return arg0, nil
		}
		out, err := arg0.FillNull(v, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "fill_nan":
		v := 0.0
		if len(n.Params) >= 1 {
			switch x := n.Params[0].(type) {
			case float64:
				v = x
			case float32:
				v = float64(x)
			case int:
				v = float64(x)
			case int64:
				v = float64(x)
			}
		}
		out, err := arg0.FillNan(v, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "forward_fill":
		limit := 0
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				limit = x
			}
		}
		out, err := arg0.ForwardFill(limit, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "backward_fill":
		limit := 0
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				limit = x
			}
		}
		out, err := arg0.BackwardFill(limit, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "median", "std", "var", "any", "all", "product", "quantile":
		// These produce a scalar column of length 1 (aggregations).
		defer func() { releaseOnErr = nil }()
		return evalScalarAgg(n, arg0)

	case "sort":
		desc := false
		if len(n.Params) >= 1 {
			if b, ok := n.Params[0].(bool); ok {
				desc = b
			}
		}
		sorted, err := compute.Sort(ctx, arg0, compute.SortOptions{Descending: desc}, kernelOpts(ec)...)
		releaseOnErr = nil
		arg0.Release()
		return sorted, err
	case "unique":
		out, err := arg0.Unique(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "hash":
		out, err := arg0.Hash(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cum_sum":
		out, err := arg0.CumSum(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cum_min":
		out, err := arg0.CumMin(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cum_max":
		out, err := arg0.CumMax(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cum_prod":
		out, err := arg0.CumProd(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "cum_count":
		out, err := arg0.CumCount(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "diff":
		periods := 1
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				periods = x
			}
		}
		out, err := arg0.Diff(periods, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "pct_change":
		periods := 1
		if len(n.Params) >= 1 {
			if x, ok := n.Params[0].(int); ok {
				periods = x
			}
		}
		out, err := arg0.PctChange(periods, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "rank":
		method := series.RankAverage
		if len(n.Params) >= 1 {
			if s, ok := n.Params[0].(string); ok {
				method = rankMethodFromString(s)
			}
		}
		out, err := arg0.Rank(method, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "is_duplicated":
		out, err := arg0.IsDuplicated(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "is_unique":
		out, err := arg0.IsUnique(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "is_first_distinct":
		out, err := arg0.IsFirstDistinct(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "is_last_distinct":
		out, err := arg0.IsLastDistinct(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "n_unique":
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		n, err := arg0.NUnique()
		if err != nil {
			return nil, err
		}
		return series.FromInt64(arg0.Name(), []int64{int64(n)}, nil)
	case "cbrt", "log1p", "expm1", "radians", "degrees",
		"arccos", "arcsin", "arctan", "cot",
		"sinh", "cosh", "tanh",
		"arcsinh", "arccosh", "arctanh":
		out, err := dispatchMathUnary(n.Name, arg0, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "skew":
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		v, err := arg0.Skew()
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(arg0.Name(), []float64{v}, []bool{!isNaN(v)})
	case "kurtosis":
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		v, err := arg0.Kurtosis()
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(arg0.Name(), []float64{v}, []bool{!isNaN(v)})
	case "entropy":
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		base := math.E
		if len(n.Params) >= 1 {
			if b, ok := n.Params[0].(float64); ok && b > 0 && b != 1 {
				base = b
			}
		}
		v, err := arg0.Entropy(base)
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(arg0.Name(), []float64{v}, []bool{!isNaN(v)})
	case "peak_max":
		out, err := arg0.PeakMax(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "peak_min":
		out, err := arg0.PeakMin(opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "approx_n_unique":
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		v, err := arg0.ApproxNUnique()
		if err != nil {
			return nil, err
		}
		return series.FromInt64(arg0.Name(), []int64{int64(v)}, nil)
	case "rolling_sum", "rolling_mean", "rolling_min", "rolling_max",
		"rolling_std", "rolling_var":
		opts := rollingOptsFrom(n)
		out, err := dispatchRolling(n.Name, arg0, opts)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	case "ewm_mean", "ewm_var", "ewm_std":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: %s requires alpha", n.Name)
		}
		alpha, ok := n.Params[0].(float64)
		if !ok {
			return nil, fmt.Errorf("eval: %s alpha must be float64, got %T", n.Name, n.Params[0])
		}
		defer func() { releaseOnErr = nil }()
		defer arg0.Release()
		switch n.Name {
		case "ewm_mean":
			return arg0.EWMMean(alpha)
		case "ewm_var":
			return arg0.EWMVar(alpha)
		case "ewm_std":
			return arg0.EWMStd(alpha)
		}
	case "arctan2":
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: arctan2 requires two args")
		}
		arg1, err := evalNode(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer arg1.Release()
		out, err := arg0.Arctan2(arg1, opt)
		releaseOnErr = nil
		arg0.Release()
		return out, err
	}
	return nil, fmt.Errorf("eval: unknown function %q", n.Name)
}

// dispatchMathUnary calls the Series method matching name. All listed
// names share the float-out unary math signature.
func dispatchMathUnary(name string, s *series.Series, opt series.Option) (*series.Series, error) {
	switch name {
	case "cbrt":
		return s.Cbrt(opt)
	case "log1p":
		return s.Log1p(opt)
	case "expm1":
		return s.Expm1(opt)
	case "radians":
		return s.Radians(opt)
	case "degrees":
		return s.Degrees(opt)
	case "arccos":
		return s.Arccos(opt)
	case "arcsin":
		return s.Arcsin(opt)
	case "arctan":
		return s.Arctan(opt)
	case "cot":
		return s.Cot(opt)
	case "sinh":
		return s.Sinh(opt)
	case "cosh":
		return s.Cosh(opt)
	case "tanh":
		return s.Tanh(opt)
	case "arcsinh":
		return s.Arcsinh(opt)
	case "arccosh":
		return s.Arccosh(opt)
	case "arctanh":
		return s.Arctanh(opt)
	}
	return nil, fmt.Errorf("eval: unknown math function %q", name)
}

// rankMethodFromString maps polars-style method names to RankMethod.
// Unknown names fall back to Average.
func rankMethodFromString(s string) series.RankMethod {
	switch s {
	case "min":
		return series.RankMin
	case "max":
		return series.RankMax
	case "dense":
		return series.RankDense
	case "ordinal":
		return series.RankOrdinal
	}
	return series.RankAverage
}

// evalScalarAgg handles the scalar aggregate functions that return a
// single-row Series. We detect the requested op from the FunctionNode
// name and call the Series-level kernel.
func evalScalarAgg(n expr.FunctionNode, s *series.Series) (*series.Series, error) {
	defer s.Release()
	switch n.Name {
	case "median":
		v, err := s.Median()
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil)
	case "std":
		v, err := s.Std()
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil)
	case "var":
		v, err := s.Var()
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil)
	case "any":
		v, err := s.Any()
		if err != nil {
			return nil, err
		}
		return series.FromBool(s.Name(), []bool{v}, nil)
	case "all":
		v, err := s.All()
		if err != nil {
			return nil, err
		}
		return series.FromBool(s.Name(), []bool{v}, nil)
	case "product":
		// polars: integer products are i64, f32 stays f32.
		if s.DType().IsInteger() {
			v, err := s.ProductInt64()
			if err != nil {
				return nil, err
			}
			return series.FromInt64(s.Name(), []int64{v}, nil)
		}
		v, err := s.Product()
		if err != nil {
			return nil, err
		}
		if s.DType().Equal(dtype.Float32()) {
			return series.FromFloat32(s.Name(), []float32{float32(v)}, nil)
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil)
	case "quantile":
		q := 0.5
		if len(n.Params) >= 1 {
			if f, ok := n.Params[0].(float64); ok {
				q = f
			}
		}
		v, err := s.Quantile(q)
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil)
	}
	return nil, fmt.Errorf("eval: unknown scalar agg %q", n.Name)
}

// seriesAlloc translates an EvalContext into a series.Option so our
// kernels honour the caller's allocator.
func seriesAlloc(ec EvalContext) series.Option {
	return series.WithAllocator(ec.Alloc)
}

// scalarOf pulls the first valid element out of a scalar-like Series
// as a plain Go value, for use as a fill/clip constant.
func scalarOf(s *series.Series) any {
	if s.Len() == 0 {
		return nil
	}
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		return a.Value(0)
	case *array.Int32:
		return a.Value(0)
	case *array.Float64:
		return a.Value(0)
	case *array.Float32:
		return a.Value(0)
	case *array.Boolean:
		return a.Value(0)
	case *array.String:
		return a.Value(0)
	}
	return nil
}

// inf is the positive infinity constant used for open-ended Clip.
const inf = 1e308
