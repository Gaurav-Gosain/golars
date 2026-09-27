package eval

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Registrations for the core Expr methods. Grouped by theme; each entry
// maps a FunctionNode name to a Series kernel.

func init() {
	registerScalarAggs()
	registerElementwise()
	registerTransforms()
	registerRollingCore()
}

func registerScalarAggs() {
	registerCore("arg_max", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		i, err := s.ArgMaxIndex()
		if err != nil {
			return nil, err
		}
		return series.ScalarUint32(s.Name(), i, i >= 0, seriesAlloc(ec))
	}))
	registerCore("arg_min", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		i, err := s.ArgMinIndex()
		if err != nil {
			return nil, err
		}
		return series.ScalarUint32(s.Name(), i, i >= 0, seriesAlloc(ec))
	}))
	for _, op := range []string{"and", "or", "xor"} {
		name := "bitwise_" + op
		registerCore(name, unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
			switch op {
			case "and":
				return s.BitwiseAnd(seriesAlloc(ec))
			case "or":
				return s.BitwiseOr(seriesAlloc(ec))
			}
			return s.BitwiseXor(seriesAlloc(ec))
		}))
	}
	registerCore("has_nulls", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return series.ScalarBool(s.Name(), s.NullCount() > 0, seriesAlloc(ec))
	}))
	registerCore("len", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return series.ScalarUint32(s.Name(), s.Len(), true, seriesAlloc(ec))
	}))
	registerCore("frame_len", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		return series.ScalarUint32("len", df.Height(), true, seriesAlloc(ec))
	})
	registerCore("nan_max", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.NanMax(seriesAlloc(ec))
	}))
	registerCore("nan_min", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.NanMin(seriesAlloc(ec))
	}))
	registerCore("item", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if s.Len() != 1 {
			return nil, fmt.Errorf("eval: item() expected a single value, got %d values", s.Len())
		}
		return s.Clone(), nil
	}))
	registerCore("get", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		i := paramInt(n, 0, 0)
		if i < 0 {
			i += s.Len()
		}
		if i < 0 || i >= s.Len() {
			return nil, fmt.Errorf("eval: get index %d out of bounds for length %d", paramInt(n, 0, 0), s.Len())
		}
		return s.Gather([]int{i}, seriesAlloc(ec))
	}))
	registerCore("index_of", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.IndexOfValue(paramAny(n, 0), seriesAlloc(ec))
	}))
	registerCore("lower_bound", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.LowerBound(seriesAlloc(ec))
	}))
	registerCore("upper_bound", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.UpperBound(seriesAlloc(ec))
	}))
	registerCore("max_by", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.MaxBy(b, seriesAlloc(ec))
	}))
	registerCore("min_by", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.MinBy(b, seriesAlloc(ec))
	}))
	registerCore("implode", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Implode(seriesAlloc(ec))
	}))
	registerCore("dot", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.Dot(b, seriesAlloc(ec))
	}))
	registerCore("n_unique", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		c, err := s.NUniqueAll()
		if err != nil {
			return nil, err
		}
		return series.ScalarUint32(s.Name(), c, true, seriesAlloc(ec))
	}))
	registerCore("approx_n_unique", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		// An exact count is a valid approximation and matches polars on
		// the inputs its HyperLogLog is exact for.
		c, err := s.NUniqueAll()
		if err != nil {
			return nil, err
		}
		return series.ScalarUint32(s.Name(), c, true, seriesAlloc(ec))
	}))
	registerCore("quantile", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if s.DType().IsBool() {
			return nil, fmt.Errorf("`quantile` operation not supported for dtype `bool`")
		}
		return aggTyped(ec, "quantile", s)(s.QuantileSeries(paramFloat(n, 0, 0.5), series.QuantileNearest, seriesAlloc(ec)))
	}))
	registerCore("quantile_with", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if s.DType().IsBool() {
			return nil, fmt.Errorf("`quantile` operation not supported for dtype `bool`")
		}
		return aggTyped(ec, "quantile", s)(s.QuantileSeries(paramFloat(n, 0, 0.5), paramString(n, 1, series.QuantileNearest), seriesAlloc(ec)))
	}))
	registerCore("median", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if out, ok, err := temporalMedian(s, ec); ok {
			return out, err
		}
		return aggTyped(ec, "median", s)(s.MedianSeries(seriesAlloc(ec)))
	}))
	registerCore("std", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return aggTyped(ec, "std", s)(s.StdSeries(1, seriesAlloc(ec)))
	}))
	registerCore("var", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return aggTyped(ec, "var", s)(s.VarSeries(1, seriesAlloc(ec)))
	}))
	registerCore("std_ddof", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return aggTyped(ec, "std", s)(s.StdSeries(paramInt(n, 0, 1), seriesAlloc(ec)))
	}))
	registerCore("var_ddof", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return aggTyped(ec, "var", s)(s.VarSeries(paramInt(n, 0, 1), seriesAlloc(ec)))
	}))
	entropy := func(s *series.Series, base float64, normalize bool, ec EvalContext) (*series.Series, error) {
		if base <= 0 {
			base = math.E
		}
		v, err := s.EntropyWith(base, normalize)
		if err != nil {
			return nil, err
		}
		return series.FromFloat64(s.Name(), []float64{v}, nil, seriesAlloc(ec))
	}
	registerCore("entropy", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return entropy(s, paramFloat(n, 0, math.E), true, ec)
	}))
	registerCore("entropy_with", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return entropy(s, paramFloat(n, 0, math.E), paramBool(n, 1, true), ec)
	}))
	registerCore("corr", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if paramString(n, 0, "pearson") == "spearman" {
			return series.SpearmanCorrSeries(a, b, seriesAlloc(ec))
		}
		return series.PearsonCorrSeries(a, b, seriesAlloc(ec))
	}))
	registerCore("cov", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return series.Cov(a, b, paramInt(n, 0, 1), seriesAlloc(ec))
	}))
}

func registerElementwise() {
	for _, kind := range []string{"is_nan", "is_not_nan", "is_finite", "is_infinite"} {
		registerCore(kind, unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
			return s.NaNCheck(kind, seriesAlloc(ec))
		}))
	}
	for _, kind := range []string{"count_ones", "count_zeros", "leading_ones", "leading_zeros", "trailing_ones", "trailing_zeros"} {
		registerCore("bitwise_"+kind, unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
			return s.BitCount(kind, seriesAlloc(ec))
		}))
	}
	for _, op := range []string{"and", "or", "xor"} {
		registerCore(op, binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
			return a.LogicOp(op, b, seriesAlloc(ec))
		}))
	}
	registerCore("floordiv", binaryCoreAdopt(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.FloorDiv(b, seriesAlloc(ec))
	}))
	registerCore("mod", binaryCoreAdopt(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.Mod(b, seriesAlloc(ec))
	}))
	registerCore("truediv", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.TrueDiv(b, seriesAlloc(ec))
	}))
	registerCore("pow_expr", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.PowSeries(b, seriesAlloc(ec))
	}))
	registerCore("log_base", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.LogBase(paramFloat(n, 0, math.E), seriesAlloc(ec))
	}))
	registerCore("round_sig_figs", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RoundSigFigs(paramInt(n, 0, 1), seriesAlloc(ec))
	}))
	registerCore("eq_missing", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.EqMissing(b, seriesAlloc(ec))
	}))
	registerCore("ne_missing", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.NeMissing(b, seriesAlloc(ec))
	}))
	registerCore("is_close", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.IsClose(b, paramFloat(n, 0, 0), paramFloat(n, 1, 1e-9), paramBool(n, 2, false), seriesAlloc(ec))
	}))
	registerCore("is_between", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) != 3 {
			return nil, fmt.Errorf("eval: is_between needs three expressions")
		}
		args, err := evalBroadcast(ctx, ec, n.Args, df)
		if err != nil {
			return nil, err
		}
		defer releaseAll(args)
		return args[0].IsBetween(args[1], args[2], paramString(n, 0, "both"), seriesAlloc(ec))
	})
	registerCore("reinterpret", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Reinterpret(paramBool(n, 0, true), seriesAlloc(ec))
	}))
	registerCore("to_physical", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.ToPhysical(seriesAlloc(ec))
	}))
	registerCore("peak_max", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Peaks(true, seriesAlloc(ec))
	}))
	registerCore("peak_min", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Peaks(false, seriesAlloc(ec))
	}))
	registerCore("cut", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		br, _ := paramAny(n, 0).([]float64)
		opts, _ := paramAny(n, 1).(expr.CutOptions)
		return s.Cut(br, seriesCutOpts(opts), seriesAlloc(ec))
	}))
	registerCore("qcut", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		qs, _ := paramAny(n, 0).([]float64)
		opts, _ := paramAny(n, 1).(expr.CutOptions)
		return s.QCut(qs, seriesCutOpts(opts), seriesAlloc(ec))
	}))
	registerCore("qcut_n", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		opts, _ := paramAny(n, 1).(expr.CutOptions)
		return s.QCutN(paramInt(n, 0, 2), seriesCutOpts(opts), seriesAlloc(ec))
	}))
	registerCore("replace", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		return evalReplace(ctx, ec, n, df, false)
	})
	registerCore("replace_strict", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		return evalReplace(ctx, ec, n, df, true)
	})
	registerCore("map_batches", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		fn, ok := paramAny(n, 0).(expr.BatchFunc)
		if !ok || fn == nil {
			return nil, fmt.Errorf("eval: map_batches needs a function")
		}
		return fn(s)
	}))
	registerCore("map_elements", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		fn, ok := paramAny(n, 0).(expr.BatchFunc)
		if !ok || fn == nil {
			return nil, fmt.Errorf("eval: map_elements needs a function")
		}
		return fn(s)
	}))
}

func seriesCutOpts(o expr.CutOptions) series.CutOptions {
	return series.CutOptions{
		Labels:          o.Labels,
		LeftClosed:      o.LeftClosed,
		IncludeBreaks:   o.IncludeBreaks,
		AllowDuplicates: o.AllowDuplicates,
	}
}

// evalBroadcast evaluates every expression and broadcasts length-1
// results to the longest length.
func evalBroadcast(ctx context.Context, ec EvalContext, args []expr.Expr, df *dataframe.DataFrame) ([]*series.Series, error) {
	ss, err := evalArgs(ctx, ec, args, df)
	if err != nil {
		return nil, err
	}
	return broadcastUnits(ss, ec)
}

// broadcastUnits broadcasts the length-1 series in ss to the length of
// the others. It consumes ss, also on error.
func broadcastUnits(ss []*series.Series, ec EvalContext) ([]*series.Series, error) {
	// Unit-length inputs broadcast to the length of the others, which
	// may be zero (an empty frame against a literal).
	n := -1
	for _, s := range ss {
		if l := s.Len(); l != 1 {
			n = max(n, l)
		}
	}
	if n < 0 {
		n = 1
	}
	for i, s := range ss {
		if s.Len() == n {
			continue
		}
		if l := s.Len(); l != 1 {
			releaseAll(ss)
			return nil, fmt.Errorf("eval: cannot broadcast length %d to %d", l, n)
		}
		b, err := s.Broadcast(n, seriesAlloc(ec))
		if err != nil {
			releaseAll(ss)
			return nil, err
		}
		s.Release()
		ss[i] = b
	}
	return ss, nil
}

// valuesSeries builds a Series from Go values and casts it to target
// when target is valid.
func valuesSeries(ctx context.Context, ec EvalContext, name string, vals []any, target arrow.DataType) (*series.Series, error) {
	s, err := series.FromValues(name, series.InferValuesDType(vals), vals, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	if target == nil || arrow.TypeEqual(target, s.Chunked().DataType()) {
		return s, nil
	}
	defer s.Release()
	return castSeriesTo(ctx, ec, s, target)
}

// castSeriesTo casts s to an arrow dtype. Null-typed inputs become an
// all-null column of the target dtype.
func castSeriesTo(ctx context.Context, ec EvalContext, s *series.Series, target arrow.DataType) (*series.Series, error) {
	if arrow.TypeEqual(target, s.Chunked().DataType()) {
		return s.Clone(), nil
	}
	if s.Chunked().DataType().ID() == arrow.NULL {
		return series.FullNull(s.Name(), target, s.Len(), seriesAlloc(ec))
	}
	if target.ID() == arrow.NULL {
		return series.FullNull(s.Name(), target, s.Len(), seriesAlloc(ec))
	}
	return compute.Cast(ctx, s, dtype.FromArrow(target), kernelOpts(ec)...)
}

// superType returns the dtype polars would unify a and b into for
// replace_strict and friends: equal types stay, Null yields the other,
// int with int is i64, any float makes f64; otherwise a.
func superType(a, b arrow.DataType) arrow.DataType {
	switch {
	case a == nil || a.ID() == arrow.NULL:
		return b
	case b == nil || b.ID() == arrow.NULL:
		return a
	case arrow.TypeEqual(a, b):
		return a
	}
	aNum := arrow.IsInteger(a.ID()) || arrow.IsFloating(a.ID())
	bNum := arrow.IsInteger(b.ID()) || arrow.IsFloating(b.ID())
	if aNum && bNum {
		if arrow.IsFloating(a.ID()) || arrow.IsFloating(b.ID()) {
			return arrow.PrimitiveTypes.Float64
		}
		return arrow.PrimitiveTypes.Int64
	}
	if a.ID() == arrow.STRING || b.ID() == arrow.STRING {
		return arrow.BinaryTypes.String
	}
	return a
}

func evalReplace(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame, strict bool) (*series.Series, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("eval: %s needs an input expression", n.Name)
	}
	s, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	oldVals, _ := paramAny(n, 0).([]any)
	newVals, _ := paramAny(n, 1).([]any)
	if len(newVals) == 1 && len(oldVals) > 1 {
		// A single replacement value applies to every old value.
		v := newVals[0]
		newVals = make([]any, len(oldVals))
		for i := range newVals {
			newVals[i] = v
		}
	}
	if len(oldVals) != len(newVals) {
		return nil, fmt.Errorf("eval: %s needs as many new values as old (%d vs %d)", n.Name, len(newVals), len(oldVals))
	}
	inDT := s.Chunked().DataType()
	old, err := valuesSeries(ctx, ec, "old", oldVals, inDT)
	if err != nil {
		return nil, err
	}
	defer old.Release()
	if !strict {
		nw, err := valuesSeries(ctx, ec, "new", newVals, inDT)
		if err != nil {
			return nil, err
		}
		defer nw.Release()
		return s.ReplaceSeries(old, nw, seriesAlloc(ec))
	}
	hasDefault := paramBool(n, 2, false)
	retDT, hasRet := paramDType(n, 3)
	nwRaw, err := series.FromValues("new", series.InferValuesDType(newVals), newVals, seriesAlloc(ec))
	if err != nil {
		return nil, err
	}
	defer nwRaw.Release()
	target := nwRaw.Chunked().DataType()
	var dflt *series.Series
	if hasDefault && len(n.Args) > 1 {
		d, err := evalScalarOrColumn(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		if d.Len() != s.Len() && d.Len() != 1 {
			b, err := d.Broadcast(s.Len(), seriesAlloc(ec))
			d.Release()
			if err != nil {
				return nil, err
			}
			d = b
		}
		defer d.Release()
		target = superType(target, d.Chunked().DataType())
		dflt, err = castSeriesTo(ctx, ec, d, target)
		if err != nil {
			return nil, err
		}
		defer dflt.Release()
	}
	if hasRet {
		target = retDT.Arrow()
		if dflt != nil {
			d2, err := castSeriesTo(ctx, ec, dflt, target)
			if err != nil {
				return nil, err
			}
			defer d2.Release()
			dflt = d2
		}
	}
	nw, err := castSeriesTo(ctx, ec, nwRaw, target)
	if err != nil {
		return nil, err
	}
	defer nw.Release()
	return s.ReplaceStrictSeries(old, nw, dflt, seriesAlloc(ec))
}
