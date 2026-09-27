package eval

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func registerTransforms() {
	registerCore("arg_sort", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		idx, err := s.ArgSortWith(paramBool(n, 0, false), paramBool(n, 1, false))
		if err != nil {
			return nil, err
		}
		return series.IdxSeries(s.Name(), idx, seriesAlloc(ec))
	}))
	registerCore("arg_true", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.ArgTrueIdx(seriesAlloc(ec))
	}))
	registerCore("arg_unique", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.ArgUniqueIdx(seriesAlloc(ec))
	}))
	registerCore("drop_nulls", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		if s.NullCount() == 0 {
			return s.Clone(), nil
		}
		arr, err := s.Consolidated()
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		idx := make([]int, 0, s.Len()-s.NullCount())
		for i := range arr.Len() {
			if arr.IsValid(i) {
				idx = append(idx, i)
			}
		}
		return s.Gather(idx, seriesAlloc(ec))
	}))
	registerCore("drop_nans", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.DropNans(seriesAlloc(ec))
	}))
	registerCore("filter", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) != 2 {
			return nil, fmt.Errorf("eval: filter needs an input and a predicate")
		}
		s, mask, err := evalPair(ctx, ec, n.Args[0], n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer s.Release()
		defer mask.Release()
		return filterSeries(ctx, ec, s, mask)
	})
	registerCore("gather", binaryNoBroadcast(func(s, idx *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.GatherSigned(idx, seriesAlloc(ec))
	}))
	registerCore("gather_idx", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		raw, _ := paramAny(n, 0).([]int64)
		idx, err := series.FromInt64("idx", raw, nil, seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		defer idx.Release()
		return s.GatherSigned(idx, seriesAlloc(ec))
	}))
	registerCore("gather_every", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.GatherEvery(paramInt(n, 0, 1), paramInt(n, 1, 0), seriesAlloc(ec))
	}))
	registerCore("extend_constant", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		s, err := evalNode(ctx, ec, n.Args[0], df)
		if err != nil {
			return nil, err
		}
		defer s.Release()
		count := paramInt(n, 1, 0)
		if count < 0 {
			return nil, fmt.Errorf("eval: extend_constant count must be non-negative")
		}
		v := paramAny(n, 0)
		if e, ok := v.(expr.Expr); ok {
			one, err := evalScalar(ctx, ec, e, df)
			if err != nil {
				return nil, err
			}
			defer one.Release()
			v = one.First()
		}
		fill, err := valuesSeries(ctx, ec, s.Name(), []any{v}, s.Chunked().DataType())
		if err != nil {
			return nil, err
		}
		defer fill.Release()
		rep, err := fill.Broadcast(count, seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		defer rep.Release()
		return s.Append(rep, seriesAlloc(ec))
	})
	registerCore("explode", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Explode(seriesAlloc(ec))
	}))
	registerCore("top_k", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.TopKValues(paramInt(n, 0, 5), seriesAlloc(ec))
	}))
	registerCore("bottom_k", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.BottomKValues(paramInt(n, 0, 5), seriesAlloc(ec))
	}))
	kBy := func(top bool) coreFn {
		return func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
			args, err := evalBroadcast(ctx, ec, n.Args, df)
			if err != nil {
				return nil, err
			}
			defer releaseAll(args)
			reverse, _ := paramAny(n, 1).([]bool)
			if top {
				return args[0].TopKBy(args[1:], paramInt(n, 0, 5), reverse, seriesAlloc(ec))
			}
			return args[0].BottomKBy(args[1:], paramInt(n, 0, 5), reverse, seriesAlloc(ec))
		}
	}
	registerCore("top_k_by", kBy(true))
	registerCore("bottom_k_by", kBy(false))
	registerCore("sort_with", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.SortWith(paramBool(n, 0, false), paramBool(n, 1, false), seriesAlloc(ec))
	}))
	registerCore("sort_by", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: sort_by needs at least one key")
		}
		args, err := evalBroadcast(ctx, ec, n.Args, df)
		if err != nil {
			return nil, err
		}
		defer releaseAll(args)
		desc, _ := paramAny(n, 0).([]bool)
		return args[0].SortByKeys(args[1:], desc, paramBool(n, 1, false), seriesAlloc(ec))
	})
	registerCore("arg_sort_by", func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		args, err := evalBroadcast(ctx, ec, n.Args, df)
		if err != nil {
			return nil, err
		}
		defer releaseAll(args)
		desc, _ := paramAny(n, 0).([]bool)
		idx, err := series.ArgSortKeys(args, desc, paramBool(n, 1, false))
		if err != nil {
			return nil, err
		}
		return series.IdxSeries(args[0].Name(), idx, seriesAlloc(ec))
	})
	registerCore("mode", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.ModeAll(seriesAlloc(ec))
	}))
	registerCore("unique_counts", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.UniqueCounts(seriesAlloc(ec))
	}))
	registerCore("value_counts", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.ValueCountsStruct(paramBool(n, 0, false), paramBool(n, 1, false), paramString(n, 2, ""), seriesAlloc(ec))
	}))
	registerCore("rle", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Rle(seriesAlloc(ec))
	}))
	registerCore("rle_id", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.RleID(seriesAlloc(ec))
	}))
	registerCore("repeat_by", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.RepeatBy(b, seriesAlloc(ec))
	}))
	registerCore("reshape", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		dims, _ := paramAny(n, 0).([]int)
		return s.Reshape(dims, seriesAlloc(ec))
	}))
	registerCore("interpolate", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.Interpolate(paramString(n, 0, "linear"), seriesAlloc(ec))
	}))
	registerCore("interpolate_by", binaryCore(func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return a.InterpolateBy(b, seriesAlloc(ec))
	}))
	registerCore("search_sorted", binaryNoBroadcast(func(s, needles *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		return s.SearchSortedSeries(needles, paramString(n, 0, "any"), seriesAlloc(ec))
	}))
	registerCore("sample", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		out, err := s.Sample(paramInt(n, 0, 1), paramBool(n, 1, false), seed(n, 3), seriesAlloc(ec))
		return sampleResult(s, out, err)
	}))
	registerCore("sample_frac", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		out, err := s.SampleFrac(paramFloat(n, 0, 1), paramBool(n, 1, false), seed(n, 3), seriesAlloc(ec))
		return sampleResult(s, out, err)
	}))
	registerCore("shuffle", unaryCore(func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error) {
		out, err := s.Shuffle(seed(n, 0), seriesAlloc(ec))
		return sampleResult(s, out, err)
	}))
	registerCore("cumulative_eval", evalCumulative)
}

// sampleResult replaces the zero-chunk empty Series that Sample returns
// for n=0 with a proper empty column of the input dtype.
func sampleResult(s, out *series.Series, err error) (*series.Series, error) {
	if err != nil {
		return nil, err
	}
	if out.Len() == 0 && out.NumChunks() == 0 {
		out.Release()
		return series.FullNull(s.Name(), s.Chunked().DataType(), 0)
	}
	return out, nil
}

func seed(n expr.FunctionNode, i int) uint64 {
	switch v := paramAny(n, i).(type) {
	case uint64:
		return v
	case int:
		return uint64(v)
	case int64:
		return uint64(v)
	}
	return 0
}

// binaryNoBroadcast evaluates two arguments without length alignment.
func binaryNoBroadcast(fn func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error)) coreFn {
	return func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: %s requires two input expressions", n.Name)
		}
		a, err := evalNode(ctx, ec, n.Args[0], df)
		if err != nil {
			return nil, err
		}
		defer a.Release()
		b, err := evalScalarOrColumn(ctx, ec, n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer b.Release()
		return fn(a, b, n, ec)
	}
}

// evalScalarOrColumn evaluates e, turning a bare literal into a single
// row instead of a frame-height column so constructors and broadcasting
// functions can use it against columns or on its own. Temporal literals
// go through the temporal builder to keep their unit and time zone.
func evalScalarOrColumn(ctx context.Context, ec EvalContext, e expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	if l, ok := e.Node().(expr.LitNode); ok {
		if s, ok, err := temporalLiteralSeries(l, 1, ec); ok {
			return s, err
		}
		return evalScalar(ctx, ec, e, df)
	}
	return evalNode(ctx, ec, e, df)
}

// evalScalar evaluates e to a single row. Literals are built directly so
// an empty frame still yields one value; other expressions must reduce
// to one row (the first row is used for longer results).
func evalScalar(ctx context.Context, ec EvalContext, e expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	if lit, ok := e.Node().(expr.LitNode); ok {
		// Temporal literals carry Go values (DateValue, time.Time) that
		// FromValues cannot store; they have their own conversion.
		if s, ok, err := temporalLiteralSeries(lit, 1, ec); ok {
			return s, err
		}
		dt := lit.DType.Arrow()
		if lit.Value == nil {
			return series.FullNull("literal", dt, 1, seriesAlloc(ec))
		}
		return series.FromValues("literal", dt, []any{lit.Value}, seriesAlloc(ec))
	}
	s, err := evalNode(ctx, ec, e, df)
	if err != nil {
		return nil, err
	}
	if s.Len() == 1 {
		return s, nil
	}
	defer s.Release()
	if s.Len() == 0 {
		return series.FullNull(s.Name(), s.Chunked().DataType(), 1, seriesAlloc(ec))
	}
	return s.Slice(0, 1)
}

// filterSeries keeps rows of s where mask is true (null counts as
// false). compute.Filter covers the hot dtypes; Gather handles the rest.
func filterSeries(ctx context.Context, ec EvalContext, s, mask *series.Series) (*series.Series, error) {
	if !mask.DType().IsBool() {
		return nil, fmt.Errorf("eval: filter predicate must be boolean, got %s", mask.DType())
	}
	if out, err := compute.Filter(ctx, s, mask, kernelOpts(ec)...); err == nil {
		return out, nil
	}
	arr, err := mask.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	b := arr.(*array.Boolean)
	idx := make([]int, 0, b.Len())
	for i := range b.Len() {
		if b.IsValid(i) && b.Value(i) {
			idx = append(idx, i)
		}
	}
	return s.Gather(idx, seriesAlloc(ec))
}

// evalCumulative implements cumulative_eval: the inner expression runs
// on every prefix of the input, exposed as the column named "".
func evalCumulative(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	inner, ok := paramAny(n, 0).(expr.Expr)
	if !ok {
		return nil, fmt.Errorf("eval: cumulative_eval needs an inner expression")
	}
	minSamples := paramInt(n, 1, 1)
	s, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	elem := s.Rename("")
	defer elem.Release()
	arr, err := s.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	h := s.Len()
	parts := make([]*series.Series, 0, h)
	defer func() { releaseAll(parts) }()
	idx := make([]int, h)
	valid := 0
	var dt arrow.DataType
	for i := range h {
		if arr.IsValid(i) {
			valid++
		}
		if valid < minSamples {
			idx[i] = -1
			continue
		}
		prefix, err := elem.Slice(0, i+1)
		if err != nil {
			return nil, err
		}
		frame, err := dataframe.New(prefix)
		if err != nil {
			prefix.Release()
			return nil, err
		}
		res, err := evalNode(ctx, ec, inner, frame)
		frame.Release()
		if err != nil {
			return nil, err
		}
		if res.Len() != 1 {
			l := res.Len()
			res.Release()
			return nil, fmt.Errorf("eval: cumulative_eval expression must return one value, got %d", l)
		}
		if dt == nil {
			dt = res.Chunked().DataType()
		}
		idx[i] = len(parts)
		parts = append(parts, res)
	}
	if len(parts) == 0 {
		return series.FullNull(s.Name(), s.Chunked().DataType(), h, seriesAlloc(ec))
	}
	pool, err := concatHarmonized(ctx, ec, s.Name(), parts)
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	return pool.Gather(idx, seriesAlloc(ec))
}
