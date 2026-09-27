// Package eval evaluates expr.Expr trees against a DataFrame.
//
// This package lives outside expr to avoid an import cycle: expr is pure AST
// and knows nothing about DataFrame or Series; eval ties them together by
// calling the compute kernels.
package eval

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// EvalContext carries allocator and parallelism settings across an evaluation.
type EvalContext struct {
	Alloc       memory.Allocator
	Parallelism int
}

// Default returns an EvalContext backed by memory.DefaultAllocator.
func Default() EvalContext {
	return EvalContext{Alloc: memory.DefaultAllocator}
}

// Eval evaluates e against df and returns a Series whose name reflects the
// expression's inferred output name.
func Eval(ctx context.Context, ec EvalContext, e expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	if ec.Alloc == nil {
		ec.Alloc = memory.DefaultAllocator
	}
	return evalNode(ctx, ec, e, df)
}

func evalNode(ctx context.Context, ec EvalContext, e expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	switch n := e.Node().(type) {
	case expr.ColNode:
		col, err := df.Column(n.Name)
		if err != nil {
			return nil, err
		}
		return col.Clone(), nil

	case expr.LitNode:
		return literalSeries(n, df.Height(), ec.Alloc)

	case expr.BinaryNode:
		return evalBinary(ctx, ec, n, df)

	case expr.UnaryNode:
		return evalUnary(ctx, ec, n, df)

	case expr.AliasNode:
		inner, err := evalNode(ctx, ec, n.Inner, df)
		if err != nil {
			return nil, err
		}
		renamed := inner.Rename(n.Name)
		inner.Release()
		return renamed, nil

	case expr.CastNode:
		inner, err := evalNode(ctx, ec, n.Inner, df)
		if err != nil {
			return nil, err
		}
		defer inner.Release()
		return compute.Cast(ctx, inner, n.To, kernelOpts(ec)...)

	case expr.AggNode:
		return evalAgg(ctx, ec, n, df)

	case expr.IsNullNode:
		inner, err := evalNode(ctx, ec, n.Inner, df)
		if err != nil {
			return nil, err
		}
		defer inner.Release()
		if n.Negate {
			return compute.IsNotNull(ctx, inner, kernelOpts(ec)...)
		}
		return compute.IsNull(ctx, inner, kernelOpts(ec)...)

	case expr.WhenThenNode:
		return evalWhenThen(ctx, ec, n, df)

	case expr.OverNode:
		return evalOver(ctx, ec, n, df)

	case expr.FunctionNode:
		return evalFunction(ctx, ec, n, df)
	}
	return nil, fmt.Errorf("eval: unknown node %T", e.Node())
}

func evalBinary(ctx context.Context, ec EvalContext, n expr.BinaryNode, df *dataframe.DataFrame) (*series.Series, error) {
	if out, took, err := evalTemporalBinary(ctx, ec, n, df); took {
		return out, err
	}
	// Fast path: one side is a scalar literal. Route to the Lit kernels
	// so we skip materialising the literal as an n-row series and halve
	// the memory traffic of the reduction.
	if out, took, err := evalBinaryLiteralFast(ctx, ec, n, df); took || err != nil {
		return out, err
	}
	left, err := evalOperand(ctx, ec, n.Left, n.Right, df)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := evalOperand(ctx, ec, n.Right, n.Left, df)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	// Broadcast a scalar side (an aggregation or a length-1 literal)
	// to the other side, as polars does for `x - x.mean()`.
	if left.Len() != right.Len() && (left.Len() == 1 || right.Len() == 1) {
		lc, rc := left.Clone(), right.Clone()
		bl, br, err := broadcastPair(lc, rc, ec)
		if err != nil {
			lc.Release()
			rc.Release()
			return nil, err
		}
		defer bl.Release()
		defer br.Release()
		left, right = bl, br
	}

	lhs, rhs, err := promoteBinary(ctx, ec, left, right)
	if err != nil {
		return nil, err
	}
	if lhs != left {
		defer lhs.Release()
	}
	if rhs != right {
		defer rhs.Release()
	}

	opts := kernelOpts(ec)
	if n.Op == expr.OpDiv && lhs.DType().IsInteger() {
		// polars `/` is true division: integers divide as f64 (x / 0 is
		// inf or NaN). FloorDiv keeps integer semantics.
		return trueDivide(ctx, ec, lhs, rhs)
	}
	switch n.Op {
	case expr.OpAdd:
		return compute.Add(ctx, lhs, rhs, opts...)
	case expr.OpSub:
		return compute.Sub(ctx, lhs, rhs, opts...)
	case expr.OpMul:
		return compute.Mul(ctx, lhs, rhs, opts...)
	case expr.OpDiv:
		return compute.Div(ctx, lhs, rhs, opts...)
	case expr.OpEq:
		return compute.Eq(ctx, lhs, rhs, opts...)
	case expr.OpNe:
		return compute.Ne(ctx, lhs, rhs, opts...)
	case expr.OpLt:
		return compute.Lt(ctx, lhs, rhs, opts...)
	case expr.OpLe:
		return compute.Le(ctx, lhs, rhs, opts...)
	case expr.OpGt:
		return compute.Gt(ctx, lhs, rhs, opts...)
	case expr.OpGe:
		return compute.Ge(ctx, lhs, rhs, opts...)
	case expr.OpAnd:
		return compute.And(ctx, lhs, rhs, opts...)
	case expr.OpOr:
		return compute.Or(ctx, lhs, rhs, opts...)
	}
	return nil, fmt.Errorf("eval: unknown binary op %d", n.Op)
}

func trueDivide(ctx context.Context, ec EvalContext, lhs, rhs *series.Series) (*series.Series, error) {
	opts := kernelOpts(ec)
	lf, err := compute.Cast(ctx, lhs, dtype.Float64(), opts...)
	if err != nil {
		return nil, err
	}
	defer lf.Release()
	rf, err := compute.Cast(ctx, rhs, dtype.Float64(), opts...)
	if err != nil {
		return nil, err
	}
	defer rf.Release()
	return compute.Div(ctx, lf, rf, opts...)
}

func evalUnary(ctx context.Context, ec EvalContext, n expr.UnaryNode, df *dataframe.DataFrame) (*series.Series, error) {
	inner, err := evalNode(ctx, ec, n.Arg, df)
	if err != nil {
		return nil, err
	}
	defer inner.Release()

	switch n.Op {
	case expr.OpNot:
		return compute.Not(ctx, inner, kernelOpts(ec)...)
	case expr.OpNeg:
		if out, ok, err := negateTemporal(ctx, ec, inner); ok {
			return out, err
		}
		switch inner.DType().ID() {
		case dtype.Int32().ID(), dtype.Int64().ID():
			one, err := series.FromInt64("one", fillInt64(-1, inner.Len()), nil,
				series.WithAllocator(ec.Alloc))
			if err != nil {
				return nil, err
			}
			defer one.Release()
			return compute.Mul(ctx, inner, one, kernelOpts(ec)...)
		case dtype.Float32().ID(), dtype.Float64().ID():
			one, err := series.FromFloat64("one", fillFloat64(-1, inner.Len()), nil,
				series.WithAllocator(ec.Alloc))
			if err != nil {
				return nil, err
			}
			defer one.Release()
			return compute.Mul(ctx, inner, one, kernelOpts(ec)...)
		}
		return nil, fmt.Errorf("eval: Neg on %s not supported", inner.DType())
	}
	return nil, fmt.Errorf("eval: unknown unary op %d", n.Op)
}

func evalAgg(ctx context.Context, ec EvalContext, n expr.AggNode, df *dataframe.DataFrame) (*series.Series, error) {
	inner, err := evalNode(ctx, ec, n.Inner, df)
	if err != nil {
		return nil, err
	}
	defer inner.Release()
	if out, ok, err := temporalAgg(n, inner, ec); ok {
		return out, err
	}
	name := expr.OutputName(n.Inner)
	opts := kernelOpts(ec)

	switch n.Op {
	case expr.AggSum:
		if inner.DType().IsFloating() {
			v, err := compute.SumFloat64(ctx, inner, opts...)
			if err != nil {
				return nil, err
			}
			return series.FromFloat64(name, []float64{v}, nil, series.WithAllocator(ec.Alloc))
		}
		v, err := compute.SumInt64(ctx, inner, opts...)
		if err != nil {
			return nil, err
		}
		return series.FromInt64(name, []int64{v}, nil, series.WithAllocator(ec.Alloc))

	case expr.AggMean:
		v, ok, err := compute.MeanFloat64(ctx, inner, opts...)
		if err != nil {
			return nil, err
		}
		vs := []float64{v}
		var valid []bool
		if !ok {
			valid = []bool{false}
		}
		return series.FromFloat64(name, vs, valid, series.WithAllocator(ec.Alloc))

	case expr.AggMin:
		if inner.DType().IsFloating() {
			v, ok, err := compute.MinFloat64(ctx, inner, opts...)
			if err != nil {
				return nil, err
			}
			vs := []float64{v}
			var valid []bool
			if !ok {
				valid = []bool{false}
			}
			return series.FromFloat64(name, vs, valid, series.WithAllocator(ec.Alloc))
		}
		v, ok, err := compute.MinInt64(ctx, inner, opts...)
		if err != nil {
			return nil, err
		}
		vs := []int64{v}
		var valid []bool
		if !ok {
			valid = []bool{false}
		}
		return series.FromInt64(name, vs, valid, series.WithAllocator(ec.Alloc))

	case expr.AggMax:
		if inner.DType().IsFloating() {
			v, ok, err := compute.MaxFloat64(ctx, inner, opts...)
			if err != nil {
				return nil, err
			}
			vs := []float64{v}
			var valid []bool
			if !ok {
				valid = []bool{false}
			}
			return series.FromFloat64(name, vs, valid, series.WithAllocator(ec.Alloc))
		}
		v, ok, err := compute.MaxInt64(ctx, inner, opts...)
		if err != nil {
			return nil, err
		}
		vs := []int64{v}
		var valid []bool
		if !ok {
			valid = []bool{false}
		}
		return series.FromInt64(name, vs, valid, series.WithAllocator(ec.Alloc))

	case expr.AggCount:
		// polars returns counts as u32.
		v := uint32(compute.Count(inner))
		return series.FromUint32(name, []uint32{v}, nil, series.WithAllocator(ec.Alloc))

	case expr.AggNullCount:
		v := uint32(compute.NullCount(inner))
		return series.FromUint32(name, []uint32{v}, nil, series.WithAllocator(ec.Alloc))

	case expr.AggFirst:
		if inner.Len() == 0 {
			// polars returns null for the first value of an empty column.
			return series.FullNull(name, inner.Chunked().DataType(), 1, series.WithAllocator(ec.Alloc))
		}
		return inner.Slice(0, 1)

	case expr.AggLast:
		if inner.Len() == 0 {
			return series.FullNull(name, inner.Chunked().DataType(), 1, series.WithAllocator(ec.Alloc))
		}
		return inner.Slice(inner.Len()-1, 1)
	}
	return nil, fmt.Errorf("eval: unknown agg op %d", n.Op)
}

func literalSeries(l expr.LitNode, n int, alloc memory.Allocator) (*series.Series, error) {
	if s, ok, err := temporalLiteralSeries(l, n, EvalContext{Alloc: alloc}); ok {
		return s, err
	}
	if l.Value == nil {
		// A typed null literal broadcasts like any other literal.
		return series.FullNull("literal", l.DType.Arrow(), n, series.WithAllocator(alloc))
	}
	switch v := l.Value.(type) {
	case int64:
		vs := fillInt64(v, n)
		return series.FromInt64(fmt.Sprintf("lit(%d)", v), vs, nil, series.WithAllocator(alloc))
	case float64:
		vs := fillFloat64(v, n)
		return series.FromFloat64(fmt.Sprintf("lit(%g)", v), vs, nil, series.WithAllocator(alloc))
	case bool:
		vs := make([]bool, n)
		if v {
			for i := range vs {
				vs[i] = true
			}
		}
		name := "lit(false)"
		if v {
			name = "lit(true)"
		}
		return series.FromBool(name, vs, nil, series.WithAllocator(alloc))
	case string:
		vs := make([]string, n)
		for i := range vs {
			vs[i] = v
		}
		return series.FromString(fmt.Sprintf("lit(%q)", v), vs, nil, series.WithAllocator(alloc))
	}
	return nil, fmt.Errorf("eval: unsupported literal type %T", l.Value)
}

func fillInt64(v int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func fillFloat64(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func promoteBinary(ctx context.Context, ec EvalContext, left, right *series.Series) (*series.Series, *series.Series, error) {
	if left.DType().Equal(right.DType()) {
		return left, right, nil
	}
	if l, r, ok, err := promoteExtra(ctx, ec, left, right); ok {
		return l, r, err
	}
	if (left.DType().IsDictionary() || right.DType().IsDictionary()) &&
		(left.DType().IsString() || right.DType().IsString() || (left.DType().IsDictionary() && right.DType().IsDictionary())) {
		// Categorical against str (or another categorical) compares by
		// category string; compute decodes the dictionary side.
		return left, right, nil
	}
	lInt := left.DType().IsInteger()
	rInt := right.DType().IsInteger()
	lFloat := left.DType().IsFloating()
	rFloat := right.DType().IsFloating()

	if lInt && rFloat {
		promoted, err := intToFloat64(ctx, left, ec.Alloc)
		if err != nil {
			return nil, nil, err
		}
		return promoted, right, nil
	}
	if lFloat && rInt {
		promoted, err := intToFloat64(ctx, right, ec.Alloc)
		if err != nil {
			return nil, nil, err
		}
		return left, promoted, nil
	}
	return nil, nil, fmt.Errorf("eval: cannot combine %s and %s: %w",
		left.DType(), right.DType(), compute.ErrDTypeMismatch)
}

func intToFloat64(ctx context.Context, s *series.Series, alloc memory.Allocator) (*series.Series, error) {
	_ = ctx
	n := s.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	hasNulls := s.NullCount() > 0
	chunk := s.Chunk(0)

	switch s.DType().ID() {
	case dtype.Int32().ID():
		raw := chunk.(*array.Int32).Int32Values()
		for i := range out {
			if hasNulls && !chunk.IsValid(i) {
				continue
			}
			out[i] = float64(raw[i])
			valid[i] = true
		}
	case dtype.Int64().ID():
		raw := chunk.(*array.Int64).Int64Values()
		for i := range out {
			if hasNulls && !chunk.IsValid(i) {
				continue
			}
			out[i] = float64(raw[i])
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("eval: intToFloat64 unsupported for %s", s.DType())
	}
	if !hasNulls {
		valid = nil
	}
	return series.FromFloat64(s.Name(), out, valid, series.WithAllocator(alloc))
}

func kernelOpts(ec EvalContext) []compute.Option {
	opts := []compute.Option{compute.WithAllocator(ec.Alloc)}
	if ec.Parallelism > 0 {
		opts = append(opts, compute.WithParallelism(ec.Parallelism))
	}
	return opts
}
