package eval

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// dynLit returns the untyped numeric literal e is (possibly under an
// alias), if any.
func dynLit(e expr.Expr) (expr.LitNode, bool) {
	neg := false
	for {
		switch n := e.Node().(type) {
		case expr.AliasNode:
			e = n.Inner
			continue
		case expr.UnaryNode:
			// -lit(2) is still an untyped literal in polars.
			if n.Op != expr.OpNeg {
				return expr.LitNode{}, false
			}
			neg = !neg
			e = n.Arg
			continue
		case expr.LitNode:
			if !n.Dyn || n.Value == nil {
				return expr.LitNode{}, false
			}
			if neg {
				switch v := n.Value.(type) {
				case int64:
					return expr.LitInt(-v).Node().(expr.LitNode), true
				case float64:
					return expr.LitFloat(-v).Node().(expr.LitNode), true
				}
			}
			return n, true
		}
		return expr.LitNode{}, false
	}
}

// dynTarget returns the dtype the untyped literal l takes next to an
// operand of dtype other. ok is false when l keeps its own dtype.
func dynTarget(l expr.LitNode, other dtype.DType) (dtype.DType, bool) {
	switch v := l.Value.(type) {
	case int64:
		return dtype.DynIntTarget(v, other)
	case float64:
		if other.IsNumeric() {
			return dtype.DynFloatTarget(other), true
		}
	}
	return dtype.DType{}, false
}

// litTargetDType returns the dtype `col OP l` computes in for a numeric
// column dtype colD: the column's dtype when an untyped literal fits,
// else the polars supertype of both.
func litTargetDType(l expr.LitNode, colD dtype.DType) (dtype.DType, bool) {
	litD := l.DType
	if l.Dyn {
		if t, ok := dynTarget(l, colD); ok {
			litD = t
		}
	}
	return dtype.NumericSupertype(colD, litD)
}

// adoptDynLiterals casts the untyped numeric literals among es (already
// evaluated into ss) to the dtype they take next to the other numeric
// operands, as polars does for when/then branches and binary operands:
// when(c).then(1).otherwise(col_i8) is i8, not i32. It replaces the
// entries of ss it casts.
func adoptDynLiterals(ctx context.Context, ec EvalContext, es []expr.Expr, ss []*series.Series) error {
	var other dtype.DType
	found := false
	for i, e := range es {
		if _, dyn := dynLit(e); dyn {
			continue
		}
		d := ss[i].DType()
		if !d.IsNumeric() && !d.IsBool() {
			continue
		}
		if !found {
			other, found = d, true
			continue
		}
		if st, ok := dtype.NumericSupertype(other, d); ok {
			other = st
		}
	}
	if !found {
		return nil
	}
	for i, e := range es {
		l, dyn := dynLit(e)
		if !dyn {
			continue
		}
		t, ok := dynTarget(l, other)
		if !ok || ss[i].DType().Equal(t) {
			continue
		}
		c, err := compute.Cast(ctx, ss[i], t, kernelOpts(ec)...)
		if err != nil {
			return err
		}
		ss[i].Release()
		ss[i] = c
	}
	return nil
}

func isLogicalOp(op expr.BinaryOp) bool { return op == expr.OpAnd || op == expr.OpOr }

// listInnerDType returns the element dtype of a list Series.
func listInnerDType(s *series.Series) (dtype.DType, bool) {
	lt, ok := s.DType().Arrow().(arrow.ListLikeType)
	if !ok {
		return dtype.DType{}, false
	}
	return dtype.FromArrow(lt.Elem()), true
}

func isUnsignedID(id arrow.Type) bool {
	switch id {
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

// concatStrings concatenates two equal-length string Series row by
// row; a null on either side gives null.
func concatStrings(a, b *series.Series, ec EvalContext) (*series.Series, error) {
	av, bv := a.ToList(), b.ToList()
	out := make([]string, len(av))
	valid := make([]bool, len(av))
	for i := range av {
		x, ok1 := av[i].(string)
		y, ok2 := bv[i].(string)
		if ok1 && ok2 {
			out[i], valid[i] = x+y, true
		}
	}
	return series.FromString(a.Name(), out, valid, seriesAlloc(ec))
}

// castAggResult casts the result of aggregation op over an input of
// dtype in to the dtype polars gives it (dtype.AggResultDType). It
// consumes out.
func castAggResult(ec EvalContext, op string, in dtype.DType, out *series.Series) (*series.Series, error) {
	want, ok := dtype.AggResultDType(op, in)
	if !ok || out.DType().Equal(want) || !(out.DType().IsNumeric() || out.DType().IsBool()) {
		return out, nil
	}
	defer out.Release()
	return compute.Cast(context.Background(), out, want, kernelOpts(ec)...)
}

// aggTyped returns a function that applies castAggResult to a kernel
// result: aggTyped(ec, "median", s)(s.MedianSeries(...)).
func aggTyped(ec EvalContext, op string, in *series.Series) func(*series.Series, error) (*series.Series, error) {
	dt := in.DType()
	return func(out *series.Series, err error) (*series.Series, error) {
		if err != nil {
			return nil, err
		}
		return castAggResult(ec, op, dt, out)
	}
}

// toUint32 casts a count result to u32, polars' dtype for lengths and
// counts. It consumes out.
func toUint32(out *series.Series, err error) (*series.Series, error) {
	if err != nil || out.DType().ID() == arrow.UINT32 {
		return out, err
	}
	defer out.Release()
	return compute.Cast(context.Background(), out, dtype.Uint32())
}

// cumTyped runs a cumulative kernel on s, widening inputs the kernel
// lacks to f64, and casts the result to the polars dtype of
// aggregation op (cum_sum of u8 is i64, of i32 stays i32).
func cumTyped(s *series.Series, op string, fn func(*series.Series) (*series.Series, error)) (*series.Series, error) {
	in := s.DType()
	out, err := fn(s)
	if err != nil && (in.IsNumeric() || in.IsBool()) {
		wide, cerr := compute.Cast(context.Background(), s, dtype.Float64())
		if cerr != nil {
			return nil, err
		}
		defer wide.Release()
		out, err = fn(wide)
	}
	if err != nil {
		return nil, err
	}
	want, ok := dtype.AggResultDType(op, in)
	if !ok || out.DType().Equal(want) {
		return out, nil
	}
	defer out.Release()
	return compute.Cast(context.Background(), out, want)
}

// temporalMedian is polars' median of a temporal column: the median of
// the physical values in f64, truncated back to ticks. A date median
// is a datetime[us] (the midpoint of two days can fall mid-day).
func temporalMedian(s *series.Series, ec EvalContext) (*series.Series, bool, error) {
	dt := s.DType()
	if !dt.IsTemporal() {
		return nil, false, nil
	}
	phys, err := compute.Cast(context.Background(), s, dtype.Int64(), kernelOpts(ec)...)
	if err != nil {
		return nil, true, err
	}
	defer phys.Release()
	med, err := phys.MedianSeries(seriesAlloc(ec))
	if err != nil {
		return nil, true, err
	}
	defer med.Release()
	target := dt
	scale := 1.0
	if dt.IsDate() {
		target = dtype.Datetime(dtype.Microsecond, "")
		scale = 86_400_000_000
	}
	vals := med.ToList()
	var ticks []any
	for _, v := range vals {
		if f, ok := v.(float64); ok {
			ticks = append(ticks, int64(f*scale))
		} else {
			ticks = append(ticks, nil)
		}
	}
	ints, err := series.FromValues(s.Name(), arrow.PrimitiveTypes.Int64, ticks, seriesAlloc(ec))
	if err != nil {
		return nil, true, err
	}
	defer ints.Release()
	out, err := compute.Cast(context.Background(), ints, target, kernelOpts(ec)...)
	return out, true, err
}

func isOrderingOp(op expr.BinaryOp) bool {
	return op == expr.OpLt || op == expr.OpLe || op == expr.OpGt || op == expr.OpGe
}
