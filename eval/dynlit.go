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
	for {
		switch n := e.Node().(type) {
		case expr.AliasNode:
			e = n.Inner
			continue
		case expr.LitNode:
			return n, n.Dyn && n.Value != nil
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
