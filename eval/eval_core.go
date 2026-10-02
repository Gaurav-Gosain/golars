package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// coreFn evaluates one FunctionNode. Implementations own the lifetime
// of every Series they evaluate and return a fresh Series.
type coreFn func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error)

// coreFuncs is the name-keyed dispatch table for the core Expr
// functions. evalFunction consults it before its legacy switch, so an
// entry here also overrides a legacy case of the same name.
var coreFuncs = map[string]coreFn{}

func registerCore(name string, fn coreFn) {
	if _, dup := coreFuncs[name]; dup {
		panic("eval: duplicate core function " + name)
	}
	coreFuncs[name] = fn
}

// unaryCore adapts a Series-level kernel that consumes only Args[0].
func unaryCore(fn func(s *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error)) coreFn {
	return func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) < 1 {
			return nil, fmt.Errorf("eval: %s requires an input expression", n.Name)
		}
		s, err := evalNode(ctx, ec, n.Args[0], df)
		if err != nil {
			return nil, err
		}
		defer s.Release()
		return fn(s, n, ec)
	}
}

// binaryCore adapts a kernel over Args[0] and Args[1], broadcasting a
// length-1 side to the other side's length.
func binaryCore(fn func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error)) coreFn {
	return func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: %s requires two input expressions", n.Name)
		}
		a, b, err := evalPair(ctx, ec, n.Args[0], n.Args[1], df)
		if err != nil {
			return nil, err
		}
		defer a.Release()
		defer b.Release()
		return fn(a, b, n, ec)
	}
}

// binaryCoreAdopt is binaryCore for arithmetic functions (mod,
// floordiv): an untyped literal operand first takes the other
// operand's dtype, as for the binary operators.
func binaryCoreAdopt(fn func(a, b *series.Series, n expr.FunctionNode, ec EvalContext) (*series.Series, error)) coreFn {
	return func(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
		if len(n.Args) < 2 {
			return nil, fmt.Errorf("eval: %s requires two input expressions", n.Name)
		}
		a, b, err := evalPair(ctx, ec, n.Args[0], n.Args[1], df)
		if err != nil {
			return nil, err
		}
		ss := []*series.Series{a, b}
		defer releaseAll(ss)
		if err := adoptDynLiterals(ctx, ec, n.Args[:2], ss); err != nil {
			return nil, err
		}
		return fn(ss[0], ss[1], n, ec)
	}
}

// evalPair evaluates two expressions and broadcasts a length-1 result
// to the other's length.
func evalPair(ctx context.Context, ec EvalContext, x, y expr.Expr, df *dataframe.DataFrame) (*series.Series, *series.Series, error) {
	a, err := evalOperand(ctx, ec, x, y, df)
	if err != nil {
		return nil, nil, err
	}
	b, err := evalOperand(ctx, ec, y, x, df)
	if err != nil {
		a.Release()
		return nil, nil, err
	}
	a2, b2, err := broadcastPair(a, b, ec)
	if err != nil {
		a.Release()
		b.Release()
		return nil, nil, err
	}
	return a2, b2, nil
}

// broadcastPair consumes a and b and returns equal-length Series. A
// length-1 side is repeated to match the other side.
func broadcastPair(a, b *series.Series, ec EvalContext) (*series.Series, *series.Series, error) {
	if a.Len() == b.Len() {
		return a, b, nil
	}
	switch {
	case a.Len() == 1:
		na, err := a.Broadcast(b.Len(), seriesAlloc(ec))
		if err != nil {
			return nil, nil, err
		}
		a.Release()
		return na, b, nil
	case b.Len() == 1:
		nb, err := b.Broadcast(a.Len(), seriesAlloc(ec))
		if err != nil {
			return nil, nil, err
		}
		b.Release()
		return a, nb, nil
	}
	return nil, nil, fmt.Errorf("eval: length mismatch %d vs %d", a.Len(), b.Len())
}

// evalArgs evaluates every argument expression. On error every already
// evaluated Series is released.
func evalArgs(ctx context.Context, ec EvalContext, args []expr.Expr, df *dataframe.DataFrame) ([]*series.Series, error) {
	out := make([]*series.Series, 0, len(args))
	allLit := true
	for _, a := range args {
		if !isLitExpr(a) {
			allLit = false
		}
	}
	for _, a := range args {
		var s *series.Series
		var err error
		if isLitExpr(a) && !allLit {
			s, err = evalScalar(ctx, ec, a, df)
		} else {
			s, err = evalNode(ctx, ec, a, df)
		}
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func releaseAll(ss []*series.Series) {
	for _, s := range ss {
		if s != nil {
			s.Release()
		}
	}
}

// Param accessors. Params are carried untyped in FunctionNode; these
// helpers coerce the usual Go numeric kinds and fall back to def.

func paramAny(n expr.FunctionNode, i int) any {
	if i < len(n.Params) {
		return n.Params[i]
	}
	return nil
}

func paramInt(n expr.FunctionNode, i int, def int) int {
	switch v := paramAny(n, i).(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	}
	return def
}

func paramFloat(n expr.FunctionNode, i int, def float64) float64 {
	switch v := paramAny(n, i).(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return def
}

func paramBool(n expr.FunctionNode, i int, def bool) bool {
	if v, ok := paramAny(n, i).(bool); ok {
		return v
	}
	return def
}

func paramString(n expr.FunctionNode, i int, def string) string {
	if v, ok := paramAny(n, i).(string); ok {
		return v
	}
	return def
}

func paramDType(n expr.FunctionNode, i int) (dtype.DType, bool) {
	if v, ok := paramAny(n, i).(dtype.DType); ok && v.IsValid() {
		return v, true
	}
	return dtype.DType{}, false
}

// renamed returns s renamed to name, consuming s.
func renamed(s *series.Series, name string) *series.Series {
	if s.Name() == name {
		return s
	}
	out := s.Rename(name)
	s.Release()
	return out
}

func isLitExpr(e expr.Expr) bool {
	_, ok := e.Node().(expr.LitNode)
	return ok
}

// evalOperand evaluates e as one operand of a binary operation. A
// literal next to a non-literal is evaluated as a single row, as polars
// does, and later broadcast; this keeps `x.sum() + 1` a scalar.
func evalOperand(ctx context.Context, ec EvalContext, e, other expr.Expr, df *dataframe.DataFrame) (*series.Series, error) {
	if isLitExpr(e) && !isLitExpr(other) {
		return evalScalar(ctx, ec, e, df)
	}
	return evalNode(ctx, ec, e, df)
}
