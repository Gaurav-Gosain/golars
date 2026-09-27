package difftest

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// Outcome classes of one engine run.
var (
	// ErrUnsupported marks a plan golars cannot express: a glr parse
	// failure or a frame operation golars lacks.
	ErrUnsupported = errors.New("unsupported")
)

// PanicError wraps a recovered golars panic.
type PanicError struct {
	Value any
	Stack string
}

func (p *PanicError) Error() string { return fmt.Sprintf("panic: %v", p.Value) }

// RunGolars executes plan over left (and right) and returns the result.
// A golars panic is recovered and returned as *PanicError.
func RunGolars(ctx context.Context, p *Plan, left, right *dataframe.DataFrame) (out *dataframe.DataFrame, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = &PanicError{Value: r, Stack: string(debug.Stack())}
		}
	}()
	lf, err := BuildLazy(p, left, right)
	if err != nil {
		return nil, err
	}
	return lf.Collect(ctx)
}

func parseExprs(es []*Expr) ([]expr.Expr, error) {
	out := make([]expr.Expr, len(es))
	for i, e := range es {
		x, err := parseExpr(e)
		if err != nil {
			return nil, err
		}
		out[i] = x
	}
	return out, nil
}

func parseExpr(e *Expr) (expr.Expr, error) {
	src := e.GLR()
	x, err := exprparse.Parse(src)
	if err != nil {
		return expr.Expr{}, fmt.Errorf("%w: glr: %v [in %s]", ErrUnsupported, err, src)
	}
	return x, nil
}

// BuildLazy turns the plan into a golars LazyFrame.
func BuildLazy(p *Plan, left, right *dataframe.DataFrame) (lazy.LazyFrame, error) {
	lf := lazy.FromDataFrame(left.Clone())
	for _, op := range p.Ops {
		var err error
		lf, err = applyOp(lf, op, right)
		if err != nil {
			return lf, err
		}
	}
	return lf, nil
}

func applyOp(lf lazy.LazyFrame, op Op, right *dataframe.DataFrame) (lazy.LazyFrame, error) {
	switch op.Op {
	case "filter":
		pred, err := parseExpr(op.Pred)
		if err != nil {
			return lf, err
		}
		return lf.Filter(pred), nil
	case "select":
		es, err := parseExprs(op.Exprs)
		if err != nil {
			return lf, err
		}
		return lf.Select(es...), nil
	case "with_columns":
		es, err := parseExprs(op.Exprs)
		if err != nil {
			return lf, err
		}
		return lf.WithColumns(es...), nil
	case "group_by":
		es, err := parseExprs(op.Exprs)
		if err != nil {
			return lf, err
		}
		g := lf.GroupBy(op.Keys...)
		if op.Maintain {
			g = g.MaintainOrder()
		}
		return g.Agg(es...), nil
	case "sort":
		opts := make([]compute.SortOptions, len(op.Keys))
		for i := range opts {
			opts[i].Descending = op.Desc[i]
			if op.NullsLast[i] {
				opts[i].Nulls = compute.NullsLast
			}
		}
		return lf.SortBy(op.Keys, opts), nil
	case "join":
		if right == nil {
			return lf, errors.New("difftest: join without right frame")
		}
		rl := lazy.FromDataFrame(right.Clone())
		switch op.How {
		case "inner":
			return lf.Join(rl, op.Keys, dataframe.InnerJoin), nil
		case "left":
			return lf.Join(rl, op.Keys, dataframe.LeftJoin), nil
		case "cross":
			return lf.Join(rl, nil, dataframe.CrossJoin), nil
		}
		return lf, fmt.Errorf("%w: join how=%s", ErrUnsupported, op.How)
	case "join_asof":
		rl := lazy.FromDataFrame(right.Clone())
		o := dataframe.AsofOptions{On: op.Keys[0], By: op.By}
		switch op.Strategy {
		case "forward":
			o.Strategy = dataframe.AsofForward
		case "nearest":
			o.Strategy = dataframe.AsofNearest
		}
		return lf.JoinAsof(rl, o), nil
	case "unique":
		if len(op.Keys) > 0 {
			return lf, fmt.Errorf("%w: unique with subset", ErrUnsupported)
		}
		return lf.Unique(), nil
	case "slice":
		return lf.Slice(op.Offset, op.N), nil
	case "head":
		return lf.Head(op.N), nil
	case "tail":
		return lf.Tail(op.N), nil
	case "explode":
		return lf.Explode(op.Keys...), nil
	case "unpivot":
		return lf.Unpivot(lazy.UnpivotOptions{On: op.On, Index: op.Index}), nil
	case "pivot":
		cols := make([]string, len(op.OnValues))
		for i, v := range op.OnValues {
			cols[i] = fmt.Sprint(v)
		}
		return lf.Pivot(lazy.PivotOptions{On: op.On[0], OnColumns: cols, Index: op.Index, Values: op.Values, Agg: dataframe.PivotAgg(op.Agg)}), nil
	case "with_row_index":
		return lf.WithRowIndex(op.Name, int64(op.Offset)), nil
	case "drop_nulls":
		return lf.DropNulls(op.Keys...), nil
	case "drop":
		return lf.Drop(op.Keys...), nil
	case "rename":
		return lf.Rename(op.Keys[0], op.Name), nil
	case "reverse":
		return lf.Reverse(), nil
	}
	return lf, fmt.Errorf("%w: op %s", ErrUnsupported, op.Op)
}
