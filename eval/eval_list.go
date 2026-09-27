package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalListFunction dispatches FunctionNodes whose Name starts with
// "list." to the matching series.ListOps kernel. Scalar arguments come
// from Params (see expr/list.go and expr/list_ns.go); concat and the
// set operations evaluate their extra Args.
func evalListFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	switch n.Name {
	case "list.eval", "list.filter":
		return evalListEvalFunction(ctx, ec, n, df)
	case "list.concat", "list.set_union", "list.set_intersection",
		"list.set_difference", "list.set_symmetric_difference":
		return evalListMulti(ctx, ec, n, df)
	}
	p := params{n}
	return evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
		lst := in.List()
		opt := seriesAlloc(ec)
		switch n.Name {
		case "list.len":
			return lst.Len(opt)
		case "list.sum":
			return lst.Sum(opt)
		case "list.mean":
			return lst.Mean(opt)
		case "list.min":
			return lst.Min(opt)
		case "list.max":
			return lst.Max(opt)
		case "list.median":
			return lst.Median(opt)
		case "list.std":
			return lst.Std(p.intOr(0, 1), opt)
		case "list.var":
			return lst.Var(p.intOr(0, 1), opt)
		case "list.n_unique":
			return lst.NUnique(opt)
		case "list.arg_min":
			return lst.ArgMin(opt)
		case "list.arg_max":
			return lst.ArgMax(opt)
		case "list.any":
			return lst.Any(opt)
		case "list.all":
			return lst.All(opt)
		case "list.first":
			return lst.First(opt)
		case "list.last":
			return lst.Last(opt)
		case "list.get":
			if len(n.Params) < 1 {
				return nil, fmt.Errorf("list.get requires an index")
			}
			idx, err := intParam(n.Params[0])
			if err != nil {
				return nil, err
			}
			return lst.GetChecked(idx, p.boolOr(1, true), opt)
		case "list.item":
			return lst.Item(opt)
		case "list.contains":
			if len(n.Params) < 1 {
				return nil, fmt.Errorf("list.contains requires a needle")
			}
			return lst.Contains(n.Params[0], opt)
		case "list.count_matches":
			v, _ := p.get(0)
			return lst.CountMatches(v, opt)
		case "list.join":
			if len(n.Params) < 1 {
				return nil, fmt.Errorf("list.join requires a separator")
			}
			sep, ok := n.Params[0].(string)
			if !ok {
				return nil, fmt.Errorf("list.join separator must be string, got %T", n.Params[0])
			}
			return lst.Join(sep, p.boolOr(1, true), opt)
		case "list.drop_nulls":
			return lst.DropNulls(opt)
		case "list.reverse":
			return lst.Reverse(opt)
		case "list.sort":
			return lst.Sort(p.boolOr(0, false), p.boolOr(1, false), opt)
		case "list.unique":
			return lst.Unique(p.boolOr(0, false), opt)
		case "list.head":
			return lst.Head(p.intOr(0, 5), opt)
		case "list.tail":
			return lst.Tail(p.intOr(0, 5), opt)
		case "list.slice":
			return lst.Slice(p.int(0), p.intOr(1, -1), opt)
		case "list.shift":
			return lst.Shift(p.intOr(0, 1), opt)
		case "list.diff":
			return lst.Diff(p.intOr(0, 1), opt)
		case "list.gather":
			v, _ := p.get(0)
			idx, ok := v.([]int)
			if !ok {
				return nil, fmt.Errorf("list.gather indices must be []int, got %T", v)
			}
			return lst.Gather(idx, p.boolOr(1, false), opt)
		case "list.gather_every":
			return lst.GatherEvery(p.int(0), p.int(1), opt)
		case "list.explode":
			return lst.Explode(opt)
		case "list.to_struct":
			return lst.ToStruct(p.strs(0), p.boolOr(1, false), opt)
		case "list.to_array":
			return lst.ToArray(p.int(0), opt)
		case "list.sample":
			return lst.Sample(p.int(0), p.boolOr(1, false), p.boolOr(2, false), p.uint64(3), opt)
		case "list.sample_fraction":
			return lst.SampleFraction(p.float(0), p.boolOr(1, false), p.boolOr(2, false), p.uint64(3), opt)
		}
		return nil, fmt.Errorf("unknown list op")
	})
}

// evalListMulti evaluates concat and the set operations, whose extra
// inputs are expressions.
func evalListMulti(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) < 2 && n.Name != "list.concat" {
		return nil, fmt.Errorf("eval: %s requires two inputs", n.Name)
	}
	inputs := make([]*series.Series, 0, len(n.Args))
	defer func() {
		for _, s := range inputs {
			s.Release()
		}
	}()
	for _, a := range n.Args {
		s, err := evalNode(ctx, ec, a, df)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, s)
	}
	lst := inputs[0].List()
	opt := seriesAlloc(ec)
	var (
		out *series.Series
		err error
	)
	switch n.Name {
	case "list.concat":
		out, err = lst.Concat(inputs[1:], opt)
	case "list.set_union":
		out, err = lst.SetOperation(inputs[1], series.SetUnion, opt)
	case "list.set_intersection":
		out, err = lst.SetOperation(inputs[1], series.SetIntersection, opt)
	case "list.set_difference":
		out, err = lst.SetOperation(inputs[1], series.SetDifference, opt)
	default:
		out, err = lst.SetOperation(inputs[1], series.SetSymmetricDifference, opt)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", n.Name, err)
	}
	return out, nil
}

// intParam coerces a positional Param to int. Lit's switch accepts
// int / int32 / int64; accept the same here.
func intParam(p any) (int, error) {
	switch v := p.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	}
	return 0, fmt.Errorf("expected integer, got %T", p)
}
