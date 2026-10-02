package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalArrFunction dispatches "arr." FunctionNodes to series.ArrOps.
func evalArrFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	p := params{n}
	return evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
		a := in.Arr()
		opt := seriesAlloc(ec)
		switch n.Name {
		case "arr.len":
			return a.Len(opt)
		case "arr.sum":
			return a.Sum(opt)
		case "arr.mean":
			return a.Mean(opt)
		case "arr.median":
			return a.Median(opt)
		case "arr.std":
			return a.Std(p.intOr(0, 1), opt)
		case "arr.var":
			return a.Var(p.intOr(0, 1), opt)
		case "arr.min":
			return a.Min(opt)
		case "arr.max":
			return a.Max(opt)
		case "arr.arg_min":
			return a.ArgMin(opt)
		case "arr.arg_max":
			return a.ArgMax(opt)
		case "arr.n_unique":
			return a.NUnique(opt)
		case "arr.any":
			return a.Any(opt)
		case "arr.all":
			return a.All(opt)
		case "arr.get":
			return a.Get(p.int(0), p.boolOr(1, false), opt)
		case "arr.first":
			return a.First(opt)
		case "arr.last":
			return a.Last(opt)
		case "arr.contains":
			v, _ := p.get(0)
			return a.Contains(v, opt)
		case "arr.count_matches":
			v, _ := p.get(0)
			return a.CountMatches(v, opt)
		case "arr.join":
			return a.Join(p.str(0), p.boolOr(1, true), opt)
		case "arr.reverse":
			return a.Reverse(opt)
		case "arr.sort":
			return a.Sort(p.boolOr(0, false), p.boolOr(1, false), opt)
		case "arr.shift":
			return a.Shift(p.intOr(0, 1), opt)
		case "arr.unique":
			return a.Unique(p.boolOr(0, false), opt)
		case "arr.head":
			return a.Head(p.intOr(0, 5), opt)
		case "arr.tail":
			return a.Tail(p.intOr(0, 5), opt)
		case "arr.slice":
			return a.Slice(p.int(0), p.intOr(1, -1), opt)
		case "arr.explode":
			return a.Explode(opt)
		case "arr.to_list":
			return a.ToList(opt)
		case "arr.to_struct":
			return a.ToStruct(p.strs(0), opt)
		}
		return nil, fmt.Errorf("unknown arr op")
	})
}
