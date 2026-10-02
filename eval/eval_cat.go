package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalCatFunction dispatches "cat." FunctionNodes to series.CatOps.
func evalCatFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer arg0.Release()
	ops := arg0.Cat()
	opt := seriesAlloc(ec)

	switch n.Name {
	case "cat.get_categories":
		return ops.GetCategories(opt)
	case "cat.len_bytes":
		return ops.LenBytes(opt)
	case "cat.len_chars":
		return ops.LenChars(opt)
	case "cat.starts_with", "cat.ends_with":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: %s: missing string parameter", n.Name)
		}
		s, ok := n.Params[0].(string)
		if !ok {
			return nil, fmt.Errorf("eval: %s: expected string parameter, got %T", n.Name, n.Params[0])
		}
		if n.Name == "cat.starts_with" {
			return ops.StartsWith(s, opt)
		}
		return ops.EndsWith(s, opt)
	case "cat.slice":
		if len(n.Params) < 2 {
			return nil, fmt.Errorf("eval: cat.slice requires offset and length")
		}
		off, err := intParam(n.Params[0])
		if err != nil {
			return nil, fmt.Errorf("eval: cat.slice offset: %w", err)
		}
		length, err := intParam(n.Params[1])
		if err != nil {
			return nil, fmt.Errorf("eval: cat.slice length: %w", err)
		}
		return ops.Slice(off, length, opt)
	}
	return nil, fmt.Errorf("eval: unknown cat op %q", n.Name)
}
