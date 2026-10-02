package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalStructFunction dispatches FunctionNodes whose Name starts with
// "struct." to the matching series.StructOps kernel.
func evalStructFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if out, ok, err := evalStrJSONFunction(ctx, ec, n, df); ok {
		return out, err
	}
	if out, ok, err := evalStructExtFunction(ctx, ec, n, df); ok {
		return out, err
	}
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer arg0.Release()
	st := arg0.Struct()

	switch n.Name {
	case "struct.field":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: struct.field requires a field name")
		}
		name, ok := n.Params[0].(string)
		if !ok {
			return nil, fmt.Errorf("eval: struct.field name must be string, got %T", n.Params[0])
		}
		return st.Field(name)
	}
	return nil, fmt.Errorf("eval: unknown struct op %q", n.Name)
}
