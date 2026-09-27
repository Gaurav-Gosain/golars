package eval

import (
	"context"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalNamespaceFunction routes FunctionNodes of the newer expression
// namespaces (arr., bin., cat., name.) to their dispatchers. ok is
// false when the name belongs to none of them, so the caller keeps
// looking in the general function table.
func evalNamespaceFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	ns, _, found := strings.Cut(n.Name, ".")
	if !found {
		return nil, false, nil
	}
	var (
		out *series.Series
		err error
	)
	switch ns {
	case "arr":
		out, err = evalArrFunction(ctx, ec, n, df)
	case "bin":
		out, err = evalBinFunction(ctx, ec, n, df)
	case "cat":
		out, err = evalCatFunction(ctx, ec, n, df)
	case "name":
		out, err = evalNameFunction(ctx, ec, n, df)
	default:
		return nil, false, nil
	}
	return out, true, err
}
