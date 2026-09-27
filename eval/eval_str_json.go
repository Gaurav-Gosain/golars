package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalStrJSONFunction handles the str.* and struct.* kernels that deal
// with transfer encodings and JSON: str.encode, str.decode,
// str.json_decode, str.json_path_match and struct.json_encode. ok is
// false when n is not one of them.
func evalStrJSONFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	switch n.Name {
	case "str.encode", "str.decode", "str.json_decode", "str.json_path_match", "struct.json_encode":
	default:
		return nil, false, nil
	}
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, true, err
	}
	defer arg0.Release()
	opt := seriesAlloc(ec)
	out, err := dispatchStrJSON(n, arg0, opt)
	return out, true, err
}

func dispatchStrJSON(n expr.FunctionNode, arg0 *series.Series, opt series.Option) (*series.Series, error) {
	switch n.Name {
	case "str.encode":
		enc, err := binParamString(n, 0)
		if err != nil {
			return nil, err
		}
		return arg0.Str().Encode(enc, opt)
	case "str.decode":
		enc, err := binParamString(n, 0)
		if err != nil {
			return nil, err
		}
		strict, err := binParamBool(n, 1, true)
		if err != nil {
			return nil, err
		}
		return arg0.Str().Decode(enc, strict, opt)
	case "str.json_decode":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: str.json_decode requires a dtype")
		}
		to, ok := n.Params[0].(dtype.DType)
		if !ok {
			return nil, fmt.Errorf("eval: str.json_decode: expected dtype.DType, got %T", n.Params[0])
		}
		return arg0.Str().JSONDecode(to, opt)
	case "str.json_path_match":
		path, err := binParamString(n, 0)
		if err != nil {
			return nil, err
		}
		return arg0.Str().JSONPathMatch(path, opt)
	}
	return arg0.Struct().JSONEncode(opt)
}
