package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalBinFunction dispatches "bin." FunctionNodes to series.BinOps.
func evalBinFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer arg0.Release()
	ops := arg0.Bin()
	opt := seriesAlloc(ec)

	switch n.Name {
	case "bin.contains", "bin.starts_with", "bin.ends_with":
		pat, err := binParamBytes(n, 0)
		if err != nil {
			return nil, err
		}
		switch n.Name {
		case "bin.contains":
			return ops.Contains(pat, opt)
		case "bin.starts_with":
			return ops.StartsWith(pat, opt)
		}
		return ops.EndsWith(pat, opt)
	case "bin.encode":
		enc, err := binParamString(n, 0)
		if err != nil {
			return nil, err
		}
		return ops.Encode(enc, opt)
	case "bin.decode":
		enc, err := binParamString(n, 0)
		if err != nil {
			return nil, err
		}
		strict, err := binParamBool(n, 1, true)
		if err != nil {
			return nil, err
		}
		return ops.Decode(enc, strict, opt)
	case "bin.size":
		unit := "b"
		if len(n.Params) > 0 {
			if unit, err = binParamString(n, 0); err != nil {
				return nil, err
			}
		}
		return ops.Size(unit, opt)
	case "bin.get":
		idx, err := strParamInt(n, 0)
		if err != nil {
			return nil, err
		}
		oob, err := binParamBool(n, 1, false)
		if err != nil {
			return nil, err
		}
		return ops.Get(idx, oob, opt)
	case "bin.head", "bin.tail":
		k := 5
		if len(n.Params) > 0 {
			if k, err = strParamInt(n, 0); err != nil {
				return nil, err
			}
		}
		if n.Name == "bin.head" {
			return ops.Head(k, opt)
		}
		return ops.Tail(k, opt)
	case "bin.slice":
		off, err := strParamInt(n, 0)
		if err != nil {
			return nil, err
		}
		length := -1
		if len(n.Params) > 1 {
			if length, err = strParamInt(n, 1); err != nil {
				return nil, err
			}
		}
		return ops.Slice(off, length, opt)
	case "bin.reinterpret":
		if len(n.Params) < 1 {
			return nil, fmt.Errorf("eval: bin.reinterpret requires a dtype")
		}
		to, ok := n.Params[0].(dtype.DType)
		if !ok {
			return nil, fmt.Errorf("eval: bin.reinterpret: expected dtype.DType, got %T", n.Params[0])
		}
		little, err := binParamBool(n, 1, true)
		if err != nil {
			return nil, err
		}
		return ops.Reinterpret(to, little, opt)
	}
	return nil, fmt.Errorf("eval: unknown bin op %q", n.Name)
}

// binParamBytes accepts []byte or string for byte-pattern params.
func binParamBytes(n expr.FunctionNode, i int) ([]byte, error) {
	if i >= len(n.Params) {
		return nil, fmt.Errorf("eval: %s: missing bytes parameter", n.Name)
	}
	switch v := n.Params[i].(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	}
	return nil, fmt.Errorf("eval: %s: expected []byte parameter, got %T", n.Name, n.Params[i])
}

func binParamString(n expr.FunctionNode, i int) (string, error) {
	if i >= len(n.Params) {
		return "", fmt.Errorf("eval: %s: missing string parameter", n.Name)
	}
	s, ok := n.Params[i].(string)
	if !ok {
		return "", fmt.Errorf("eval: %s: expected string parameter, got %T", n.Name, n.Params[i])
	}
	return s, nil
}

// binParamBool reads an optional bool param, falling back to def.
func binParamBool(n expr.FunctionNode, i int, def bool) (bool, error) {
	if i >= len(n.Params) {
		return def, nil
	}
	b, ok := n.Params[i].(bool)
	if !ok {
		return false, fmt.Errorf("eval: %s: expected bool parameter, got %T", n.Name, n.Params[i])
	}
	return b, nil
}
