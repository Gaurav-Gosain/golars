package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalStrFunction dispatches FunctionNodes whose Name starts with
// "str." to the matching series.StrOps kernel. Params are positional
// and typed per-kernel: Like/Contains/etc take a single string;
// Slice takes (int, int). Mismatches return an error.
//
// The dispatcher materialises arg0 first (the string column),
// defers its Release for the error path, and on success hands the
// freshly-built result back up.
func evalStrFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if out, ok, err := evalStrJSONFunction(ctx, ec, n, df); ok {
		return out, err
	}
	if out, ok, err := evalStrExtFunction(ctx, ec, n, df); ok {
		return out, err
	}
	arg0, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	release := arg0
	defer func() {
		if release != nil {
			release.Release()
		}
	}()
	opt := seriesAlloc(ec)
	strops := arg0.Str()

	switch n.Name {
	// ---- predicates ----
	case "str.contains":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.Contains(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.contains_regex":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.ContainsRegex(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.starts_with":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.StartsWith(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.ends_with":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.EndsWith(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.like":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.Like(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.not_like":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.NotLike(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})

	// ---- transforms ----
	case "str.to_lower":
		out, err := strops.Lower(opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.to_upper":
		out, err := strops.Upper(opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.trim":
		out, err := strops.Trim(opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.strip_prefix":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.StripPrefix(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.strip_suffix":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.StripSuffix(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})

	// ---- replace ----
	case "str.replace":
		return dispatchStr2(n, release, func(a, b string) (*series.Series, error) {
			out, err := strops.Replace(a, b, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.replace_all":
		return dispatchStr2(n, release, func(a, b string) (*series.Series, error) {
			out, err := strops.ReplaceAll(a, b, opt)
			release = nil
			arg0.Release()
			return out, err
		})

	// ---- measurement ----
	case "str.len_bytes":
		out, err := toUint32(strops.LenBytes(opt))
		release = nil
		arg0.Release()
		return out, err
	case "str.len_chars":
		out, err := toUint32(strops.LenChars(opt))
		release = nil
		arg0.Release()
		return out, err
	case "str.count_matches":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.CountMatches(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	case "str.find":
		return dispatchStr1(n, release, func(s string) (*series.Series, error) {
			out, err := strops.Find(s, opt)
			release = nil
			arg0.Release()
			return out, err
		})

	// ---- slicing ----
	case "str.head":
		n0, err := strParamInt(n, 0)
		if err != nil {
			return nil, err
		}
		out, err := strops.Head(n0, opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.tail":
		n0, err := strParamInt(n, 0)
		if err != nil {
			return nil, err
		}
		out, err := strops.Tail(n0, opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.slice":
		start, err := strParamInt(n, 0)
		if err != nil {
			return nil, err
		}
		length, err := strParamInt(n, 1)
		if err != nil {
			return nil, err
		}
		out, err := strops.Slice(start, length, opt)
		release = nil
		arg0.Release()
		return out, err
	case "str.split_exact":
		return dispatchStr1(n, release, func(sep string) (*series.Series, error) {
			out, err := strops.SplitExact(sep, opt)
			release = nil
			arg0.Release()
			return out, err
		})
	}
	return nil, fmt.Errorf("eval: unknown string function %q", n.Name)
}

// dispatchStr1 is the common shape for str.* kernels that take one
// string parameter in Params[0]. The closure owns the arg0.Release
// handoff so the caller's defer doesn't double-release.
func dispatchStr1(n expr.FunctionNode, _ *series.Series, do func(string) (*series.Series, error)) (*series.Series, error) {
	if len(n.Params) < 1 {
		return nil, fmt.Errorf("eval: %s: missing string parameter", n.Name)
	}
	s, ok := n.Params[0].(string)
	if !ok {
		return nil, fmt.Errorf("eval: %s: expected string parameter, got %T", n.Name, n.Params[0])
	}
	return do(s)
}

// dispatchStr2 is the two-string-parameter variant (Replace, ReplaceAll).
func dispatchStr2(n expr.FunctionNode, _ *series.Series, do func(a, b string) (*series.Series, error)) (*series.Series, error) {
	if len(n.Params) < 2 {
		return nil, fmt.Errorf("eval: %s: missing string parameters", n.Name)
	}
	a, ok := n.Params[0].(string)
	if !ok {
		return nil, fmt.Errorf("eval: %s: expected string, got %T", n.Name, n.Params[0])
	}
	b, ok := n.Params[1].(string)
	if !ok {
		return nil, fmt.Errorf("eval: %s: expected string, got %T", n.Name, n.Params[1])
	}
	return do(a, b)
}

// strParamInt extracts n.Params[i] as an int. We accept both int and
// int64 because fn1p callers don't coerce for us.
func strParamInt(n expr.FunctionNode, i int) (int, error) {
	if i >= len(n.Params) {
		return 0, fmt.Errorf("eval: %s: missing int parameter at index %d", n.Name, i)
	}
	switch v := n.Params[i].(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case int32:
		return int(v), nil
	}
	return 0, fmt.Errorf("eval: %s: expected int at parameter %d, got %T", n.Name, i, n.Params[i])
}
