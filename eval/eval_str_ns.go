package eval

import (
	"context"
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalStrExtFunction covers the str.* kernels added with the full
// polars string namespace (see expr/str_ns.go). ok is false for any
// other name so evalStrFunction can keep dispatching.
func evalStrExtFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	var run func(o series.StrOps, opt series.Option) (*series.Series, error)
	p := params{n}
	switch n.Name {
	case "str.split":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.Split(p.str(0), p.boolOr(1, false), opt)
		}
	case "str.splitn":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.SplitNStruct(p.str(0), p.int(1), opt)
		}
	case "str.split_exact_n":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.SplitExactStruct(p.str(0), p.int(1), p.boolOr(2, false), opt)
		}
	case "str.join":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.Join(p.str(0), p.boolOr(1, true), opt)
		}
	case "str.contains_any":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ContainsAny(p.strs(0), p.boolOr(1, false), opt)
		}
	case "str.escape_regex":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.EscapeRegex(opt) }
	case "str.explode":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Explode(opt) }
	case "str.extract":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.Extract(p.str(0), p.intOr(1, 1), opt)
		}
	case "str.extract_all":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractAll(p.str(0), opt)
		}
	case "str.extract_groups":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractGroups(p.str(0), opt)
		}
	case "str.extract_many":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractMany(p.strs(0), p.boolOr(1, false), p.boolOr(2, false), opt)
		}
	case "str.find_many":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.FindMany(p.strs(0), p.boolOr(1, false), p.boolOr(2, false), opt)
		}
	case "str.replace_many":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ReplaceMany(p.strs(0), p.strs(1), p.boolOr(2, false), opt)
		}
	case "str.reverse":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Reverse(opt) }
	case "str.strip_chars":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripChars(p.strs(0), opt)
		}
	case "str.strip_chars_start":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripCharsStart(p.strs(0), opt)
		}
	case "str.strip_chars_end":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripCharsEnd(p.strs(0), opt)
		}
	case "str.to_integer":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ToInteger(p.intOr(0, 10), p.dtype(1), p.boolOr(2, true), opt)
		}
	case "str.to_titlecase":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.ToTitlecase(opt) }
	case "str.pad_start":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.PadStart(p.int(0), p.runeOr(1, ' '), opt)
		}
	case "str.pad_end":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.PadEnd(p.int(0), p.runeOr(1, ' '), opt)
		}
	case "str.zfill":
		run = func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ZFill(p.int(0), opt)
		}
	default:
		return nil, false, nil
	}
	out, err := evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
		return run(in.Str(), seriesAlloc(ec))
	})
	return out, true, err
}

// evalUnaryWith evaluates n.Args[0], runs fn on it and releases the
// input. It is the shared shape of the namespace dispatchers.
func evalUnaryWith(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame, fn func(*series.Series) (*series.Series, error)) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: %s requires an input expression", n.Name)
	}
	in, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer in.Release()
	out, err := fn(in)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", n.Name, err)
	}
	return out, nil
}

// params reads typed positional FunctionNode parameters, falling back
// to a default when a parameter is missing or has another type.
type params struct {
	n expr.FunctionNode
}

func (p params) get(i int) (any, bool) {
	if i >= len(p.n.Params) {
		return nil, false
	}
	return p.n.Params[i], true
}

func (p params) str(i int) string {
	v, _ := p.get(i)
	s, _ := v.(string)
	return s
}

func (p params) strs(i int) []string {
	v, _ := p.get(i)
	switch x := v.(type) {
	case []string:
		return x
	case string:
		return []string{x}
	}
	return nil
}

func (p params) int(i int) int { return p.intOr(i, 0) }

func (p params) intOr(i int, def int) int {
	v, ok := p.get(i)
	if !ok {
		return def
	}
	if x, err := intParam(v); err == nil {
		return x
	}
	return def
}

func (p params) boolOr(i int, def bool) bool {
	v, ok := p.get(i)
	if !ok {
		return def
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return def
}

func (p params) runeOr(i int, def rune) rune {
	v, ok := p.get(i)
	if !ok {
		return def
	}
	switch x := v.(type) {
	case rune:
		return x
	case string:
		for _, r := range x {
			return r
		}
	case byte:
		return rune(x)
	}
	return def
}

func (p params) dtype(i int) dtype.DType {
	v, _ := p.get(i)
	if d, ok := v.(dtype.DType); ok {
		return d
	}
	return dtype.DType{}
}

func (p params) uint64(i int) uint64 {
	v, _ := p.get(i)
	switch x := v.(type) {
	case uint64:
		return x
	case int:
		return uint64(x)
	case int64:
		return uint64(x)
	}
	return 0
}

func (p params) float(i int) float64 {
	v, _ := p.get(i)
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

func (p params) expr(i int) (expr.Expr, bool) {
	v, _ := p.get(i)
	e, ok := v.(expr.Expr)
	return e, ok
}
