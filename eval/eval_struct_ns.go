package eval

import (
	"context"
	"fmt"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// structScopeKey carries the struct whose fields expr.Field refers to
// while struct.with_fields evaluates its expressions.
type structScopeKey struct{}

// evalStructExtFunction covers the struct.* kernels beyond field
// access: rename_fields, with_fields, field references and the
// multi-output forms that the planner normally expands.
func evalStructExtFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	switch n.Name {
	case "struct.field_ref":
		s, err := evalFieldRef(ctx, n)
		return s, true, err
	case "struct.rename_fields":
		out, err := evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
			return in.Struct().RenameFields(params{n}.strs(0))
		})
		return out, true, err
	case "struct.with_fields":
		out, err := evalWithFields(ctx, ec, n, df)
		return out, true, err
	case "struct.unnest":
		return nil, true, fmt.Errorf("eval: struct.unnest produces several columns; use it in a lazy select or with_columns")
	case "struct.field":
		if len(n.Params) > 1 {
			return nil, true, fmt.Errorf("eval: struct.field with %d names produces several columns; use it in a lazy select or with_columns", len(n.Params))
		}
	}
	return nil, false, nil
}

func evalFieldRef(ctx context.Context, n expr.FunctionNode) (*series.Series, error) {
	name := params{n}.str(0)
	scope, ok := ctx.Value(structScopeKey{}).(*series.Series)
	if !ok || scope == nil {
		return nil, fmt.Errorf("eval: field(%q) is only valid inside struct.with_fields", name)
	}
	return scope.Struct().Field(name)
}

func evalWithFields(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("eval: struct.with_fields requires a struct input")
	}
	st, err := evalNode(ctx, ec, n.Args[0], df)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	if !st.DType().IsStruct() {
		return nil, fmt.Errorf("eval: struct.with_fields: %q has dtype %s (need struct)", st.Name(), st.DType())
	}
	scoped := context.WithValue(ctx, structScopeKey{}, st)
	cols := make([]*series.Series, 0, len(n.Args)-1)
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	for _, a := range n.Args[1:] {
		s, err := evalNode(scoped, ec, a, df)
		if err != nil {
			return nil, err
		}
		name := expr.OutputName(a)
		if s.Name() != name {
			r := s.Rename(name)
			s.Release()
			s = r
		}
		cols = append(cols, s)
	}
	return st.Struct().WithFields(cols, seriesAlloc(ec))
}

// evalNameFunction evaluates the name.* namespace. Plain renames keep
// the values and take the resolved output name; the *_fields variants
// rename the fields of a struct result.
func evalNameFunction(ctx context.Context, ec EvalContext, n expr.FunctionNode, df *dataframe.DataFrame) (*series.Series, error) {
	return evalUnaryWith(ctx, ec, n, df, func(in *series.Series) (*series.Series, error) {
		var mapper func(string) string
		switch n.Name {
		case "name.prefix_fields":
			p := params{n}.str(0)
			mapper = func(s string) string { return p + s }
		case "name.suffix_fields":
			p := params{n}.str(0)
			mapper = func(s string) string { return s + p }
		case "name.map_fields":
			v, _ := params{n}.get(0)
			fn, ok := v.(expr.NameMapper)
			if !ok || fn == nil {
				return nil, fmt.Errorf("name.map_fields requires a mapping function")
			}
			mapper = fn
		}
		if mapper != nil {
			out, err := in.Struct().MapFieldNames(mapper)
			if err != nil {
				return nil, err
			}
			if name := n.OutputName(); out.Name() != name {
				r := out.Rename(name)
				out.Release()
				out = r
			}
			return out, nil
		}
		if !strings.HasPrefix(n.Name, "name.") {
			return nil, fmt.Errorf("unknown name op")
		}
		switch n.Name {
		case "name.keep", "name.prefix", "name.suffix", "name.map",
			"name.to_lowercase", "name.to_uppercase", "name.replace":
		default:
			return nil, fmt.Errorf("unknown name op")
		}
		return in.Rename(n.OutputName()), nil
	})
}
