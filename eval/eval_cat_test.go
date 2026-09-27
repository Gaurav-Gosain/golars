package eval_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func catEvalCells(s *series.Series) string {
	a := s.Chunk(0)
	out := make([]string, a.Len())
	for i := range out {
		if a.IsNull(i) {
			out[i] = "<null>"
		} else {
			out[i] = a.ValueStr(i)
		}
	}
	return strings.Join(out, ",")
}

// Expected values match polars 1.39.3 on
// pl.Series(["b", None, "a", "b", "", "héllo"]).cast(pl.Categorical).
func TestEvalCatNamespace(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("c", []string{"b", "", "a", "b", "", "héllo"},
		[]bool{true, false, true, true, true, true}, series.WithAllocator(mem))
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	ec := eval.EvalContext{Alloc: mem}
	c := expr.Col("c").Cast(dtype.Categorical())

	cases := []struct {
		name string
		e    expr.Expr
		dt   string
		want string
	}{
		{"cast", c, "cat", "b,<null>,a,b,,héllo"},
		{"get_categories", c.Cat().GetCategories(), "str", "b,a,,héllo"},
		{"len_bytes", c.Cat().LenBytes(), "u32", "1,<null>,1,1,0,6"},
		{"len_chars", c.Cat().LenChars(), "u32", "1,<null>,1,1,0,5"},
		{"starts_with", c.Cat().StartsWith("b"), "bool", "true,<null>,false,true,false,false"},
		{"ends_with", c.Cat().EndsWith("o"), "bool", "false,<null>,false,false,false,true"},
		{"slice", c.Cat().Slice(1, 2), "str", ",<null>,,,,él"},
		{"slice neg", c.Cat().Slice(-2, -1), "str", "b,<null>,a,b,,lo"},
		{"eq literal", c.Eq(expr.Lit("b")), "bool", "true,<null>,false,true,false,false"},
		{"back to str", c.Cast(dtype.String()), "str", "b,<null>,a,b,,héllo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := eval.Eval(ctx, ec, tc.e, df)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.DType().String() != tc.dt {
				t.Fatalf("dtype = %s want %s", out.DType(), tc.dt)
			}
			if got := catEvalCells(out); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestEvalEnumCastStrict(t *testing.T) {
	ctx := context.Background()
	s, _ := series.FromString("s", []string{"x", "q", "z"}, nil)
	df, _ := dataframe.New(s)
	defer df.Release()
	_, err := eval.Eval(ctx, eval.Default(), expr.Col("s").Cast(dtype.Enum([]string{"x", "y", "z"})), df)
	if err == nil || !strings.Contains(err.Error(), "for 1 out of 3 values") {
		t.Fatalf("err = %v", err)
	}
}
