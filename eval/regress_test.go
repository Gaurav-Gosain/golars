package eval_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// A length-1 aggregate broadcasts inside list.eval. polars:
// pl.select(pl.lit([1, 2, 3, 4, 5]).list.eval(pl.element() >= pl.element().mean()))
// == [[False, False, True, True, True]].
func TestListEvalBroadcastsAggregate(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	e := expr.Col("a").Implode().List().Eval(expr.Element().Ge(expr.Element().Mean()))
	out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e, df)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := testutil.PyRepr(out.Chunk(0)); got != "[[False, False, True, True, True]]" {
		t.Errorf("list.eval = %s", got)
	}
}

// polars compares a Categorical column with a str column by category
// string: c=["b", "a", None, "c", "a"], s=["b", "x", "a", None, "a"]
// gives c == s -> [True, False, None, None, True], c < s -> [False, True, None, None, False].
func TestCategoricalVsStringCompare(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	c, _ := series.FromString("c", []string{"b", "a", "", "c", "a"}, []bool{true, true, false, true, true}, opt)
	s, _ := series.FromString("s", []string{"b", "x", "a", "", "a"}, []bool{true, true, true, false, true}, opt)
	df, err := dataframe.New(c, s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	C := expr.Col("c").Cast(dtype.Categorical())
	S := expr.Col("s")
	for name, tc := range map[string]struct {
		e    expr.Expr
		want string
	}{
		"c == s": {C.Eq(S), "[True, False, None, None, True]"},
		"s == c": {S.Eq(C), "[True, False, None, None, True]"},
		"c < s":  {C.Lt(S), "[False, True, None, None, False]"},
	} {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, tc.e, df)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != tc.want {
			t.Errorf("%s = %s, want %s", name, got, tc.want)
		}
		out.Release()
	}
}
