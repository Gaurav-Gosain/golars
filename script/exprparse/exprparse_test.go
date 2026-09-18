package exprparse

import (
	"reflect"
	"testing"

	"github.com/Gaurav-Gosain/golars/expr"
)

func TestExpressionCompatibility(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  expr.Expr
	}{
		{`coalesce(primary, backup).str.trim()`, expr.Coalesce(expr.Col("primary"), expr.Col("backup")).Str().Trim()},
		{`col("price").sum()`, expr.Col("price").Sum()},
		{`(price + tax).abs()`, expr.Col("price").Add(expr.Col("tax")).Abs()},
		{`.5`, expr.LitFloat64(0.5)},
		{`1e-3`, expr.LitFloat64(0.001)},
		{`2E+2`, expr.LitFloat64(200)},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := Parse(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Node(), tc.want.Node()) {
				t.Fatalf("got %#v, want %#v", got.Node(), tc.want.Node())
			}
		})
	}
}

func TestRejectIgnoredArguments(t *testing.T) {
	for _, input := range []string{`price.sum(2)`, `name.str.upper("unused")`, `price.abs(1)`, `name.str.trim(1)`, `price.cum_sum(2)`} {
		if _, err := Parse(input); err == nil {
			t.Errorf("accepted unused arguments in %q", input)
		}
	}
}
