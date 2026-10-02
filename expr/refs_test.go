package expr_test

import (
	"slices"
	"testing"

	"github.com/Gaurav-Gosain/golars/expr"
)

func TestReferencedColumns(t *testing.T) {
	e := expr.Col("v").CumSum().Over("k").Alias("cum")
	cols, ok := expr.ReferencedColumns(e)
	if !ok {
		t.Fatal("expected complete")
	}
	// Over keys are collected before the inner walk.
	if !slices.Equal(cols, []string{"k", "v"}) {
		t.Fatalf("got %v", cols)
	}
	w := expr.When(expr.Col("a").Gt(expr.Lit(int64(1)))).Then(expr.Col("b")).Otherwise(expr.Col("c"))
	cols, ok = expr.ReferencedColumns(w)
	if !ok || len(cols) != 3 {
		t.Fatalf("when/then: got %v complete=%v", cols, ok)
	}
	if _, ok := expr.ReferencedColumns(expr.Expr{}); ok {
		t.Fatal("nil node must be incomplete")
	}
}
