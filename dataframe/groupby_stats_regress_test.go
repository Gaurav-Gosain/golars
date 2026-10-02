package dataframe_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// std and var work inside GroupBy.Agg; an all-null group gives null.
// polars: i=[1, None, 4, None, None] by g=[1, 1, 1, 2, 2] ->
// std [2.1213203435596424, None], var [4.5, None].
func TestGroupByStdVar(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	g, _ := series.FromInt64("g", []int64{1, 1, 1, 2, 2}, nil, opt)
	i, _ := series.FromInt64("i", []int64{1, 0, 4, 0, 0}, []bool{true, false, true, false, false}, opt)
	df, err := dataframe.New(g, i)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	I := expr.Col("i")
	out, err := df.GroupBy("g").Agg(context.Background(), []expr.Expr{I.Std().Alias("sd"), I.Var().Alias("v")}, dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	sorted, err := out.Sort(context.Background(), "g", false)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Release()
	if got := colRepr(t, sorted, "sd"); got != "[2.1213203435596424, None]" {
		t.Errorf("std = %s", got)
	}
	if got := colRepr(t, sorted, "v"); got != "[4.5, None]" {
		t.Errorf("var = %s", got)
	}
}
