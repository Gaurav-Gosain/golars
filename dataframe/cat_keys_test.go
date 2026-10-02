package dataframe_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func catDFCells(s *series.Series) []string {
	a := s.Chunk(0)
	out := make([]string, a.Len())
	for i := range out {
		if a.IsNull(i) {
			out[i] = "<null>"
		} else {
			out[i] = a.ValueStr(i)
		}
	}
	return out
}

func catFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	k, err := series.NewCategorical("k", []string{"x", "y", "", "x"}, []bool{true, true, false, true})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4}, nil)
	df, err := dataframe.New(k, v)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Parity with polars 1.39.3:
//
//	df.group_by("k", maintain_order=True).agg(pl.col("v").sum())
//	x -> 5, y -> 2, null -> 3, key dtype cat
func TestCategoricalGroupBy(t *testing.T) {
	ctx := context.Background()
	df := catFrame(t)
	defer df.Release()
	out, err := df.GroupBy("k").Agg(ctx, []expr.Expr{expr.Col("v").Sum()})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	k, _ := out.Column("k")
	v, _ := out.Column("v")
	if !k.DType().IsCategorical() {
		t.Fatalf("key dtype = %s", k.DType())
	}
	got := map[string]string{}
	keys, vals := catDFCells(k), catDFCells(v)
	for i := range keys {
		got[keys[i]] = vals[i]
	}
	want := map[string]string{"x": "5", "y": "2", "<null>": "3"}
	if len(got) != len(want) {
		t.Fatalf("groups = %v", got)
	}
	for kk, vv := range want {
		if got[kk] != vv {
			t.Fatalf("group %s = %s want %s (all %v)", kk, got[kk], vv, got)
		}
	}
}

func TestEnumGroupByKeepsCategories(t *testing.T) {
	ctx := context.Background()
	e := dtype.Enum([]string{"lo", "mid", "hi"})
	k, err := series.NewEnum("k", []string{"hi", "lo", "hi"}, nil, e)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := series.FromInt64("v", []int64{1, 2, 3}, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()
	out, err := df.GroupBy("k").Agg(ctx, []expr.Expr{expr.Col("v").Sum()})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	kc, _ := out.Column("k")
	if !kc.DType().IsEnum() {
		t.Fatalf("key dtype = %s", kc.DType())
	}
	cats, err := kc.Cat().GetCategories()
	if err != nil {
		t.Fatal(err)
	}
	defer cats.Release()
	if strings.Join(catDFCells(cats), ",") != "lo,mid,hi" {
		t.Fatalf("categories = %v", catDFCells(cats))
	}
}

// Parity with polars 1.39.3 left and inner join on a categorical key.
func TestCategoricalJoin(t *testing.T) {
	ctx := context.Background()
	left := catFrame(t)
	defer left.Release()
	rk, _ := series.NewCategorical("k", []string{"x", "z"}, nil)
	rw, _ := series.FromInt64("w", []int64{10, 20}, nil)
	right, _ := dataframe.New(rk, rw)
	defer right.Release()

	lj, err := left.Join(ctx, right, []string{"k"}, dataframe.LeftJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer lj.Release()
	k, _ := lj.Column("k")
	w, _ := lj.Column("w")
	if !k.DType().IsCategorical() {
		t.Fatalf("key dtype = %s", k.DType())
	}
	if got := strings.Join(catDFCells(k), ","); got != "x,y,<null>,x" {
		t.Fatalf("left keys = %s", got)
	}
	if got := strings.Join(catDFCells(w), ","); got != "10,<null>,<null>,10" {
		t.Fatalf("left w = %s", got)
	}

	ij, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin)
	if err != nil {
		t.Fatal(err)
	}
	defer ij.Release()
	k2, _ := ij.Column("k")
	if got := strings.Join(catDFCells(k2), ","); got != "x,x" {
		t.Fatalf("inner keys = %s", got)
	}
}

func TestCategoricalFormat(t *testing.T) {
	df := catFrame(t)
	defer df.Release()
	txt := df.String()
	for _, want := range []string{"cat", "null", "x"} {
		if !strings.Contains(txt, want) {
			t.Fatalf("missing %q in\n%s", want, txt)
		}
	}
}

func TestCategoricalFilterSliceConcat(t *testing.T) {
	ctx := context.Background()
	df := catFrame(t)
	defer df.Release()
	sl, err := df.Slice(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Release()
	k, _ := sl.Column("k")
	if got := strings.Join(catDFCells(k), ","); got != "y,<null>" {
		t.Fatalf("slice = %s", got)
	}
	other, _ := series.NewCategorical("k", []string{"z"}, nil)
	ov, _ := series.FromInt64("v", []int64{9}, nil)
	df2, _ := dataframe.New(other, ov)
	defer df2.Release()
	cc, err := dataframe.Concat(df, df2)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Release()
	kc, _ := cc.Column("k")
	if got := strings.Join(catDFCells(kc), ","); got != "x,y,<null>,x,z" {
		t.Fatalf("concat = %s", got)
	}
	mask, _ := series.FromBool("m", []bool{true, false, true, true}, nil)
	defer mask.Release()
	f, err := df.Filter(ctx, mask)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	fk, _ := f.Column("k")
	if got := strings.Join(catDFCells(fk), ","); got != "x,<null>,x" {
		t.Fatalf("filter = %s", got)
	}
}
