package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Expected values are polars 1.39.3 results for the same frame:
// i8=[1, 2, null], u8=[1, 2, 3], f32=[1.5, 2, 3], b=[true, false, true].
func TestLiteralSemantics(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	i8, _ := series.FromValues("i8", arrow.PrimitiveTypes.Int8, []any{int64(1), int64(2), nil}, opt)
	u8, _ := series.FromValues("u8", arrow.PrimitiveTypes.Uint8, []any{int64(1), int64(2), int64(3)}, opt)
	f32, _ := series.FromFloat32("f32", []float32{1.5, 2, 3}, nil, opt)
	b, _ := series.FromBool("b", []bool{true, false, true}, nil, opt)
	df, err := dataframe.New(i8, u8, f32, b)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	I8, U8, F32, B := expr.Col("i8"), expr.Col("u8"), expr.Col("f32"), expr.Col("b")
	cases := []struct {
		name  string
		e     expr.Expr
		dtype string
		want  string
	}{
		{"lit(3)", expr.Lit(3), "i32", "[3]"},
		{"lit(2**31)", expr.Lit(1 << 31), "i64", "[2147483648]"},
		{"lit(1.5)", expr.Lit(1.5), "f64", "[1.5]"},
		{"lit(1).count()", expr.Lit(1).Count(), "u32", "[1]"},
		{"lit(1).shift(1)", expr.Lit(1).Shift(1), "i32", "[None]"},
		{"i8 + 3", I8.AddLit(3), "i8", "[4, 5, None]"},
		{"i8 + 1000", I8.Add(expr.Lit(1000)), "i16", "[1001, 1002, None]"},
		{"i8 + LitInt64(1)", I8.Add(expr.LitInt64(1)), "i64", "[2, 3, None]"},
		{"u8 - 2", U8.SubLit(2), "u8", "[255, 0, 1]"},
		{"2 - u8", expr.Lit(2).Sub(U8), "u8", "[1, 0, 255]"},
		{"u8 + -200", U8.Add(expr.Lit(-200)), "i16", "[-199, -198, -197]"},
		{"f32 * 2.5", F32.MulLit(2.5), "f32", "[3.75, 5.0, 7.5]"},
		{"f32 + 1", F32.Add(expr.Lit(1)), "f32", "[2.5, 3.0, 4.0]"},
		{"b + 1", B.Add(expr.Lit(1)), "i32", "[2, 1, 2]"},
		{"u8 + i8", U8.Add(I8), "i16", "[2, 4, None]"},
		{"when b then 1 otherwise u8", expr.When(B).Then(expr.Lit(1)).Otherwise(U8), "u8", "[1, 2, 1]"},
		{"-i8", I8.Neg(), "i8", "[-1, -2, None]"},
	}
	for _, c := range cases {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, c.e, df)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if out.DType().String() != c.dtype {
			t.Errorf("%s dtype = %s, want %s", c.name, out.DType(), c.dtype)
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
		out.Release()
	}
	if _, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, U8.Neg(), df); err == nil {
		t.Error("-u8: want an error, as polars raises")
	}
}

func TestLiteralInGroupByAggIsScalar(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	k, _ := series.FromInt64("k", []int64{1, 1, 2}, nil, opt)
	df, err := dataframe.New(k)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := eval.GroupByAgg(context.Background(), eval.EvalContext{Alloc: mem}, df, []string{"k"}, []expr.Expr{
		expr.Lit("s").Shift(1).Alias("a"),
		expr.Lit(1).Alias("b"),
		expr.Lit(true).Std().Alias("c"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	want := map[string]string{"a": "[None, None]", "b": "[1, 1]", "c": "[None, None]"}
	for name, w := range want {
		c, err := out.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := testutil.PyRepr(c.Chunk(0)); got != w {
			t.Errorf("%s = %s (%s), want %s", name, got, c.DType(), w)
		}
	}
}
