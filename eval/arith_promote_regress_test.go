package eval_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars 1.39 with i=[10, -7, 0, None], j=[3, 2, 0, 1], f=[1.5, 2, 0, 1]:
// `/` is true division (f64), integer + float literal is f64, and
// float division by zero gives inf / NaN. For i / 12 polars multiplies
// by the reciprocal, so its last digit can differ from true division.
func TestArithmeticPromotion(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	i, _ := series.FromInt64("i", []int64{10, -7, 0, 0}, []bool{true, true, true, false}, opt)
	j, _ := series.FromInt64("j", []int64{3, 2, 0, 1}, nil, opt)
	j32, _ := series.FromInt32("j32", []int32{3, 2, 0, 1}, nil, opt)
	f, _ := series.FromFloat64("f", []float64{1.5, 2, 0, 1}, nil, opt)
	df, err := dataframe.New(i, j, j32, f)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	I, J, J32, F := expr.Col("i"), expr.Col("j"), expr.Col("j32"), expr.Col("f")
	cases := []struct {
		name  string
		e     expr.Expr
		dtype string
		want  string
	}{
		{"i / j", I.Div(J), "f64", "[3.3333333333333335, -3.5, nan, None]"},
		{"i / 0", I.DivLit(int64(0)), "f64", "[inf, -inf, nan, None]"},
		{"i / 12", I.DivLit(int64(12)), "f64", "[0.8333333333333334, -0.5833333333333334, 0.0, None]"},
		{"12 / j", expr.LitInt64(12).Div(J), "f64", "[4.0, 6.0, inf, 12.0]"},
		{"j32 / j32", J32.Div(J32), "f64", "[1.0, 1.0, nan, 1.0]"},
		{"i + f", I.Add(F), "f64", "[11.5, -5.0, 0.0, None]"},
		{"i + 0.5", I.AddLit(0.5), "f64", "[10.5, -6.5, 0.5, None]"},
		{"i * 2.5", I.MulLit(2.5), "f64", "[25.0, -17.5, 0.0, None]"},
		{"2.5 * i", expr.LitFloat64(2.5).Mul(I), "f64", "[25.0, -17.5, 0.0, None]"},
		{"j32 + 1.5", J32.AddLit(1.5), "f64", "[4.5, 3.5, 1.5, 2.5]"},
		{"j32 + 1", J32.AddLit(int64(1)), "i32", "[4, 3, 1, 2]"},
		{"i + j32", I.Add(J32), "i64", "[13, -5, 0, None]"},
		{"f / 0", F.DivLit(int64(0)), "f64", "[inf, inf, nan, inf]"},
		{"i // j", I.FloorDiv(J), "i64", "[3, -4, None, None]"},
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
}

// polars 1.39 result dtypes: count, len and null_count are u32; the
// product of any integer column is i64; f32 products stay f32.
func TestAggResultDTypes(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	g, _ := series.FromInt64("g", []int64{1, 1, 2}, nil, opt)
	x, _ := series.FromInt64("x", []int64{2, 0, 3}, []bool{true, false, true}, opt)
	i32, _ := series.FromInt32("i32", []int32{2, 3, 4}, nil, opt)
	f32, _ := series.FromFloat32("f32", []float32{2, 3, 4}, nil, opt)
	df, err := dataframe.New(g, x, i32, f32)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	X := expr.Col("x")
	cases := []struct {
		name  string
		e     expr.Expr
		dtype string
		want  string
	}{
		{"count", X.Count(), "u32", "[2]"},
		{"len", X.Len(), "u32", "[3]"},
		{"null_count", X.NullCount(), "u32", "[1]"},
		{"product i64", X.Product(), "i64", "[6]"},
		{"product i32", expr.Col("i32").Product(), "i64", "[24]"},
		{"product f32", expr.Col("f32").Product(), "f32", "[24.0]"},
		{"count over", X.Count().Over("g"), "u32", "[1, 1, 1]"},
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
}

// polars: median, std and quantile of an all-null column are null.
func TestAllNullStatsAreNull(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	i, _ := series.FromInt64("i", []int64{0, 0}, []bool{false, false}, series.WithAllocator(mem))
	df, err := dataframe.New(i)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	I := expr.Col("i")
	for name, e := range map[string]expr.Expr{"median": I.Median(), "std": I.Std(), "var": I.Var(), "quantile": I.Quantile(0.5)} {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e, df)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != "[None]" {
			t.Errorf("%s of all-null = %s, want [None]", name, got)
		}
		out.Release()
	}
}
