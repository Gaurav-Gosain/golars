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

func TestEvalStrJSONKernels(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	j, err := series.FromString("j",
		[]string{`{"a":1,"b":"x"}`, "", `{"a":2.5,"b":"héllo"}`, `{"b":null,"c":[1,2]}`, `null`},
		[]bool{true, false, true, true, true}, opt)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(j)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	ec := eval.EvalContext{Alloc: mem}
	ctx := context.Background()
	st := dtype.Struct(
		dtype.StructField{Name: "a", DType: dtype.Int64()},
		dtype.StructField{Name: "b", DType: dtype.String()},
	)

	cases := []struct {
		name string
		e    expr.Expr
		dt   dtype.DType
		want string
	}{
		{"json_decode", expr.Col("j").Str().JSONDecode(st), st,
			`{"a":1,"b":"x"} | null | {"a":2,"b":"héllo"} | {"a":null,"b":null} | null`},
		// Polars: a null struct row encodes to the text "null".
		{"json_roundtrip", expr.Col("j").Str().JSONDecode(st).Struct().JSONEncode(), dtype.String(),
			`{"a":1,"b":"x"} | null | {"a":2,"b":"héllo"} | {"a":null,"b":null} | null`},
		{"json_path_match", expr.Col("j").Str().JSONPathMatch("$.b"), dtype.String(),
			"x | null | héllo | null | null"},
		{"encode_hex", expr.Col("j").Str().JSONPathMatch("$.b").Str().Encode("hex"), dtype.String(),
			"78 | null | 68c3a96c6c6f | null | null"},
		{"decode_hex", expr.Col("j").Str().JSONPathMatch("$.b").Str().Encode("hex").Str().Decode("hex", true).Bin().Size("b"),
			dtype.Uint32(), "1 | null | 6 | null | null"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := eval.Eval(ctx, ec, c.e, df)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Name() != "j" {
				t.Errorf("name = %q, want j", out.Name())
			}
			if !out.DType().Equal(c.dt) {
				t.Fatalf("dtype = %s, want %s", out.DType(), c.dt)
			}
			if got := binRenderSeries(out); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}

	bad := []expr.Expr{
		expr.Col("j").Str().JSONPathMatch("a.b"),
		expr.Col("j").Str().JSONDecode(dtype.Bool()),
		expr.Col("j").Str().Decode("hex", true),
		expr.Col("j").Struct().JSONEncode(),
	}
	for _, e := range bad {
		if out, err := eval.Eval(ctx, ec, e, df); err == nil {
			out.Release()
			t.Errorf("%s: expected error", e)
		}
	}
}
