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

// binRenderSeries prints each row with ValueStr ("null" for nulls).
func binRenderSeries(s *series.Series) string {
	a := s.Chunk(0)
	parts := make([]string, a.Len())
	for i := range parts {
		if a.IsNull(i) {
			parts[i] = "null"
		} else {
			parts[i] = a.ValueStr(i)
		}
	}
	return strings.Join(parts, " | ")
}

func TestEvalBinNamespace(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	b, err := series.FromBinary("b",
		[][]byte{[]byte("hello"), nil, {}, []byte("héllo"), {0x00, 0xff}},
		[]bool{true, false, true, true, true}, opt)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(b)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	ec := eval.EvalContext{Alloc: mem}
	ctx := context.Background()
	col := expr.Col("b").Bin()

	// Expected renders mirror polars 1.39.3 results; binary cells
	// render as base64 through arrow's ValueStr.
	cases := []struct {
		name string
		e    expr.Expr
		dt   dtype.DType
		want string
	}{
		{"contains", col.Contains([]byte("ll")), dtype.Bool(), "true | null | false | true | false"},
		{"starts_with", col.StartsWith([]byte("he")), dtype.Bool(), "true | null | false | false | false"},
		{"ends_with", col.EndsWith([]byte("lo")), dtype.Bool(), "true | null | false | true | false"},
		{"encode_hex", col.Encode("hex"), dtype.String(), "68656c6c6f | null |  | 68c3a96c6c6f | 00ff"},
		{"encode_base64", col.Encode("base64"), dtype.String(), "aGVsbG8= | null |  | aMOpbGxv | AP8="},
		{"size", col.Size("b"), dtype.Uint32(), "5 | null | 0 | 6 | 2"},
		{"size_kb", col.Size("kb"), dtype.Float64(), "0.0048828125 | null | 0 | 0.005859375 | 0.001953125"},
		{"get", col.Get(1, true), dtype.Uint8(), "101 | null | null | 195 | 255"},
		{"head_roundtrip", col.Head(2).Bin().Encode("hex"), dtype.String(), "6865 | null |  | 68c3 | 00ff"},
		{"tail_roundtrip", col.Tail(2).Bin().Encode("hex"), dtype.String(), "6c6f | null |  | 6c6f | 00ff"},
		{"slice_roundtrip", col.Slice(1, 2).Bin().Encode("hex"), dtype.String(), "656c | null |  | c3a9 | ff"},
		{"decode_roundtrip", col.Encode("hex").Str().Decode("hex", true).Bin().Encode("base64"),
			dtype.String(), "aGVsbG8= | null |  | aMOpbGxv | AP8="},
		{"bin_decode", col.Encode("hex").Cast(dtype.String()).Str().Encode("hex").Str().Decode("hex", true).Bin().Decode("hex", true).Bin().Size("b"),
			dtype.Uint32(), "5 | null | 0 | 6 | 2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := eval.Eval(ctx, ec, c.e, df)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Name() != "b" {
				t.Errorf("name = %q, want b", out.Name())
			}
			if !out.DType().Equal(c.dt) {
				t.Fatalf("dtype = %s, want %s", out.DType(), c.dt)
			}
			if got := binRenderSeries(out); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}

	if _, err := eval.Eval(ctx, ec, col.Get(1, false), df); err == nil {
		t.Error("get out of bounds without null_on_oob should error")
	}
	if _, err := eval.Eval(ctx, ec, col.Size("zb"), df); err == nil {
		t.Error("unknown size unit should error")
	}
}
