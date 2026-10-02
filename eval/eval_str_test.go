package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestEvalStrFunctions drives the full expr -> eval -> series pipeline
// for every str.* kernel we wired up, confirming both correctness and
// that nothing leaks through the EvalContext.
func TestEvalStrFunctions(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	col, _ := series.FromString("s",
		[]string{"MEDIUM POLISHED STEEL", "LARGE BRUSHED COPPER", "small polished copper"},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(col)
	defer df.Release()

	ec := eval.EvalContext{Alloc: alloc}
	ctx := context.Background()

	cases := []struct {
		name  string
		e     expr.Expr
		check func(t *testing.T, s *series.Series)
	}{
		{"contains", expr.Col("s").Str().Contains("POLISHED"), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.Boolean)
			if !arr.Value(0) || arr.Value(1) || arr.Value(2) {
				t.Errorf("contains: %v %v %v", arr.Value(0), arr.Value(1), arr.Value(2))
			}
		}},
		{"like", expr.Col("s").Str().Like("%POLISHED%"), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.Boolean)
			if !arr.Value(0) || arr.Value(1) || arr.Value(2) {
				t.Errorf("like: %v %v %v", arr.Value(0), arr.Value(1), arr.Value(2))
			}
		}},
		{"starts_with", expr.Col("s").Str().StartsWith("MEDIUM"), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.Boolean)
			if !arr.Value(0) || arr.Value(1) || arr.Value(2) {
				t.Errorf("starts_with: %v %v %v", arr.Value(0), arr.Value(1), arr.Value(2))
			}
		}},
		{"to_lower", expr.Col("s").Str().ToLower(), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.String)
			if arr.Value(0) != "medium polished steel" {
				t.Errorf("to_lower: %q", arr.Value(0))
			}
		}},
		{"len_bytes", expr.Col("s").Str().LenBytes(), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.Uint32) // polars: u32
			if arr.Value(0) != 21 || arr.Value(1) != 20 || arr.Value(2) != 21 {
				t.Errorf("len_bytes: %v", arr)
			}
		}},
		{"head", expr.Col("s").Str().Head(6), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.String)
			if arr.Value(0) != "MEDIUM" || arr.Value(1) != "LARGE " || arr.Value(2) != "small " {
				t.Errorf("head: %q %q %q", arr.Value(0), arr.Value(1), arr.Value(2))
			}
		}},
		{"find", expr.Col("s").Str().Find("STEEL"), func(t *testing.T, s *series.Series) {
			arr := s.Chunk(0).(*array.Uint32) // polars: u32, null when absent
			if arr.Value(0) != 16 || arr.IsValid(1) {
				t.Errorf("find: %v %v", arr.Value(0), arr)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := eval.Eval(ctx, ec, tc.e, df)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			defer s.Release()
			tc.check(t, s)
		})
	}
}
