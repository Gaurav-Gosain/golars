package eval_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func jsonSeries(t *testing.T, mem memory.Allocator, name string, dt arrow.DataType, js string) *series.Series {
	t.Helper()
	arr, _, err := array.FromJSON(mem, dt, strings.NewReader(js))
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	s, err := series.New(name, arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func reprOf(t *testing.T, s *series.Series) string {
	t.Helper()
	a, err := s.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	return testutil.PyRepr(a)
}

// evalRepr evaluates e on df and returns (dtype, python repr).
func evalRepr(t *testing.T, mem memory.Allocator, df *dataframe.DataFrame, e expr.Expr) (string, string) {
	t.Helper()
	out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e, df)
	if err != nil {
		t.Fatalf("%s: %v", e, err)
	}
	defer out.Release()
	return out.DType().String(), reprOf(t, out)
}

func listPairFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	i64 := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	a := jsonSeries(t, mem, "a", i64, `[[1, 2, 2, null], [], null, [3], [null], [4, 5]]`)
	b := jsonSeries(t, mem, "b", i64, `[[2, 3], [1], [1], null, [null, 1], [5, 4, 4]]`)
	x, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 5, 6}, nil, series.WithAllocator(mem))
	s := jsonSeries(t, mem, "s", arrow.ListOf(arrow.BinaryTypes.String),
		`[["a", "B"], null, [], ["é", null], ["x"], ["y", "z"]]`)
	df, err := dataframe.New(a, b, x, s)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Expected values captured from polars 1.39.3.
func TestListExprParity(t *testing.T) {
	a := expr.Col("a").List()
	el := expr.Element()
	cases := []struct {
		name  string
		e     expr.Expr
		dtype string
		want  string
	}{
		{"set_union", a.SetUnion(expr.Col("b")), "list[i64]", "[[1, 2, None, 3], [1], None, None, [None, 1], [4, 5]]"},
		{"set_intersection", a.SetIntersection(expr.Col("b")), "list[i64]", "[[2], [], None, None, [None], [4, 5]]"},
		{"set_difference", a.SetDifference(expr.Col("b")), "list[i64]", "[[1, None], [], None, None, [], []]"},
		{"set_symmetric_difference", a.SetSymmetricDifference(expr.Col("b")), "list[i64]", "[[1, None, 3], [1], None, None, [1], []]"},
		{"concat", a.Concat(expr.Col("b")), "list[i64]", "[[1, 2, 2, None, 2, 3], [1], None, None, [None, None, 1], [4, 5, 5, 4, 4]]"},
		{"concat_lit", a.Concat(expr.Lit(9)), "list[i64]", "[[1, 2, 2, None, 9], [9], None, [3, 9], [None, 9], [4, 5, 9]]"},
		{"concat_col", a.Concat(expr.Col("x")), "list[i64]", "[[1, 2, 2, None, 1], [2], None, [3, 4], [None, 5], [4, 5, 6]]"},
		{"filter", a.Filter(el.Gt(expr.Lit(1))), "list[i64]", "[[2, 2], [], None, [3], [], [4, 5]]"},
		{"eval_mul", a.Eval(el.Mul(expr.Lit(2))), "list[i64]", "[[2, 4, 4, None], [], None, [6], [None], [8, 10]]"},
		{"eval_sum", a.Eval(el.Sum()), "list[i64]", "[[5], [0], None, [3], [0], [9]]"},
		{"eval_cum_sum", a.Eval(el.CumSum()), "list[i64]", "[[1, 3, 5, None], [], None, [3], [None], [4, 9]]"},
		{"eval_str", expr.Col("s").List().Eval(el.Str().ToUpper()), "list[str]", "[['A', 'B'], None, [], ['É', None], ['X'], ['Y', 'Z']]"},
		{"eval_filter_rank", a.Filter(el.CumSum().Le(expr.Lit(3))), "list[i64]", "[[1, 2], [], None, [3], [], []]"},
		{"len", a.Len(), "u32", "[4, 0, None, 1, 1, 2]"},
		{"join", expr.Col("s").List().Join("-"), "str", "['a-B', None, '', 'é', 'x', 'y-z']"},
		{"join_nulls", expr.Col("s").List().JoinWith("-", false), "str", "['a-B', None, '', None, 'x', 'y-z']"},
		{"to_array", expr.Col("s").List().Slice(0, 1).List().Get(0), "str", "['a', None, None, 'é', 'x', 'y']"},
		{"gather", a.Gather([]int{0, -1}, true), "list[i64]", "[[1, None], [None, None], None, [3, 3], [None, None], [4, 5]]"},
		{"unique", a.Unique(false), "list[i64]", "[[None, 1, 2], [], None, [3], [None], [4, 5]]"},
		{"to_struct", a.ToStruct("p", "q"), "struct{p: i64, q: i64}", "[{'p': 1, 'q': 2}, {'p': None, 'q': None}, {'p': None, 'q': None}, {'p': 3, 'q': None}, {'p': None, 'q': None}, {'p': 4, 'q': 5}]"},
		{"count_matches", a.CountMatches(2), "u32", "[2, 0, None, 0, 0, 0]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			df := listPairFrame(t, mem)
			defer df.Release()
			dt, got := evalRepr(t, mem, df, c.e)
			if dt != c.dtype {
				t.Errorf("dtype = %s, want %s", dt, c.dtype)
			}
			if got != c.want {
				t.Errorf("\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

func TestListExprErrors(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := listPairFrame(t, mem)
	defer df.Release()
	for _, e := range []expr.Expr{
		expr.Col("a").List().GetWithOOB(5, false),
		expr.Col("a").List().Gather([]int{7}, false),
		expr.Col("a").List().Item(),
		expr.Col("a").List().ToArray(2),
		expr.Col("a").List().Any(),
		expr.Col("s").List().Diff(1),
		expr.Col("a").List().GatherEvery(0, 0),
	} {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e, df)
		if err == nil {
			out.Release()
			t.Errorf("%s: expected an error", e)
		}
	}
}
