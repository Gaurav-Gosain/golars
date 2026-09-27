package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func strFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	s, _ := series.FromString("s", []string{"a,b", "", "", "héllo"}, []bool{true, true, false, true},
		series.WithAllocator(mem))
	arr := jsonSeries(t, mem, "arr", arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64),
		`[[3, 1], [null, 2], null, [5, 5]]`)
	df, err := dataframe.New(s, arr)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Every new str.* / arr.* / name.* FunctionNode dispatches through the
// evaluator with polars' result (expected values from polars 1.39.3).
func TestNamespaceDispatch(t *testing.T) {
	s := expr.Col("s").Str()
	a := expr.Col("arr").Arr()
	cases := []struct {
		name  string
		e     expr.Expr
		out   string
		dtype string
		want  string
	}{
		{"split", s.Split(","), "s", "list[str]", "[['a', 'b'], [''], None, ['héllo']]"},
		{"split_inclusive", s.SplitInclusive(","), "s", "list[str]", "[['a,', 'b'], [], None, ['héllo']]"},
		{"splitn", s.SplitN(",", 2), "s", "struct{field_0: str, field_1: str}",
			"[{'field_0': 'a', 'field_1': 'b'}, {'field_0': '', 'field_1': None}, {'field_0': None, 'field_1': None}, {'field_0': 'héllo', 'field_1': None}]"},
		{"split_exact", s.SplitExactN(",", 0, false), "s", "struct{field_0: str}",
			"[{'field_0': 'a'}, {'field_0': ''}, {'field_0': None}, {'field_0': 'héllo'}]"},
		{"join", s.Join("|", true), "s", "str", "['a,b||héllo']"},
		{"contains_any", s.ContainsAny([]string{"é", "b"}, false), "s", "bool", "[True, False, None, True]"},
		{"escape_regex", s.EscapeRegex(), "s", "str", "['a,b', '', None, 'héllo']"},
		{"explode", s.Explode(), "s", "str", "['a', ',', 'b', '', None, 'h', 'é', 'l', 'l', 'o']"},
		{"extract", s.Extract(`(\w)$`, 1), "s", "str", "['b', None, None, 'o']"},
		{"extract_all", s.ExtractAll(`l`), "s", "list[str]", "[[], [], None, ['l', 'l']]"},
		{"extract_groups", s.ExtractGroups(`(?P<first>\w)`), "s", "struct{first: str}",
			"[{'first': 'a'}, {'first': None}, None, {'first': 'h'}]"},
		{"extract_many", s.ExtractMany([]string{"l", "a"}, false, false), "s", "list[str]", "[['a'], [], None, ['l', 'l']]"},
		{"find_many", s.FindMany([]string{"l"}, false, false), "s", "list[u32]", "[[], [], None, [3, 4]]"},
		{"replace_many", s.ReplaceMany([]string{"l"}, []string{"L"}, false), "s", "str", "['a,b', '', None, 'héLLo']"},
		{"reverse", s.Reverse(), "s", "str", "['b,a', '', None, 'olléh']"},
		{"strip_chars", s.StripChars("ah"), "s", "str", "[',b', '', None, 'éllo']"},
		{"strip_chars_start", s.StripCharsStart("a"), "s", "str", "[',b', '', None, 'héllo']"},
		{"strip_chars_end", s.StripCharsEnd("o"), "s", "str", "['a,b', '', None, 'héll']"},
		{"to_titlecase", s.ToTitlecase(), "s", "str", "['A,B', '', None, 'Héllo']"},
		{"pad_start", s.PadStart(4, '_'), "s", "str", "['_a,b', '____', None, 'héllo']"},
		{"pad_end", s.PadEnd(4, '_'), "s", "str", "['a,b_', '____', None, 'héllo']"},
		{"zfill", s.ZFill(4), "s", "str", "['0a,b', '0000', None, 'héllo']"},
		{"to_integer", expr.Col("s").Str().Replace(",", "").Str().ToInteger(16, dtype.Int64(), false), "s", "i64", "[171, None, None, None]"},
		{"arr_sum", a.Sum(), "arr", "i64", "[4, 2, None, 10]"},
		{"arr_max", a.Max(), "arr", "i64", "[3, 2, None, 5]"},
		{"arr_sort", a.Sort(false, false), "arr", "list[i64; 2]", "[[1, 3], [None, 2], None, [5, 5]]"},
		{"arr_unique", a.Unique(true), "arr", "list[i64]", "[[3, 1], [None, 2], None, [5]]"},
		{"arr_get", a.Get(-1, false), "arr", "i64", "[1, 2, None, 5]"},
		{"arr_to_list", a.ToList(), "arr", "list[i64]", "[[3, 1], [None, 2], None, [5, 5]]"},
		{"arr_to_struct", a.ToStruct(), "arr", "struct{field_0: i64, field_1: i64}",
			"[{'field_0': 3, 'field_1': 1}, {'field_0': None, 'field_1': 2}, {'field_0': None, 'field_1': None}, {'field_0': 5, 'field_1': 5}]"},
		{"arr_contains", a.Contains(5), "arr", "bool", "[False, False, None, True]"},
		{"arr_join_err_free", a.CountMatches(5), "arr", "u32", "[0, 0, None, 2]"},
		{"name_suffix", expr.Col("s").Name().Suffix("_x"), "s_x", "str", "['a,b', '', None, 'héllo']"},
		{"name_keep", expr.Col("s").Alias("q").Name().Keep(), "s", "str", "['a,b', '', None, 'héllo']"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			df := strFrame(t, mem)
			defer df.Release()
			out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, c.e, df)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Name() != c.out {
				t.Errorf("name = %q, want %q", out.Name(), c.out)
			}
			if got := out.DType().String(); got != c.dtype {
				t.Errorf("dtype = %s, want %s", got, c.dtype)
			}
			if got := reprOf(t, out); got != c.want {
				t.Errorf("\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

func TestNamespaceDispatchErrors(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := strFrame(t, mem)
	defer df.Release()
	for _, e := range []expr.Expr{
		expr.Field("x"),
		expr.Col("s").Struct().Unnest(),
		expr.Col("s").Str().ToInteger(10, dtype.Int64(), true),
		expr.Col("arr").Arr().Get(5, false),
		expr.Col("arr").Arr().Join("-", true),
		expr.Col("s").Str().ReplaceMany([]string{"a", "b"}, []string{"1", "2", "3"}, false),
	} {
		out, err := eval.Eval(context.Background(), eval.EvalContext{Alloc: mem}, e, df)
		if err == nil {
			out.Release()
			t.Errorf("%s: expected an error", e)
		}
	}
}
