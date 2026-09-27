package series_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// seriesRepr renders s like Python's repr(s.to_list()).
func seriesRepr(t *testing.T, s *series.Series) string {
	t.Helper()
	a, err := s.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	return testutil.PyRepr(a)
}

func strFixture(t *testing.T, mem memory.Allocator) *series.Series {
	t.Helper()
	s, err := series.FromString("s",
		[]string{"a,b,c", "", "", "héllo,wörld", ",x,", "abc", "A::b::"},
		[]bool{true, true, false, true, true, true, true},
		series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Expected values captured from polars 1.39.3 on the same input.
func TestStrNamespaceParity(t *testing.T) {
	type op func(o series.StrOps, opt series.Option) (*series.Series, error)
	cases := []struct {
		name  string
		dtype string
		run   op
		want  string
	}{
		{"split", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Split(",", false, opt) },
			"[['a', 'b', 'c'], [''], None, ['héllo', 'wörld'], ['', 'x', ''], ['abc'], ['A::b::']]"},
		{"split_empty", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Split("", false, opt) },
			"[['a', ',', 'b', ',', 'c'], [], None, ['h', 'é', 'l', 'l', 'o', ',', 'w', 'ö', 'r', 'l', 'd'], [',', 'x', ','], ['a', 'b', 'c'], ['A', ':', ':', 'b', ':', ':']]"},
		{"split_incl", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Split(",", true, opt) },
			"[['a,', 'b,', 'c'], [], None, ['héllo,', 'wörld'], [',', 'x,'], ['abc'], ['A::b::']]"},
		{"split_multi", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Split("::", false, opt) },
			"[['a,b,c'], [''], None, ['héllo,wörld'], [',x,'], ['abc'], ['A', 'b', '']]"},
		{"split_multi_incl", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Split("::", true, opt) },
			"[['a,b,c'], [], None, ['héllo,wörld'], [',x,'], ['abc'], ['A::', 'b::']]"},
		{"splitn2", "struct{field_0: str, field_1: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.SplitNStruct(",", 2, opt) },
			"[{'field_0': 'a', 'field_1': 'b,c'}, {'field_0': '', 'field_1': None}, {'field_0': None, 'field_1': None}, {'field_0': 'héllo', 'field_1': 'wörld'}, {'field_0': '', 'field_1': 'x,'}, {'field_0': 'abc', 'field_1': None}, {'field_0': 'A::b::', 'field_1': None}]"},
		{"splitn1", "struct{field_0: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.SplitNStruct(",", 1, opt) },
			"[{'field_0': 'a,b,c'}, {'field_0': ''}, {'field_0': None}, {'field_0': 'héllo,wörld'}, {'field_0': ',x,'}, {'field_0': 'abc'}, {'field_0': 'A::b::'}]"},
		{"splitn3_empty", "struct{field_0: str, field_1: str, field_2: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.SplitNStruct("", 3, opt) },
			"[{'field_0': 'a', 'field_1': ',', 'field_2': 'b,c'}, {'field_0': None, 'field_1': None, 'field_2': None}, {'field_0': None, 'field_1': None, 'field_2': None}, {'field_0': 'h', 'field_1': 'é', 'field_2': 'llo,wörld'}, {'field_0': ',', 'field_1': 'x', 'field_2': ','}, {'field_0': 'a', 'field_1': 'b', 'field_2': 'c'}, {'field_0': 'A', 'field_1': ':', 'field_2': ':b::'}]"},
		{"split_exact1", "struct{field_0: str, field_1: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.SplitExactStruct(",", 1, false, opt)
		}, "[{'field_0': 'a', 'field_1': 'b'}, {'field_0': '', 'field_1': None}, {'field_0': None, 'field_1': None}, {'field_0': 'héllo', 'field_1': 'wörld'}, {'field_0': '', 'field_1': 'x'}, {'field_0': 'abc', 'field_1': None}, {'field_0': 'A::b::', 'field_1': None}]"},
		{"split_exact1_incl", "struct{field_0: str, field_1: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.SplitExactStruct(",", 1, true, opt)
		}, "[{'field_0': 'a,', 'field_1': 'b,'}, {'field_0': None, 'field_1': None}, {'field_0': None, 'field_1': None}, {'field_0': 'héllo,', 'field_1': 'wörld'}, {'field_0': ',', 'field_1': 'x,'}, {'field_0': 'abc', 'field_1': None}, {'field_0': 'A::b::', 'field_1': None}]"},
		{"join", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Join("-", true, opt) },
			"['a,b,c--héllo,wörld-,x,-abc-A::b::']"},
		{"join_nulls", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Join("-", false, opt) },
			"[None]"},
		{"contains_any", "bool", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ContainsAny([]string{"b", "wö"}, false, opt)
		}, "[True, False, None, True, False, True, True]"},
		{"contains_any_ci", "bool", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ContainsAny([]string{"B", "::"}, true, opt)
		}, "[True, False, None, False, False, True, True]"},
		{"escape_regex", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.EscapeRegex(opt) },
			"['a,b,c', '', None, 'héllo,wörld', ',x,', 'abc', 'A::b::']"},
		{"explode", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Explode(opt) },
			"['a', ',', 'b', ',', 'c', '', None, 'h', 'é', 'l', 'l', 'o', ',', 'w', 'ö', 'r', 'l', 'd', ',', 'x', ',', 'a', 'b', 'c', 'A', ':', ':', 'b', ':', ':']"},
		{"extract_all", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.ExtractAll(`\w+`, opt) },
			"[['a', 'b', 'c'], [], None, ['héllo', 'wörld'], ['x'], ['abc'], ['A', 'b']]"},
		{"extract_groups", "struct{1: str, second: str}", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractGroups(`(\w+),(?P<second>\w+)`, opt)
		}, "[{'1': 'a', 'second': 'b'}, {'1': None, 'second': None}, None, {'1': 'héllo', 'second': 'wörld'}, {'1': None, 'second': None}, {'1': None, 'second': None}, {'1': None, 'second': None}]"},
		{"extract_many", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractMany([]string{"a", "b", "ab", "bc", "::"}, false, false, opt)
		}, "[['a', 'b'], [], None, [], [], ['a', 'b'], ['::', 'b', '::']]"},
		{"extract_many_ov", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractMany([]string{"a", "ab", "bc", "b"}, false, true, opt)
		}, "[['a', 'b'], [], None, [], [], ['a', 'ab', 'b', 'bc'], ['b']]"},
		{"extract_many_ci", "list[str]", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ExtractMany([]string{"a", "b"}, true, false, opt)
		}, "[['a', 'b'], [], None, [], [], ['a', 'b'], ['A', 'b']]"},
		{"find_many", "list[u32]", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.FindMany([]string{"b", "l", "::"}, false, false, opt)
		}, "[[2], [], None, [3, 4, 11], [], [1], [1, 3, 4]]"},
		{"replace_many", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ReplaceMany([]string{"a", "bc", "é", ","}, []string{"X", "", "e", ";"}, false, opt)
		}, "['X;b;c', '', None, 'hello;wörld', ';x;', 'X', 'A::b::']"},
		{"replace_many_bcast", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.ReplaceMany([]string{"a", "b"}, []string{"Z"}, false, opt)
		}, "['Z,Z,c', '', None, 'héllo,wörld', ',x,', 'ZZc', 'A::Z::']"},
		{"reverse", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Reverse(opt) },
			"['c,b,a', '', None, 'dlröw,olléh', ',x,', 'cba', '::b::A']"},
		{"strip_chars_set", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripChars([]string{",a"}, opt)
		}, "['b,c', '', None, 'héllo,wörld', 'x', 'bc', 'A::b::']"},
		{"strip_chars_start", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripCharsStart([]string{",a"}, opt)
		}, "['b,c', '', None, 'héllo,wörld', 'x,', 'bc', 'A::b::']"},
		{"strip_chars_end", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) {
			return o.StripCharsEnd([]string{":,"}, opt)
		}, "['a,b,c', '', None, 'héllo,wörld', ',x', 'abc', 'A::b']"},
		{"to_titlecase", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.ToTitlecase(opt) },
			"['A,B,C', '', None, 'Héllo,Wörld', ',X,', 'Abc', 'A::B::']"},
		{"pad_start", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.PadStart(6, '*', opt) },
			"['*a,b,c', '******', None, 'héllo,wörld', '***,x,', '***abc', 'A::b::']"},
		{"pad_end", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.PadEnd(6, ' ', opt) },
			"['a,b,c ', '      ', None, 'héllo,wörld', ',x,   ', 'abc   ', 'A::b::']"},
		{"zfill", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.ZFill(6, opt) },
			"['0a,b,c', '000000', None, 'héllo,wörld', '000,x,', '000abc', 'A::b::']"},
		{"extract", "str", func(o series.StrOps, opt series.Option) (*series.Series, error) { return o.Extract(`(\w+),`, 1, opt) },
			"['a', None, None, 'héllo', 'x', None, None]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			s := strFixture(t, mem)
			defer s.Release()
			out, err := c.run(s.Str(), series.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Name() != "s" {
				t.Errorf("name = %q, want s", out.Name())
			}
			if got := out.DType().String(); got != c.dtype {
				t.Errorf("dtype = %s, want %s", got, c.dtype)
			}
			if got := seriesRepr(t, out); got != c.want {
				t.Errorf("\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

func TestStrStripWhitespaceParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("w", []string{"  xa x ", "", "", "\t\n hi \u00a0"},
		[]bool{true, false, true, true}, series.WithAllocator(mem))
	defer s.Release()
	opt := series.WithAllocator(mem)
	check := func(out *series.Series, err error, want string) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		if got := seriesRepr(t, out); got != want {
			t.Errorf("\n got: %s\nwant: %s", got, want)
		}
	}
	out, err := s.Str().StripChars(nil, opt)
	check(out, err, "['xa x', None, '', 'hi']")
	out, err = s.Str().StripCharsStart(nil, opt)
	check(out, err, `['xa x ', None, '', 'hi \xa0']`)
	out, err = s.Str().StripCharsEnd(nil, opt)
	check(out, err, `['  xa x', None, '', '\t\n hi']`)
	out, err = s.Str().StripChars([]string{""}, opt)
	check(out, err, `['  xa x ', None, '', '\t\n hi \xa0']`)
}

func TestStrToIntegerParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("i",
		[]string{"1", "-2", "", "ff", " 3", "+4", "", "99999999999999999999"},
		[]bool{true, true, false, true, true, true, true, true}, series.WithAllocator(mem))
	defer s.Release()
	opt := series.WithAllocator(mem)
	cases := []struct {
		base int
		dt   dtype.DType
		dts  string
		want string
	}{
		{10, dtype.DType{}, "i64", "[1, -2, None, None, None, 4, None, None]"},
		{16, dtype.Int64(), "i64", "[1, -2, None, 255, None, 4, None, None]"},
		{10, dtype.Uint8(), "u8", "[1, None, None, None, None, 4, None, None]"},
		{10, dtype.Int8(), "i8", "[1, -2, None, None, None, 4, None, None]"},
	}
	for _, c := range cases {
		out, err := s.Str().ToInteger(c.base, c.dt, false, opt)
		if err != nil {
			t.Fatal(err)
		}
		if got := out.DType().String(); got != c.dts {
			t.Errorf("dtype = %s, want %s", got, c.dts)
		}
		if got := seriesRepr(t, out); got != c.want {
			t.Errorf("base %d %s:\n got: %s\nwant: %s", c.base, c.dts, got, c.want)
		}
		out.Release()
	}
	_, err := s.Str().ToInteger(10, dtype.Int64(), true, opt)
	if err == nil || !strings.Contains(err.Error(), "strict integer parsing failed for 4 value(s)") ||
		!strings.Contains(err.Error(), "invalid digit found in string") {
		t.Errorf("strict error = %v", err)
	}
	if _, err := s.Str().ToInteger(1, dtype.Int64(), false, opt); err == nil {
		t.Error("base 1 should fail")
	}
	if _, err := s.Str().ToInteger(10, dtype.Float64(), false, opt); err == nil {
		t.Error("float target should fail")
	}
}

func TestStrMiscEdgeCases(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s, _ := series.FromString("s",
		[]string{"a👍🏽b", "🇺🇸🇫🇷", "a\r\nb", "x\u200dy", "e\u0301x", "it's DONE", "ßx ﬁne"}, nil, opt)
	defer s.Release()
	rev, err := s.Str().Reverse(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Release()
	want := "['b\U0001F44D\U0001F3FDa', '\U0001F1EB\U0001F1F7\U0001F1FA\U0001F1F8', 'b\\r\\na', 'yx\\u200d', 'xé', \"ENOD s'ti\", 'enﬁ xß']"
	if got := seriesRepr(t, rev); got != want {
		t.Errorf("reverse:\n got: %s\nwant: %s", got, want)
	}
	title, err := s.Str().ToTitlecase(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer title.Release()
	arr, _ := title.Consolidated()
	defer arr.Release()
	got := testutil.PyRepr(arr)
	if !strings.Contains(got, `"It'S Done"`) || !strings.Contains(got, "'SSx FIne'") {
		t.Errorf("titlecase: %s", got)
	}

	// Empty input keeps dtype and length for every list and struct kernel.
	e := series.Empty("e", dtype.String())
	defer e.Release()
	for _, run := range []func() (*series.Series, error){
		func() (*series.Series, error) { return e.Str().Split(",", false, opt) },
		func() (*series.Series, error) { return e.Str().SplitNStruct(",", 2, opt) },
		func() (*series.Series, error) { return e.Str().ExtractGroups(`(a)`, opt) },
		func() (*series.Series, error) { return e.Str().FindMany([]string{"a"}, false, false, opt) },
		func() (*series.Series, error) { return e.Str().Join(",", true, opt) },
	} {
		out, err := run()
		if err != nil {
			t.Fatal(err)
		}
		out.Release()
	}
	if _, err := s.Str().ReplaceMany([]string{"a", "b"}, []string{"x", "y", "z"}, false, opt); err == nil {
		t.Error("replace_many with mismatched replacements should fail")
	}
	em, err := s.Str().ExtractMany([]string{""}, false, false, opt)
	if err != nil {
		t.Fatal(err)
	}
	em.Release()
}
