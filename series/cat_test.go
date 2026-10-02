package series_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// catFixture mirrors the polars parity script: nulls, empty string,
// multi-byte UTF-8.
var catFixtureVals = []string{"abc", "", "", "héllo", "日本語テキスト", "b"}
var catFixtureValid = []bool{true, false, true, true, true, true}

func catFixture(t *testing.T, opts ...series.Option) *series.Series {
	t.Helper()
	s, err := series.NewCategorical("c", catFixtureVals, catFixtureValid, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// catCells renders a Series as strings with "<null>" for nulls, going
// through ValueStr so it works for any dtype.
func catCells(t *testing.T, s *series.Series) []string {
	t.Helper()
	a := s.Chunk(0)
	out := make([]string, a.Len())
	for i := range out {
		if a.IsNull(i) {
			out[i] = "<null>"
			continue
		}
		out[i] = a.ValueStr(i)
	}
	return out
}

func TestCategoricalConstruct(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := catFixture(t, series.WithAllocator(mem))
	defer s.Release()
	if !s.DType().IsCategorical() || s.DType().String() != "cat" {
		t.Fatalf("dtype = %s", s.DType())
	}
	d := s.Chunk(0).(*array.Dictionary)
	// Codes follow first appearance; the null row does not add a category.
	dict := d.Dictionary().(*array.String)
	var cats []string
	for i := range dict.Len() {
		cats = append(cats, dict.Value(i))
	}
	testutil.AssertStrings(t, cats, []string{"abc", "", "héllo", "日本語テキスト", "b"})
	if s.NullCount() != 1 {
		t.Fatalf("null count = %d", s.NullCount())
	}
}

func TestCatOpsParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := catFixture(t, series.WithAllocator(mem))
	defer s.Release()
	opt := series.WithAllocator(mem)

	cases := []struct {
		name string
		run  func() (*series.Series, error)
		want []string
		dt   string
	}{
		{"len_bytes", func() (*series.Series, error) { return s.Cat().LenBytes(opt) },
			[]string{"3", "<null>", "0", "6", "21", "1"}, "u32"},
		{"len_chars", func() (*series.Series, error) { return s.Cat().LenChars(opt) },
			[]string{"3", "<null>", "0", "5", "7", "1"}, "u32"},
		{"starts_with empty", func() (*series.Series, error) { return s.Cat().StartsWith("", opt) },
			[]string{"true", "<null>", "true", "true", "true", "true"}, "bool"},
		{"ends_with unicode", func() (*series.Series, error) { return s.Cat().EndsWith("語テキスト", opt) },
			[]string{"false", "<null>", "false", "false", "true", "false"}, "bool"},
		{"get_categories", func() (*series.Series, error) { return s.Cat().GetCategories(opt) },
			[]string{"abc", "", "héllo", "日本語テキスト", "b"}, "str"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := c.run()
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.DType().String() != c.dt {
				t.Fatalf("dtype = %s want %s", out.DType(), c.dt)
			}
			if out.Name() != "c" {
				t.Fatalf("name = %q", out.Name())
			}
			testutil.AssertStrings(t, catCells(t, out), c.want)
		})
	}
}

func TestCatSliceParity(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := catFixture(t, series.WithAllocator(mem))
	defer s.Release()
	// Expected values produced by polars 1.39.3 (length -1 is None).
	cases := []struct {
		off, length int
		want        []string
	}{
		{0, 2, []string{"ab", "<null>", "", "hé", "日本", "b"}},
		{1, -1, []string{"bc", "<null>", "", "éllo", "本語テキスト", ""}},
		{-2, -1, []string{"bc", "<null>", "", "lo", "スト", "b"}},
		{-10, 2, []string{"", "<null>", "", "", "", ""}},
		{-2, 1, []string{"b", "<null>", "", "l", "ス", ""}},
		{10, -1, []string{"", "<null>", "", "", "", ""}},
		{2, 0, []string{"", "<null>", "", "", "", ""}},
		{-4, 3, []string{"ab", "<null>", "", "éll", "テキス", ""}},
	}
	for _, c := range cases {
		out, err := s.Cat().Slice(c.off, c.length, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if !out.DType().IsString() {
			t.Fatalf("dtype = %s", out.DType())
		}
		got := catCells(t, out)
		out.Release()
		if !slices.Equal(got, c.want) {
			t.Errorf("slice(%d, %d) = %q want %q", c.off, c.length, got, c.want)
		}
	}
}

func TestEnumConstruct(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	e := dtype.Enum([]string{"b", "a", "c"})
	s, err := series.NewEnum("e", []string{"a", "", "c", "a"}, []bool{true, false, true, true}, e, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	if !s.DType().IsEnum() || s.DType().String() != "enum" {
		t.Fatalf("dtype = %s", s.DType())
	}
	cats, err := s.Cat().GetCategories(series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer cats.Release()
	testutil.AssertStrings(t, catCells(t, cats), []string{"b", "a", "c"})
	codes := s.Chunk(0).(*array.Dictionary)
	if codes.GetValueIndex(0) != 1 || codes.GetValueIndex(2) != 2 {
		t.Fatalf("physical codes wrong: %v", codes)
	}

	_, err = series.NewEnum("e", []string{"a", "q", "q"}, nil, e)
	if err == nil || !strings.Contains(err.Error(), "for 2 out of 3 values: [\"q\"]") {
		t.Fatalf("err = %v", err)
	}
}

func TestCatOpsWrongDType(t *testing.T) {
	s, _ := series.FromString("s", []string{"a"}, nil)
	defer s.Release()
	if _, err := s.Cat().LenBytes(); err == nil {
		t.Fatal("expected error on str")
	}
}

func TestCatEmpty(t *testing.T) {
	s := series.Empty("c", dtype.Categorical())
	defer s.Release()
	out, err := s.Cat().LenBytes()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len = %d", out.Len())
	}
}

func TestCatFormat(t *testing.T) {
	s := catFixture(t)
	defer s.Release()
	txt := s.String()
	if !strings.Contains(txt, "\"héllo\"") || !strings.Contains(txt, "cat") {
		t.Fatalf("format:\n%s", txt)
	}
}
