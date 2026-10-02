package exprparse

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// A variadic method called with no arguments must see a nil slice, the
// same as a Go call. str.strip_chars() strips whitespace in polars; an
// empty non-nil set would strip nothing. Found by internal/difftest.
func TestVariadicNoArgsIsNil(t *testing.T) {
	s, err := series.FromString("s", []string{" x ", "\ty\n"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	cases := map[string][]string{
		`s.str.strip_chars()`:       {"x", "y"},
		`s.str.strip_chars_start()`: {"x ", "y\n"},
		`s.str.strip_chars_end()`:   {" x", "\ty"},
	}
	for src, want := range cases {
		e, err := Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		out, err := lazy.FromDataFrame(df.Clone()).Select(e.Alias("o")).Collect(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		col, err := out.Column("o")
		if err != nil {
			t.Fatal(err)
		}
		for i, w := range want {
			if got := col.Chunk(0).(interface{ Value(int) string }).Value(i); got != w {
				t.Errorf("%s row %d: got %q, want %q", src, i, got, w)
			}
		}
		out.Release()
	}
}
