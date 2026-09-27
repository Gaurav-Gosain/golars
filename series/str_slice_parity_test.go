package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// Expected values come from polars 1.39.3 Series.str.slice. A negative
// offset past the start shortens the window (found by
// internal/difftest).
func TestStrSliceNegativeOffsetParity(t *testing.T) {
	in := []string{"c0", "abcdef", "", "é日本x"}
	cases := []struct {
		off, n int // n < 0 means no length
		want   []string
	}{
		{-3, 2, []string{"c", "de", "", "日本"}},
		{-10, 3, []string{"", "", "", ""}},
		{-10, 20, []string{"c0", "abcdef", "", "é日本x"}},
		{1, -1, []string{"0", "bcdef", "", "日本x"}},
		{-2, -1, []string{"c0", "ef", "", "本x"}},
		{10, 2, []string{"", "", "", ""}},
		{2, 0, []string{"", "", "", ""}},
		{-1, 1, []string{"0", "f", "", "x"}},
		{0, 100, []string{"c0", "abcdef", "", "é日本x"}},
	}
	s, err := series.FromString("s", in, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	for _, tc := range cases {
		out, err := s.Str().Slice(tc.off, tc.n)
		if err != nil {
			t.Fatal(err)
		}
		a := out.Chunk(0).(*array.String)
		for i, w := range tc.want {
			if got := a.Value(i); got != w {
				t.Errorf("slice(%d, %d) of %q: got %q, want %q", tc.off, tc.n, in[i], got, w)
			}
		}
		out.Release()
	}
}
