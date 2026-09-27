package series_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// The whole-buffer kernels must not report matches that straddle two
// rows, and must honour slice offsets and nulls.
func TestStrWholeBufferKernels(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	base, _ := series.FromString("s",
		[]string{"zz", "ab", "cd", "abc", "", "x-y", "bcd", "q"},
		[]bool{true, true, true, true, true, true, false, true}, opt)
	defer base.Release()
	s, err := base.Slice(1, 6)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
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
	out, err := s.Str().Contains("bc", opt)
	check(out, err, "[False, False, True, False, False, None]")
	out, err = s.Str().Contains("-", opt)
	check(out, err, "[False, False, False, False, True, None]")
	out, err = s.Str().Split("-", false, opt)
	check(out, err, "[['ab'], ['cd'], ['abc'], [''], ['x', 'y'], None]")
	out, err = s.Str().Split("c", false, opt)
	check(out, err, "[['ab'], ['', 'd'], ['ab', ''], [''], ['x-y'], None]")
}
