package series_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars 1.39.3 on x = [1, 2, 1e308, -1e308, 3]: a window with no more
// values than ddof is null, and overflow gives inf, then NaN, the way
// polars' Welford update does.
func TestRollingVarMatchesPolars(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromFloat64("x", []float64{1, 2, 1e308, -1e308, 3}, nil, series.WithAllocator(mem))
	defer s.Release()
	cases := []struct {
		name string
		std  bool
		opts series.RollingOptions
		want string
	}{
		{"std(1, 1)", true, series.RollingOptions{WindowSize: 1, MinPeriods: 1}, "[None, None, None, None, None]"},
		{"var(2, 1)", false, series.RollingOptions{WindowSize: 2, MinPeriods: 1}, "[None, 0.5, inf, nan, nan]"},
		{"var(3, 1)", false, series.RollingOptions{WindowSize: 3, MinPeriods: 1}, "[None, 0.5, inf, nan, nan]"},
		{"std(2)", true, series.RollingOptions{WindowSize: 2}, "[None, 0.7071067811865476, inf, nan, nan]"},
	}
	for _, c := range cases {
		var out *series.Series
		var err error
		if c.std {
			out, err = s.RollingStd(c.opts, series.WithAllocator(mem))
		} else {
			out, err = s.RollingVar(c.opts, series.WithAllocator(mem))
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := testutil.PyRepr(out.Chunk(0)); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
		out.Release()
	}
}
