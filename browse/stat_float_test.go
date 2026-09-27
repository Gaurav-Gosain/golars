package browse

import (
	"math"
	"testing"
)

func TestStatFloat(t *testing.T) {
	cases := []struct {
		v    float64
		want string
	}{
		{1152.2166666666667, "1152.216667"},
		{2.5, "2.5"},
		{4, "4.0"},
		{0, "0.0"},
		{-3.25, "-3.25"},
		{1e20, "1.000000e+20"},
		{math.NaN(), "NaN"},
	}
	for _, c := range cases {
		if got := statFloat(c.v); got != c.want {
			t.Errorf("statFloat(%v) = %q, want %q", c.v, got, c.want)
		}
	}
}
