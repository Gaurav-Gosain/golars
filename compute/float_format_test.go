package compute

import (
	"math"
	"testing"
)

// Expected strings are polars 1.39.3 Series.cast(pl.String) results.
func TestFormatFloatPolars(t *testing.T) {
	in := []float64{1.0, 0.1, 1e20, 1.5e-7, math.NaN(), math.Inf(1), math.Copysign(0, -1), 123456789.0, 1e16, 1e15, 0.0001, 1e-5}
	want64 := []string{"1.0", "0.1", "1e+20", "1.5e-7", "NaN", "inf", "-0.0", "123456789.0", "1e+16", "1000000000000000.0", "0.0001", "0.00001"}
	want32 := []string{"1.0", "0.1", "1e+20", "1.5e-7", "NaN", "inf", "-0.0", "123456790.0", "1e+16", "1e+15", "0.0001", "0.00001"}
	for i, v := range in {
		if got := FormatFloatPolars(v, 64); got != want64[i] {
			t.Errorf("f64 %v = %q, want %q", v, got, want64[i])
		}
		if got := FormatFloatPolars(float64(float32(v)), 32); got != want32[i] {
			t.Errorf("f32 %v = %q, want %q", v, got, want32[i])
		}
	}
}
