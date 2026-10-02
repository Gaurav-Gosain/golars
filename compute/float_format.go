package compute

import (
	"math"
	"strconv"
	"strings"
)

// FormatFloatPolars formats v the way polars casts a float to a string
// (the zmij shortest round-trip format): fixed notation with at least
// one decimal ("1.0") when the decimal exponent is in -5..=15 for f64
// or -6..=12 for f32, scientific otherwise ("1e+20", "1.5e-7"), and
// "NaN", "inf", "-inf" for non-finite values. bits is 32 or 64.
func FormatFloatPolars(v float64, bits int) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	case v == 0:
		if math.Signbit(v) {
			return "-0.0"
		}
		return "0.0"
	}
	sci := strconv.FormatFloat(v, 'e', -1, bits)
	mant, expStr, _ := strings.Cut(sci, "e")
	exp, _ := strconv.Atoi(expStr)
	lo, hi := -5, 15
	if bits == 32 {
		lo, hi = -6, 12
	}
	if exp >= lo && exp <= hi {
		s := strconv.FormatFloat(v, 'f', -1, bits)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	sign := "+"
	if exp < 0 {
		sign, exp = "-", -exp
	}
	return mant + "e" + sign + strconv.Itoa(exp)
}
