package json

import (
	"encoding/json"
	"math"
	"testing"
)

// TestFastFormatMatchesMarshal checks the direct formatters against
// encoding/json.
func TestFastFormatMatchesMarshal(t *testing.T) {
	floats := []float64{0, math.Copysign(0, -1), 1, -1.5, 0.1, 1e-6, 9.99e-7, 1e-7, 1.5e-9, 1e20, 1e21, 123456789.125, -3.4e38, math.MaxFloat64, math.SmallestNonzeroFloat64}
	for _, f := range floats {
		want, _ := json.Marshal(f)
		if got := appendFloat(nil, f); string(got) != string(want) {
			t.Errorf("appendFloat(%v) = %s, want %s", f, got, want)
		}
	}
	strs := []string{"", "plain", `quo"te`, `back\slash`, "<tag>&", "tab\there", "nl\n", "café", " ", "\xff\xfe", "~\x7f"}
	for _, s := range strs {
		want, _ := json.Marshal(s)
		if got := appendString(nil, s); string(got) != string(want) {
			t.Errorf("appendString(%q) = %s, want %s", s, got, want)
		}
	}
}
