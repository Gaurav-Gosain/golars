package framerepr

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// AssertFrame fails t when df does not render to want. Float cells (any
// cell holding a '.', an exponent, nan or inf) compare with a relative
// tolerance of 1e-9 so that summation order differences between golars
// and polars do not cause spurious failures.
func AssertFrame(t testing.TB, df *dataframe.DataFrame, want string) {
	t.Helper()
	got := Frame(df)
	if !Match(got, want) {
		t.Fatalf("frame mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

// AssertSeries is AssertFrame for a single Series.
func AssertSeries(t testing.TB, s *series.Series, want string) {
	t.Helper()
	got := Series(s)
	if !Match(got, want) {
		t.Fatalf("series mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

// Match reports whether two canonical renderings agree, allowing a small
// relative error on float cells.
func Match(got, want string) bool {
	want = strings.TrimSpace(want)
	if got == want {
		return true
	}
	gl := strings.Split(got, "\n")
	wl := strings.Split(want, "\n")
	if len(gl) != len(wl) {
		return false
	}
	for i := range gl {
		if gl[i] == strings.TrimSpace(wl[i]) {
			continue
		}
		if i == 0 {
			return false
		}
		gc := strings.Split(gl[i], "|")
		wc := strings.Split(strings.TrimSpace(wl[i]), "|")
		if len(gc) != len(wc) {
			return false
		}
		for j := range gc {
			if gc[j] != wc[j] && !floatClose(gc[j], wc[j]) {
				return false
			}
		}
	}
	return true
}

func floatClose(a, b string) bool {
	if !looksFloat(a) || !looksFloat(b) {
		return false
	}
	x, err1 := strconv.ParseFloat(a, 64)
	y, err2 := strconv.ParseFloat(b, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	if math.IsNaN(x) || math.IsNaN(y) {
		return math.IsNaN(x) && math.IsNaN(y)
	}
	if x == y {
		return true
	}
	diff := math.Abs(x - y)
	scale := math.Max(math.Abs(x), math.Abs(y))
	return diff <= 1e-9*scale || diff <= 1e-12
}

func looksFloat(s string) bool {
	return strings.ContainsAny(s, ".e") || s == "nan" || s == "inf" || s == "-inf"
}
