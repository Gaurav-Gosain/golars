package csv

import (
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"
)

func checkFloat(t *testing.T, s string) {
	t.Helper()
	want, err := strconv.ParseFloat(s, 64)
	wantOK := err == nil
	if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
		wantOK = true
	}
	got, ok := parseFloat64([]byte(s))
	if ok != wantOK {
		t.Fatalf("parseFloat64(%q) ok=%v, strconv ok=%v", s, ok, wantOK)
	}
	if ok && math.Float64bits(got) != math.Float64bits(want) && !(math.IsNaN(got) && math.IsNaN(want)) {
		t.Fatalf("parseFloat64(%q) = %v (%x), want %v (%x)", s, got, math.Float64bits(got), want, math.Float64bits(want))
	}
}

func TestParseFloat64MatchesStrconv(t *testing.T) {
	fixed := []string{
		"0", "-0", "+0", "0.0", "-0.0", "1", "-1", "1.5", ".5", "5.", "-.5", "+.5", ".", "-", "+", "",
		"1e10", "1E10", "1e+10", "1e-10", "1.5e-300", "2.2250738585072014e-308", "4.9e-324", "5e-324",
		"1e-400", "1e400", "-1e400", "1.7976931348623157e308", "1.7976931348623159e308",
		"9007199254740993", "9007199254740992.5", "123456789012345678", "1234567890123456789",
		"12345678901234567890", "0.1", "0.2", "0.3", "3.141592653589793", "2.718281828459045",
		"NaN", "nan", "Inf", "-Inf", "+inf", "infinity", "0x1p-2", "1_000", "1e", "1e+", "e5",
		"1.2.3", "1,5", " 1", "1 ", "00012.5000", "0.000000000000000000000001234",
		"100000000000000000000000", "1e23", "8.98846567431158e307", "-0.43510931015298766",
		"4503599627370496.5", "4503599627370497.5", "1e22", "1e-22", "123e-25",
	}
	for _, s := range fixed {
		checkFloat(t, s)
	}
	r := rand.New(rand.NewPCG(9, 10))
	n := 300_000
	if testing.Short() {
		n = 20_000
	}
	for range n {
		var s string
		switch r.IntN(6) {
		case 0:
			s = strconv.FormatFloat(r.NormFloat64(), 'g', -1, 64)
		case 1:
			s = strconv.FormatFloat(math.Float64frombits(r.Uint64()), 'g', -1, 64)
		case 2:
			s = strconv.FormatFloat(r.Float64()*1e6, 'f', r.IntN(12), 64)
		case 3:
			s = strconv.FormatFloat(math.Float64frombits(r.Uint64()), 'e', r.IntN(20), 64)
		case 4:
			// Random digit strings, including more than 19 digits and
			// halfway cases.
			buf := make([]byte, 0, 32)
			nd := 1 + r.IntN(24)
			for i := range nd {
				if i == r.IntN(nd+1) {
					buf = append(buf, '.')
				}
				buf = append(buf, byte('0'+r.IntN(10)))
			}
			if r.IntN(2) == 0 {
				buf = append(buf, 'e')
				buf = strconv.AppendInt(buf, int64(r.IntN(700)-350), 10)
			}
			s = string(buf)
		case 5:
			s = strconv.FormatInt(r.Int64(), 10) + "e" + strconv.Itoa(r.IntN(40)-20)
		}
		checkFloat(t, s)
	}
}

func TestParseInt64(t *testing.T) {
	cases := []string{"0", "-0", "+5", "-", "+", "", "12a", "9223372036854775807", "-9223372036854775808",
		"9223372036854775808", "-9223372036854775809", "123456789012345678", "0000000000000000000001", " 1", "1.0"}
	for _, s := range cases {
		want, err := strconv.ParseInt(s, 10, 64)
		got, ok := parseInt64([]byte(s))
		if ok != (err == nil) || (ok && got != want) {
			t.Errorf("parseInt64(%q) = %d,%v want %d,%v", s, got, ok, want, err)
		}
	}
}

func TestParseDate32(t *testing.T) {
	cases := []string{"1970-01-01", "2024-02-29", "2023-02-29", "2000-02-29", "1900-02-29", "0001-01-01",
		"9999-12-31", "2024-13-01", "2024-00-10", "2024-04-31", "2024-4-01", "20240401", "1969-12-31", "1600-03-01"}
	for _, s := range cases {
		tm, err := time.Parse("2006-01-02", s)
		got, ok := parseDate32([]byte(s))
		if ok != (err == nil) {
			t.Errorf("parseDate32(%q) ok=%v, time.Parse err=%v", s, ok, err)
			continue
		}
		if ok {
			want := int32(tm.Unix() / 86400)
			if tm.Unix() < 0 && tm.Unix()%86400 != 0 {
				want--
			}
			if got != want {
				t.Errorf("parseDate32(%q) = %d, want %d", s, got, want)
			}
		}
	}
}
