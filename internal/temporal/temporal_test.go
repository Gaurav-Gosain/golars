package temporal

import (
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/Gaurav-Gosain/golars/dtype"
)

func TestCivilRoundTripMatchesTimePackage(t *testing.T) {
	for days := int64(-800_000); days <= 800_000; days += 37 {
		y, m, d := CivilFromDays(days)
		want := time.Unix(days*86400, 0).UTC()
		if int(y) != want.Year() || m != int(want.Month()) || d != want.Day() {
			t.Fatalf("CivilFromDays(%d) = %d-%d-%d, want %s", days, y, m, d, want.Format(time.DateOnly))
		}
		if got := DaysFromCivil(y, m, d); got != days {
			t.Fatalf("DaysFromCivil(%d-%d-%d) = %d, want %d", y, m, d, got, days)
		}
		if wd := ISOWeekday(days); wd%7 != int(want.Weekday()) {
			t.Fatalf("ISOWeekday(%d) = %d, want %s", days, wd, want.Weekday())
		}
		iy, iw := ISOWeek(days)
		wy, ww := want.ISOWeek()
		if int(iy) != wy || iw != ww {
			t.Fatalf("ISOWeek(%d) = %d/%d, want %d/%d", days, iy, iw, wy, ww)
		}
	}
}

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		want Duration
		str  string
	}{
		{"1d", Duration{Days: 1}, "1d"},
		{"2h30m", Duration{Nsecs: 2*NsHour + 30*NsMinute}, "9000s"},
		{"1mo", Duration{Months: 1}, "1mo"},
		{"1y", Duration{Months: 12}, "12mo"},
		{"1q", Duration{Months: 3}, "3mo"},
		{"1w", Duration{Weeks: 1}, "1w"},
		{"-3d", Duration{Days: 3, Negative: true}, "-3d"},
		{"1y2mo3d4h", Duration{Months: 14, Days: 3, Nsecs: 4 * NsHour}, "14mo3d14400s"},
		{"5i", Duration{Nsecs: 5, ParsedInt: true}, "5ns"},
		{"1h5000ns", Duration{Nsecs: NsHour + 5000}, "3600000005us"},
	}
	for _, c := range cases {
		got, err := ParseDuration(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("ParseDuration(%q) = %+v, want %+v", c.in, got, c.want)
		}
		if got.String() != c.str {
			t.Errorf("String(%q) = %q, want %q", c.in, got.String(), c.str)
		}
	}
	for _, bad := range []string{"", "d", "1", "1x", "1d-2h", "--1d"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("ParseDuration(%q) should fail", bad)
		}
	}
}

func TestAddMonthsClampsToMonthEnd(t *testing.T) {
	day := func(y int64, m, d int) int64 { return DaysFromCivil(y, m, d) * UnitsPerDay(dtype.Microsecond) }
	cases := []struct {
		from int64
		n    int64
		want int64
	}{
		{day(2020, 1, 31), 1, day(2020, 2, 29)},
		{day(2021, 1, 31), 1, day(2021, 2, 28)},
		{day(2020, 3, 31), -1, day(2020, 2, 29)},
		{day(2020, 2, 29), 12, day(2021, 2, 28)},
		{day(1969, 12, 31), 2, day(1970, 2, 28)},
		{day(2000, 5, 31), -15, day(1999, 2, 28)},
	}
	for _, c := range cases {
		if got := AddMonths(c.from, dtype.Microsecond, c.n); got != c.want {
			t.Errorf("AddMonths(%s, %d) = %s, want %s", FormatNaive(c.from, dtype.Microsecond), c.n,
				FormatNaive(got, dtype.Microsecond), FormatNaive(c.want, dtype.Microsecond))
		}
	}
}

func TestZoneCandidatesAcrossDST(t *testing.T) {
	z, err := NewZone("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	local := func(y int64, m, d, h, mi int) int64 {
		return DaysFromCivil(y, m, d)*SecondsPerDay + int64(h*3600+mi*60)
	}
	// 2024-03-10 02:30 does not exist.
	if _, _, n := z.Candidates(local(2024, 3, 10, 2, 30)); n != 0 {
		t.Errorf("non-existent time: n=%d", n)
	}
	// 2024-11-03 01:30 happens twice: EDT then EST.
	e, l, n := z.Candidates(local(2024, 11, 3, 1, 30))
	if n != 2 || e != 1730611800 || l != 1730615400 {
		t.Errorf("ambiguous time: %d %d n=%d", e, l, n)
	}
	e, _, n = z.Candidates(local(2024, 7, 1, 12, 0))
	if n != 1 || e != 1719849600 {
		t.Errorf("summer time: %d n=%d", e, n)
	}
}

func TestFormatAndParseRoundTrip(t *testing.T) {
	f, err := CompileFormat("%Y-%m-%d %H:%M:%S%.f")
	if err != nil {
		t.Fatal(err)
	}
	for _, us := range []int64{0, -1, 1582983930123456, -2208988800000000, 253402300799999999} {
		var fl Fields
		fl.SetLocal(us, us, UnitsPerSecond(dtype.Microsecond))
		s, err := f.Append(nil, &fl)
		if err != nil {
			t.Fatal(err)
		}
		r, ok := f.Parse(string(s))
		if !ok {
			t.Fatalf("parse %q failed", s)
		}
		if got := r.Ticks(dtype.Microsecond, true); got != us {
			t.Errorf("round trip %d -> %q -> %d", us, s, got)
		}
	}
}

func TestParseRejectsTrailingInput(t *testing.T) {
	f, _ := CompileFormat("%Y-%m-%d")
	if _, ok := f.Parse("2021-01-02 extra"); ok {
		t.Error("trailing text should fail an exact parse")
	}
	if r, ok := f.ParseSearch("id 2021-01-02 extra"); !ok || r.Days != DaysFromCivil(2021, 1, 2) {
		t.Error("search parse should find the embedded date")
	}
}

func TestBusinessCalendar(t *testing.T) {
	cal, err := NewBusinessCalendar(DefaultWeekMask, []int64{DaysFromCivil(2024, 1, 8)})
	if err != nil {
		t.Fatal(err)
	}
	fri := DaysFromCivil(2024, 1, 5)
	got, err := cal.AddBusinessDays(fri, 1, RollRaise)
	if err != nil {
		t.Fatal(err)
	}
	if want := DaysFromCivil(2024, 1, 9); got != want {
		t.Errorf("friday + 1 over a monday holiday = %d, want %d", got, want)
	}
	if _, err := cal.AddBusinessDays(DaysFromCivil(2024, 1, 6), 1, RollRaise); err == nil {
		t.Error("saturday start should raise")
	}
}
