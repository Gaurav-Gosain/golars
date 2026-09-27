// Package temporal holds the calendar, time zone, duration, window and
// format primitives shared by golars' temporal kernels.
//
// Everything here works on plain int64 physical values (the storage of
// arrow Date32, Timestamp, Duration and Time arrays) so the series,
// dataframe and compute layers can share one implementation of the
// polars semantics without depending on each other.
package temporal

import "github.com/Gaurav-Gosain/golars/dtype"

// Nanosecond counts per calendar unit.
const (
	NsMicrosecond int64 = 1_000
	NsMillisecond int64 = 1_000_000
	NsSecond      int64 = 1_000_000_000
	NsMinute            = 60 * NsSecond
	NsHour              = 60 * NsMinute
	NsDay               = 24 * NsHour
	NsWeek              = 7 * NsDay

	// NteNsDay and NteNsWeek are not-to-exceed estimates that account for
	// a daylight saving shift of up to one hour.
	NteNsDay  = 25*NsHour + 1
	NteNsWeek = 6*NsDay + NteNsDay

	SecondsPerDay int64 = 86_400
)

// FloorDiv returns a/b rounded towards negative infinity.
func FloorDiv(a, b int64) int64 {
	q := a / b
	if r := a % b; r != 0 && (r < 0) != (b < 0) {
		q--
	}
	return q
}

// FloorMod returns a mod b with the sign of b.
func FloorMod(a, b int64) int64 {
	m := a % b
	if m != 0 && (m < 0) != (b < 0) {
		m += b
	}
	return m
}

// UnitsPerSecond returns how many ticks of u make one second.
func UnitsPerSecond(u dtype.TimeUnit) int64 {
	switch u {
	case dtype.Second:
		return 1
	case dtype.Millisecond:
		return 1_000
	case dtype.Microsecond:
		return 1_000_000
	}
	return 1_000_000_000
}

// NsPerUnit returns the number of nanoseconds in one tick of u.
func NsPerUnit(u dtype.TimeUnit) int64 { return NsSecond / UnitsPerSecond(u) }

// UnitsPerDay returns the number of ticks of u in a 24h day.
func UnitsPerDay(u dtype.TimeUnit) int64 { return SecondsPerDay * UnitsPerSecond(u) }

// IsLeapYear reports whether y is a proleptic Gregorian leap year.
func IsLeapYear(y int64) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

var daysPerMonth = [2][12]int{
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
}

// DaysInMonth returns the length of month m (1-12) in year y.
func DaysInMonth(y int64, m int) int {
	leap := 0
	if IsLeapYear(y) {
		leap = 1
	}
	return daysPerMonth[leap][m-1]
}

// DaysFromCivil converts a proleptic Gregorian date to days since
// 1970-01-01. Month and day are not validated.
func DaysFromCivil(y int64, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := FloorDiv(y, 400)
	yoe := y - era*400
	mp := int64((m + 9) % 12)
	doy := (153*mp+2)/5 + int64(d) - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// CivilFromDays converts days since 1970-01-01 to a proleptic
// Gregorian (year, month, day).
func CivilFromDays(z int64) (y int64, m, d int) {
	z += 719468
	era := FloorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	if mp < 10 {
		m = int(mp + 3)
	} else {
		m = int(mp - 9)
	}
	if m <= 2 {
		y++
	}
	return y, m, d
}

// ValidDate reports whether (y, m, d) names a real calendar day.
func ValidDate(y int64, m, d int) bool {
	return m >= 1 && m <= 12 && d >= 1 && d <= DaysInMonth(y, m)
}

// ISOWeekday returns the ISO weekday (Monday=1 .. Sunday=7) of a day
// number. 1970-01-01 was a Thursday.
func ISOWeekday(days int64) int { return int(FloorMod(days+3, 7)) + 1 }

// OrdinalDay returns the 1-based day of the year.
func OrdinalDay(days int64, y int64) int {
	return int(days-DaysFromCivil(y, 1, 1)) + 1
}

// ISOWeek returns the ISO 8601 week-numbering year and week.
func ISOWeek(days int64) (int64, int) {
	thu := days - int64(ISOWeekday(days)) + 4
	ty, _, _ := CivilFromDays(thu)
	jan1 := DaysFromCivil(ty, 1, 1)
	return ty, int((thu-jan1)/7) + 1
}

// SplitDays splits a tick count in unit u into whole days since the
// epoch plus the remaining ticks within that day (always >= 0).
func SplitDays(t int64, u dtype.TimeUnit) (days, rem int64) {
	per := UnitsPerDay(u)
	days = FloorDiv(t, per)
	return days, t - days*per
}

// Civil is a broken-down naive date and time.
type Civil struct {
	Year                 int64
	Month, Day           int
	Hour, Minute, Second int
	Nanosecond           int
}

// ToCivil breaks a naive tick value down into calendar fields.
func ToCivil(t int64, u dtype.TimeUnit) Civil {
	days, rem := SplitDays(t, u)
	ns := rem * NsPerUnit(u)
	y, m, d := CivilFromDays(days)
	sec := ns / NsSecond
	return Civil{
		Year: y, Month: m, Day: d,
		Hour:       int(sec / 3600),
		Minute:     int(sec / 60 % 60),
		Second:     int(sec % 60),
		Nanosecond: int(ns % NsSecond),
	}
}

// FromCivil converts calendar fields back to a naive tick value in u.
// Sub-unit nanoseconds are truncated towards negative infinity.
func FromCivil(c Civil, u dtype.TimeUnit) int64 {
	days := DaysFromCivil(c.Year, c.Month, c.Day)
	ns := int64(c.Hour)*NsHour + int64(c.Minute)*NsMinute + int64(c.Second)*NsSecond + int64(c.Nanosecond)
	return days*UnitsPerDay(u) + FloorDiv(ns, NsPerUnit(u))
}

// ConvertUnit rescales a tick count between units. Converting to a
// coarser unit floors when floor is true and truncates towards zero
// otherwise (polars floors datetimes but truncates durations).
func ConvertUnit(v int64, from, to dtype.TimeUnit, floor bool) int64 {
	if from == to {
		return v
	}
	fp, tp := UnitsPerSecond(from), UnitsPerSecond(to)
	if tp > fp {
		return v * (tp / fp)
	}
	f := fp / tp
	if floor {
		return FloorDiv(v, f)
	}
	return v / f
}

// CoarserUnit returns the lower resolution of two units; polars uses
// it as the supertype of mixed-unit temporal operands.
func CoarserUnit(a, b dtype.TimeUnit) dtype.TimeUnit {
	if a < b {
		return a
	}
	return b
}
