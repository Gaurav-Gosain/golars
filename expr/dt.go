package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// DtOps is the expression-level mirror of series.DtOps: the temporal
// namespace reachable via Expr.Dt(). Every function name is prefixed
// with "dt." so the evaluator can route the whole family to one
// dispatcher, the same model as polars' `pl.col("t").dt.*`.
type DtOps struct{ e Expr }

// Dt returns the temporal-ops view over e.
//
// # Examples
//
//	expr.Col("ts").Dt().Year()
//	expr.Col("ts").Dt().Truncate("1h")
//	expr.Col("ts").Dt().ConvertTimeZone("Europe/Amsterdam")
func (e Expr) Dt() DtOps { return DtOps{e: e} }

// ---- calendar fields ----

// Year returns the calendar year (Int32).
func (o DtOps) Year() Expr { return fn1("dt.year", o.e) }

// IsoYear returns the ISO 8601 week-numbering year (Int32).
func (o DtOps) IsoYear() Expr { return fn1("dt.iso_year", o.e) }

// Quarter returns the quarter of the year, 1-4 (Int8).
func (o DtOps) Quarter() Expr { return fn1("dt.quarter", o.e) }

// Month returns the month, 1-12 (Int8).
func (o DtOps) Month() Expr { return fn1("dt.month", o.e) }

// Week returns the ISO week number, 1-53 (Int8).
func (o DtOps) Week() Expr { return fn1("dt.week", o.e) }

// Weekday returns the ISO weekday, Monday=1 .. Sunday=7 (Int8).
func (o DtOps) Weekday() Expr { return fn1("dt.weekday", o.e) }

// Day returns the day of the month, 1-31 (Int8).
func (o DtOps) Day() Expr { return fn1("dt.day", o.e) }

// OrdinalDay returns the day of the year, 1-366 (Int16).
func (o DtOps) OrdinalDay() Expr { return fn1("dt.ordinal_day", o.e) }

// IsLeapYear reports whether the year is a leap year (Boolean).
func (o DtOps) IsLeapYear() Expr { return fn1("dt.is_leap_year", o.e) }

// DaysInMonth returns the number of days in the month (Int8).
func (o DtOps) DaysInMonth() Expr { return fn1("dt.days_in_month", o.e) }

// Century returns the century (Int32).
func (o DtOps) Century() Expr { return fn1("dt.century", o.e) }

// Millennium returns the millennium (Int32).
func (o DtOps) Millennium() Expr { return fn1("dt.millennium", o.e) }

// Hour returns the hour, 0-23 (Int8).
func (o DtOps) Hour() Expr { return fn1("dt.hour", o.e) }

// Minute returns the minute, 0-59 (Int8).
func (o DtOps) Minute() Expr { return fn1("dt.minute", o.e) }

// Second returns the second, 0-59 (Int8).
func (o DtOps) Second() Expr { return fn1("dt.second", o.e) }

// Millisecond returns the millisecond within the second (Int32).
func (o DtOps) Millisecond() Expr { return fn1("dt.millisecond", o.e) }

// Microsecond returns the microsecond within the second (Int32).
func (o DtOps) Microsecond() Expr { return fn1("dt.microsecond", o.e) }

// Nanosecond returns the nanosecond within the second (Int32).
func (o DtOps) Nanosecond() Expr { return fn1("dt.nanosecond", o.e) }

// ---- conversions ----

// Date extracts the calendar date (Date).
func (o DtOps) Date() Expr { return fn1("dt.date", o.e) }

// Time extracts the time of day (Time).
func (o DtOps) Time() Expr { return fn1("dt.time", o.e) }

// Datetime drops the time zone keeping the local wall clock. Same as
// ReplaceTimeZone("").
func (o DtOps) Datetime() Expr { return fn1("dt.datetime", o.e) }

// Epoch returns the time since 1970-01-01 in unit "d", "s", "ms", "us"
// or "ns" (floored).
func (o DtOps) Epoch(unit string) Expr { return fn1p("dt.epoch", o.e, unit) }

// Timestamp returns the time since the epoch in "ms", "us" or "ns".
func (o DtOps) Timestamp(unit string) Expr { return fn1p("dt.timestamp", o.e, unit) }

// TotalDays returns the whole days in a Duration (Int64).
func (o DtOps) TotalDays() Expr { return fn1p("dt.total_days", o.e, false) }

// TotalHours returns the whole hours in a Duration (Int64).
func (o DtOps) TotalHours() Expr { return fn1p("dt.total_hours", o.e, false) }

// TotalMinutes returns the whole minutes in a Duration (Int64).
func (o DtOps) TotalMinutes() Expr { return fn1p("dt.total_minutes", o.e, false) }

// TotalSeconds returns the whole seconds in a Duration (Int64).
func (o DtOps) TotalSeconds() Expr { return fn1p("dt.total_seconds", o.e, false) }

// TotalMilliseconds returns the whole milliseconds in a Duration.
func (o DtOps) TotalMilliseconds() Expr { return fn1p("dt.total_milliseconds", o.e, false) }

// TotalMicroseconds returns the whole microseconds in a Duration.
func (o DtOps) TotalMicroseconds() Expr { return fn1p("dt.total_microseconds", o.e, false) }

// TotalNanoseconds returns the nanoseconds in a Duration.
func (o DtOps) TotalNanoseconds() Expr { return fn1p("dt.total_nanoseconds", o.e, false) }

// TotalSecondsFractional returns the seconds in a Duration as Float64
// (polars' total_seconds(fractional=True)). The other Total* units
// have the same form via TotalFractional.
func (o DtOps) TotalSecondsFractional() Expr { return fn1p("dt.total_seconds", o.e, true) }

// TotalFractional returns a Duration expressed in unit ("days",
// "hours", "minutes", "seconds", "milliseconds", "microseconds",
// "nanoseconds") as Float64.
func (o DtOps) TotalFractional(unit string) Expr { return fn1p("dt.total_"+unit, o.e, true) }

// CastTimeUnit rescales a Datetime or Duration to another resolution.
func (o DtOps) CastTimeUnit(unit dtype.TimeUnit) Expr {
	return fn1p("dt.cast_time_unit", o.e, unit)
}

// WithTimeUnit reinterprets the physical values under another unit.
func (o DtOps) WithTimeUnit(unit dtype.TimeUnit) Expr {
	return fn1p("dt.with_time_unit", o.e, unit)
}

// ---- formatting ----

// Strftime formats values with a chrono format string.
func (o DtOps) Strftime(format string) Expr { return fn1p("dt.strftime", o.e, format) }

// ToString formats values; an empty format uses polars' default for the
// dtype (ISO dates and datetimes, "iso" durations).
func (o DtOps) ToString(format string) Expr { return fn1p("dt.to_string", o.e, format) }

// ---- calendar arithmetic ----

// Truncate rounds down to a multiple of every ("1h", "1d", "1w", "1mo").
func (o DtOps) Truncate(every string) Expr { return fn1p("dt.truncate", o.e, every) }

// Round rounds to the nearest multiple of every.
func (o DtOps) Round(every string) Expr { return fn1p("dt.round", o.e, every) }

// OffsetBy shifts values by a duration string ("1mo", "-3d", "2h30m").
func (o DtOps) OffsetBy(by string) Expr { return fn1p("dt.offset_by", o.e, by) }

// OffsetByExpr is OffsetBy with a per-row String expression.
func (o DtOps) OffsetByExpr(by Expr) Expr {
	return Expr{FunctionNode{Name: "dt.offset_by", Args: []Expr{o.e, by}}}
}

// MonthStart rolls back to the first day of the month.
func (o DtOps) MonthStart() Expr { return fn1("dt.month_start", o.e) }

// MonthEnd rolls forward to the last day of the month.
func (o DtOps) MonthEnd() Expr { return fn1("dt.month_end", o.e) }

// ---- time zones ----

// ReplaceTimeZone sets (or with "" removes) the time zone keeping the
// wall clock. Ambiguous and non-existent local times raise.
func (o DtOps) ReplaceTimeZone(tz string) Expr {
	return fn1p("dt.replace_time_zone", o.e, tz, "raise", "raise")
}

// ReplaceTimeZoneWith is ReplaceTimeZone with explicit policies:
// ambiguous is "raise", "earliest", "latest" or "null"; nonExistent is
// "raise" or "null".
func (o DtOps) ReplaceTimeZoneWith(tz, ambiguous, nonExistent string) Expr {
	return fn1p("dt.replace_time_zone", o.e, tz, ambiguous, nonExistent)
}

// ConvertTimeZone converts a tz-aware Datetime to another zone keeping
// the instant.
func (o DtOps) ConvertTimeZone(tz string) Expr { return fn1p("dt.convert_time_zone", o.e, tz) }

// ---- composition ----

// Combine builds a Datetime from the date part of e and a Time
// expression, in the given unit.
func (o DtOps) Combine(tm Expr, unit dtype.TimeUnit) Expr {
	return Expr{FunctionNode{Name: "dt.combine", Args: []Expr{o.e, tm}, Params: []any{unit}}}
}

// ReplaceArgs lists the components for DtOps.Replace. Zero-value
// fields keep the original component.
type ReplaceArgs struct {
	Year, Month, Day, Hour, Minute, Second, Microsecond Expr
	// Ambiguous is "raise" (default), "earliest", "latest" or "null".
	Ambiguous string
}

// Replace swaps calendar components; invalid dates raise.
func (o DtOps) Replace(r ReplaceArgs) Expr {
	args := []Expr{o.e}
	var mask []any
	for _, x := range []Expr{r.Year, r.Month, r.Day, r.Hour, r.Minute, r.Second, r.Microsecond} {
		if x.node == nil {
			mask = append(mask, false)
			continue
		}
		mask = append(mask, true)
		args = append(args, x)
	}
	amb := r.Ambiguous
	if amb == "" {
		amb = "raise"
	}
	return Expr{FunctionNode{Name: "dt.replace", Args: args, Params: append(mask, amb)}}
}

// ---- business days ----

// BusinessDayArgs configures IsBusinessDay / AddBusinessDays. WeekMask
// is Monday first (nil means Monday to Friday); Holidays are Date
// literals built with LitDate or day numbers.
type BusinessDayArgs struct {
	WeekMask *[7]bool
	Holidays []DateValue
	// Roll is "raise" (default), "forward" or "backward".
	Roll string
}

// IsBusinessDay reports whether each value is a business day.
func (o DtOps) IsBusinessDay(b BusinessDayArgs) Expr {
	return fn1p("dt.is_business_day", o.e, b)
}

// AddBusinessDays shifts values by n business days (an integer
// expression; use Lit(3) for a constant).
func (o DtOps) AddBusinessDays(n Expr, b BusinessDayArgs) Expr {
	return Expr{FunctionNode{Name: "dt.add_business_days", Args: []Expr{o.e, n}, Params: []any{b}}}
}
