package golars

import (
	"time"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/series"
)

// Temporal re-exports. The canonical homes are expr (Dt namespace,
// constructors), series (eager kernels) and dataframe (time windows).

type (
	// DynamicGroupOptions configures DataFrame.GroupByDynamic and
	// LazyFrame.GroupByDynamic.
	DynamicGroupOptions = dataframe.DynamicGroupOptions
	// RollingGroupOptions configures DataFrame.Rolling and
	// LazyFrame.Rolling.
	RollingGroupOptions = dataframe.RollingGroupOptions
	// DatetimeArgs lists the optional parts of Datetime.
	DatetimeArgs = expr.DatetimeArgs
	// DurationArgs lists the parts of Duration.
	DurationArgs = expr.DurationArgs
	// StrptimeOptions configures Str().ToDatetimeWith and friends.
	StrptimeOptions = expr.StrptimeOptions
)

// Time units re-exported for Dt().CastTimeUnit and friends.
const (
	Milliseconds = dtype.Millisecond
	Microseconds = dtype.Microsecond
	Nanoseconds  = dtype.Nanosecond
)

// Date builds a Date from year, month and day expressions (pl.date).
func Date(year, month, day Expr) Expr { return expr.Date(year, month, day) }

// Datetime builds a Datetime from components (pl.datetime).
func Datetime(year, month, day Expr, a DatetimeArgs) Expr {
	return expr.Datetime(year, month, day, a)
}

// Duration builds a Duration from parts (pl.duration).
func Duration(a DurationArgs) Expr { return expr.Duration(a) }

// LitDate returns a Date literal.
func LitDate(year, month, day int) Expr { return expr.LitDate(year, month, day) }

// LitTime returns a Time (time of day) literal.
func LitTime(hour, minute, second, nanosecond int) Expr {
	return expr.LitTime(hour, minute, second, nanosecond)
}

// LitDatetime returns a Datetime literal with an explicit unit and
// zone. Lit(t) is the shorthand for a naive microsecond literal.
func LitDatetime(t time.Time, unit dtype.TimeUnit, tz string) Expr {
	return expr.LitDatetime(t, unit, tz)
}

// DateRange is pl.date_range as an expression.
func DateRange(start, end Expr, interval, closed string) Expr {
	return expr.DateRange(start, end, interval, closed)
}

// DatetimeRange is pl.datetime_range as an expression.
func DatetimeRange(start, end Expr, interval, closed, unit, tz string) Expr {
	return expr.DatetimeRange(start, end, interval, closed, unit, tz)
}

// TimeRange is pl.time_range as an expression.
func TimeRange(start, end Expr, interval, closed string) Expr {
	return expr.TimeRange(start, end, interval, closed)
}

// DateRangeSeries eagerly builds a Date Series (pl.date_range with
// eager=True). Only the calendar dates of start and end are used.
func DateRangeSeries(name string, start, end time.Time, interval, closed string) (*Series, error) {
	day := func(t time.Time) int64 {
		y, m, d := t.Date()
		return series.DateToDays(y, int(m), d)
	}
	return series.DateRange(name, day(start), day(end), interval, closed)
}

// DatetimeRangeSeries eagerly builds a Datetime Series. start and end
// wall clocks are used; tz localizes the result.
func DatetimeRangeSeries(name string, start, end time.Time, interval, closed string, unit dtype.TimeUnit, tz string) (*Series, error) {
	return series.DatetimeRange(name, series.TimeToTicks(start, unit, true), series.TimeToTicks(end, unit, true),
		interval, closed, unit, tz)
}

// FromTimes builds a Datetime Series from Go times (see
// series.FromTimes for how locations are handled).
func FromTimes(name string, ts []time.Time, valid []bool, unit dtype.TimeUnit, tz string) (*Series, error) {
	return series.FromTimes(name, ts, valid, unit, tz)
}

// FromDates builds a Date Series from the calendar dates of Go times.
func FromDates(name string, ts []time.Time, valid []bool) (*Series, error) {
	days := make([]int32, len(ts))
	for i, t := range ts {
		y, m, d := t.Date()
		days[i] = int32(series.DateToDays(y, int(m), d))
	}
	return series.FromDate(name, days, valid)
}

// FromDurations builds a Duration Series from Go durations.
func FromDurations(name string, ds []time.Duration, valid []bool, unit dtype.TimeUnit) (*Series, error) {
	vals := make([]int64, len(ds))
	per := int64(time.Second) / map[dtype.TimeUnit]int64{
		dtype.Second: 1, dtype.Millisecond: 1e3, dtype.Microsecond: 1e6, dtype.Nanosecond: 1e9,
	}[unit]
	for i, d := range ds {
		vals[i] = int64(d) / per
	}
	return series.FromDuration(name, vals, valid, unit)
}

// WithTryParseDates is the CSV reader option matching polars'
// try_parse_dates.
func WithTryParseDates(on bool) iocsv.Option { return iocsv.WithTryParseDates(on) }
