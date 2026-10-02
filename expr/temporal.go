package expr

import (
	"fmt"
	"time"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// DateValue is the Value of a Date literal.
type DateValue struct{ Year, Month, Day int }

func (d DateValue) String() string { return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day) }

// TimeOfDayValue is the Value of a Time literal: nanoseconds since
// midnight.
type TimeOfDayValue struct{ Nanos int64 }

func (t TimeOfDayValue) String() string {
	s := t.Nanos / 1e9
	return fmt.Sprintf("%02d:%02d:%02d.%09d", s/3600, s/60%60, s%60, t.Nanos%1e9)
}

// litTemporal builds literals for Go time values. time.Time becomes a
// naive Datetime (microseconds unless it has sub-microsecond digits)
// whose wall clock is the value's wall clock in its own location;
// comparing it with a zoned column uses its instant instead.
// time.Duration becomes a Duration literal.
func litTemporal(v any) (Expr, bool) {
	switch x := v.(type) {
	case time.Time:
		unit := dtype.Microsecond
		if x.Nanosecond()%1000 != 0 {
			unit = dtype.Nanosecond
		}
		return Expr{LitNode{DType: dtype.Datetime(unit, ""), Value: x}}, true
	case time.Duration:
		unit := dtype.Microsecond
		if x%time.Microsecond != 0 {
			unit = dtype.Nanosecond
		}
		return Expr{LitNode{DType: dtype.Duration(unit), Value: x}}, true
	case DateValue:
		return Expr{LitNode{DType: dtype.Date(), Value: x}}, true
	case TimeOfDayValue:
		return Expr{LitNode{DType: dtype.Time(dtype.Nanosecond), Value: x}}, true
	}
	return Expr{}, false
}

// LitDate returns a Date literal. Invalid dates fail at evaluation.
func LitDate(year, month, day int) Expr {
	return Expr{LitNode{DType: dtype.Date(), Value: DateValue{year, month, day}}}
}

// LitDatetime returns a Datetime literal with an explicit unit and
// zone. With tz == "" the wall clock of t is used; otherwise t's
// instant is stored and tz attached.
func LitDatetime(t time.Time, unit dtype.TimeUnit, tz string) Expr {
	return Expr{LitNode{DType: dtype.Datetime(unit, tz), Value: t}}
}

// LitDuration returns a Duration literal in the given unit.
func LitDuration(d time.Duration, unit dtype.TimeUnit) Expr {
	return Expr{LitNode{DType: dtype.Duration(unit), Value: d}}
}

// LitTime returns a Time literal (time of day).
func LitTime(hour, minute, second, nanosecond int) Expr {
	ns := int64(hour)*3600e9 + int64(minute)*60e9 + int64(second)*1e9 + int64(nanosecond)
	return Expr{LitNode{DType: dtype.Time(dtype.Nanosecond), Value: TimeOfDayValue{ns}}}
}

// Date builds a Date from year, month and day expressions (polars'
// pl.date). Invalid components raise at evaluation.
func Date(year, month, day Expr) Expr {
	return Expr{FunctionNode{Name: "date", Args: []Expr{year, month, day}}}.Alias("date")
}

// DatetimeArgs lists the optional parts of Datetime. Zero-value Expr
// fields default to 0.
type DatetimeArgs struct {
	Hour, Minute, Second, Microsecond Expr
	// Unit defaults to microseconds.
	Unit dtype.TimeUnit
	// TimeZone localizes the wall clock into a zone.
	TimeZone string
	// Ambiguous is "raise" (default), "earliest", "latest" or "null".
	Ambiguous string
}

// Datetime builds a Datetime from components (polars' pl.datetime).
func Datetime(year, month, day Expr, a DatetimeArgs) Expr {
	args := []Expr{year, month, day}
	var mask []any
	for _, x := range []Expr{a.Hour, a.Minute, a.Second, a.Microsecond} {
		if x.node == nil {
			mask = append(mask, false)
			continue
		}
		mask = append(mask, true)
		args = append(args, x)
	}
	unit := a.Unit
	if unit == dtype.Second {
		unit = dtype.Microsecond
	}
	amb := a.Ambiguous
	if amb == "" {
		amb = "raise"
	}
	return Expr{FunctionNode{Name: "datetime", Args: args, Params: append(mask, unit, a.TimeZone, amb)}}.Alias("datetime")
}

// DurationArgs lists the parts of Duration. Zero-value fields are 0.
type DurationArgs struct {
	Weeks, Days, Hours, Minutes, Seconds, Milliseconds, Microseconds, Nanoseconds Expr
	// Unit is "" (microseconds, or nanoseconds when Nanoseconds is
	// set), "ms", "us" or "ns".
	Unit string
}

// Duration builds a Duration from parts (polars' pl.duration).
func Duration(a DurationArgs) Expr {
	var args []Expr
	var mask []any
	for _, x := range []Expr{a.Weeks, a.Days, a.Hours, a.Minutes, a.Seconds, a.Milliseconds, a.Microseconds, a.Nanoseconds} {
		if x.node == nil {
			mask = append(mask, false)
			continue
		}
		mask = append(mask, true)
		args = append(args, x)
	}
	return Expr{FunctionNode{Name: "duration", Args: args, Params: append(mask, a.Unit)}}.Alias("duration")
}

// DateRange builds a Date range from start to end (Date expressions or
// literals) stepping by interval ("1d", "1w", "1mo"); closed is "both",
// "left", "right" or "none".
func DateRange(start, end Expr, interval, closed string) Expr {
	return Expr{FunctionNode{Name: "date_range", Args: []Expr{start, end}, Params: []any{interval, closed}}}.Alias("literal")
}

// DatetimeRange builds a Datetime range. unit is "" (microseconds),
// "ms", "us" or "ns"; tz localizes the range.
func DatetimeRange(start, end Expr, interval, closed, unit, tz string) Expr {
	return Expr{FunctionNode{Name: "datetime_range", Args: []Expr{start, end}, Params: []any{interval, closed, unit, tz}}}.Alias("literal")
}

// TimeRange builds a Time range between two Time expressions (use
// LitTime). A zero-value start or end defaults to 00:00 and 23:59:59.999999999.
func TimeRange(start, end Expr, interval, closed string) Expr {
	if start.node == nil {
		start = LitTime(0, 0, 0, 0)
	}
	if end.node == nil {
		end = LitTime(23, 59, 59, 999_999_999)
	}
	return Expr{FunctionNode{Name: "time_range", Args: []Expr{start, end}, Params: []any{interval, closed}}}.Alias("literal")
}

// RollingByOption configures the rolling_*_by expressions.
type RollingByOption func(*RollingBySpec)

// RollingBySpec is the resolved configuration of a rolling_*_by call.
type RollingBySpec struct {
	WindowSize    string
	MinPeriods    int
	Closed        string
	Ddof          int
	Quantile      float64
	Interpolation string
}

// WithMinPeriods sets the minimum number of rows a window needs.
func WithMinPeriods(n int) RollingByOption { return func(s *RollingBySpec) { s.MinPeriods = n } }

// WithClosed sets which window endpoints are inclusive ("right" by
// default for rolling_*_by).
func WithClosed(c string) RollingByOption { return func(s *RollingBySpec) { s.Closed = c } }

// WithDdof sets the delta degrees of freedom for std/var (default 1).
func WithDdof(d int) RollingByOption { return func(s *RollingBySpec) { s.Ddof = d } }

// WithInterpolation sets the quantile interpolation ("nearest"
// default, "lower", "higher", "midpoint", "linear").
func WithInterpolation(m string) RollingByOption {
	return func(s *RollingBySpec) { s.Interpolation = m }
}

func rollingBy(name string, e, by Expr, window string, q float64, opts []RollingByOption) Expr {
	spec := RollingBySpec{WindowSize: window, MinPeriods: 1, Closed: "right", Ddof: 1, Quantile: q, Interpolation: "nearest"}
	for _, o := range opts {
		o(&spec)
	}
	return Expr{FunctionNode{Name: name, Args: []Expr{e, by}, Params: []any{spec}}}
}

// RollingSumBy sums over a time-based window of the by column
// (polars' rolling_sum_by). windowSize is a duration string.
func (e Expr) RollingSumBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_sum_by", e, by, windowSize, 0, opts)
}

// RollingMeanBy is RollingSumBy for the mean.
func (e Expr) RollingMeanBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_mean_by", e, by, windowSize, 0, opts)
}

// RollingMinBy is RollingSumBy for the minimum.
func (e Expr) RollingMinBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_min_by", e, by, windowSize, 0, opts)
}

// RollingMaxBy is RollingSumBy for the maximum.
func (e Expr) RollingMaxBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_max_by", e, by, windowSize, 0, opts)
}

// RollingStdBy is RollingSumBy for the standard deviation.
func (e Expr) RollingStdBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_std_by", e, by, windowSize, 0, opts)
}

// RollingVarBy is RollingSumBy for the variance.
func (e Expr) RollingVarBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_var_by", e, by, windowSize, 0, opts)
}

// RollingMedianBy is RollingSumBy for the median.
func (e Expr) RollingMedianBy(by Expr, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_median_by", e, by, windowSize, 0, opts)
}

// RollingQuantileBy is RollingSumBy for the q-th quantile.
func (e Expr) RollingQuantileBy(by Expr, q float64, windowSize string, opts ...RollingByOption) Expr {
	return rollingBy("rolling_quantile_by", e, by, windowSize, q, opts)
}
