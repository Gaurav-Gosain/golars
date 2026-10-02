package series

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

func validityFill(valid []bool) func([]byte) {
	return func(vb []byte) {
		if valid == nil {
			return
		}
		for i, ok := range valid {
			if !ok {
				clearValid(vb, i)
			}
		}
	}
}

// FromDate builds a Date Series from day numbers since 1970-01-01.
func FromDate(name string, days []int32, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(days) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	vf := validityFill(valid)
	return buildFixed(name, len(days), cfg.alloc, arrow.FixedWidthTypes.Date32, nil, func(out []int32, vb []byte) error {
		copy(out, days)
		vf(vb)
		return nil
	})
}

// FromDatetime builds a Datetime Series from ticks of unit since the
// epoch. For a non-empty tz the values are UTC instants.
func FromDatetime(name string, values []int64, valid []bool, unit dtype.TimeUnit, tz string, opts ...Option) (*Series, error) {
	return fromInt64Typed(name, values, valid, dtype.Datetime(unit, tz), opts)
}

// FromDuration builds a Duration Series from ticks of unit.
func FromDuration(name string, values []int64, valid []bool, unit dtype.TimeUnit, opts ...Option) (*Series, error) {
	return fromInt64Typed(name, values, valid, dtype.Duration(unit), opts)
}

func fromInt64Typed(name string, values []int64, valid []bool, dt dtype.DType, opts []Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	vf := validityFill(valid)
	return buildFixed(name, len(values), cfg.alloc, dt.Arrow(), nil, func(out []int64, vb []byte) error {
		copy(out, values)
		vf(vb)
		return nil
	})
}

// FromTimes builds a Datetime Series from Go time values. With tz == ""
// each value's wall clock (in its own location) is stored as a naive
// datetime; otherwise the instant is stored and tz is attached.
func FromTimes(name string, ts []time.Time, valid []bool, unit dtype.TimeUnit, tz string, opts ...Option) (*Series, error) {
	vals := make([]int64, len(ts))
	for i, t := range ts {
		vals[i] = TimeToTicks(t, unit, tz == "")
	}
	return FromDatetime(name, vals, valid, unit, tz, opts...)
}

// TimeToTicks converts a Go time to ticks of unit since the epoch.
// When wall is true the wall clock of t's location is used (a naive
// datetime); otherwise the UTC instant. Sub-unit precision floors.
func TimeToTicks(t time.Time, unit dtype.TimeUnit, wall bool) int64 {
	sec := t.Unix()
	if wall {
		_, off := t.Zone()
		sec += int64(off)
	}
	per := temporal.UnitsPerSecond(unit)
	return sec*per + int64(t.Nanosecond())/temporal.NsPerUnit(unit)
}

// FromTimeOfDay builds a Time Series (nanosecond resolution) from
// nanoseconds since midnight.
func FromTimeOfDay(name string, ns []int64, valid []bool, opts ...Option) (*Series, error) {
	return FromTime(name, ns, valid, dtype.Time(dtype.Nanosecond), opts...)
}

// DateRange returns the Date Series start, start+interval, ... up to
// end (days since the epoch). interval must be whole days, weeks,
// months, quarters or years; closed is "both", "left", "right" or
// "none".
func DateRange(name string, start, end int64, interval, closed string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(interval)
	if err != nil {
		return nil, err
	}
	if !d.IsFullDays() {
		return nil, fmt.Errorf("`interval` input for `date_range` must consist of full days, got: %s", d)
	}
	c, err := parseClosedDefault(closed)
	if err != nil {
		return nil, err
	}
	const msDay = 86_400_000
	vals, err := temporal.RangeValues(start*msDay, end*msDay, d, c, dtype.Millisecond, nil)
	if err != nil {
		return nil, err
	}
	days := make([]int32, len(vals))
	for i, v := range vals {
		days[i] = int32(v / msDay)
	}
	return FromDate(name, days, nil, opts...)
}

func parseClosedDefault(s string) (temporal.ClosedWindow, error) {
	if s == "" {
		return temporal.ClosedBoth, nil
	}
	return temporal.ParseClosed(s)
}

// DatetimeRange returns a Datetime Series from start to end (naive
// ticks of unit; they are localized into tz when tz is set) stepping
// by interval. Calendar intervals ("1mo") are applied as multiples of
// the start, so month-end starts stay clamped month ends.
func DatetimeRange(name string, start, end int64, interval, closed string, unit dtype.TimeUnit, tz string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(interval)
	if err != nil {
		return nil, err
	}
	c, err := parseClosedDefault(closed)
	if err != nil {
		return nil, err
	}
	z, err := zoneFor(tz)
	if err != nil {
		return nil, err
	}
	if z != nil {
		if start, err = z.FromLocalRaise(start, unit); err != nil {
			return nil, err
		}
		if end, err = z.FromLocalRaise(end, unit); err != nil {
			return nil, err
		}
	}
	vals, err := temporal.RangeValues(start, end, d, c, unit, z)
	if err != nil {
		return nil, err
	}
	return FromDatetime(name, vals, nil, unit, tz, opts...)
}

// TimeRange returns a Time Series from start to end (nanoseconds since
// midnight) stepping by interval.
func TimeRange(name string, start, end int64, interval, closed string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(interval)
	if err != nil {
		return nil, err
	}
	c, err := parseClosedDefault(closed)
	if err != nil {
		return nil, err
	}
	vals, err := temporal.RangeValues(start, end, d, c, dtype.Nanosecond, nil)
	if err != nil {
		return nil, err
	}
	return FromTimeOfDay(name, vals, nil, opts...)
}
