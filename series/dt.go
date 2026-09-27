package series

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// DtOps exposes temporal operations on Date, Datetime, Duration and
// Time Series. Access via Series.Dt(). It mirrors polars' `s.dt.*`
// namespace: every method returns a fresh Series (keeping the input
// name) and leaves the receiver untouched. Datetimes with a time zone
// are stored as UTC instants; calendar fields are computed on the
// local wall clock, like polars.
type DtOps struct{ s *Series }

// Dt returns the temporal-ops view over s.
func (s *Series) Dt() DtOps { return DtOps{s: s} }

const (
	allowDate uint8 = 1 << iota
	allowDatetime
	allowDuration
	allowTime
)

func kindAllowed(k temporalKind, mask uint8) bool {
	switch k {
	case tkDate:
		return mask&allowDate != 0
	case tkDatetime:
		return mask&allowDatetime != 0
	case tkDuration:
		return mask&allowDuration != 0
	case tkTime:
		return mask&allowTime != 0
	}
	return false
}

// dtField evaluates fn(days, nsOfDay) for every row of a Date,
// Datetime or Time series and stores the result as T in an array of
// dtype out. Datetimes with a zone are converted to local time first.
func dtField[T fixedNum](o DtOps, op string, out arrow.DataType, mask uint8, opts []Option, fn func(days, ns int64) T) (*Series, error) {
	info := temporalInfoOf(o.s.DType())
	if !kindAllowed(info.kind, mask) {
		return nil, errTemporalOp(op, o.s.DType())
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	n := arr.Len()
	return buildFixed(o.s.Name(), n, cfg.alloc, out, arr, func(dst []T, valid []byte) error {
		switch info.kind {
		case tkDate:
			vals := date32Values(arr)
			return forRanges(n, func(lo, hi int) error {
				for i := lo; i < hi; i++ {
					dst[i] = fn(int64(vals[i]), 0)
				}
				return nil
			})
		case tkTime:
			vals := physicalInt64(arr)
			per := temporal.NsPerUnit(info.unit)
			return forRanges(n, func(lo, hi int) error {
				for i := lo; i < hi; i++ {
					dst[i] = fn(0, vals[i]*per)
				}
				return nil
			})
		}
		vals := physicalInt64(arr)
		perDay := temporal.UnitsPerDay(info.unit)
		per := temporal.NsPerUnit(info.unit)
		base, err := zoneFor(info.tz)
		if err != nil {
			return err
		}
		return forRanges(n, func(lo, hi int) error {
			z := base.Clone()
			for i := lo; i < hi; i++ {
				t := vals[i]
				if z != nil {
					if !isValidBit(valid, i) {
						continue
					}
					t = z.ToLocal(t, info.unit)
				}
				days := temporal.FloorDiv(t, perDay)
				dst[i] = fn(days, (t-days*perDay)*per)
			}
			return nil
		})
	})
}

func yearOf(days int64) int64 {
	y, _, _ := temporal.CivilFromDays(days)
	return y
}

// Year returns the calendar year as Int32.
func (o DtOps) Year(opts ...Option) (*Series, error) {
	return dtField(o, "year", arrow.PrimitiveTypes.Int32, allowDate|allowDatetime, opts,
		func(days, _ int64) int32 { return int32(yearOf(days)) })
}

// IsoYear returns the ISO 8601 week-numbering year as Int32.
func (o DtOps) IsoYear(opts ...Option) (*Series, error) {
	return dtField(o, "iso_year", arrow.PrimitiveTypes.Int32, allowDate|allowDatetime, opts,
		func(days, _ int64) int32 { y, _ := temporal.ISOWeek(days); return int32(y) })
}

// Quarter returns the quarter of the year (1-4) as Int8.
func (o DtOps) Quarter(opts ...Option) (*Series, error) {
	return dtField(o, "quarter", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 { _, m, _ := temporal.CivilFromDays(days); return int8((m-1)/3 + 1) })
}

// Month returns the month (1-12) as Int8.
func (o DtOps) Month(opts ...Option) (*Series, error) {
	return dtField(o, "month", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 { _, m, _ := temporal.CivilFromDays(days); return int8(m) })
}

// Week returns the ISO week number (1-53) as Int8.
func (o DtOps) Week(opts ...Option) (*Series, error) {
	return dtField(o, "week", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 { _, w := temporal.ISOWeek(days); return int8(w) })
}

// Weekday returns the ISO weekday (Monday=1 .. Sunday=7) as Int8.
func (o DtOps) Weekday(opts ...Option) (*Series, error) {
	return dtField(o, "weekday", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 { return int8(temporal.ISOWeekday(days)) })
}

// Day returns the day of the month (1-31) as Int8.
func (o DtOps) Day(opts ...Option) (*Series, error) {
	return dtField(o, "day", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 { _, _, d := temporal.CivilFromDays(days); return int8(d) })
}

// OrdinalDay returns the day of the year (1-366) as Int16.
func (o DtOps) OrdinalDay(opts ...Option) (*Series, error) {
	return dtField(o, "ordinal_day", arrow.PrimitiveTypes.Int16, allowDate|allowDatetime, opts,
		func(days, _ int64) int16 { return int16(temporal.OrdinalDay(days, yearOf(days))) })
}

// IsLeapYear reports whether the year of each value is a leap year.
func (o DtOps) IsLeapYear(opts ...Option) (*Series, error) {
	info := temporalInfoOf(o.s.DType())
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("is_leap_year", o.s.DType())
	}
	years, err := o.Year(opts...)
	if err != nil {
		return nil, err
	}
	defer years.Release()
	cfg := resolve(opts)
	arr := years.Chunk(0)
	vals := physicalInt64(arr)
	return buildBool(o.s.Name(), arr.Len(), cfg.alloc, arr, func(bits, _ []byte) error {
		for i, y := range vals {
			if temporal.IsLeapYear(y) {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
		return nil
	})
}

// DaysInMonth returns the number of days in the month of each value as
// Int8.
func (o DtOps) DaysInMonth(opts ...Option) (*Series, error) {
	return dtField(o, "days_in_month", arrow.PrimitiveTypes.Int8, allowDate|allowDatetime, opts,
		func(days, _ int64) int8 {
			y, m, _ := temporal.CivilFromDays(days)
			return int8(temporal.DaysInMonth(y, m))
		})
}

// Century returns the century (year 2000 is in the 20th) as Int32.
func (o DtOps) Century(opts ...Option) (*Series, error) {
	return dtField(o, "century", arrow.PrimitiveTypes.Int32, allowDate|allowDatetime, opts,
		func(days, _ int64) int32 {
			y := yearOf(days)
			if y > 0 {
				return int32((y-1)/100 + 1)
			}
			return int32(y/100 - 1)
		})
}

// Millennium returns the millennium (year 2000 is in the 2nd) as Int32.
func (o DtOps) Millennium(opts ...Option) (*Series, error) {
	return dtField(o, "millennium", arrow.PrimitiveTypes.Int32, allowDate|allowDatetime, opts,
		func(days, _ int64) int32 {
			y := yearOf(days)
			if y > 0 {
				return int32((y-1)/1000 + 1)
			}
			return int32(y/1000 - 1)
		})
}

// Hour returns the hour (0-23) as Int8.
func (o DtOps) Hour(opts ...Option) (*Series, error) {
	return dtField(o, "hour", arrow.PrimitiveTypes.Int8, allowDatetime|allowTime, opts,
		func(_, ns int64) int8 { return int8(ns / temporal.NsHour) })
}

// Minute returns the minute (0-59) as Int8.
func (o DtOps) Minute(opts ...Option) (*Series, error) {
	return dtField(o, "minute", arrow.PrimitiveTypes.Int8, allowDatetime|allowTime, opts,
		func(_, ns int64) int8 { return int8(ns / temporal.NsMinute % 60) })
}

// Second returns the second (0-59) as Int8.
func (o DtOps) Second(opts ...Option) (*Series, error) {
	return dtField(o, "second", arrow.PrimitiveTypes.Int8, allowDatetime|allowTime, opts,
		func(_, ns int64) int8 { return int8(ns / temporal.NsSecond % 60) })
}

// Millisecond returns the millisecond within the second as Int32.
func (o DtOps) Millisecond(opts ...Option) (*Series, error) {
	return dtField(o, "millisecond", arrow.PrimitiveTypes.Int32, allowDatetime|allowTime, opts,
		func(_, ns int64) int32 { return int32(ns % temporal.NsSecond / temporal.NsMillisecond) })
}

// Microsecond returns the microsecond within the second as Int32.
func (o DtOps) Microsecond(opts ...Option) (*Series, error) {
	return dtField(o, "microsecond", arrow.PrimitiveTypes.Int32, allowDatetime|allowTime, opts,
		func(_, ns int64) int32 { return int32(ns % temporal.NsSecond / temporal.NsMicrosecond) })
}

// Nanosecond returns the nanosecond within the second as Int32.
func (o DtOps) Nanosecond(opts ...Option) (*Series, error) {
	return dtField(o, "nanosecond", arrow.PrimitiveTypes.Int32, allowDatetime|allowTime, opts,
		func(_, ns int64) int32 { return int32(ns % temporal.NsSecond) })
}

// Date extracts the calendar date of a Date or Datetime series.
func (o DtOps) Date(opts ...Option) (*Series, error) {
	info := temporalInfoOf(o.s.DType())
	switch info.kind {
	case tkDate:
		return o.s.Clone(), nil
	case tkDatetime:
		return dtField(o, "date", arrow.FixedWidthTypes.Date32, allowDatetime, opts,
			func(days, _ int64) int32 { return int32(days) })
	}
	return nil, errTemporalOp("date", o.s.DType())
}

// Time extracts the time of day (Time dtype, nanosecond resolution)
// of a Datetime series.
func (o DtOps) Time(opts ...Option) (*Series, error) {
	info := temporalInfoOf(o.s.DType())
	switch info.kind {
	case tkTime:
		return o.s.Clone(), nil
	case tkDatetime:
		return dtField(o, "time", dtype.Time(dtype.Nanosecond).Arrow(), allowDatetime, opts,
			func(_, ns int64) int64 { return ns })
	}
	return nil, errTemporalOp("time", o.s.DType())
}
