package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// mapPhys applies fn to the physical value of every valid row and
// writes the result as T into an array of dtype out. fn may mark a row
// null by returning ok=false. A zone cache for tz (nil for naive/UTC)
// is handed to fn; each parallel chunk gets its own cache.
func mapPhys[T fixedNum](o DtOps, out arrow.DataType, tz string, opts []Option, fn func(z *temporal.Zone, v int64) (T, bool, error)) (*Series, error) {
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	n := arr.Len()
	base, err := zoneFor(tz)
	if err != nil {
		return nil, err
	}
	vals := physicalInt64(arr)
	return buildFixed(o.s.Name(), n, cfg.alloc, out, arr, func(dst []T, valid []byte) error {
		return forRanges(n, func(lo, hi int) error {
			z := base.Clone()
			for i := lo; i < hi; i++ {
				if !isValidBit(valid, i) {
					dst[i] = 0
					continue
				}
				v, ok, err := fn(z, vals[i])
				if err != nil {
					return err
				}
				if !ok {
					clearValid(valid, i)
					v = 0
				}
				dst[i] = v
			}
			return nil
		})
	})
}

func (o DtOps) info() tinfo { return temporalInfoOf(o.s.DType()) }

// Epoch returns the time since 1970-01-01 in the given unit ("d", "s",
// "ms", "us" or "ns"). "d" yields Int32, the others Int64. Values are
// floored, so instants before the epoch round down.
func (o DtOps) Epoch(unit string, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("epoch", o.s.DType())
	}
	src := info.unit
	if info.kind == tkDate {
		src = dtype.Millisecond
	}
	scale := func(v int64) int64 {
		if info.kind == tkDate {
			return v * temporal.UnitsPerDay(dtype.Millisecond)
		}
		return v
	}
	switch unit {
	case "d":
		per := temporal.UnitsPerDay(src)
		return mapPhys(o, arrow.PrimitiveTypes.Int32, "", opts, func(_ *temporal.Zone, v int64) (int32, bool, error) {
			return int32(temporal.FloorDiv(scale(v), per)), true, nil
		})
	case "s":
		return mapPhys(o, arrow.PrimitiveTypes.Int64, "", opts, func(_ *temporal.Zone, v int64) (int64, bool, error) {
			return temporal.ConvertUnit(scale(v), src, dtype.Second, true), true, nil
		})
	}
	return o.Timestamp(unit, opts...)
}

// Timestamp returns the physical time since the epoch in unit "ms",
// "us" (default when empty) or "ns" as Int64.
func (o DtOps) Timestamp(unit string, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("timestamp", o.s.DType())
	}
	if unit == "" {
		unit = "us"
	}
	tu, ok := dtype.ParseTimeUnit(unit)
	if !ok || tu == dtype.Second {
		return nil, fmt.Errorf("invalid time unit %q: expected one of 'ms', 'us', 'ns'", unit)
	}
	return mapPhys(o, arrow.PrimitiveTypes.Int64, "", opts, func(_ *temporal.Zone, v int64) (int64, bool, error) {
		if info.kind == tkDate {
			return v * temporal.UnitsPerDay(tu), true, nil
		}
		return temporal.ConvertUnit(v, info.unit, tu, true), true, nil
	})
}

func (o DtOps) total(op string, perNs int64, fractional bool, opts []Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDuration {
		return nil, errTemporalOp(op, o.s.DType())
	}
	unitNs := temporal.NsPerUnit(info.unit)
	if fractional {
		return mapPhys(o, arrow.PrimitiveTypes.Float64, "", opts, func(_ *temporal.Zone, v int64) (float64, bool, error) {
			return float64(v) * float64(unitNs) / float64(perNs), true, nil
		})
	}
	return mapPhys(o, arrow.PrimitiveTypes.Int64, "", opts, func(_ *temporal.Zone, v int64) (int64, bool, error) {
		if perNs >= unitNs {
			return v / (perNs / unitNs), true, nil
		}
		return v * (unitNs / perNs), true, nil
	})
}

// TotalDays returns the whole number of days in each duration
// (truncated towards zero), or a Float64 when fractional is true.
func (o DtOps) TotalDays(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_days", temporal.NsDay, fractional, opts)
}

// TotalHours is TotalDays for hours.
func (o DtOps) TotalHours(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_hours", temporal.NsHour, fractional, opts)
}

// TotalMinutes is TotalDays for minutes.
func (o DtOps) TotalMinutes(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_minutes", temporal.NsMinute, fractional, opts)
}

// TotalSeconds is TotalDays for seconds.
func (o DtOps) TotalSeconds(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_seconds", temporal.NsSecond, fractional, opts)
}

// TotalMilliseconds is TotalDays for milliseconds.
func (o DtOps) TotalMilliseconds(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_milliseconds", temporal.NsMillisecond, fractional, opts)
}

// TotalMicroseconds is TotalDays for microseconds.
func (o DtOps) TotalMicroseconds(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_microseconds", temporal.NsMicrosecond, fractional, opts)
}

// TotalNanoseconds is TotalDays for nanoseconds.
func (o DtOps) TotalNanoseconds(fractional bool, opts ...Option) (*Series, error) {
	return o.total("total_nanoseconds", 1, fractional, opts)
}

// CastTimeUnit rescales a Datetime or Duration to another resolution.
// Datetimes floor and durations truncate when losing precision.
func (o DtOps) CastTimeUnit(unit dtype.TimeUnit, opts ...Option) (*Series, error) {
	info := o.info()
	var out dtype.DType
	switch info.kind {
	case tkDatetime:
		out = dtype.Datetime(unit, info.tz)
	case tkDuration:
		out = dtype.Duration(unit)
	default:
		return nil, errTemporalOp("cast_time_unit", o.s.DType())
	}
	if unit == dtype.Second {
		return nil, fmt.Errorf("invalid time unit %q: expected one of 'ms', 'us', 'ns'", unit)
	}
	floor := info.kind == tkDatetime
	return mapPhys(o, out.Arrow(), "", opts, func(_ *temporal.Zone, v int64) (int64, bool, error) {
		return temporal.ConvertUnit(v, info.unit, unit, floor), true, nil
	})
}

// WithTimeUnit reinterprets the physical values under another unit
// without rescaling them.
func (o DtOps) WithTimeUnit(unit dtype.TimeUnit, opts ...Option) (*Series, error) {
	info := o.info()
	var out dtype.DType
	switch info.kind {
	case tkDatetime:
		out = dtype.Datetime(unit, info.tz)
	case tkDuration:
		out = dtype.Duration(unit)
	default:
		return nil, errTemporalOp("with_time_unit", o.s.DType())
	}
	return mapPhys(o, out.Arrow(), "", opts, func(_ *temporal.Zone, v int64) (int64, bool, error) {
		return v, true, nil
	})
}

// dateOrDatetime runs fn on each value expressed as ticks of unit with
// the zone of the column. Dates are handled as millisecond datetimes
// and converted back by truncating division, like polars.
func (o DtOps) dateOrDatetime(op string, opts []Option, fn func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error)) (*Series, error) {
	info := o.info()
	switch info.kind {
	case tkDate:
		const msDay = 86_400_000
		return mapPhys(o, arrow.FixedWidthTypes.Date32, "", opts, func(_ *temporal.Zone, v int64) (int32, bool, error) {
			r, err := fn(nil, v*msDay, dtype.Millisecond)
			return int32(r / msDay), err == nil, err
		})
	case tkDatetime:
		return mapPhys(o, o.s.DType().Arrow(), info.tz, opts, func(z *temporal.Zone, v int64) (int64, bool, error) {
			r, err := fn(z, v, info.unit)
			return r, err == nil, err
		})
	}
	return nil, errTemporalOp(op, o.s.DType())
}

// Truncate rounds each value down to a multiple of every (a polars
// duration string such as "1h", "1d", "1w", "1mo").
func (o DtOps) Truncate(every string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(every)
	if err != nil {
		return nil, err
	}
	if d.Negative {
		return nil, fmt.Errorf("cannot truncate a %s to a negative duration", kindName(o.info().kind))
	}
	if out, ok, err := o.fixedStep(d, false, opts); ok {
		return out, err
	}
	return o.dateOrDatetime("truncate", opts, func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error) {
		return d.Truncate(t, u, z)
	})
}

// Round rounds each value to the nearest multiple of every. Ties round
// up; calendar durations use polars' estimate (a month counts as 28
// days when locating the midpoint).
func (o DtOps) Round(every string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(every)
	if err != nil {
		return nil, err
	}
	if d.Negative {
		return nil, fmt.Errorf("cannot round a %s to a negative duration", kindName(o.info().kind))
	}
	if out, ok, err := o.fixedStep(d, true, opts); ok {
		return out, err
	}
	return o.dateOrDatetime("round", opts, func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error) {
		return d.Round(t, u, z)
	})
}

func kindName(k temporalKind) string {
	if k == tkDate {
		return "Date"
	}
	return "Datetime"
}

// OffsetBy shifts each value by a polars duration string ("1mo",
// "-2d3h", ...). Month and year offsets clamp to the end of the month.
// For time-zone-aware data calendar units keep the local wall clock.
func (o DtOps) OffsetBy(by string, opts ...Option) (*Series, error) {
	d, err := temporal.ParseDuration(by)
	if err != nil {
		return nil, err
	}
	info := o.info()
	if info.kind == tkDate {
		// polars casts to Datetime(us), offsets and casts back (floor).
		const usDay = 86_400_000_000
		return mapPhys(o, arrow.FixedWidthTypes.Date32, "", opts, func(_ *temporal.Zone, v int64) (int32, bool, error) {
			r, err := d.Add(v*usDay, dtype.Microsecond, nil)
			return int32(temporal.FloorDiv(r, usDay)), err == nil, err
		})
	}
	return o.dateOrDatetime("offset_by", opts, func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error) {
		return d.Add(t, u, z)
	})
}

// OffsetBySeries is OffsetBy with a per-row String series of durations
// (length 1 broadcasts). Null offsets give null results.
func (o DtOps) OffsetBySeries(by *Series, opts ...Option) (*Series, error) {
	if by.Len() == 1 {
		sa, ok := by.Chunk(0).(interface {
			IsValid(int) bool
			Value(int) string
		})
		if !ok {
			return nil, fmt.Errorf("offset_by: expected a String offset, got %s", by.DType())
		}
		if !sa.IsValid(0) {
			return nullLike(o.s, o.s.DType(), opts)
		}
		return o.OffsetBy(sa.Value(0), opts...)
	}
	strs, ok := by.Chunk(0).(interface {
		IsValid(int) bool
		Value(int) string
	})
	if !ok {
		return nil, fmt.Errorf("offset_by: expected a String offset, got %s", by.DType())
	}
	if by.Len() != o.s.Len() {
		return nil, fmt.Errorf("offset_by: length mismatch %d vs %d", o.s.Len(), by.Len())
	}
	cache := map[string]temporal.Duration{}
	parse := func(s string) (temporal.Duration, error) {
		if d, ok := cache[s]; ok {
			return d, nil
		}
		d, err := temporal.ParseDuration(s)
		if err == nil {
			cache[s] = d
		}
		return d, err
	}
	info := o.info()
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	cfg := resolve(opts)
	n := arr.Len()
	switch info.kind {
	case tkDate:
		const usDay = 86_400_000_000
		return buildFixed(o.s.Name(), n, cfg.alloc, arrow.FixedWidthTypes.Date32, arr, func(dst []int32, valid []byte) error {
			for i := range n {
				if !isValidBit(valid, i) || !strs.IsValid(i) {
					clearValid(valid, i)
					continue
				}
				d, err := parse(strs.Value(i))
				if err != nil {
					return err
				}
				r, err := d.Add(vals[i]*usDay, dtype.Microsecond, nil)
				if err != nil {
					return err
				}
				dst[i] = int32(temporal.FloorDiv(r, usDay))
			}
			return nil
		})
	case tkDatetime:
		z, err := zoneFor(info.tz)
		if err != nil {
			return nil, err
		}
		return buildFixed(o.s.Name(), n, cfg.alloc, o.s.DType().Arrow(), arr, func(dst []int64, valid []byte) error {
			for i := range n {
				if !isValidBit(valid, i) || !strs.IsValid(i) {
					clearValid(valid, i)
					continue
				}
				d, err := parse(strs.Value(i))
				if err != nil {
					return err
				}
				if dst[i], err = d.Add(vals[i], info.unit, z); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return nil, errTemporalOp("offset_by", o.s.DType())
}

// nullLike returns an all-null Series with s's name and length.
func nullLike(s *Series, dt dtype.DType, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	n := s.Len()
	return buildFixed(s.Name(), n, cfg.alloc, dt.Arrow(), nil, func(_ []int64, valid []byte) error {
		for i := range valid {
			valid[i] = 0
		}
		return nil
	})
}

// MonthStart rolls each value back to the first day of its month,
// keeping the time of day.
func (o DtOps) MonthStart(opts ...Option) (*Series, error) {
	return o.dateOrDatetime("month_start", opts, func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error) {
		l := t
		if z != nil {
			l = z.ToLocal(t, u)
		}
		days, rem := temporal.SplitDays(l, u)
		y, m, _ := temporal.CivilFromDays(days)
		r := temporal.DaysFromCivil(y, m, 1)*temporal.UnitsPerDay(u) + rem
		if z != nil {
			return z.FromLocalRaise(r, u)
		}
		return r, nil
	})
}

// MonthEnd rolls each value forward to the last day of its month,
// keeping the time of day.
func (o DtOps) MonthEnd(opts ...Option) (*Series, error) {
	return o.dateOrDatetime("month_end", opts, func(z *temporal.Zone, t int64, u dtype.TimeUnit) (int64, error) {
		l := t
		if z != nil {
			l = z.ToLocal(t, u)
		}
		days, rem := temporal.SplitDays(l, u)
		y, m, _ := temporal.CivilFromDays(days)
		r := temporal.DaysFromCivil(y, m, temporal.DaysInMonth(y, m))*temporal.UnitsPerDay(u) + rem
		if z != nil {
			return z.FromLocalRaise(r, u)
		}
		return r, nil
	})
}

// ReplaceTimeZone reinterprets the wall clock of each value in a new
// zone (tz == "" makes the column naive). ambiguous and nonExistent
// take the polars spellings ("raise", "earliest", "latest", "null").
func (o DtOps) ReplaceTimeZone(tz string, ambiguous, nonExistent string, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDatetime {
		return nil, errTemporalOp("replace_time_zone", o.s.DType())
	}
	amb, err := temporal.ParseAmbiguous(ambiguous)
	if err != nil {
		return nil, err
	}
	ne, err := temporal.ParseNonExistent(nonExistent)
	if err != nil {
		return nil, err
	}
	src, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	dst, err := zoneFor(tz)
	if err != nil {
		return nil, err
	}
	out := dtype.Datetime(info.unit, tz)
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	n := arr.Len()
	vals := physicalInt64(arr)
	return buildFixed(o.s.Name(), n, cfg.alloc, out.Arrow(), arr, func(res []int64, valid []byte) error {
		return forRanges(n, func(lo, hi int) error {
			zs, zd := src.Clone(), dst.Clone()
			for i := lo; i < hi; i++ {
				if !isValidBit(valid, i) {
					res[i] = 0
					continue
				}
				l := vals[i]
				if zs != nil {
					l = zs.ToLocal(l, info.unit)
				}
				if zd == nil {
					res[i] = l
					continue
				}
				v, ok, err := zd.FromLocal(l, info.unit, amb, ne)
				if err != nil {
					return err
				}
				if !ok {
					clearValid(valid, i)
				}
				res[i] = v
			}
			return nil
		})
	})
}

// ConvertTimeZone changes the zone of a tz-aware Datetime without
// changing the instant it represents.
func (o DtOps) ConvertTimeZone(tz string) (*Series, error) {
	info := o.info()
	if info.kind != tkDatetime {
		return nil, errTemporalOp("convert_time_zone", o.s.DType())
	}
	// Naive values are treated as UTC, like polars.
	if _, err := temporal.LoadLocation(tz); err != nil && tz != "UTC" {
		return nil, err
	}
	return o.s.reinterpret(dtype.Datetime(info.unit, tz))
}

// reinterpret returns a Series sharing s's buffers under another dtype
// with the same physical layout.
func (s *Series) reinterpret(dt dtype.DType) (*Series, error) {
	arr := s.Chunk(0)
	data := arr.Data()
	bufs := data.Buffers()
	nd := array.NewData(dt.Arrow(), data.Len(), bufs, nil, data.NullN(), data.Offset())
	defer nd.Release()
	return New(s.Name(), array.MakeFromData(nd))
}

// Datetime drops the time zone keeping the local wall clock; the same
// as ReplaceTimeZone("").
func (o DtOps) Datetime(opts ...Option) (*Series, error) {
	if o.info().kind != tkDatetime {
		return nil, fmt.Errorf("expected Datetime, got %s", o.s.DType())
	}
	return o.ReplaceTimeZone("", "raise", "raise", opts...)
}
