package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// BusinessDayOptions configures IsBusinessDay and AddBusinessDays.
// WeekMask is Monday first; the zero value means Monday to Friday.
// Holidays are day numbers since 1970-01-01 (use series.DaysFromDate or
// civil dates via DateToDays).
type BusinessDayOptions struct {
	WeekMask *[7]bool
	Holidays []int64
	// Roll is "raise" (default), "forward" or "backward".
	Roll string
}

func (b BusinessDayOptions) calendar() (*temporal.BusinessCalendar, error) {
	mask := temporal.DefaultWeekMask
	if b.WeekMask != nil {
		mask = *b.WeekMask
	}
	return temporal.NewBusinessCalendar(mask, b.Holidays)
}

// DateToDays converts a calendar date to the day number used by the
// Date dtype.
func DateToDays(year, month, day int) int64 {
	return temporal.DaysFromCivil(int64(year), month, day)
}

// localDays returns, for row value v, the local day number and the
// ticks within that day for Date and Datetime inputs.
func splitLocal(info tinfo, z *temporal.Zone, v int64) (days, rem int64) {
	if info.kind == tkDate {
		return v, 0
	}
	if z != nil {
		v = z.ToLocal(v, info.unit)
	}
	return temporal.SplitDays(v, info.unit)
}

// joinLocal is the inverse of splitLocal.
func joinLocal(info tinfo, z *temporal.Zone, days, rem int64) (int64, error) {
	if info.kind == tkDate {
		return days, nil
	}
	l := days*temporal.UnitsPerDay(info.unit) + rem
	if z != nil {
		return z.FromLocalRaise(l, info.unit)
	}
	return l, nil
}

// IsBusinessDay reports whether each Date or Datetime falls on a
// business day.
func (o DtOps) IsBusinessDay(bd BusinessDayOptions, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("is_business_day", o.s.DType())
	}
	cal, err := bd.calendar()
	if err != nil {
		return nil, err
	}
	z, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	return buildBool(o.s.Name(), arr.Len(), cfg.alloc, arr, func(bits, valid []byte) error {
		for i, v := range vals {
			if !isValidBit(valid, i) {
				continue
			}
			days, _ := splitLocal(info, z, v)
			if cal.IsBusinessDay(days) {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
		return nil
	})
}

// AddBusinessDays shifts each value by n business days; n is either a
// length-1 or a same-length integer Series. The time of day of
// datetimes is preserved.
func (o DtOps) AddBusinessDays(n *Series, bd BusinessDayOptions, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("add_business_days", o.s.DType())
	}
	cal, err := bd.calendar()
	if err != nil {
		return nil, err
	}
	roll, err := temporal.ParseRoll(bd.Roll)
	if err != nil {
		return nil, err
	}
	z, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	nArr := n.Chunk(0)
	nVals := physicalInt64(nArr)
	if nVals == nil {
		return nil, fmt.Errorf("add_business_days: n must be an integer, got %s", n.DType())
	}
	if n.Len() != 1 && n.Len() != o.s.Len() {
		return nil, fmt.Errorf("add_business_days: length mismatch %d vs %d", o.s.Len(), n.Len())
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	cnt := arr.Len()
	step := func(valid []byte, i int) (int64, bool, error) {
		j := i
		if n.Len() == 1 {
			j = 0
		}
		if !nArr.IsValid(j) {
			return 0, false, nil
		}
		days, rem := splitLocal(info, z, vals[i])
		nd, err := cal.AddBusinessDays(days, nVals[j], roll)
		if err != nil {
			return 0, false, err
		}
		r, err := joinLocal(info, z, nd, rem)
		return r, true, err
	}
	if info.kind == tkDate {
		return buildFixed(o.s.Name(), cnt, cfg.alloc, arrow.FixedWidthTypes.Date32, arr, func(dst []int32, valid []byte) error {
			for i := range cnt {
				if !isValidBit(valid, i) {
					continue
				}
				r, ok, err := step(valid, i)
				if err != nil {
					return err
				}
				if !ok {
					clearValid(valid, i)
				}
				dst[i] = int32(r)
			}
			return nil
		})
	}
	return buildFixed(o.s.Name(), cnt, cfg.alloc, o.s.DType().Arrow(), arr, func(dst []int64, valid []byte) error {
		for i := range cnt {
			if !isValidBit(valid, i) {
				continue
			}
			r, ok, err := step(valid, i)
			if err != nil {
				return err
			}
			if !ok {
				clearValid(valid, i)
			}
			dst[i] = r
		}
		return nil
	})
}

// Combine builds a Datetime of the given unit from the date part of a
// Date or Datetime series and a Time series (length 1 broadcasts). The
// time zone of a Datetime input is preserved.
func (o DtOps) Combine(tm *Series, unit dtype.TimeUnit, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("combine", o.s.DType())
	}
	tinfoT := temporalInfoOf(tm.DType())
	if tinfoT.kind != tkTime {
		return nil, fmt.Errorf("combine: expected a Time argument, got %s", tm.DType())
	}
	if tm.Len() != 1 && tm.Len() != o.s.Len() {
		return nil, fmt.Errorf("combine: length mismatch %d vs %d", o.s.Len(), tm.Len())
	}
	z, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	tArr := tm.Chunk(0)
	tVals := physicalInt64(tArr)
	tPer := temporal.NsPerUnit(tinfoT.unit)
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	out := dtype.Datetime(unit, info.tz)
	uPer := temporal.NsPerUnit(unit)
	return buildFixed(o.s.Name(), arr.Len(), cfg.alloc, out.Arrow(), arr, func(dst []int64, valid []byte) error {
		for i := range vals {
			if !isValidBit(valid, i) {
				continue
			}
			j := i
			if tm.Len() == 1 {
				j = 0
			}
			if !tArr.IsValid(j) {
				clearValid(valid, i)
				continue
			}
			days, _ := splitLocal(info, z, vals[i])
			l := days*temporal.UnitsPerDay(unit) + tVals[j]*tPer/uPer
			if z != nil {
				r, err := z.FromLocalRaise(l, unit)
				if err != nil {
					return err
				}
				l = r
			}
			dst[i] = l
		}
		return nil
	})
}

// ReplaceParts carries the optional replacement components for
// DtOps.Replace. A nil field keeps the original component; a non-nil
// field is an integer Series of length 1 (broadcast) or the input
// length.
type ReplaceParts struct {
	Year, Month, Day, Hour, Minute, Second, Microsecond *Series
	// Ambiguous resolves wall-clock times that occur twice in the zone
	// ("raise", "earliest", "latest", "null").
	Ambiguous string
}

// Replace swaps calendar components of Date or Datetime values.
// Invalid dates (February 30th) raise an error like polars.
func (o DtOps) Replace(p ReplaceParts, opts ...Option) (*Series, error) {
	info := o.info()
	if info.kind != tkDate && info.kind != tkDatetime {
		return nil, errTemporalOp("replace", o.s.DType())
	}
	amb, err := temporal.ParseAmbiguous(p.Ambiguous)
	if err != nil {
		return nil, err
	}
	z, err := zoneFor(info.tz)
	if err != nil {
		return nil, err
	}
	type comp struct {
		arr  arrow.Array
		vals []int64
		len1 bool
	}
	parts := [7]*comp{}
	for k, s := range []*Series{p.Year, p.Month, p.Day, p.Hour, p.Minute, p.Second, p.Microsecond} {
		if s == nil {
			continue
		}
		if s.Len() != 1 && s.Len() != o.s.Len() {
			return nil, fmt.Errorf("replace: length mismatch %d vs %d", o.s.Len(), s.Len())
		}
		a := s.Chunk(0)
		v := physicalInt64(a)
		if v == nil {
			return nil, fmt.Errorf("replace: expected integer components, got %s", s.DType())
		}
		parts[k] = &comp{arr: a, vals: v, len1: s.Len() == 1}
	}
	cfg := resolve(opts)
	arr := o.s.Chunk(0)
	vals := physicalInt64(arr)
	row := func(valid []byte, i int) (int64, bool, error) {
		var c temporal.Civil
		if info.kind == tkDate {
			c.Year, c.Month, c.Day = temporal.CivilFromDays(vals[i])
		} else {
			l := vals[i]
			if z != nil {
				l = z.ToLocal(l, info.unit)
			}
			c = temporal.ToCivil(l, info.unit)
		}
		for k, pc := range parts {
			if pc == nil {
				continue
			}
			j := i
			if pc.len1 {
				j = 0
			}
			if !pc.arr.IsValid(j) {
				continue
			}
			v := pc.vals[j]
			switch k {
			case 0:
				c.Year = v
			case 1:
				c.Month = int(v)
			case 2:
				c.Day = int(v)
			case 3:
				c.Hour = int(v)
			case 4:
				c.Minute = int(v)
			case 5:
				c.Second = int(v)
			case 6:
				c.Nanosecond = int(v) * 1000
			}
		}
		if !temporal.ValidDate(c.Year, c.Month, c.Day) {
			return 0, false, fmt.Errorf("invalid date components (%d, %d, %d) supplied", c.Year, c.Month, c.Day)
		}
		if c.Hour < 0 || c.Hour > 23 || c.Minute < 0 || c.Minute > 59 || c.Second < 0 || c.Second > 59 || c.Nanosecond < 0 || c.Nanosecond >= 1_000_000_000 {
			return 0, false, fmt.Errorf("invalid time components (%d, %d, %d, %d) supplied", c.Hour, c.Minute, c.Second, c.Nanosecond/1000)
		}
		if info.kind == tkDate {
			return temporal.DaysFromCivil(c.Year, c.Month, c.Day), true, nil
		}
		l := temporal.FromCivil(c, info.unit)
		if z != nil {
			return z.FromLocal(l, info.unit, amb, temporal.NonExistentRaise)
		}
		return l, true, nil
	}
	if info.kind == tkDate {
		return buildFixed(o.s.Name(), arr.Len(), cfg.alloc, arrow.FixedWidthTypes.Date32, arr, func(dst []int32, valid []byte) error {
			for i := range vals {
				if !isValidBit(valid, i) {
					continue
				}
				r, ok, err := row(valid, i)
				if err != nil {
					return err
				}
				if !ok {
					clearValid(valid, i)
				}
				dst[i] = int32(r)
			}
			return nil
		})
	}
	return buildFixed(o.s.Name(), arr.Len(), cfg.alloc, o.s.DType().Arrow(), arr, func(dst []int64, valid []byte) error {
		for i := range vals {
			if !isValidBit(valid, i) {
				continue
			}
			r, ok, err := row(valid, i)
			if err != nil {
				return err
			}
			if !ok {
				clearValid(valid, i)
			}
			dst[i] = r
		}
		return nil
	})
}
