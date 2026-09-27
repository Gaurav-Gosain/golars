package temporal

import (
	"fmt"
	"slices"
)

// Roll selects how add_business_days treats a start date that is not a
// business day.
type Roll int8

const (
	RollRaise Roll = iota
	RollForward
	RollBackward
)

// ParseRoll maps the polars spelling to a Roll value.
func ParseRoll(s string) (Roll, error) {
	switch s {
	case "", "raise":
		return RollRaise, nil
	case "forward":
		return RollForward, nil
	case "backward":
		return RollBackward, nil
	}
	return 0, fmt.Errorf("invalid roll value %q: expected one of 'raise', 'forward', 'backward'", s)
}

// BusinessCalendar is a week mask (Monday first) plus a sorted list of
// holiday day numbers that fall on masked-in weekdays.
type BusinessCalendar struct {
	Mask     [7]bool
	holidays []int64
	perWeek  int64
}

// DefaultWeekMask is Monday through Friday.
var DefaultWeekMask = [7]bool{true, true, true, true, true, false, false}

// NewBusinessCalendar normalizes the holidays (dedupe, sort, drop
// holidays that already fall on non-business weekdays).
func NewBusinessCalendar(mask [7]bool, holidays []int64) (*BusinessCalendar, error) {
	c := &BusinessCalendar{Mask: mask}
	for _, m := range mask {
		if m {
			c.perWeek++
		}
	}
	if c.perWeek == 0 {
		return nil, fmt.Errorf("`week_mask` must have at least one business day")
	}
	for _, h := range holidays {
		if mask[ISOWeekday(h)-1] {
			c.holidays = append(c.holidays, h)
		}
	}
	slices.Sort(c.holidays)
	c.holidays = slices.Compact(c.holidays)
	return c, nil
}

func (c *BusinessCalendar) isHoliday(d int64) bool {
	_, ok := slices.BinarySearch(c.holidays, d)
	return ok
}

// IsBusinessDay reports whether day d is a business day.
func (c *BusinessCalendar) IsBusinessDay(d int64) bool {
	return c.Mask[ISOWeekday(d)-1] && !c.isHoliday(d)
}

// countHolidays returns the number of holidays in (lo, hi].
func (c *BusinessCalendar) countHolidays(lo, hi int64) int64 {
	a, _ := slices.BinarySearch(c.holidays, lo+1)
	b, _ := slices.BinarySearch(c.holidays, hi+1)
	return int64(b - a)
}

// AddBusinessDays shifts day d by n business days.
func (c *BusinessCalendar) AddBusinessDays(d, n int64, roll Roll) (int64, error) {
	if !c.IsBusinessDay(d) {
		switch roll {
		case RollForward:
			for !c.IsBusinessDay(d) {
				d++
			}
		case RollBackward:
			for !c.IsBusinessDay(d) {
				d--
			}
		default:
			y, m, dd := CivilFromDays(d)
			return 0, fmt.Errorf("date %04d-%02d-%02d is not a business date; use `roll` to roll forwards (or backwards) to the next (or previous) valid date", y, m, dd)
		}
	}
	switch {
	case n > 0:
		start := d
		d += (n / c.perWeek) * 7
		n %= c.perWeek
		n += c.countHolidays(start, d)
		for n > 0 {
			d++
			if c.IsBusinessDay(d) {
				n--
			}
		}
	case n < 0:
		n = -n
		start := d
		d -= (n / c.perWeek) * 7
		n %= c.perWeek
		n += c.countHolidays(d-1, start-1)
		for n > 0 {
			d--
			if c.IsBusinessDay(d) {
				n--
			}
		}
	}
	return d, nil
}
