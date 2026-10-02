package temporal

import (
	"fmt"
	"math"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// ClosedWindow selects which window endpoints are inclusive.
type ClosedWindow int8

const (
	ClosedLeft ClosedWindow = iota
	ClosedRight
	ClosedBoth
	ClosedNone
)

// ParseClosed maps the polars spelling to a ClosedWindow.
func ParseClosed(s string) (ClosedWindow, error) {
	switch s {
	case "left":
		return ClosedLeft, nil
	case "right":
		return ClosedRight, nil
	case "both":
		return ClosedBoth, nil
	case "none":
		return ClosedNone, nil
	}
	return 0, fmt.Errorf("invalid closed value %q: expected one of 'left', 'right', 'both', 'none'", s)
}

// Bounds is a [Start, Stop] window in physical ticks whose endpoint
// inclusivity is decided by a ClosedWindow.
type Bounds struct{ Start, Stop int64 }

// IsMember reports whether t lies inside the window.
func (b Bounds) IsMember(t int64, c ClosedWindow) bool {
	switch c {
	case ClosedRight:
		return t > b.Start && t <= b.Stop
	case ClosedLeft:
		return t >= b.Start && t < b.Stop
	case ClosedNone:
		return t > b.Start && t < b.Stop
	}
	return t >= b.Start && t <= b.Stop
}

func (b Bounds) isMemberEntry(t int64, c ClosedWindow) bool {
	if c == ClosedLeft || c == ClosedBoth {
		return t >= b.Start
	}
	return t > b.Start
}

func (b Bounds) isMemberExit(t int64, c ClosedWindow) bool {
	if c == ClosedRight || c == ClosedBoth {
		return t <= b.Stop
	}
	return t < b.Stop
}

func (b Bounds) isFuture(t int64, c ClosedWindow) bool {
	if c == ClosedLeft || c == ClosedNone {
		return b.Stop <= t
	}
	return b.Stop < t
}

func (b Bounds) isPast(t int64, c ClosedWindow) bool {
	if c == ClosedLeft || c == ClosedBoth {
		return b.Start > t
	}
	return b.Start >= t
}

// StartBy picks how the first window of a dynamic group-by is placed.
type StartBy int8

const (
	StartByWindowBound StartBy = iota
	StartByDataPoint
	StartByMonday
	StartByTuesday
	StartByWednesday
	StartByThursday
	StartByFriday
	StartBySaturday
	StartBySunday
)

// ParseStartBy maps the polars spelling to a StartBy.
func ParseStartBy(s string) (StartBy, error) {
	switch s {
	case "", "window":
		return StartByWindowBound, nil
	case "datapoint":
		return StartByDataPoint, nil
	case "monday":
		return StartByMonday, nil
	case "tuesday":
		return StartByTuesday, nil
	case "wednesday":
		return StartByWednesday, nil
	case "thursday":
		return StartByThursday, nil
	case "friday":
		return StartByFriday, nil
	case "saturday":
		return StartBySaturday, nil
	case "sunday":
		return StartBySunday, nil
	}
	return 0, fmt.Errorf("invalid start_by value %q", s)
}

// Window describes the dynamic windows start_i = zero + every*i with
// length period, shifted by offset.
type Window struct {
	Every, Period, Offset Duration
}

// earliestBounds returns the first window that contains t or starts
// after it.
func (w Window) earliestBounds(t int64, c ClosedWindow, u dtype.TimeUnit, z *Zone) (Bounds, error) {
	start, err := w.Every.Truncate(t, u, z)
	if err != nil {
		return Bounds{}, err
	}
	if start, err = w.Offset.Add(start, u, z); err != nil {
		return Bounds{}, err
	}
	return ensureInOrInFront(w.Every, t, w.Period, start, c, u, z)
}

// ensureInOrInFront shifts the window back by multiples of every until
// t is inside or in front of it.
func ensureInOrInFront(every Duration, t int64, period Duration, start int64, c ClosedWindow, u dtype.TimeUnit, z *Zone) (Bounds, error) {
	every = every.Neg()
	stop, err := period.Add(start, u, z)
	if err != nil {
		return Bounds{}, err
	}
	nte := every.NteIn(u)
	for (Bounds{start, stop}).isPast(t, c) {
		gap := start - t
		if c == ClosedRight || c == ClosedNone {
			gap++
		}
		stride := max((gap+nte-1)/nte, 1)
		if start, err = every.Mul(stride).Add(start, u, z); err != nil {
			return Bounds{}, err
		}
		if stop, err = period.Add(start, u, z); err != nil {
			return Bounds{}, err
		}
	}
	if start > stop {
		return Bounds{}, fmt.Errorf("boundary start must be smaller than stop; is your time column sorted in ascending order?")
	}
	return Bounds{start, stop}, nil
}

type boundsIter struct {
	w        Window
	boundary Bounds
	bi       Bounds
	u        dtype.TimeUnit
	z        *Zone
	err      error
}

func newBoundsIter(w Window, c ClosedWindow, boundary Bounds, u dtype.TimeUnit, z *Zone, sb StartBy) (*boundsIter, error) {
	it := &boundsIter{w: w, boundary: boundary, u: u, z: z}
	switch sb {
	case StartByDataPoint:
		stop, err := w.Period.Add(boundary.Start, u, z)
		if err != nil {
			return nil, err
		}
		it.bi = Bounds{boundary.Start, stop}
	case StartByWindowBound:
		b, err := w.earliestBounds(boundary.Start, c, u, z)
		if err != nil {
			return nil, err
		}
		it.bi = b
	default:
		// Beginning of the week (Monday 00:00) in local time, then shift
		// to the requested weekday and apply the offset.
		l := boundary.Start
		if z != nil {
			l = z.ToLocal(l, u)
		}
		days, _ := SplitDays(l, u)
		monday := (days - int64(ISOWeekday(days)-1)) * UnitsPerDay(u)
		start := monday
		if z != nil {
			var err error
			if start, err = z.FromLocalRaise(monday, u); err != nil {
				return nil, err
			}
		}
		var err error
		wd := int64(sb - StartByMonday)
		if start, err = (Duration{Days: wd}).Add(start, u, z); err != nil {
			return nil, err
		}
		if start, err = w.Offset.Add(start, u, z); err != nil {
			return nil, err
		}
		b, err := ensureInOrInFront(w.Every, boundary.Start, w.Period, start, c, u, z)
		if err != nil {
			return nil, err
		}
		it.bi = b
	}
	return it, nil
}

// nth advances n windows then returns the current one (Iterator::nth).
func (it *boundsIter) nth(n int64) (Bounds, bool) {
	if n > 0 && it.bi.Start < it.boundary.Stop {
		s, err := it.w.Every.Mul(n).Add(it.bi.Start, it.u, it.z)
		if err != nil {
			it.err = err
			return Bounds{}, false
		}
		e, err := it.w.Period.Add(s, it.u, it.z)
		if err != nil {
			it.err = err
			return Bounds{}, false
		}
		it.bi = Bounds{s, e}
	}
	if it.bi.Start >= it.boundary.Stop {
		return Bounds{}, false
	}
	out := it.bi
	s, err := it.w.Every.Add(it.bi.Start, it.u, it.z)
	if err != nil {
		it.err = err
		return Bounds{}, false
	}
	e, err := it.w.Period.Add(s, it.u, it.z)
	if err != nil {
		it.err = err
		return Bounds{}, false
	}
	it.bi = Bounds{s, e}
	return out, true
}

func (it *boundsIter) stride(target int64) int64 {
	if it.bi.Start < it.boundary.Stop && target > it.bi.Start {
		gap := target - it.bi.Start
		en, pn := it.w.Every.NteIn(it.u), it.w.Period.NteIn(it.u)
		if gap > en+pn {
			return (gap - pn) / en
		}
	}
	return 0
}

// WindowGroups is the result of GroupByWindows: one [Start, Start+Len)
// row range per emitted window plus the window bounds.
type WindowGroups struct {
	Starts, Lens []int
	Lower, Upper []int64
}

// GroupByWindows assigns the sorted time values to dynamic windows,
// mirroring polars' group_by_windows. time must be sorted ascending and
// non-empty.
func GroupByWindows(w Window, time []int64, c ClosedWindow, u dtype.TimeUnit, z *Zone, sb StartBy) (WindowGroups, error) {
	var out WindowGroups
	if len(time) == 0 {
		return out, nil
	}
	if w.Every.Negative {
		return out, fmt.Errorf("'every' argument must be positive")
	}
	start := time[0]
	stop := start + 1
	if len(time) > 1 {
		stop = time[len(time)-1] + 1
	}
	if start > stop-1 {
		return out, fmt.Errorf("boundary start must be smaller than stop; is your time column sorted in ascending order?")
	}
	it, err := newBoundsIter(w, c, Bounds{start, stop}, u, z, sb)
	if err != nil {
		return out, err
	}
	pos := 0
	var stride int64
outer:
	for {
		bi, ok := it.nth(stride)
		if !ok {
			break
		}
		hasMember := false
		for pos < len(time)-1 {
			t := time[pos]
			if bi.isFuture(t, c) {
				stride = it.stride(t)
				continue outer
			}
			if bi.isMemberEntry(t, c) {
				hasMember = true
				break
			}
			pos++
		}
		if hasMember {
			stride = 0
		} else {
			stride = it.stride(time[pos])
		}
		end := pos
		if end == len(time)-1 {
			if bi.IsMember(time[end], c) {
				out.Starts = append(out.Starts, end)
				out.Lens = append(out.Lens, 1)
				out.Lower = append(out.Lower, bi.Start)
				out.Upper = append(out.Upper, bi.Stop)
			}
			continue
		}
		for end < len(time) && bi.isMemberExit(time[end], c) {
			end++
		}
		out.Starts = append(out.Starts, pos)
		out.Lens = append(out.Lens, end-pos)
		out.Lower = append(out.Lower, bi.Start)
		out.Upper = append(out.Upper, bi.Stop)
	}
	if it.err != nil {
		return WindowGroups{}, it.err
	}
	return out, nil
}

// GroupByValues computes one window per row: [t+offset, t+offset+period]
// with endpoints chosen by c. time must be sorted ascending. starts and
// lens receive the row range of each window. This mirrors polars'
// group_by_values, which backs DataFrame.rolling and rolling_*_by.
func GroupByValues(period, offset Duration, time []int64, c ClosedWindow, u dtype.TimeUnit, z *Zone, starts, lens []int) error {
	n := len(time)
	if n == 0 {
		return nil
	}
	lookbehind := offset.Negative && !offset.IsZero() && offset.EstimateNs() == period.EstimateNs()
	start, end := 0, 0
	last := int64(math.MinInt64)
	for i, t := range time {
		if i > 0 && t == last {
			starts[i], lens[i] = start, end-start
			continue
		}
		last = t
		lower, err := offset.Add(t, u, z)
		if err != nil {
			return err
		}
		upper := t
		if !lookbehind {
			if upper, err = period.Add(lower, u, z); err != nil {
				return err
			}
		}
		b := Bounds{lower, upper}
		for start < n && !b.isMemberEntry(time[start], c) {
			start++
		}
		end = max(end, start)
		for end < n && b.isMemberExit(time[end], c) {
			end++
		}
		starts[i], lens[i] = start, end-start
	}
	return nil
}

// RangeValues generates start, start+interval, ... up to end (bounds
// included per closed), mirroring polars' datetime_range_i64. Values
// are ticks of unit u; z localizes calendar steps for zoned ranges.
func RangeValues(start, end int64, interval Duration, closed ClosedWindow, u dtype.TimeUnit, z *Zone) ([]int64, error) {
	if start > end {
		return nil, nil
	}
	if interval.Negative || interval.IsZero() {
		return nil, fmt.Errorf("`interval` must be positive")
	}
	step := interval.EstimateIn(u)
	tzName := ""
	if z != nil {
		tzName = z.Name
	}
	if interval.IsConstant(tzName) {
		if step == 0 {
			return nil, fmt.Errorf("interval %s is too small for time unit %s and was rounded down to zero", interval, u)
		}
		lo, hi := start, end
		incl := true
		switch closed {
		case ClosedNone:
			lo, incl = start+step, false
		case ClosedLeft:
			incl = false
		case ClosedRight:
			lo = start + step
		}
		var out []int64
		if hi >= lo {
			out = make([]int64, 0, (hi-lo)/step+1)
		}
		for v := lo; v < hi || (incl && v == hi); v += step {
			out = append(out, v)
		}
		return out, nil
	}
	var out []int64
	i := int64(0)
	if closed == ClosedRight || closed == ClosedNone {
		i = 1
	}
	for {
		t, err := interval.Mul(i).Add(start, u, z)
		if err != nil {
			return nil, err
		}
		if t > end || ((closed == ClosedLeft || closed == ClosedNone) && t == end) {
			break
		}
		out = append(out, t)
		i++
	}
	return out, nil
}
