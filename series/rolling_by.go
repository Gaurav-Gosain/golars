package series

import (
	"fmt"
	"slices"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// RollingByOptions configures RollingBy. The zero value of MinPeriods
// means 1 and of Closed means "right", the polars defaults.
type RollingByOptions struct {
	// WindowSize is a duration string such as "2h" or "3d" ("5i" for
	// integer index columns).
	WindowSize string
	MinPeriods int
	// Closed is "right" (default), "left", "both" or "none".
	Closed string
	// Ddof applies to std and var.
	Ddof int
	// Quantile and Interpolation apply to "quantile".
	Quantile      float64
	Interpolation string
}

// Timeline is a sorted-or-not index column converted to physical ticks
// with the unit and zone window arithmetic runs in.
type Timeline struct {
	Values []int64
	Unit   dtype.TimeUnit
	TZ     string
	// IsInt marks integer index columns (durations must use "i").
	IsInt bool
}

// TimelineOf converts a Date, Datetime or integer index column into a
// Timeline. Dates become microsecond datetimes like polars does.
func TimelineOf(by *Series) (Timeline, error) {
	if by.NullCount() > 0 {
		return Timeline{}, fmt.Errorf("null values in `rolling` not supported, fill nulls")
	}
	arr := by.Chunk(0)
	dt := by.DType()
	switch {
	case dt.IsDate():
		vals := physicalInt64(arr)
		per := temporal.UnitsPerDay(dtype.Microsecond)
		out := make([]int64, len(vals))
		for i, v := range vals {
			out[i] = v * per
		}
		return Timeline{Values: out, Unit: dtype.Microsecond}, nil
	case dt.IsDatetime():
		u, _ := dt.TimeUnit()
		v := physicalInt64(arr)
		return Timeline{Values: v, Unit: u, TZ: dt.TimeZone()}, nil
	case dt.IsInteger():
		v := physicalInt64(arr)
		return Timeline{Values: v, Unit: dtype.Nanosecond, IsInt: true}, nil
	}
	return Timeline{}, fmt.Errorf("expected any of the following dtypes: { Date, Datetime, Int32, Int64, UInt32, UInt64 }, got %s", dt)
}

// CheckDuration applies polars' rule that integer index columns take
// "i" durations and temporal ones do not.
func (t Timeline) CheckDuration(d temporal.Duration, what string) error {
	if t.IsInt && !d.ParsedInt && !d.IsZero() {
		return fmt.Errorf("`%s` duration must be a parsed integer (i.e. use '2i', not '2d') when working with a numeric column", what)
	}
	if !t.IsInt && d.ParsedInt {
		return fmt.Errorf("`%s` duration may not be a parsed integer (i.e. use '2d', not '2i') when working with a temporal column", what)
	}
	return nil
}

// RollingBy computes a rolling aggregation over a time-based window of
// the index column by (polars' rolling_*_by). Each row i aggregates the
// rows whose index lies in (t_i - window, t_i] (endpoints per Closed).
// op is one of sum, mean, min, max, std, var, median, quantile. by
// need not be sorted; results are returned in the original row order.
func (s *Series) RollingBy(op string, by *Series, o RollingByOptions, opts ...Option) (*Series, error) {
	if by.Len() != s.Len() {
		return nil, fmt.Errorf("rolling_%s_by: `by` length %d does not match %d", op, by.Len(), s.Len())
	}
	tl, err := TimelineOf(by)
	if err != nil {
		return nil, err
	}
	period, err := temporal.ParseDuration(o.WindowSize)
	if err != nil {
		return nil, err
	}
	if period.IsZero() || period.Negative {
		return nil, fmt.Errorf("`window_size` must be strictly positive")
	}
	if err := tl.CheckDuration(period, "window_size"); err != nil {
		return nil, err
	}
	closed := o.Closed
	if closed == "" {
		closed = "right"
	}
	cw, err := temporal.ParseClosed(closed)
	if err != nil {
		return nil, err
	}
	z, err := zoneFor(tl.TZ)
	if err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	n := s.Len()
	times := tl.Values
	vals := s
	var perm []int
	if !slices.IsSorted(times) {
		perm = make([]int, n)
		for i := range perm {
			perm[i] = i
		}
		slices.SortStableFunc(perm, func(a, b int) int {
			switch {
			case times[a] < times[b]:
				return -1
			case times[a] > times[b]:
				return 1
			}
			return 0
		})
		sorted := make([]int64, n)
		for i, p := range perm {
			sorted[i] = times[p]
		}
		times = sorted
		if vals, err = takeNullable(s, perm, cfg.alloc); err != nil {
			return nil, err
		}
		defer vals.Release()
	}
	starts := make([]int, n)
	lens := make([]int, n)
	if err := temporal.GroupByValues(period, period.Neg(), times, cw, tl.Unit, z, starts, lens); err != nil {
		return nil, err
	}
	mp := o.MinPeriods
	if mp <= 0 {
		mp = 1
	}
	out, err := AggRanges(vals, op, starts, lens, RangeAggOptions{
		MinPeriods: mp, Ddof: o.Ddof, Quantile: o.Quantile, Interpolation: o.Interpolation,
	}, opts...)
	if err != nil || perm == nil {
		return out, err
	}
	defer out.Release()
	inv := make([]int, n)
	for i, p := range perm {
		inv[p] = i
	}
	res, err := takeNullable(out, inv, cfg.alloc)
	if err != nil {
		return nil, err
	}
	if res.Name() != s.Name() {
		r := res.Rename(s.Name())
		res.Release()
		res = r
	}
	return res, nil
}
