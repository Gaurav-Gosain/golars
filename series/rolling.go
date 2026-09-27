package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)

// RollingOptions configures a rolling aggregation. Mirrors polars'
// RollingOptions for fixed-size windows.
type RollingOptions struct {
	// WindowSize is the number of rows per window. Must be >= 1.
	WindowSize int
	// MinPeriods is the minimum non-null count required to produce a
	// value; windows with fewer valid entries emit null. Defaults to
	// WindowSize when 0.
	MinPeriods int
}

func (o RollingOptions) resolve() (int, int, error) {
	if o.WindowSize < 1 {
		return 0, 0, fmt.Errorf("series: rolling window must be >= 1")
	}
	mp := o.MinPeriods
	if mp <= 0 {
		mp = o.WindowSize
	}
	if mp > o.WindowSize {
		mp = o.WindowSize
	}
	return o.WindowSize, mp, nil
}

// RollingSum returns a rolling sum using a right-aligned window of
// size opts.WindowSize. Rows where the valid-count is less than
// MinPeriods emit null.
func (s *Series) RollingSum(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	w, mp, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	cfg := resolve(callerOpts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	// No-null fast paths: skip the closure + validity-check per row.
	switch a := chunk.(type) {
	case *array.Int64:
		if a.NullN() == 0 {
			return rollingSumInt64NoNull(s.Name(), a.Int64Values(), n, w, mp, cfg.alloc)
		}
		vals := a.Int64Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFused(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return float64(vals[i]), a.IsValid(i) },
				rollingSumReducer)
		})
	case *array.Float64:
		if a.NullN() == 0 {
			return rollingSumFloat64NoNull(s.Name(), a.Float64Values(), n, w, mp, cfg.alloc)
		}
		vals := a.Float64Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFused(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return vals[i], a.IsValid(i) },
				rollingSumReducer)
		})
	case *array.Int32:
		vals := a.Int32Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFused(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return float64(vals[i]), a.IsValid(i) },
				rollingSumReducer)
		})
	case *array.Float32:
		vals := a.Float32Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFused(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return float64(vals[i]), a.IsValid(i) },
				rollingSumReducer)
		})
	}
	return nil, fmt.Errorf("series: RollingSum unsupported for dtype %s", s.DType())
}

// rollingSumInt64NoNull is the specialised hot path: no validity
// checks, no closure, constant-time slide. Output has no nulls past
// the first mp-1 leading rows, so the validity bitmap is written in
// bulk afterwards rather than one bit per row inside the loop.
func rollingSumInt64NoNull(
	name string, vals []int64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, mempool.Pooling(alloc), func(out []float64, validBits []byte) int {
		warm := min(w, n)
		var total int64
		for i := range warm {
			total += vals[i]
			out[i] = float64(total)
		}
		if n > warm {
			add, drop, dst := vals[warm:n], vals[:n-warm], out[warm:n]
			drop, dst = drop[:len(add)], dst[:len(add)]
			for i, v := range add {
				total += v - drop[i]
				dst[i] = float64(total)
			}
		}
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

// rollingSumFloat64NoNull slides a float64 total. The add and the
// evict fuse into one difference per row, like the int64 path.
func rollingSumFloat64NoNull(
	name string, vals []float64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, mempool.Pooling(alloc), func(out []float64, validBits []byte) int {
		warm := min(w, n)
		var total float64
		for i := range warm {
			total += vals[i]
			out[i] = total
		}
		if n > warm {
			add, drop, dst := vals[warm:n], vals[:n-warm], out[warm:n]
			drop, dst = drop[:len(add)], dst[:len(add)]
			for i, v := range add {
				total += v - drop[i]
				dst[i] = total
			}
		}
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

// RollingMean returns the rolling average.
func (s *Series) RollingMean(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	w, mp, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	cfg := resolve(callerOpts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	// No-null fast paths mirror the sum specialisation, dividing by the
	// window count. Totals accumulate in float64 like meanReducer; the
	// fused slide matches it to float tolerance (same caveat as the
	// sum specialisation, which also fuses add/remove).
	switch a := chunk.(type) {
	case *array.Int64:
		if a.NullN() == 0 {
			return rollingMeanInt64NoNull(s.Name(), a.Int64Values(), n, w, mp, cfg.alloc)
		}
	case *array.Float64:
		if a.NullN() == 0 {
			return rollingMeanFloat64NoNull(s.Name(), a.Float64Values(), n, w, mp, cfg.alloc)
		}
	}
	return s.rollingReduce(opts, callerOpts, rollingMeanReducer, "RollingMean")
}

// rollingMeanInt64NoNull slides a float64 total over int64 values,
// dividing by the row count while the window warms up and by w after.
func rollingMeanInt64NoNull(
	name string, vals []int64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, mempool.Pooling(alloc), func(out []float64, validBits []byte) int {
		warm := min(w, n)
		var total float64
		for i := range warm {
			total += float64(vals[i])
			out[i] = total / float64(i+1)
		}
		if n > warm {
			fw := float64(w)
			add, drop, dst := vals[warm:n], vals[:n-warm], out[warm:n]
			drop, dst = drop[:len(add)], dst[:len(add)]
			for i, v := range add {
				total += float64(v) - float64(drop[i])
				dst[i] = total / fw
			}
		}
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

func rollingMeanFloat64NoNull(
	name string, vals []float64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, mempool.Pooling(alloc), func(out []float64, validBits []byte) int {
		warm := min(w, n)
		var total float64
		for i := range warm {
			total += vals[i]
			out[i] = total / float64(i+1)
		}
		if n > warm {
			fw := float64(w)
			add, drop, dst := vals[warm:n], vals[:n-warm], out[warm:n]
			drop, dst = drop[:len(add)], dst[:len(add)]
			for i, v := range add {
				total += v - drop[i]
				dst[i] = total / fw
			}
		}
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

// RollingMin returns the rolling minimum.
func (s *Series) RollingMin(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	return s.rollingMinMax(opts, callerOpts, true)
}

// RollingMax returns the rolling maximum.
func (s *Series) RollingMax(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	return s.rollingMinMax(opts, callerOpts, false)
}

// rollingMinMax dispatches to the monotonic-deque drivers below.
func (s *Series) rollingMinMax(opts RollingOptions, callerOpts []Option, isMin bool) (*Series, error) {
	w, mp, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	name := "RollingMin"
	if !isMin {
		name = "RollingMax"
	}
	cfg := resolve(callerOpts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		off := a.Data().Offset()
		if a.NullN() == 0 {
			return rollingMinMaxIntNoNull(s.Name(), vals, n, w, mp, isMin, cfg.alloc)
		}
		return rollingMinMaxNullable(s.Name(), vals, a.NullBitmapBytes(), off, n, w, mp, isMin, cfg.alloc)
	case *array.Float64:
		vals := a.Float64Values()
		off := a.Data().Offset()
		if a.NullN() == 0 {
			return rollingMinMaxNoNull(s.Name(), vals, n, w, mp, isMin, cfg.alloc)
		}
		return rollingMinMaxNullable(s.Name(), vals, a.NullBitmapBytes(), off, n, w, mp, isMin, cfg.alloc)
	case *array.Int32:
		vals := a.Int32Values()
		off := a.Data().Offset()
		if a.NullN() == 0 {
			return rollingMinMaxIntNoNull(s.Name(), vals, n, w, mp, isMin, cfg.alloc)
		}
		return rollingMinMaxNullable(s.Name(), vals, a.NullBitmapBytes(), off, n, w, mp, isMin, cfg.alloc)
	}
	return nil, fmt.Errorf("series: %s unsupported for dtype %s", name, s.DType())
}

// rollingOrdered covers the dtypes with deque min/max drivers.
type rollingOrdered interface {
	~int64 | ~float64 | ~int32
}

// rollingMinMaxNoNull is the O(1)-amortised deque driver for columns
// without nulls. The deque holds window indices with monotonic
// values, so the front is always the current min (or max) and both
// insert and age-eviction are index operations. NaN never enters the
// deque (an incomparable entry would barrier later minima); a NaN
// surfaces exactly while it is the oldest entry, matching the scalar
// scan it replaces.
func rollingMinMaxNoNull[T rollingOrdered](
	name string, vals []T, n, w, mp int, isMin bool, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		var zero T
		_, isFloat := any(zero).(float64)
		dq := newIdxRing(min(w, n) + 1)
		for i := range n {
			if ev := i - w; !dq.empty() && dq.front() <= ev {
				dq.popFront()
			}
			vi := vals[i]
			if !isFloat || !math.IsNaN(float64(vi)) {
				if isMin {
					for !dq.empty() && vals[dq.back()] >= vi {
						dq.popBack()
					}
				} else {
					for !dq.empty() && vals[dq.back()] <= vi {
						dq.popBack()
					}
				}
				dq.pushBack(i)
			}
			oldest := max(i-w+1, 0)
			if isFloat && math.IsNaN(float64(vals[oldest])) {
				out[i] = math.NaN()
			} else {
				out[i] = float64(vals[dq.front()])
			}
		}
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

// rollingMinMaxIntNoNull is the integer driver for columns without
// nulls; see rollingMinMaxIntKernel.
func rollingMinMaxIntNoNull[T int64 | int32](
	name string, vals []T, n, w, mp int, isMin bool, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, mempool.Pooling(alloc), func(out []float64, validBits []byte) int {
		rollingMinMaxIntKernel(out, vals, w, isMin)
		return markValidFrom(validBits, rollingLead(n, mp), n)
	})
}

// rollingMinMaxNullable is the deque driver for columns with nulls.
// Only non-NaN valid indices enter the deque; a separate FIFO tracks
// valid indices in age order so the oldest is always known. A NaN
// surfaces exactly while it is the oldest valid entry.
func rollingMinMaxNullable[T rollingOrdered](
	name string, vals []T, nullBits []byte, off, n, w, mp int, isMin bool, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		var zero T
		_, isFloat := any(zero).(float64)
		dq := newIdxRing(min(w, n) + 1)
		order := newIdxRing(min(w, n) + 1)
		nulls := 0
		for i := range n {
			ev := i - w
			if !dq.empty() && dq.front() <= ev {
				dq.popFront()
			}
			if !order.empty() && order.front() <= ev {
				order.popFront()
			}
			if nullBits[(i+off)>>3]&(1<<uint((i+off)&7)) != 0 {
				order.pushBack(i)
				vi := vals[i]
				if !isFloat || !math.IsNaN(float64(vi)) {
					if isMin {
						for !dq.empty() && vals[dq.back()] >= vi {
							dq.popBack()
						}
					} else {
						for !dq.empty() && vals[dq.back()] <= vi {
							dq.popBack()
						}
					}
					dq.pushBack(i)
				}
			}
			if order.size() >= mp {
				if oldest := order.front(); isFloat && math.IsNaN(float64(vals[oldest])) {
					out[i] = math.NaN()
				} else {
					out[i] = float64(vals[dq.front()])
				}
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		return nulls
	})
}

// RollingStd returns the rolling sample standard deviation (ddof=1).
func (s *Series) RollingStd(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	return s.rollingReduce(opts, callerOpts, rollingStdReducer, "RollingStd")
}

// RollingVar returns the rolling sample variance (ddof=1).
func (s *Series) RollingVar(opts RollingOptions, callerOpts ...Option) (*Series, error) {
	return s.rollingReduce(opts, callerOpts, rollingVarReducer, "RollingVar")
}

type rollingReducer interface {
	reset()
	add(v float64)
	remove(v float64)
	result(count int) float64
}

func (s *Series) rollingReduce(opts RollingOptions, callerOpts []Option, make func() rollingReducer, name string) (*Series, error) {
	w, mp, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	cfg := resolve(callerOpts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFusedWithReducer(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return float64(vals[i]), a.IsValid(i) }, make)
		})
	case *array.Float64:
		vals := a.Float64Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFusedWithReducer(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return vals[i], a.IsValid(i) }, make)
		})
	case *array.Int32:
		vals := a.Int32Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return rollingReduceFusedWithReducer(out, validBits, n, w, mp,
				func(i int) (float64, bool) { return float64(vals[i]), a.IsValid(i) }, make)
		})
	}
	return nil, fmt.Errorf("series: %s unsupported for dtype %s", name, s.DType())
}

// rollingSumReducer is a fast specialisation: O(1) add/remove lets
// RollingSum skip the interface dispatch used by min/max/std.
type rollingSumReducerStateless struct{}

var rollingSumReducer = rollingSumReducerStateless{}

type sumReducer struct{ total float64 }

func (r *sumReducer) reset()               { r.total = 0 }
func (r *sumReducer) add(v float64)        { r.total += v }
func (r *sumReducer) remove(v float64)     { r.total -= v }
func (r *sumReducer) result(_ int) float64 { return r.total }

type meanReducer struct {
	total float64
}

func (r *meanReducer) reset()           { r.total = 0 }
func (r *meanReducer) add(v float64)    { r.total += v }
func (r *meanReducer) remove(v float64) { r.total -= v }
func (r *meanReducer) result(count int) float64 {
	if count == 0 {
		return math.NaN()
	}
	return r.total / float64(count)
}

// varReducer is polars' rolling variance: a Welford state updated one
// value at a time (weight, mean and the sum of squared deviations dp).
// Non-finite values enter as 0 and make the window NaN, and a window
// with no more values than ddof is null. Following the same update
// order as polars also reproduces its overflow results (inf, NaN).
type varReducer struct {
	weight, mean, dp float64
	nonFinite        int
	ddof             int
}

func (r *varReducer) reset() { *r = varReducer{ddof: r.ddof} }

func (r *varReducer) add(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		r.nonFinite++
		v = 0
	}
	w := r.weight + 1
	delta := v - r.mean
	mean := r.mean + delta/w
	r.dp += float64((v - mean) * delta) // float64() blocks FMA fusion
	r.weight, r.mean = w, mean
	r.clearZeroWeight()
}

func (r *varReducer) remove(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		r.nonFinite--
		v = 0
	}
	w := r.weight - 1
	delta := v - r.mean
	mean := r.mean - delta/w
	r.dp -= float64((v - mean) * delta)
	r.weight, r.mean = w, mean
	r.clearZeroWeight()
}

func (r *varReducer) clearZeroWeight() {
	if r.weight == 0 {
		r.mean, r.dp = 0, 0
	}
}

func (r *varReducer) result(count int) float64 {
	v, _ := r.resultOK(count)
	return v
}

func (r *varReducer) resultOK(int) (float64, bool) {
	if r.weight <= float64(r.ddof) {
		return 0, false
	}
	v := r.dp / (r.weight - float64(r.ddof))
	if v < 0 {
		v = 0
	}
	if r.nonFinite > 0 {
		v = math.NaN()
	}
	return v, true
}

type stdReducer struct{ v varReducer }

func (r *stdReducer) reset()           { r.v.reset() }
func (r *stdReducer) add(v float64)    { r.v.add(v) }
func (r *stdReducer) remove(v float64) { r.v.remove(v) }
func (r *stdReducer) result(count int) float64 {
	v, _ := r.resultOK(count)
	return v
}

func (r *stdReducer) resultOK(count int) (float64, bool) {
	v, ok := r.v.resultOK(count)
	return math.Sqrt(v), ok
}

// nullableReducer is a rollingReducer whose result can be null (a
// variance over no more values than ddof).
type nullableReducer interface {
	resultOK(count int) (float64, bool)
}

func rollingMeanReducer() rollingReducer { return &meanReducer{} }
func rollingStdReducer() rollingReducer  { return &stdReducer{v: varReducer{ddof: 1}} }
func rollingVarReducer() rollingReducer  { return &varReducer{ddof: 1} }

// rollingReduceFused is the specialised SUM driver: O(1) slide.
func rollingReduceFused(
	out []float64, validBits []byte, n, w, mp int,
	get func(i int) (float64, bool),
	_ rollingSumReducerStateless,
) int {
	var total float64
	count := 0
	nulls := 0
	for i := range n {
		// Add the incoming row.
		if v, ok := get(i); ok {
			total += v
			count++
		}
		// Evict the row falling off the window's left edge.
		if i >= w {
			if v, ok := get(i - w); ok {
				total -= v
				count--
			}
		}
		if count >= mp {
			out[i] = total
			setValidBit(validBits, i)
		} else {
			nulls++
		}
	}
	return nulls
}

// rollingReduceFusedWithReducer is the general driver using the
// rollingReducer interface. Slower than the sum/min/max
// specialisations but handles mean/std/var.
func rollingReduceFusedWithReducer(
	out []float64, validBits []byte, n, w, mp int,
	get func(i int) (float64, bool),
	makeReducer func() rollingReducer,
) int {
	r := makeReducer()
	nr, nullable := r.(nullableReducer)
	count := 0
	nulls := 0
	for i := range n {
		// Remove the value leaving the window before adding the new
		// one, as polars does: the order shows in overflow results.
		if i >= w {
			if v, ok := get(i - w); ok {
				r.remove(v)
				count--
			}
		}
		if v, ok := get(i); ok {
			r.add(v)
			count++
		}
		if count < mp {
			nulls++
			continue
		}
		if nullable {
			v, ok := nr.resultOK(count)
			if !ok {
				nulls++
				continue
			}
			out[i] = v
		} else {
			out[i] = r.result(count)
		}
		setValidBit(validBits, i)
	}
	return nulls
}

// setValidBit is a helper since bitutil.SetBit lives in an arrow
// package; duplicating the 2-liner avoids a new import per call site.
func setValidBit(b []byte, i int) {
	b[i>>3] |= 1 << (i & 7)
}
