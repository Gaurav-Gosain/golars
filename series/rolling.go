package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
// the first mp-1 leading rows.
func rollingSumInt64NoNull(
	name string, vals []int64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		var total int64
		nulls := 0
		warm := min(w, n)
		for i := 0; i < warm; i++ {
			total += vals[i]
			count := i + 1
			if count >= mp {
				out[i] = float64(total)
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		i := warm
		for ; i+4 <= n; i += 4 {
			d0 := vals[i+0] - vals[i+0-w]
			d1 := vals[i+1] - vals[i+1-w]
			d2 := vals[i+2] - vals[i+2-w]
			d3 := vals[i+3] - vals[i+3-w]
			total += d0
			out[i+0] = float64(total)
			total += d1
			out[i+1] = float64(total)
			total += d2
			out[i+2] = float64(total)
			total += d3
			out[i+3] = float64(total)
			validBits[(i+0)>>3] |= 1 << uint((i+0)&7)
			validBits[(i+1)>>3] |= 1 << uint((i+1)&7)
			validBits[(i+2)>>3] |= 1 << uint((i+2)&7)
			validBits[(i+3)>>3] |= 1 << uint((i+3)&7)
		}
		for ; i < n; i++ {
			total += vals[i] - vals[i-w]
			out[i] = float64(total)
			validBits[i>>3] |= 1 << uint(i&7)
		}
		return nulls
	})
}

func rollingSumFloat64NoNull(
	name string, vals []float64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		// Two-phase loop: phase 1 warms up the window (i < w), phase 2
		// slides it (i >= w). Splitting eliminates the `i >= w` branch
		// from the hot loop and lets the compiler hoist the min/count
		// check. Measured ~30% wall-time reduction on 1M rows vs the
		// old single-loop form.
		var total float64
		nulls := 0
		warm := min(w, n)
		for i := 0; i < warm; i++ {
			total += vals[i]
			count := i + 1
			if count >= mp {
				out[i] = total
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		// Phase 2: both bounds are strictly inside vals, no branches
		// on count (always >= mp once i >= w). Go auto-vectorises the
		// subtract-and-accumulate chain less well than the explicit
		// 4-way unroll below; manual unrolling exposes ILP to the
		// compiler.
		i := warm
		for ; i+4 <= n; i += 4 {
			d0 := vals[i+0] - vals[i+0-w]
			d1 := vals[i+1] - vals[i+1-w]
			d2 := vals[i+2] - vals[i+2-w]
			d3 := vals[i+3] - vals[i+3-w]
			total += d0
			out[i+0] = total
			total += d1
			out[i+1] = total
			total += d2
			out[i+2] = total
			total += d3
			out[i+3] = total
			validBits[(i+0)>>3] |= 1 << uint((i+0)&7)
			validBits[(i+1)>>3] |= 1 << uint((i+1)&7)
			validBits[(i+2)>>3] |= 1 << uint((i+2)&7)
			validBits[(i+3)>>3] |= 1 << uint((i+3)&7)
		}
		for ; i < n; i++ {
			total += vals[i] - vals[i-w]
			out[i] = total
			validBits[i>>3] |= 1 << uint(i&7)
		}
		return nulls
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
// dividing by the full window once warm.
func rollingMeanInt64NoNull(
	name string, vals []int64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		var total float64
		nulls := 0
		warm := min(w, n)
		for i := 0; i < warm; i++ {
			total += float64(vals[i])
			count := i + 1
			if count >= mp {
				out[i] = total / float64(count)
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		fw := float64(w)
		i := warm
		for ; i+4 <= n; i += 4 {
			d0 := float64(vals[i+0]) - float64(vals[i+0-w])
			d1 := float64(vals[i+1]) - float64(vals[i+1-w])
			d2 := float64(vals[i+2]) - float64(vals[i+2-w])
			d3 := float64(vals[i+3]) - float64(vals[i+3-w])
			total += d0
			out[i+0] = total / fw
			total += d1
			out[i+1] = total / fw
			total += d2
			out[i+2] = total / fw
			total += d3
			out[i+3] = total / fw
			validBits[(i+0)>>3] |= 1 << uint((i+0)&7)
			validBits[(i+1)>>3] |= 1 << uint((i+1)&7)
			validBits[(i+2)>>3] |= 1 << uint((i+2)&7)
			validBits[(i+3)>>3] |= 1 << uint((i+3)&7)
		}
		for ; i < n; i++ {
			total += float64(vals[i]) - float64(vals[i-w])
			out[i] = total / fw
			validBits[i>>3] |= 1 << uint(i&7)
		}
		return nulls
	})
}

func rollingMeanFloat64NoNull(
	name string, vals []float64, n, w, mp int, alloc memory.Allocator,
) (*Series, error) {
	return BuildFloat64DirectFused(name, n, alloc, func(out []float64, validBits []byte) int {
		if n == 0 {
			return 0
		}
		var total float64
		nulls := 0
		warm := min(w, n)
		for i := 0; i < warm; i++ {
			total += vals[i]
			count := i + 1
			if count >= mp {
				out[i] = total / float64(count)
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		fw := float64(w)
		i := warm
		for ; i+4 <= n; i += 4 {
			d0 := vals[i+0] - vals[i+0-w]
			d1 := vals[i+1] - vals[i+1-w]
			d2 := vals[i+2] - vals[i+2-w]
			d3 := vals[i+3] - vals[i+3-w]
			total += d0
			out[i+0] = total / fw
			total += d1
			out[i+1] = total / fw
			total += d2
			out[i+2] = total / fw
			total += d3
			out[i+3] = total / fw
			validBits[(i+0)>>3] |= 1 << uint((i+0)&7)
			validBits[(i+1)>>3] |= 1 << uint((i+1)&7)
			validBits[(i+2)>>3] |= 1 << uint((i+2)&7)
			validBits[(i+3)>>3] |= 1 << uint((i+3)&7)
		}
		for ; i < n; i++ {
			total += vals[i] - vals[i-w]
			out[i] = total / fw
			validBits[i>>3] |= 1 << uint(i&7)
		}
		return nulls
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
			return rollingMinMaxNoNull(s.Name(), vals, n, w, mp, isMin, cfg.alloc)
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
			return rollingMinMaxNoNull(s.Name(), vals, n, w, mp, isMin, cfg.alloc)
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
		dq := make([]int, 0, min(w, n)+1)
		nulls := 0
		for i := range n {
			if ev := i - w; len(dq) > 0 && dq[0] <= ev {
				dq = dq[1:]
			}
			vi := vals[i]
			if !isFloat || !math.IsNaN(float64(vi)) {
				if isMin {
					for len(dq) > 0 && vals[dq[len(dq)-1]] >= vi {
						dq = dq[:len(dq)-1]
					}
				} else {
					for len(dq) > 0 && vals[dq[len(dq)-1]] <= vi {
						dq = dq[:len(dq)-1]
					}
				}
				dq = append(dq, i)
			}
			if i+1 >= mp {
				oldest := i - w + 1
				if oldest < 0 {
					oldest = 0
				}
				if isFloat && math.IsNaN(float64(vals[oldest])) {
					out[i] = math.NaN()
				} else {
					out[i] = float64(vals[dq[0]])
				}
				validBits[i>>3] |= 1 << uint(i&7)
			} else {
				nulls++
			}
		}
		return nulls
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
		dq := make([]int, 0, min(w, n)+1)
		order := make([]int, 0, min(w, n)+1)
		nulls := 0
		for i := range n {
			ev := i - w
			if len(dq) > 0 && dq[0] <= ev {
				dq = dq[1:]
			}
			if len(order) > 0 && order[0] <= ev {
				order = order[1:]
			}
			if nullBits[(i+off)>>3]&(1<<uint((i+off)&7)) != 0 {
				order = append(order, i)
				vi := vals[i]
				if !isFloat || !math.IsNaN(float64(vi)) {
					if isMin {
						for len(dq) > 0 && vals[dq[len(dq)-1]] >= vi {
							dq = dq[:len(dq)-1]
						}
					} else {
						for len(dq) > 0 && vals[dq[len(dq)-1]] <= vi {
							dq = dq[:len(dq)-1]
						}
					}
					dq = append(dq, i)
				}
			}
			if len(order) >= mp {
				if oldest := order[0]; isFloat && math.IsNaN(float64(vals[oldest])) {
					out[i] = math.NaN()
				} else {
					out[i] = float64(vals[dq[0]])
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

type varReducer struct {
	sum, sumSq float64
	ddof       int
}

func (r *varReducer) reset() { r.sum, r.sumSq = 0, 0 }
func (r *varReducer) add(v float64) {
	r.sum += v
	r.sumSq += v * v
}
func (r *varReducer) remove(v float64) {
	r.sum -= v
	r.sumSq -= v * v
}
func (r *varReducer) result(count int) float64 {
	if count < r.ddof+1 {
		return math.NaN()
	}
	mean := r.sum / float64(count)
	num := r.sumSq - float64(count)*mean*mean
	return num / float64(count-r.ddof)
}

type stdReducer struct{ v varReducer }

func (r *stdReducer) reset()           { r.v.reset() }
func (r *stdReducer) add(v float64)    { r.v.add(v) }
func (r *stdReducer) remove(v float64) { r.v.remove(v) }
func (r *stdReducer) result(count int) float64 {
	vv := r.v.result(count)
	if math.IsNaN(vv) {
		return vv
	}
	return math.Sqrt(vv)
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
	count := 0
	nulls := 0
	for i := range n {
		if v, ok := get(i); ok {
			r.add(v)
			count++
		}
		if i >= w {
			if v, ok := get(i - w); ok {
				r.remove(v)
				count--
			}
		}
		if count >= mp {
			out[i] = r.result(count)
			setValidBit(validBits, i)
		} else {
			nulls++
		}
	}
	return nulls
}

// setValidBit is a helper since bitutil.SetBit lives in an arrow
// package; duplicating the 2-liner avoids a new import per call site.
func setValidBit(b []byte, i int) {
	b[i>>3] |= 1 << (i & 7)
}
