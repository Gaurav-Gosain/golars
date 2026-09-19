package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// CumSum returns a new Series where out[i] = sum of s[0..i] (inclusive),
// skipping nulls as if they were zero. Mirrors polars'
// Series.cum_sum(). If the source has any nulls, the output at those
// positions is also null: a null cannot be "summed into" a running
// total without a reset rule, and polars preserves nulls here.
//
// Only numeric dtypes (int64, int32, float64) are supported; other
// dtypes return an error.
func (s *Series) CumSum(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		if a.NullN() == 0 {
			return BuildInt64Direct(s.Name(), n, cfg.alloc, func(out []int64) {
				var acc int64
				for i, v := range raw {
					acc += v
					out[i] = acc
				}
			})
		}
		return BuildInt64DirectFused(s.Name(), n, cfg.alloc, func(out []int64, validBits []byte) int {
			var acc int64
			nulls := 0
			for i := range n {
				if a.IsValid(i) {
					acc += raw[i]
					out[i] = acc
					validBits[i>>3] |= 1 << uint(i&7)
				} else {
					nulls++
				}
			}
			return nulls
		})
	case *array.Float64:
		raw := a.Float64Values()
		if a.NullN() == 0 {
			return BuildFloat64Direct(s.Name(), n, cfg.alloc, func(out []float64) {
				var acc float64
				for i, v := range raw {
					acc += v
					out[i] = acc
				}
			})
		}
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			var acc float64
			nulls := 0
			for i := range n {
				if a.IsValid(i) {
					acc += raw[i]
					out[i] = acc
					validBits[i>>3] |= 1 << uint(i&7)
				} else {
					nulls++
				}
			}
			return nulls
		})
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		var acc int32
		if a.NullN() == 0 {
			for i, v := range raw {
				acc += v
				out[i] = acc
			}
			return FromInt32(s.Name(), out, nil, WithAllocator(cfg.alloc))
		}
		return BuildInt32DirectFused(s.Name(), n, cfg.alloc, func(out []int32, validBits []byte) int {
			nulls := 0
			for i := range n {
				if a.IsValid(i) {
					acc += raw[i]
					out[i] = acc
					validBits[i>>3] |= 1 << uint(i&7)
				} else {
					nulls++
				}
			}
			return nulls
		})
	}
	return nil, fmt.Errorf("series: CumSum unsupported for dtype %s", s.DType())
}

// Diff returns the elementwise difference s[i] - s[i-periods]. The
// first periods positions are null. Negative periods diff against a
// future row (tail becomes null). Mirrors polars Series.diff().
//
// Only numeric dtypes are supported.
func (s *Series) Diff(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	if periods == 0 {
		return nil, fmt.Errorf("series: Diff periods must be non-zero")
	}
	if periods >= n || -periods >= n {
		return nullSeries(s.Name(), s.DType(), n, cfg.alloc)
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		return BuildInt64DirectFused(s.Name(), n, cfg.alloc, func(out []int64, validBits []byte) int {
			nulls := 0
			if periods > 0 {
				nulls = periods
				for i := periods; i < n; i++ {
					if a.IsValid(i) && a.IsValid(i-periods) {
						out[i] = raw[i] - raw[i-periods]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			} else {
				off := -periods
				nulls = off
				for i := 0; i < n-off; i++ {
					if a.IsValid(i) && a.IsValid(i+off) {
						out[i] = raw[i] - raw[i+off]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			}
			return nulls
		})
	case *array.Float64:
		raw := a.Float64Values()
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			nulls := 0
			if periods > 0 {
				nulls = periods
				for i := periods; i < n; i++ {
					if a.IsValid(i) && a.IsValid(i-periods) {
						out[i] = raw[i] - raw[i-periods]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			} else {
				off := -periods
				nulls = off
				for i := 0; i < n-off; i++ {
					if a.IsValid(i) && a.IsValid(i+off) {
						out[i] = raw[i] - raw[i+off]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			}
			return nulls
		})
	case *array.Int32:
		raw := a.Int32Values()
		return BuildInt32DirectFused(s.Name(), n, cfg.alloc, func(out []int32, validBits []byte) int {
			nulls := 0
			if periods > 0 {
				nulls = periods
				for i := periods; i < n; i++ {
					if a.IsValid(i) && a.IsValid(i-periods) {
						out[i] = raw[i] - raw[i-periods]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			} else {
				off := -periods
				nulls = off
				for i := 0; i < n-off; i++ {
					if a.IsValid(i) && a.IsValid(i+off) {
						out[i] = raw[i] - raw[i+off]
						validBits[i>>3] |= 1 << uint(i&7)
					} else {
						nulls++
					}
				}
			}
			return nulls
		})
	}
	return nil, fmt.Errorf("series: Diff unsupported for dtype %s", s.DType())
}
