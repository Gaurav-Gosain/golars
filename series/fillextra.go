package series

import (
	"fmt"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)

// FillNan replaces every NaN in a float Series with value. Nulls are
// preserved. Mirrors polars Series.fill_nan(value).
//
// Non-float inputs are cloned unchanged: NaN is a float-only value and
// applying fill_nan to an integer column is a no-op in polars.
func (s *Series) FillNan(value float64, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Float64:
		raw := a.Float64Values()
		// Fast path: no NaNs means we can share the existing buffer.
		if !slices.ContainsFunc(raw, math.IsNaN) {
			return s.Clone(), nil
		}
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			copy(out, raw)
			nullCount := a.NullN()
			for i := range n {
				if a.IsNull(i) {
					continue
				}
				if math.IsNaN(out[i]) {
					out[i] = value
				}
				bitutil.SetBit(validBits, i)
			}
			// If the source had nulls, their bits stay unset (init is all-invalid).
			// Otherwise every bit is valid.
			if nullCount == 0 {
				for b := range validBits {
					validBits[b] = 0xff
				}
			}
			return nullCount
		})
	case *array.Float32:
		v := float32(value)
		raw := a.Float32Values()
		out := make([]float32, n)
		copy(out, raw)
		valid := validFromChunk(chunk)
		for i := range n {
			if (valid == nil || valid[i]) && math.IsNaN(float64(out[i])) {
				out[i] = v
			}
		}
		return FromFloat32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	// Non-float dtypes: NaN is not representable. Clone unchanged.
	return s.Clone(), nil
}

// ForwardFill propagates the last non-null value forward into nulls.
// limit caps the number of consecutive fills (0 = unlimited), matching
// polars' fill_null(strategy="forward", limit=N). Mirrors
// Series.forward_fill().
//
// Leading nulls (before any value) remain null. Types without a
// meaningful "previous" reading (numeric, bool, string) are supported;
// other dtypes return an error.
func (s *Series) ForwardFill(limit int, opts ...Option) (*Series, error) {
	return s.directionalFill(limit, true, opts)
}

// BackwardFill is ForwardFill in reverse: nulls are replaced by the
// next non-null value. Trailing nulls (after the last value) remain
// null.
func (s *Series) BackwardFill(limit int, opts ...Option) (*Series, error) {
	return s.directionalFill(limit, false, opts)
}

func (s *Series) directionalFill(limit int, forward bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	if n == 0 || s.NullCount() == 0 {
		return s.Clone(), nil
	}
	switch a := chunk.(type) {
	case *array.Int64:
		src := a.Int64Values()
		if limit <= 0 {
			bits, off := a.NullBitmapBytes(), a.Data().Offset()
			return BuildInt64DirectFused(s.Name(), n, mempool.Pooling(cfg.alloc), func(out []int64, validBits []byte) int {
				return directionalFillNoLimit(as64(out), as64(src), bits, off, forward, validBits)
			})
		}
		return BuildInt64DirectFused(s.Name(), n, cfg.alloc, func(out []int64, validBits []byte) int {
			return directionalFillFused(out, src, a, n, limit, forward, validBits)
		})
	case *array.Float64:
		src := a.Float64Values()
		if limit <= 0 {
			bits, off := a.NullBitmapBytes(), a.Data().Offset()
			return BuildFloat64DirectFused(s.Name(), n, mempool.Pooling(cfg.alloc), func(out []float64, validBits []byte) int {
				return directionalFillNoLimit(as64(out), as64(src), bits, off, forward, validBits)
			})
		}
		return BuildFloat64DirectFused(s.Name(), n, cfg.alloc, func(out []float64, validBits []byte) int {
			return directionalFillFused(out, src, a, n, limit, forward, validBits)
		})
	case *array.Int32:
		src := a.Int32Values()
		out, valid := directionalFillPrim(src, a, n, limit, forward)
		return FromInt32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float32:
		src := a.Float32Values()
		out, valid := directionalFillPrim(src, a, n, limit, forward)
		return FromFloat32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Boolean:
		out := make([]bool, n)
		for i := range n {
			out[i] = a.Value(i)
		}
		valid := directionalFillBits(a, n, limit, forward, func(dst, src int) { out[dst] = out[src] })
		return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.String:
		out := make([]string, n)
		for i := range n {
			out[i] = a.Value(i)
		}
		valid := directionalFillBits(a, n, limit, forward, func(dst, src int) { out[dst] = out[src] })
		return FromString(s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: ForwardFill/BackwardFill unsupported for dtype %s", s.DType())
}

// directionalFillFused is the single-pass path for primitives with
// packed arrow validity. It copies src into out and replays the null
// bitmap, propagating values across runs and packing the final
// validity bitmap in one pass. Returns the final null count.
//
// Speed trick: the hot loop reads the ORIGINAL source validity bitmap
// (one byte covers 8 elements, streaming-friendly) rather than calling
// arrow IsValid per element. Output validity is written as we go.
func directionalFillFused[T any](
	out []T, src []T,
	chunk interface {
		IsValid(int) bool
		NullN() int
		NullBitmapBytes() []byte
	},
	n, limit int, forward bool, validBits []byte,
) int {
	copy(out, src)
	nulls := chunk.NullN()
	srcBits := chunk.NullBitmapBytes()
	if len(srcBits) > 0 {
		// Copy source validity into output validity. If the source
		// chunk has an offset (slice), we'd need to shift: fall back
		// to per-element IsValid in that uncommon case.
		if chunkOffsetIsZero(chunk) {
			copy(validBits, srcBits)
		} else {
			for i := range n {
				if chunk.IsValid(i) {
					bitutil.SetBit(validBits, i)
				}
			}
		}
	} else {
		// No nulls in source; every bit valid.
		for b := range validBits {
			validBits[b] = 0xff
		}
		return 0
	}
	if forward {
		lastValid := -1
		streak := 0
		for i := range n {
			if bitutil.BitIsSet(validBits, i) {
				lastValid = i
				streak = 0
				continue
			}
			if lastValid == -1 {
				continue
			}
			if limit > 0 && streak >= limit {
				continue
			}
			out[i] = out[lastValid]
			bitutil.SetBit(validBits, i)
			streak++
			nulls--
		}
		return nulls
	}
	nextValid := -1
	streak := 0
	for i := n - 1; i >= 0; i-- {
		if bitutil.BitIsSet(validBits, i) {
			nextValid = i
			streak = 0
			continue
		}
		if nextValid == -1 {
			continue
		}
		if limit > 0 && streak >= limit {
			continue
		}
		out[i] = out[nextValid]
		bitutil.SetBit(validBits, i)
		streak++
		nulls--
	}
	return nulls
}

// chunkOffsetIsZero returns true when the arrow chunk's internal
// offset is zero: for a zero-offset chunk the packed validity bitmap
// bytes match element positions directly.
func chunkOffsetIsZero(chunk any) bool {
	type offsetAware interface {
		Data() interface{ Offset() int }
	}
	if oa, ok := chunk.(offsetAware); ok {
		return oa.Data().Offset() == 0
	}
	return true
}

// directionalFillPrim fills nulls in src in-place (via out copy). On
// return, valid[i] is true iff out[i] is filled (original or propagated).
// limit=0 means unlimited; otherwise a run of more than limit nulls
// only gets its first `limit` entries filled.
func directionalFillPrim[T any](src []T, chunk interface{ IsValid(int) bool }, n, limit int, forward bool) ([]T, []bool) {
	out := make([]T, n)
	copy(out, src)
	valid := make([]bool, n)
	for i := range n {
		valid[i] = chunk.IsValid(i)
	}
	iterateFill(n, limit, forward, valid, func(dst, srcIdx int) { out[dst] = out[srcIdx] })
	return out, valid
}

// directionalFillBits is the variant used by dtypes whose value copy
// isn't an indexable primitive (string, bool already unpacked). copy
// is called with (dst, src) whenever we propagate.
func directionalFillBits(chunk interface{ IsValid(int) bool }, n, limit int, forward bool, copyFn func(dst, src int)) []bool {
	valid := make([]bool, n)
	for i := range n {
		valid[i] = chunk.IsValid(i)
	}
	iterateFill(n, limit, forward, valid, copyFn)
	return valid
}

// iterateFill walks the mask and marks fills. Direction and limit are
// honoured; `fill(dst, src)` is invoked whenever dst borrows from src.
func iterateFill(n, limit int, forward bool, valid []bool, fill func(dst, src int)) {
	if forward {
		lastValid := -1
		streak := 0
		for i := range n {
			if valid[i] {
				lastValid = i
				streak = 0
				continue
			}
			if lastValid == -1 {
				continue
			}
			if limit > 0 && streak >= limit {
				continue
			}
			fill(i, lastValid)
			valid[i] = true
			streak++
		}
		return
	}
	nextValid := -1
	streak := 0
	for i := n - 1; i >= 0; i-- {
		if valid[i] {
			nextValid = i
			streak = 0
			continue
		}
		if nextValid == -1 {
			continue
		}
		if limit > 0 && streak >= limit {
			continue
		}
		fill(i, nextValid)
		valid[i] = true
		streak++
	}
}
