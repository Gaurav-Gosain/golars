package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Abs returns a new Series with the absolute value of every element.
// For unsigned integer dtypes this is an identity clone. For signed
// integers and floats, nulls are preserved. Mirrors polars' Series.abs().
func (s *Series) Abs(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		for i, v := range raw {
			if v < 0 {
				out[i] = -v
			} else {
				out[i] = v
			}
		}
		return FromInt64(s.Name(), out, validFromChunk(chunk), WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		for i, v := range raw {
			if v < 0 {
				out[i] = -v
			} else {
				out[i] = v
			}
		}
		return FromInt32(s.Name(), out, validFromChunk(chunk), WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		return BuildFloat64DirectNullable(s.Name(), n, cfg.alloc, func(out []float64) {
			for i, v := range raw {
				out[i] = math.Abs(v)
			}
		}, validFromChunk(chunk))
	case *array.Float32:
		raw := a.Float32Values()
		out := make([]float32, n)
		for i, v := range raw {
			out[i] = float32(math.Abs(float64(v)))
		}
		return FromFloat32(s.Name(), out, validFromChunk(chunk), WithAllocator(cfg.alloc))
	case *array.Uint32, *array.Uint64:
		// Unsigned: already non-negative.
		return s.Clone(), nil
	}
	return nil, fmt.Errorf("series: Abs unsupported for dtype %s", s.DType())
}

// Sqrt returns a new float64 Series with the element-wise square root.
// Integer inputs are promoted to float64. Negative inputs produce NaN.
func (s *Series) Sqrt(opts ...Option) (*Series, error) {
	return s.mathUnary("Sqrt", math.Sqrt, opts)
}

// Exp returns e**x element-wise as a float64 Series.
func (s *Series) Exp(opts ...Option) (*Series, error) {
	return s.mathUnary("Exp", math.Exp, opts)
}

// Log returns natural log element-wise as a float64 Series.
// Negative inputs produce NaN, zero produces -Inf.
func (s *Series) Log(opts ...Option) (*Series, error) {
	return s.mathUnary("Log", math.Log, opts)
}

// Log2 returns log₂ element-wise.
func (s *Series) Log2(opts ...Option) (*Series, error) {
	return s.mathUnary("Log2", math.Log2, opts)
}

// Log10 returns log₁₀ element-wise.
func (s *Series) Log10(opts ...Option) (*Series, error) {
	return s.mathUnary("Log10", math.Log10, opts)
}

// Sin / Cos / Tan are the obvious trigonometric wrappers.
func (s *Series) Sin(opts ...Option) (*Series, error) { return s.mathUnary("Sin", math.Sin, opts) }
func (s *Series) Cos(opts ...Option) (*Series, error) { return s.mathUnary("Cos", math.Cos, opts) }
func (s *Series) Tan(opts ...Option) (*Series, error) { return s.mathUnary("Tan", math.Tan, opts) }

// Sign returns -1, 0, or 1 per element. The output dtype matches the
// input (int64 → int64, float64 → float64). NaN input yields NaN.
func (s *Series) Sign(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	valid := validFromChunk(chunk)
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		for i, v := range raw {
			switch {
			case v < 0:
				out[i] = -1
			case v > 0:
				out[i] = 1
			}
		}
		return FromInt64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		return BuildFloat64DirectNullable(s.Name(), n, cfg.alloc, func(out []float64) {
			for i, v := range raw {
				switch {
				case math.IsNaN(v):
					out[i] = math.NaN()
				case v < 0:
					out[i] = -1
				case v > 0:
					out[i] = 1
				}
			}
		}, valid)
	}
	return nil, fmt.Errorf("series: Sign unsupported for dtype %s", s.DType())
}

// Round rounds each value to the given number of decimal places with
// ties to even, polars' default mode: 25.65 is 25.6 (25.65 * 10 is
// exactly 256.5 in f64). f32 values are rounded in f64 and cast back.
// decimals=0 rounds to integer. Integers are a no-op clone. A
// non-finite result (NaN, Inf, overflow) keeps the input value.
func (s *Series) Round(decimals int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	round := func(v float64) float64 {
		if decimals == 0 {
			return math.RoundToEven(v)
		}
		if decimals >= 326 {
			return v
		}
		scale := math.Pow10(decimals)
		r := math.RoundToEven(v*scale) / scale
		if math.IsNaN(r) || math.IsInf(r, 0) {
			return v
		}
		return r
	}
	switch a := chunk.(type) {
	case *array.Float64:
		raw := a.Float64Values()
		return BuildFloat64DirectNullable(s.Name(), n, cfg.alloc, func(out []float64) {
			for i, v := range raw {
				out[i] = round(v)
			}
		}, validFromChunk(chunk))
	case *array.Float32:
		raw := a.Float32Values()
		out := make([]float32, n)
		for i, v := range raw {
			r := float32(round(float64(v)))
			if math.IsInf(float64(r), 0) {
				r = v
			}
			out[i] = r
		}
		return FromFloat32(s.Name(), out, validFromChunk(chunk), WithAllocator(cfg.alloc))
	case *array.Int64, *array.Int32, *array.Uint32, *array.Uint64,
		*array.Int8, *array.Int16, *array.Uint8, *array.Uint16:
		return s.Clone(), nil
	}
	return nil, fmt.Errorf("series: Round unsupported for dtype %s", s.DType())
}

// Floor returns the greatest integer ≤ x, as a float Series.
func (s *Series) Floor(opts ...Option) (*Series, error) {
	return s.mathUnary("Floor", math.Floor, opts)
}

// Ceil returns the least integer ≥ x, as a float Series.
func (s *Series) Ceil(opts ...Option) (*Series, error) {
	return s.mathUnary("Ceil", math.Ceil, opts)
}

// Clip bounds each element into [lo, hi]. The bounds are interpreted
// in the Series' dtype: callers pass a typed scalar via ClipInt64 or
// ClipFloat64 when they want dtype-specific bounds without overflow.
// Nil bounds (math.Inf for lo / +math.Inf for hi) leave that side open.
func (s *Series) Clip(lo, hi float64, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Float64:
		raw := a.Float64Values()
		return BuildFloat64DirectNullable(s.Name(), n, cfg.alloc, func(out []float64) {
			for i, v := range raw {
				if v < lo {
					out[i] = lo
				} else if v > hi {
					out[i] = hi
				} else {
					out[i] = v
				}
			}
		}, validFromChunk(chunk))
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		ilo, ihi := int64(lo), int64(hi)
		if math.IsInf(lo, -1) {
			ilo = math.MinInt64
		}
		if math.IsInf(hi, 1) {
			ihi = math.MaxInt64
		}
		for i, v := range raw {
			if v < ilo {
				out[i] = ilo
			} else if v > ihi {
				out[i] = ihi
			} else {
				out[i] = v
			}
		}
		return FromInt64(s.Name(), out, validFromChunk(chunk), WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: Clip unsupported for dtype %s", s.DType())
}

// Pow raises each element to exponent. The result is float (f32 stays
// f32, other numeric inputs become f64) and nulls are kept.
func (s *Series) Pow(exponent float64, opts ...Option) (*Series, error) {
	return s.floatUnary("Pow", func(v float64) float64 { return math.Pow(v, exponent) }, opts)
}

// mathUnary applies fn to each element. Integer inputs are promoted to
// float64, f32 stays f32 and nulls are preserved, as in polars. NaN
// handling is fn's responsibility (stdlib math fns propagate NaN).
func (s *Series) mathUnary(name string, fn func(float64) float64, opts []Option) (*Series, error) {
	return s.floatUnary(name, fn, opts)
}

// validFromChunk extracts a []bool validity slice from a chunk, or
// nil when the chunk has no nulls. Used by math ops that preserve
// the input's null pattern.
func validFromChunk(a interface {
	Len() int
	NullN() int
	IsValid(int) bool
}) []bool {
	if a.NullN() == 0 {
		return nil
	}
	valid := make([]bool, a.Len())
	for i := range valid {
		valid[i] = a.IsValid(i)
	}
	return valid
}
