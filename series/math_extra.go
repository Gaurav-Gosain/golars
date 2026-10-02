package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Cbrt returns the cube root element-wise.
func (s *Series) Cbrt(opts ...Option) (*Series, error) {
	return s.mathUnary("Cbrt", math.Cbrt, opts)
}

// Log1p returns log(1+x) element-wise (more accurate than Log(1+x)
// near zero).
func (s *Series) Log1p(opts ...Option) (*Series, error) {
	return s.mathUnary("Log1p", math.Log1p, opts)
}

// Expm1 returns e**x - 1 element-wise (more accurate than Exp(x)-1
// near zero).
func (s *Series) Expm1(opts ...Option) (*Series, error) {
	return s.mathUnary("Expm1", math.Expm1, opts)
}

// Radians converts degrees to radians.
func (s *Series) Radians(opts ...Option) (*Series, error) {
	return s.mathUnary("Radians", func(x float64) float64 { return x * math.Pi / 180.0 }, opts)
}

// Degrees converts radians to degrees.
func (s *Series) Degrees(opts ...Option) (*Series, error) {
	return s.mathUnary("Degrees", func(x float64) float64 { return x * 180.0 / math.Pi }, opts)
}

// Arccos returns arccos element-wise (inverse cosine).
func (s *Series) Arccos(opts ...Option) (*Series, error) {
	return s.mathUnary("Arccos", math.Acos, opts)
}

// Arcsin returns arcsin element-wise.
func (s *Series) Arcsin(opts ...Option) (*Series, error) {
	return s.mathUnary("Arcsin", math.Asin, opts)
}

// Arctan returns arctan element-wise.
func (s *Series) Arctan(opts ...Option) (*Series, error) {
	return s.mathUnary("Arctan", math.Atan, opts)
}

// Cot returns 1/tan element-wise.
func (s *Series) Cot(opts ...Option) (*Series, error) {
	return s.mathUnary("Cot", func(x float64) float64 { return 1.0 / math.Tan(x) }, opts)
}

// Sinh / Cosh / Tanh return the hyperbolic functions element-wise.
func (s *Series) Sinh(opts ...Option) (*Series, error) { return s.mathUnary("Sinh", math.Sinh, opts) }
func (s *Series) Cosh(opts ...Option) (*Series, error) { return s.mathUnary("Cosh", math.Cosh, opts) }
func (s *Series) Tanh(opts ...Option) (*Series, error) { return s.mathUnary("Tanh", math.Tanh, opts) }

// Arcsinh / Arccosh / Arctanh return the inverse hyperbolic functions.
func (s *Series) Arcsinh(opts ...Option) (*Series, error) {
	return s.mathUnary("Arcsinh", math.Asinh, opts)
}

func (s *Series) Arccosh(opts ...Option) (*Series, error) {
	return s.mathUnary("Arccosh", math.Acosh, opts)
}

func (s *Series) Arctanh(opts ...Option) (*Series, error) {
	return s.mathUnary("Arctanh", math.Atanh, opts)
}

// Arctan2 returns atan2(s, other) element-wise. Both inputs must be
// numeric and the same length. Mirrors polars' arctan2.
func (s *Series) Arctan2(other *Series, opts ...Option) (*Series, error) {
	if s.Len() != other.Len() {
		return nil, fmt.Errorf("series: Arctan2 length mismatch (%d vs %d)", s.Len(), other.Len())
	}
	cfg := resolve(opts)
	a, err := asFloat64Values(s.Chunk(0))
	if err != nil {
		return nil, err
	}
	b, err := asFloat64Values(other.Chunk(0))
	if err != nil {
		return nil, err
	}
	n := len(a)
	valid := mergeValid(validFromChunk(s.Chunk(0)), validFromChunk(other.Chunk(0)))
	return BuildFloat64DirectNullable(s.Name(), n, cfg.alloc, func(out []float64) {
		for i := range n {
			out[i] = math.Atan2(a[i], b[i])
		}
	}, valid)
}

// asFloat64Values extracts a []float64 view/copy from a numeric chunk.
// Integer chunks are promoted. Non-numeric chunks return an error.
func asFloat64Values(arr any) ([]float64, error) {
	switch a := arr.(type) {
	case *array.Float64:
		return a.Float64Values(), nil
	case *array.Float32:
		raw := a.Float32Values()
		out := make([]float64, len(raw))
		for i, v := range raw {
			out[i] = float64(v)
		}
		return out, nil
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]float64, len(raw))
		for i, v := range raw {
			out[i] = float64(v)
		}
		return out, nil
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]float64, len(raw))
		for i, v := range raw {
			out[i] = float64(v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("series: asFloat64Values unsupported for %T", arr)
}

// mergeValid ANDs two validity slices; nil means all valid.
func mergeValid(a, b []bool) []bool {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	out := make([]bool, len(a))
	for i := range out {
		out[i] = a[i] && b[i]
	}
	return out
}
