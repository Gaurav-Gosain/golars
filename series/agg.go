package series

import (
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Sum returns the sum of non-null elements as a float64 (for numeric
// dtypes) or errors on non-numeric dtypes. Polars returns the sum as
// the same dtype for integers; we always widen to float64 for
// simplicity and overflow safety. Empty or all-null input returns 0.
//
// For bool, Sum returns the count of true values.
func (s *Series) Sum() (float64, error) {
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		var sum int64
		if a.NullN() == 0 {
			for _, v := range raw {
				sum += v
			}
		} else {
			for i, v := range raw {
				if a.IsValid(i) {
					sum += v
				}
			}
		}
		return float64(sum), nil
	case *array.Int32:
		raw := a.Int32Values()
		var sum int64
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				sum += int64(v)
			}
		}
		return float64(sum), nil
	case *array.Float64:
		raw := a.Float64Values()
		var sum float64
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				sum += v
			}
		}
		return sum, nil
	case *array.Float32:
		raw := a.Float32Values()
		var sum float64
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				sum += float64(v)
			}
		}
		return sum, nil
	case *array.Boolean:
		var count float64
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) && a.Value(i) {
				count++
			}
		}
		return count, nil
	}
	return 0, fmt.Errorf("series: Sum unsupported for dtype %s", s.DType())
}

// Mean returns the arithmetic mean of non-null elements. Returns
// (NaN, nil) when the series is empty or all-null.
func (s *Series) Mean() (float64, error) {
	n := s.Len() - s.NullCount()
	if n == 0 {
		return math.NaN(), nil
	}
	sum, err := s.Sum()
	if err != nil {
		return 0, err
	}
	return sum / float64(n), nil
}

// Min returns the minimum non-null value as a float64 for numeric
// dtypes. Empty or all-null input returns (NaN, nil). Float NaN is
// ignored unless all non-null values are NaN.
func (s *Series) Min() (float64, error) {
	chunk := s.Chunk(0)
	if s.Len()-s.NullCount() == 0 {
		return math.NaN(), nil
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		m := int64(math.MaxInt64)
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v < m {
				m = v
			}
		}
		return float64(m), nil
	case *array.Int32:
		raw := a.Int32Values()
		m := int32(math.MaxInt32)
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v < m {
				m = v
			}
		}
		return float64(m), nil
	case *array.Float64:
		raw := a.Float64Values()
		m, seen := math.Inf(1), false
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(v) {
				seen = true
				if v < m {
					m = v
				}
			}
		}
		return nanIfUnseen(m, seen), nil
	case *array.Float32:
		raw := a.Float32Values()
		m, seen := math.Inf(1), false
		for i, v := range raw {
			f := float64(v)
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(f) {
				seen = true
				if f < m {
					m = f
				}
			}
		}
		return nanIfUnseen(m, seen), nil
	}
	return 0, fmt.Errorf("series: Min unsupported for dtype %s", s.DType())
}

// nanIfUnseen returns NaN when every non-null value was NaN, which is
// the polars min/max result for such input.
func nanIfUnseen(m float64, seen bool) float64 {
	if !seen {
		return math.NaN()
	}
	return m
}

// Max is the symmetric counterpart of Min.
func (s *Series) Max() (float64, error) {
	chunk := s.Chunk(0)
	if s.Len()-s.NullCount() == 0 {
		return math.NaN(), nil
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		m := int64(math.MinInt64)
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v > m {
				m = v
			}
		}
		return float64(m), nil
	case *array.Int32:
		raw := a.Int32Values()
		m := int32(math.MinInt32)
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v > m {
				m = v
			}
		}
		return float64(m), nil
	case *array.Float64:
		raw := a.Float64Values()
		m, seen := math.Inf(-1), false
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(v) {
				seen = true
				if v > m {
					m = v
				}
			}
		}
		return nanIfUnseen(m, seen), nil
	case *array.Float32:
		raw := a.Float32Values()
		m, seen := math.Inf(-1), false
		for i, v := range raw {
			f := float64(v)
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(f) {
				seen = true
				if f > m {
					m = f
				}
			}
		}
		return nanIfUnseen(m, seen), nil
	}
	return 0, fmt.Errorf("series: Max unsupported for dtype %s", s.DType())
}

// Std returns the sample standard deviation (ddof=1). Use Var for the
// variance and `StdPopulation` if polars-compatible population stats
// are wanted (we follow polars' default).
func (s *Series) Std() (float64, error) {
	v, err := s.Var()
	if err != nil {
		return 0, err
	}
	return math.Sqrt(v), nil
}

// Var returns the sample variance (ddof=1). Empty or single-value
// series returns (NaN, nil), matching polars.
func (s *Series) Var() (float64, error) {
	count := s.Len() - s.NullCount()
	if count < 2 {
		return math.NaN(), nil
	}
	mean, err := s.Mean()
	if err != nil {
		return 0, err
	}
	chunk := s.Chunk(0)
	var sq float64
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				d := float64(v) - mean
				sq += d * d
			}
		}
	case *array.Float64:
		raw := a.Float64Values()
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				d := v - mean
				sq += d * d
			}
		}
	case *array.Int32:
		raw := a.Int32Values()
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				d := float64(v) - mean
				sq += d * d
			}
		}
	case *array.Float32:
		raw := a.Float32Values()
		for i, v := range raw {
			if a.NullN() == 0 || a.IsValid(i) {
				d := float64(v) - mean
				sq += d * d
			}
		}
	default:
		return 0, fmt.Errorf("series: Var unsupported for dtype %s", s.DType())
	}
	return sq / float64(count-1), nil
}

// Median returns the 50-percentile. For even-count data, polars
// averages the two middle values (linear interpolation), which we
// follow.
func (s *Series) Median() (float64, error) {
	return s.Quantile(0.5)
}

// Quantile returns the q-th quantile (0..1) using linear interpolation.
// Empty/all-null returns NaN.
func (s *Series) Quantile(q float64) (float64, error) {
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("series: Quantile q=%v out of range [0,1]", q)
	}
	chunk := s.Chunk(0)
	var vals []float64
	switch a := chunk.(type) {
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				vals = append(vals, float64(v))
			}
		}
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				vals = append(vals, v)
			}
		}
	case *array.Int32:
		for i, v := range a.Int32Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				vals = append(vals, float64(v))
			}
		}
	case *array.Float32:
		for i, v := range a.Float32Values() {
			f := float64(v)
			if a.NullN() == 0 || a.IsValid(i) {
				vals = append(vals, f)
			}
		}
	default:
		return 0, fmt.Errorf("series: Quantile unsupported for dtype %s", s.DType())
	}
	if len(vals) == 0 {
		return math.NaN(), nil
	}
	sortNaNLast(vals)
	pos := q * float64(len(vals)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return vals[lo], nil
	}
	frac := pos - float64(lo)
	return vals[lo]*(1-frac) + vals[hi]*frac, nil
}

// sortNaNLast sorts ascending with NaN after every number, the polars
// order, so a quantile that lands on a NaN is NaN like in polars
// ([1, NaN].median() is NaN, [1, NaN, 3].median() is 3).
func sortNaNLast(vals []float64) {
	n := 0
	for _, v := range vals {
		if v == v {
			vals[n] = v
			n++
		}
	}
	sort.Float64s(vals[:n])
	for i := n; i < len(vals); i++ {
		vals[i] = math.NaN()
	}
}

// Any returns true when at least one non-null element is truthy. For
// numeric inputs, truthy means non-zero. Empty/all-null returns false.
func (s *Series) Any() (bool, error) {
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) && a.Value(i) {
				return true, nil
			}
		}
		return false, nil
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if (a.NullN() == 0 || a.IsValid(i)) && v != 0 {
				return true, nil
			}
		}
		return false, nil
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if (a.NullN() == 0 || a.IsValid(i)) && v != 0 {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("series: Any unsupported for dtype %s", s.DType())
}

// All returns true when every non-null element is truthy. Empty or
// all-null returns true (the vacuous-truth convention polars uses).
func (s *Series) All() (bool, error) {
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Boolean:
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) && !a.Value(i) {
				return false, nil
			}
		}
		return true, nil
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if (a.NullN() == 0 || a.IsValid(i)) && v == 0 {
				return false, nil
			}
		}
		return true, nil
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if (a.NullN() == 0 || a.IsValid(i)) && v == 0 {
				return false, nil
			}
		}
		return true, nil
	}
	return false, fmt.Errorf("series: All unsupported for dtype %s", s.DType())
}

// Product returns the product of non-null elements as float64.
// Empty/all-null returns 1 (the identity), matching polars.
func (s *Series) Product() (float64, error) {
	chunk := s.Chunk(0)
	prod := 1.0
	switch a := chunk.(type) {
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				prod *= float64(v)
			}
		}
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				prod *= v
			}
		}
	case *array.Float32:
		for i, v := range a.Float32Values() {
			if a.NullN() == 0 || a.IsValid(i) {
				prod *= float64(v)
			}
		}
	default:
		if s.DType().IsInteger() {
			p, err := s.ProductInt64()
			return float64(p), err
		}
		return 0, fmt.Errorf("series: Product unsupported for dtype %s", s.DType())
	}
	return prod, nil
}

// ProductInt64 returns the product of the non-null values of an integer
// Series as i64, the polars result dtype for integer products. Like
// polars it wraps on overflow. Empty or all-null input returns 1.
func (s *Series) ProductInt64() (int64, error) {
	if !s.DType().IsInteger() {
		return 0, fmt.Errorf("series: ProductInt64 needs an integer dtype, got %s", s.DType())
	}
	prod := int64(1)
	for _, chunk := range s.Chunks() {
		for i := range chunk.Len() {
			if chunk.IsNull(i) {
				continue
			}
			v, ok := intValueAt(chunk, i)
			if !ok {
				return 0, fmt.Errorf("series: ProductInt64 unsupported for dtype %s", s.DType())
			}
			prod *= v
		}
	}
	return prod, nil
}

func intValueAt(a arrow.Array, i int) (int64, bool) {
	switch x := a.(type) {
	case *array.Int8:
		return int64(x.Value(i)), true
	case *array.Int16:
		return int64(x.Value(i)), true
	case *array.Int32:
		return int64(x.Value(i)), true
	case *array.Int64:
		return x.Value(i), true
	case *array.Uint8:
		return int64(x.Value(i)), true
	case *array.Uint16:
		return int64(x.Value(i)), true
	case *array.Uint32:
		return int64(x.Value(i)), true
	case *array.Uint64:
		return int64(x.Value(i)), true
	}
	return 0, false
}

// HasNulls is a convenience predicate: NullCount() > 0.
func (s *Series) HasNulls() bool { return s.NullCount() > 0 }

// IsEmpty reports whether the series has zero rows.
func (s *Series) IsEmpty() bool { return s.Len() == 0 }
