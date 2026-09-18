package series

import (
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// ArgMin returns the index of the smallest non-null value. For floats
// NaN is ignored. Empty or all-null input returns (-1, nil). Mirrors
// polars' Series.arg_min.
func (s *Series) ArgMin() (int, error) {
	chunk := s.Chunk(0)
	if s.Len()-s.NullCount() == 0 {
		return -1, nil
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		best, bestIdx := int64(math.MaxInt64), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v < best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	case *array.Float64:
		raw := a.Float64Values()
		best, bestIdx := math.Inf(1), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(v) && v < best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	case *array.Int32:
		raw := a.Int32Values()
		best, bestIdx := int32(math.MaxInt32), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v < best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	}
	return -1, fmt.Errorf("series: ArgMin unsupported for dtype %s", s.DType())
}

// ArgMax returns the index of the largest non-null value.
func (s *Series) ArgMax() (int, error) {
	chunk := s.Chunk(0)
	if s.Len()-s.NullCount() == 0 {
		return -1, nil
	}
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		best, bestIdx := int64(math.MinInt64), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v > best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	case *array.Float64:
		raw := a.Float64Values()
		best, bestIdx := math.Inf(-1), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && !math.IsNaN(v) && v > best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	case *array.Int32:
		raw := a.Int32Values()
		best, bestIdx := int32(math.MinInt32), -1
		for i, v := range raw {
			if (a.NullN() == 0 || a.IsValid(i)) && v > best {
				best, bestIdx = v, i
			}
		}
		return bestIdx, nil
	}
	return -1, fmt.Errorf("series: ArgMax unsupported for dtype %s", s.DType())
}

// ArgSort returns the permutation that would sort s ascending. Nulls
// are placed at the end. Mirrors polars' Series.arg_sort default.
func (s *Series) ArgSort() ([]int, error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		return argSortInt64Asc(a, a.Int64Values(), n), nil
	case *array.Float64:
		return argSortFloat64Asc(a, a.Float64Values(), n), nil
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	switch a := chunk.(type) {
	case *array.String:
		sort.SliceStable(idx, func(i, j int) bool {
			vi, vj := idx[i], idx[j]
			vInull := a.NullN() > 0 && !a.IsValid(vi)
			vJnull := a.NullN() > 0 && !a.IsValid(vj)
			if vInull && vJnull {
				return false
			}
			if vInull {
				return false
			}
			if vJnull {
				return true
			}
			return a.Value(vi) < a.Value(vj)
		})
		return idx, nil
	}
	return nil, fmt.Errorf("series: ArgSort unsupported for dtype %s", s.DType())
}
