package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// valueIDs assigns every row the id of its distinct value, numbered in
// order of first appearance. Nulls share one id; NaNs share one id and
// -0.0 equals 0.0, matching polars' hashing rules. first[id] is the
// first row holding that value.
type valueIDs struct {
	ids    []int32
	first  []int
	nullID int32 // -1 when there are no nulls
}

func (v valueIDs) count() int { return len(v.first) }

func denseIDs(arr arrow.Array) (valueIDs, error) {
	n := arr.Len()
	out := valueIDs{ids: make([]int32, n), nullID: -1}
	nullAt := func(i int) bool { return arr.NullN() > 0 && arr.IsNull(i) }
	assignNull := func(i int) {
		if out.nullID < 0 {
			out.nullID = int32(len(out.first))
			out.first = append(out.first, i)
		}
		out.ids[i] = out.nullID
	}
	switch a := arr.(type) {
	case *array.String:
		m := make(map[string]int32)
		for i := range n {
			if nullAt(i) {
				assignNull(i)
				continue
			}
			k := a.Value(i)
			id, ok := m[k]
			if !ok {
				id = int32(len(out.first))
				m[k] = id
				out.first = append(out.first, i)
			}
			out.ids[i] = id
		}
		return out, nil
	case *array.Boolean:
		ids := [2]int32{-1, -1}
		for i := range n {
			if nullAt(i) {
				assignNull(i)
				continue
			}
			k := 0
			if a.Value(i) {
				k = 1
			}
			if ids[k] < 0 {
				ids[k] = int32(len(out.first))
				out.first = append(out.first, i)
			}
			out.ids[i] = ids[k]
		}
		return out, nil
	case *array.Float64:
		v := a.Float64Values()
		bits := make([]uint64, n)
		for i, x := range v {
			bits[i] = math.Float64bits(canonFloat64(x))
		}
		denseFixed(bits, n, nullAt, assignNull, &out)
		return out, nil
	case *array.Float32:
		v := a.Float32Values()
		bits := make([]uint64, n)
		for i, x := range v {
			bits[i] = math.Float64bits(canonFloat64(float64(x)))
		}
		denseFixed(bits, n, nullAt, assignNull, &out)
		return out, nil
	}
	switch fixedWidthBytes(arr.DataType()) {
	case 1:
		denseFixed(fixedValues[uint8](arr), n, nullAt, assignNull, &out)
	case 2:
		denseFixed(fixedValues[uint16](arr), n, nullAt, assignNull, &out)
	case 4:
		denseFixed(fixedValues[uint32](arr), n, nullAt, assignNull, &out)
	case 8:
		denseFixed(fixedValues[uint64](arr), n, nullAt, assignNull, &out)
	default:
		if arr.DataType().ID() == arrow.NULL {
			for i := range n {
				assignNull(i)
			}
			return out, nil
		}
		return out, fmt.Errorf("series: cannot hash values of dtype %s", arr.DataType())
	}
	return out, nil
}

func canonFloat64(x float64) float64 {
	if x != x {
		return math.NaN()
	}
	if x == 0 {
		return 0
	}
	return x
}

// denseFixed maps fixed-width keys into dense ids.
func denseFixed[T comparable](v []T, n int, nullAt func(int) bool, assignNull func(int), out *valueIDs) {
	m := make(map[T]int32)
	for i := range n {
		if nullAt(i) {
			assignNull(i)
			continue
		}
		k := v[i]
		id, ok := m[k]
		if !ok {
			id = int32(len(out.first))
			m[k] = id
			out.first = append(out.first, i)
		}
		out.ids[i] = id
	}
}

// denseIDsOf is denseIDs for a Series.
func (s *Series) denseIDs() (valueIDs, error) {
	arr, err := s.single(nil)
	if err != nil {
		return valueIDs{}, err
	}
	defer arr.Release()
	return denseIDs(arr)
}
