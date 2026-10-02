package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
)

// DistinctMask marks the first (first=true) or last occurrence of every
// distinct value, nulls included, for any hashable dtype. Mirrors
// polars' is_first_distinct and is_last_distinct.
func (s *Series) DistinctMask(first bool, opts ...Option) (*Series, error) {
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	n := len(ids.ids)
	out := make([]bool, n)
	seen := make([]bool, ids.count())
	for k := range n {
		i := k
		if !first {
			i = n - 1 - k
		}
		id := ids.ids[i]
		if !seen[id] {
			seen[id] = true
			out[i] = true
		}
	}
	return FromBool(s.name, out, nil, opts...)
}

// OccurrenceMask marks values that occur exactly once (unique=true) or
// more than once, nulls counted as a value. Mirrors polars' is_unique
// and is_duplicated.
func (s *Series) OccurrenceMask(unique bool, opts ...Option) (*Series, error) {
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	counts := make([]int, ids.count())
	for _, id := range ids.ids {
		counts[id]++
	}
	out := make([]bool, len(ids.ids))
	for i, id := range ids.ids {
		out[i] = (counts[id] == 1) == unique
	}
	return FromBool(s.name, out, nil, opts...)
}

// PctChangeAll returns s / s.shift(periods) - 1 as floats, with booleans
// read as 0/1. Nulls on either side give null. Mirrors polars'
// pct_change.
func (s *Series) PctChangeAll(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) && arr.DataType().ID() != arrow.BOOL {
		if arr.DataType().ID() == arrow.STRING {
			return FullNull(s.name, arrow.PrimitiveTypes.Float64, arr.Len(), opts...)
		}
		return nil, fmt.Errorf("series: pct_change needs numeric input, got %s", s.DType())
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	n := len(vals)
	out := make([]float64, n)
	valid := make([]bool, n)
	for i := range n {
		j := i - periods
		if j < 0 || j >= n || arr.IsNull(i) || arr.IsNull(j) {
			continue
		}
		out[i] = vals[i]/vals[j] - 1
		valid[i] = true
	}
	return floatSeries(s.name, out, validOrNil(valid), false, cfg.alloc)
}

// Arctan2With returns atan2(s, x) element-wise with null propagation.
func (s *Series) Arctan2With(x *Series, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, b, err := twoArrays(s, x, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	y, err := floatVals(a)
	if err != nil {
		return nil, err
	}
	xv, err := floatVals(b)
	if err != nil {
		return nil, err
	}
	out := make([]float64, len(y))
	for i := range out {
		out[i] = math.Atan2(y[i], xv[i])
	}
	f32 := a.DataType().ID() == arrow.FLOAT32 && b.DataType().ID() == arrow.FLOAT32
	return floatSeries(s.name, out, andValid(validSlice(a), validSlice(b)), f32, cfg.alloc)
}

// RankWith ranks values with polars' semantics: "average" gives f64,
// "min", "max", "dense" and "ordinal" give u32; nulls stay null and NaN
// ranks above every number. descending reverses the order.
func (s *Series) RankWith(method string, descending bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if !descending {
		switch s.data.DataType().ID() {
		case arrow.INT64, arrow.FLOAT64:
			// The typed rank kernels match polars for these dtypes; only
			// the output dtype needs adjusting.
			if out, ok, err := s.rankFast(method, cfg); ok || err != nil {
				return out, err
			}
		}
	}
	order, err := s.ArgSortWith(descending, true)
	if err != nil {
		return nil, err
	}
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	c, err := orderCmp(arr)
	if err != nil {
		return nil, err
	}
	n := arr.Len()
	ranks := make([]float64, n)
	dense := 0
	for i := 0; i < n; {
		row := order[i]
		if arr.IsNull(row) {
			break
		}
		j := i + 1
		for j < n && !arr.IsNull(order[j]) && c(order[j], row) == 0 {
			j++
		}
		dense++
		for k := i; k < j; k++ {
			var r float64
			switch method {
			case "min":
				r = float64(i + 1)
			case "max":
				r = float64(j)
			case "dense":
				r = float64(dense)
			case "ordinal":
				r = float64(k + 1)
			default:
				r = float64(i+1+j) / 2
			}
			ranks[order[k]] = r
		}
		i = j
	}
	vs := validSlice(arr)
	if method == "average" || method == "" {
		return floatSeries(s.name, ranks, vs, false, cfg.alloc)
	}
	ints := make([]int, n)
	for i, r := range ranks {
		ints[i] = int(r)
	}
	return u32Series(s.name, ints, vs, cfg.alloc)
}

func (s *Series) rankFast(method string, cfg config) (*Series, bool, error) {
	var m RankMethod
	switch method {
	case "average", "":
		m = RankAverage
	case "min":
		m = RankMin
	case "max":
		m = RankMax
	case "dense":
		m = RankDense
	case "ordinal":
		m = RankOrdinal
	default:
		return nil, false, nil
	}
	r, err := s.Rank(m, WithAllocator(cfg.alloc))
	if err != nil {
		return nil, false, nil
	}
	if m == RankAverage {
		return r, true, nil
	}
	defer r.Release()
	arr, err := r.single(cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer arr.Release()
	vals := fixedValues[float64](arr)
	bits := CopyValidityBitmap(arr, cfg.alloc)
	if bits != nil {
		defer bits.Release()
	}
	out, err := buildFixedBits(s.name, arrow.PrimitiveTypes.Uint32, arr.Len(), bits, arr.NullN(), cfg.alloc, func(out []uint32) {
		for i, v := range vals {
			out[i] = uint32(v)
		}
	})
	return out, true, err
}
