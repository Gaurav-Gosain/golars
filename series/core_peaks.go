package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Peaks marks local maxima (isMax) or minima. polars compares each value
// with both neighbours, treating the missing neighbour at either end as
// zero (false for booleans), and combines the two comparisons with
// Kleene AND, so a null neighbour gives null unless the other side
// already decides false. NaN compares greater than every number.
func (s *Series) Peaks(isMax bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	dt := arr.DataType()
	if !isNumericType(dt) && dt.ID() != arrow.BOOL {
		return nil, fmt.Errorf("series: peak_max/peak_min not supported for dtype %s", s.DType())
	}
	var vals []float64
	if b, ok := arr.(*array.Boolean); ok {
		vals = make([]float64, b.Len())
		for i := range vals {
			if b.Value(i) {
				vals[i] = 1
			}
		}
	} else {
		vals, err = floatVals(arr)
		if err != nil {
			return nil, err
		}
	}
	n := len(vals)
	out := make([]bool, n)
	valid := make([]bool, n)
	// cmpSide returns (result, known) for x vs its neighbour at j.
	cmpSide := func(i, j int) (bool, bool) {
		var nb float64
		if j >= 0 && j < n {
			if arr.IsNull(j) {
				return false, false
			}
			nb = vals[j]
		}
		c := cmpTotalFloat(vals[i], nb)
		if isMax {
			return c > 0, true
		}
		return c < 0, true
	}
	for i := range n {
		if arr.IsNull(i) {
			continue
		}
		l, lk := cmpSide(i, i-1)
		r, rk := cmpSide(i, i+1)
		switch {
		case lk && !l, rk && !r:
			valid[i] = true
		case lk && rk:
			out[i], valid[i] = true, true
		}
	}
	return FromBool(s.name, out, validOrNil(valid), WithAllocator(cfg.alloc))
}
