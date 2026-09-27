package series

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// cmpTotalFloat orders floats the way polars does: NaN is larger than every
// other value and equal to itself.
func cmpTotalFloat(a, b float64) int {
	an, bn := a != a, b != b
	switch {
	case an && bn:
		return 0
	case an:
		return 1
	case bn:
		return -1
	}
	return cmp.Compare(a, b)
}

// orderCmp returns a comparator over the non-null rows of arr using
// polars' total order. Callers handle nulls themselves.
func orderCmp(arr arrow.Array) (func(i, j int) int, error) {
	switch a := arr.(type) {
	case *array.Float64:
		v := a.Float64Values()
		return func(i, j int) int { return cmpTotalFloat(v[i], v[j]) }, nil
	case *array.Float32:
		v := a.Float32Values()
		return func(i, j int) int { return cmpTotalFloat(float64(v[i]), float64(v[j])) }, nil
	case *array.String:
		return func(i, j int) int { return strings.Compare(a.Value(i), a.Value(j)) }, nil
	case *array.LargeString:
		return func(i, j int) int { return strings.Compare(a.Value(i), a.Value(j)) }, nil
	case *array.Binary:
		return func(i, j int) int { return bytes.Compare(a.Value(i), a.Value(j)) }, nil
	case *array.Boolean:
		return func(i, j int) int {
			x, y := a.Value(i), a.Value(j)
			switch {
			case x == y:
				return 0
			case !x:
				return -1
			}
			return 1
		}, nil
	}
	switch arr.DataType().ID() {
	case arrow.INT8:
		return fixedCmp[int8](arr), nil
	case arrow.INT16:
		return fixedCmp[int16](arr), nil
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		return fixedCmp[int32](arr), nil
	case arrow.INT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		return fixedCmp[int64](arr), nil
	case arrow.UINT8:
		return fixedCmp[uint8](arr), nil
	case arrow.UINT16:
		return fixedCmp[uint16](arr), nil
	case arrow.UINT32:
		return fixedCmp[uint32](arr), nil
	case arrow.UINT64:
		return fixedCmp[uint64](arr), nil
	}
	return nil, fmt.Errorf("series: cannot order values of dtype %s", arr.DataType())
}

type orderedFixed interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func fixedCmp[T orderedFixed](arr arrow.Array) func(i, j int) int {
	v := fixedValues[T](arr)
	return func(i, j int) int { return cmp.Compare(v[i], v[j]) }
}

// SortKey describes one key of a multi-key sort.
type SortKey struct {
	Descending bool
	NullsLast  bool
}

// ArgSortMulti returns the stable permutation that sorts rows by the
// given key columns. Each key may be ascending or descending and place
// nulls first or last independently. All keys must share one length.
func ArgSortMulti(keys []*Series, opts []SortKey) ([]int, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("series: ArgSortMulti needs at least one key")
	}
	if len(opts) != len(keys) {
		return nil, fmt.Errorf("series: ArgSortMulti got %d options for %d keys", len(opts), len(keys))
	}
	n := keys[0].Len()
	arrs := make([]arrow.Array, len(keys))
	defer func() {
		for _, a := range arrs {
			if a != nil {
				a.Release()
			}
		}
	}()
	cmps := make([]func(i, j int) int, len(keys))
	for k, s := range keys {
		if s.Len() != n {
			return nil, fmt.Errorf("series: sort key %q has length %d, want %d", s.Name(), s.Len(), n)
		}
		a, err := s.single(nil)
		if err != nil {
			return nil, err
		}
		arrs[k] = a
		c, err := orderCmp(a)
		if err != nil {
			return nil, err
		}
		cmps[k] = c
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	// Single fast key: no nulls, ascending, int64 or float64 uses the
	// radix path (stable, NaN last).
	if len(keys) == 1 && !opts[0].Descending && arrs[0].NullN() == 0 {
		switch a := arrs[0].(type) {
		case *array.Int64:
			return argSortInt64Asc(a, a.Int64Values(), n), nil
		case *array.Float64:
			return argSortFloat64Asc(a, a.Float64Values(), n), nil
		}
	}
	anyNulls := false
	for _, a := range arrs {
		if a.NullN() > 0 {
			anyNulls = true
		}
	}
	slices.SortStableFunc(idx, func(x, y int) int {
		for k, a := range arrs {
			o := opts[k]
			if anyNulls && a.NullN() > 0 {
				xn, yn := a.IsNull(x), a.IsNull(y)
				if xn || yn {
					if xn && yn {
						continue
					}
					// Null placement is independent of the direction.
					if xn == !o.NullsLast {
						return -1
					}
					return 1
				}
			}
			c := cmps[k](x, y)
			if c != 0 {
				if o.Descending {
					return -c
				}
				return c
			}
		}
		return 0
	})
	return idx, nil
}

// ArgSortWith returns the stable sorting permutation with explicit
// direction and null placement. Mirrors polars' Series.arg_sort.
func (s *Series) ArgSortWith(descending, nullsLast bool) ([]int, error) {
	return ArgSortMulti([]*Series{s}, []SortKey{{Descending: descending, NullsLast: nullsLast}})
}

// SortWith returns the sorted Series with explicit direction and null
// placement. polars' default is ascending with nulls first.
func (s *Series) SortWith(descending, nullsLast bool, opts ...Option) (*Series, error) {
	idx, err := s.ArgSortWith(descending, nullsLast)
	if err != nil {
		return nil, err
	}
	return s.Gather(idx, opts...)
}

// idxSeries wraps int indices as a u32 Series (polars' IDX dtype).
func idxSeries(name string, idx []int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	return buildFixedValid(name, arrow.PrimitiveTypes.Uint32, len(idx), nil, cfg.alloc, func(out []uint32) {
		for i, v := range idx {
			out[i] = uint32(v)
		}
	})
}

// IdxSeries is the exported form of idxSeries for other packages: it
// wraps row indices as a u32 Series.
func IdxSeries(name string, idx []int, opts ...Option) (*Series, error) {
	return idxSeries(name, idx, opts...)
}

// ArgMaxIndex returns the index of the largest non-null value using
// polars' rules: NaN is skipped unless every non-null value is NaN, and
// ties resolve to the first row. It returns -1 for empty or all-null
// input. Every orderable dtype is supported.
func (s *Series) ArgMaxIndex() (int, error) { return s.argExtreme(true) }

// ArgMinIndex is the arg-min counterpart of ArgMaxIndex.
func (s *Series) ArgMinIndex() (int, error) { return s.argExtreme(false) }

func (s *Series) argExtreme(isMax bool) (int, error) {
	arr, err := s.single(nil)
	if err != nil {
		return -1, err
	}
	defer arr.Release()
	c, err := orderCmp(arr)
	if err != nil {
		return -1, err
	}
	isNaN := nanTester(arr)
	best := -1
	bestNaN := -1
	for i := range arr.Len() {
		if arr.IsNull(i) {
			continue
		}
		if isNaN != nil && isNaN(i) {
			if bestNaN < 0 {
				bestNaN = i
			}
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		r := c(i, best)
		if (isMax && r > 0) || (!isMax && r < 0) {
			best = i
		}
	}
	if best < 0 {
		return bestNaN, nil
	}
	return best, nil
}

// nanTester returns a NaN predicate for float arrays and nil otherwise.
func nanTester(arr arrow.Array) func(i int) bool {
	switch a := arr.(type) {
	case *array.Float64:
		v := a.Float64Values()
		return func(i int) bool { return math.IsNaN(v[i]) }
	case *array.Float32:
		v := a.Float32Values()
		return func(i int) bool { return v[i] != v[i] }
	}
	return nil
}
