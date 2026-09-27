package compute

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// NullPosition controls where null values sort relative to non-null values.
type NullPosition uint8

const (
	// NullsFirst places null values before all non-null values. It is the
	// zero value because polars sorts with nulls_last=False by default, in
	// both ascending and descending order.
	NullsFirst NullPosition = iota
	// NullsLast places null values after all non-null values.
	NullsLast
)

// SortOptions tune the sort behavior.
type SortOptions struct {
	Descending bool
	Nulls      NullPosition
}

// SortIndices returns the stable permutation that would sort s. The result
// is a []int of length s.Len(). Apply it via Take to materialize the sorted
// Series or to permute other columns.
func SortIndices(ctx context.Context, s *series.Series, so SortOptions, opts ...Option) ([]int, error) {
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	if sa, ok := arr.(*array.String); ok {
		return sortIndicesString(sa, so), nil
	}

	n := arr.Len()
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}

	cmp, err := singleKeyCompare(arr, so)
	if err != nil {
		return nil, err
	}
	// slices.SortStableFunc operates on the typed []int directly and avoids
	// sort.SliceStable's reflect-based Swapper, which pprof identified as
	// ~10% of GroupBy's CPU.
	slices.SortStableFunc(idx, func(a, b int) int { return cmp(a, b) })
	return idx, nil
}

// Sort returns a new Series with values arranged by a stable sort.
//
// Fast paths: when s has no nulls and a simple primitive dtype, Sort skips
// the index-sort machinery and sorts the raw []T with a typed pdqsort.
// Benchmarked at ~80x the throughput of the general path for int64.
func Sort(ctx context.Context, s *series.Series, so SortOptions, opts ...Option) (*series.Series, error) {
	if s.NullCount() == 0 {
		if out, ok, err := sortValuesFast(ctx, s, so, opts); ok {
			return out, err
		}
	}
	idx, err := SortIndices(ctx, s, so, opts...)
	if err != nil {
		return nil, err
	}
	return Take(ctx, s, idx, opts...)
}

// sortValuesFast handles the single-column, no-null case for numeric dtypes
// by sorting the raw value slice directly. Returns ok=false when the dtype
// is unsupported, so the caller can fall back to the indexed path.
func sortValuesFast(ctx context.Context, s *series.Series, so SortOptions, opts []Option) (*series.Series, bool, error) {
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer arr.Release()
	name := cfg.outName(s.Name())
	switch a := arr.(type) {
	case *array.Int32:
		src := a.Int32Values()
		out := make([]int32, len(src))
		copy(out, src)
		radixSortInt32(out)
		if so.Descending {
			slices.Reverse(out)
		}
		s, err := series.FromInt32(name, out, nil, series.WithAllocator(cfg.alloc))
		return s, true, err
	case *array.Int64:
		src := a.Int64Values()
		// poolingMem skips the mallocgc zero-init on the output buffer
		// (the radix fully overwrites it via copy+sort). At 262K this
		// saves ~70 µs of memclr per call, ~5 % of end-to-end time on
		// the SortInt64 256K bench.
		s, err := series.BuildInt64Direct(name, len(src), poolingMem(cfg.alloc), func(out []int64) {
			copy(out, src)
			radixSortInt64(out)
			if so.Descending {
				slices.Reverse(out)
			}
		})
		return s, true, err
	case *array.Uint32:
		src := a.Uint32Values()
		out := make([]uint32, len(src))
		copy(out, src)
		slices.Sort(out)
		if so.Descending {
			slices.Reverse(out)
		}
		s, err := series.FromUint32(name, out, nil, series.WithAllocator(cfg.alloc))
		return s, true, err
	case *array.Uint64:
		src := a.Uint64Values()
		out := make([]uint64, len(src))
		copy(out, src)
		radixSortUint64(out)
		if so.Descending {
			slices.Reverse(out)
		}
		s, err := series.FromUint64(name, out, nil, series.WithAllocator(cfg.alloc))
		return s, true, err
	case *array.Float32:
		out := make([]float32, a.Len())
		copy(out, a.Float32Values())
		slices.SortFunc(out, func(x, y float32) int {
			// slices.Sort for floats would use < which misorders NaN.
			// Our semantics: NaN goes last in ascending, first in descending.
			xNaN, yNaN := x != x, y != y
			if xNaN && yNaN {
				return 0
			}
			if xNaN {
				return 1
			}
			if yNaN {
				return -1
			}
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			default:
				return 0
			}
		})
		if so.Descending {
			slices.Reverse(out)
		}
		s, err := series.FromFloat32(name, out, nil, series.WithAllocator(cfg.alloc))
		return s, true, err
	case *array.Float64:
		src := a.Float64Values()
		n := len(src)
		// Three-tier dispatch:
		//   A) all finite non-negative: uint64 bit order == IEEE order, so
		//      we alias src bits as-is (zeroing any -0 sign bit) and sort
		//      as uint64. No transform.
		//   B) all finite (possibly negative), any sign mix: IEEE bit trick
		//      (flip sign bit for positives, complement for negatives) then
		//      uint64 radix, then reverse transform.
		//   C) has NaN: shrink keys to non-NaN count, write NaN tail.
		// poolingMem skips the 8 MB mallocgc zero at 1 M since we fully
		// overwrite the output during sort.
		s, err := series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
			keys := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(out))), n)
			// Fast path: the radix fuses float reinterpret + NaN/neg
			// detection into pass 0 counting. If detection fires,
			// needsIEEE is true and we fall through to the transform
			// path below: wasting only pass 0 histograms, which cost
			// ~1 µs. Saves a full read of src on the common case.
			needsIEEE := false
			if n >= parallelFloatRadixCutoff {
				needsIEEE = radixSortUint64FromFloatParallel(src, keys)
			} else if n >= 1024 {
				needsIEEE = radixSortUint64FromFloatSerial(src, keys)
			} else {
				// Tiny path: seed + detect in one small loop. Only the
				// sign bit matters for detection (see
				// serialFromFloatWithDetect for the rationale).
				var orBits uint64
				for i, v := range src {
					b := math.Float64bits(v)
					keys[i] = b
					orBits |= b
				}
				if orBits>>63 != 0 {
					needsIEEE = true
				} else {
					radixSortUint64(keys)
				}
			}

			if needsIEEE {
				// Mixed signs or NaN/Inf: full IEEE transform (skip NaN).
				j := 0
				for _, v := range src {
					if v != v {
						continue
					}
					bits := math.Float64bits(v)
					if bits>>63 == 0 {
						bits |= 1 << 63
					} else {
						bits = ^bits
					}
					keys[j] = bits
					j++
				}
				sorted := keys[:j]
				radixSortUint64(sorted)
				for i, k := range sorted {
					if k>>63 == 1 {
						k &^= 1 << 63
					} else {
						k = ^k
					}
					out[i] = math.Float64frombits(k)
				}
				for i := j; i < n; i++ {
					out[i] = math.NaN()
				}
			}
			if so.Descending {
				slices.Reverse(out)
			}
		})
		return s, true, err
	}
	_ = ctx
	return nil, false, nil
}

// SortIndicesMulti returns the stable permutation that would sort by the
// given columns with per-column options. All columns must have the same
// length as cols[0].
func SortIndicesMulti(ctx context.Context, cols []*series.Series, opts []SortOptions, kernelOpts ...Option) ([]int, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("compute: SortIndicesMulti requires at least one column")
	}
	if len(opts) != len(cols) {
		return nil, fmt.Errorf("compute: SortIndicesMulti opts length %d must match cols %d", len(opts), len(cols))
	}
	cfg := resolve(kernelOpts)

	n := cols[0].Len()
	for _, c := range cols[1:] {
		if c.Len() != n {
			return nil, fmt.Errorf("%w: sort columns differ in length", ErrLengthMismatch)
		}
	}

	// Fast path: all int64 keys, all ascending, no nulls. Use arg-radix for
	// each key in reverse order (stable: later sort preserves earlier tie
	// order), which produces the lexicographic ordering.
	if allInt64AscNoNull(cols, opts) {
		indices := make([]int, n)
		for i := range indices {
			indices[i] = i
		}
		// Sort from last key to first; stability gives lexicographic order
		// by (cols[0], cols[1], ..., cols[k-1]).
		for i := len(cols) - 1; i >= 0; i-- {
			arr, err := extractChunk(cols[i], cfg.alloc)
			if err != nil {
				return nil, err
			}
			keys := int64Values(arr)
			argRadixSortInt64(keys, indices)
			arr.Release()
		}
		return indices, nil
	}

	// Single string key: prefix radix path in sort_string.go.
	if len(cols) == 1 && cols[0].DType().ID() == arrow.STRING {
		return SortIndices(ctx, cols[0], opts[0], kernelOpts...)
	}

	arrs := make([]arrow.Array, len(cols))
	lessFuncs := make([]func(i, j int) int, len(cols))

	release := func() {
		for _, a := range arrs {
			if a != nil {
				a.Release()
			}
		}
	}

	for k, c := range cols {
		a, err := extractChunk(c, cfg.alloc)
		if err != nil {
			release()
			return nil, err
		}
		arrs[k] = a
		cmp, err := singleKeyCompare(a, opts[k])
		if err != nil {
			release()
			return nil, err
		}
		lessFuncs[k] = cmp
	}
	defer release()

	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		for _, cmp := range lessFuncs {
			switch cmp(a, b) {
			case -1:
				return -1
			case 1:
				return 1
			}
		}
		return 0
	})
	return idx, nil
}

// singleKeyCompare returns a three-way comparator (-1, 0, 1) for index pairs
// into arr accounting for nulls and direction.
func singleKeyCompare(arr arrow.Array, so SortOptions) (func(i, j int) int, error) {
	nullCmp := func(aValid, bValid bool) (int, bool) {
		if aValid && bValid {
			return 0, false
		}
		if !aValid && !bValid {
			return 0, true
		}
		// one is null
		if so.Nulls == NullsFirst {
			if !aValid {
				return -1, true
			}
			return 1, true
		}
		// NullsLast
		if !aValid {
			return 1, true
		}
		return -1, true
	}

	flip := func(c int) int {
		if so.Descending {
			return -c
		}
		return c
	}

	switch a := arr.(type) {
	case *array.Int8:
		vals := a.Int8Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Int16:
		vals := a.Int16Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Int32:
		vals := a.Int32Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Int64:
		vals := a.Int64Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Uint8:
		vals := a.Uint8Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Uint16:
		vals := a.Uint16Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Uint32:
		vals := a.Uint32Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Uint64:
		vals := a.Uint64Values()
		return buildCmp(vals, a, nullCmp, flip), nil
	case *array.Float32:
		vals := a.Float32Values()
		return buildFloatCmp(vals, a, nullCmp, flip), nil
	case *array.Float64:
		vals := a.Float64Values()
		return buildFloatCmp(vals, a, nullCmp, flip), nil
	case *array.Boolean:
		return func(i, j int) int {
			ivalid, jvalid := a.IsValid(i), a.IsValid(j)
			if r, ok := nullCmp(ivalid, jvalid); ok {
				return r
			}
			av, bv := a.Value(i), a.Value(j)
			if av == bv {
				return 0
			}
			if !av {
				return flip(-1)
			}
			return flip(1)
		}, nil
	case *array.String:
		return func(i, j int) int {
			ivalid, jvalid := a.IsValid(i), a.IsValid(j)
			if r, ok := nullCmp(ivalid, jvalid); ok {
				return r
			}
			av, bv := a.Value(i), a.Value(j)
			switch {
			case av < bv:
				return flip(-1)
			case av > bv:
				return flip(1)
			default:
				return 0
			}
		}, nil
	}
	if vals := temporalSortKeys(arr); vals != nil {
		return buildCmp(vals, arr, nullCmp, flip), nil
	}
	if d, ok := arr.(*array.Dictionary); ok {
		if ranks := lexicalDictRanks(d); ranks != nil {
			// polars 1.39: Categorical sorts lexically by category
			// string, Enum by category order.
			return buildCmp(ranks, arr, nullCmp, flip), nil
		}
	}
	return nil, fmt.Errorf("%w: sort on %s", ErrUnsupportedDType, arr.DataType())
}

// buildCmp constructs an ordered-integer comparator.
func buildCmp[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](
	vals []T,
	a arrow.Array,
	nullCmp func(bool, bool) (int, bool),
	flip func(int) int,
) func(i, j int) int {
	return func(i, j int) int {
		ivalid, jvalid := a.IsValid(i), a.IsValid(j)
		if r, ok := nullCmp(ivalid, jvalid); ok {
			return r
		}
		av, bv := vals[i], vals[j]
		switch {
		case av < bv:
			return flip(-1)
		case av > bv:
			return flip(1)
		default:
			return 0
		}
	}
}

// buildFloatCmp compares float values. NaN sorts after all non-NaN values per
// polars' default ordering.
func buildFloatCmp[T float32 | float64](
	vals []T,
	a arrow.Array,
	nullCmp func(bool, bool) (int, bool),
	flip func(int) int,
) func(i, j int) int {
	return func(i, j int) int {
		ivalid, jvalid := a.IsValid(i), a.IsValid(j)
		if r, ok := nullCmp(ivalid, jvalid); ok {
			return r
		}
		av, bv := vals[i], vals[j]
		aNaN := av != av
		bNaN := bv != bv
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			return flip(1)
		}
		if bNaN {
			return flip(-1)
		}
		switch {
		case av < bv:
			return flip(-1)
		case av > bv:
			return flip(1)
		default:
			return 0
		}
	}
}

// lexicalDictRanks maps each row of a string dictionary array to a sort
// rank: the lexical rank of its category for Categorical, the code for
// Enum. It returns nil for non-string dictionaries.
func lexicalDictRanks(d *array.Dictionary) []int64 {
	dict, ok := d.Dictionary().(*array.String)
	if !ok {
		return nil
	}
	if d.DataType().(*arrow.DictionaryType).Ordered {
		// Enum: the codes follow the declared category order.
		out := make([]int64, d.Len())
		for i := range out {
			if d.IsValid(i) {
				out[i] = int64(d.GetValueIndex(i))
			}
		}
		return out
	}
	order := make([]int, dict.Len())
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return strings.Compare(dict.Value(a), dict.Value(b))
	})
	rankOf := make([]int64, dict.Len())
	for r, code := range order {
		rankOf[code] = int64(r)
	}
	out := make([]int64, d.Len())
	for i := range out {
		if d.IsValid(i) {
			out[i] = rankOf[d.GetValueIndex(i)]
		}
	}
	return out
}
