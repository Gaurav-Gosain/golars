package series

import (
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Reductions that return a length-1 Series so the result keeps the
// polars output dtype. Use Item() to read the Go value.

// NUniqueAll counts distinct values with null counted as one value,
// which is polars' n_unique rule.
func (s *Series) NUniqueAll() (int, error) {
	ids, err := s.denseIDs()
	if err != nil {
		return 0, err
	}
	return ids.count(), nil
}

// UniqueCounts returns the number of occurrences (u32) of each distinct
// value in order of first appearance. Nulls count as a value. Mirrors
// polars' unique_counts.
func (s *Series) UniqueCounts(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	counts := make([]int, ids.count())
	for _, id := range ids.ids {
		counts[id]++
	}
	return u32Series(s.name, counts, nil, cfg.alloc)
}

// ArgUniqueIdx returns the first row index of every distinct value (null
// counted as a value) as a u32 Series. Mirrors polars' arg_unique.
func (s *Series) ArgUniqueIdx(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	return u32Series(s.name, ids.first, nil, cfg.alloc)
}

// ArgTrueIdx returns the u32 positions where a boolean Series is true.
func (s *Series) ArgTrueIdx(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	a, ok := arr.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("series: arg_true requires bool input, got %s", arr.DataType())
	}
	var idx []int
	for i := range a.Len() {
		if a.IsValid(i) && a.Value(i) {
			idx = append(idx, i)
		}
	}
	return u32Series(s.name, idx, nil, cfg.alloc)
}

// ModeAll returns every value that ties for the highest count, nulls
// included, in order of first appearance. polars leaves the order
// unspecified.
func (s *Series) ModeAll(opts ...Option) (*Series, error) {
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	counts := make([]int, ids.count())
	best := 0
	for _, id := range ids.ids {
		counts[id]++
		best = max(best, counts[id])
	}
	var pick []int
	for id, c := range counts {
		if c == best {
			pick = append(pick, ids.first[id])
		}
	}
	return s.Gather(pick, opts...)
}

// MaxBy returns the value of s at the row where by is largest (first
// row on ties, NaN skipped unless all NaN). Mirrors polars' max_by.
func (s *Series) MaxBy(by *Series, opts ...Option) (*Series, error) {
	return s.extremeBy(by, true, opts)
}

// MinBy returns the value of s at the row where by is smallest.
func (s *Series) MinBy(by *Series, opts ...Option) (*Series, error) {
	return s.extremeBy(by, false, opts)
}

func (s *Series) extremeBy(by *Series, isMax bool, opts []Option) (*Series, error) {
	if by.Len() != s.Len() {
		return nil, fmt.Errorf("series: max_by/min_by length mismatch %d vs %d", s.Len(), by.Len())
	}
	idx, err := by.argExtreme(isMax)
	if err != nil {
		return nil, err
	}
	return s.Gather([]int{idx}, opts...)
}

// NanMax is max where any NaN makes the result NaN. Non-float inputs
// behave like max. The result keeps the input dtype.
func (s *Series) NanMax(opts ...Option) (*Series, error) { return s.nanExtreme(true, opts) }

// NanMin is min where any NaN makes the result NaN.
func (s *Series) NanMin(opts ...Option) (*Series, error) { return s.nanExtreme(false, opts) }

func (s *Series) nanExtreme(isMax bool, opts []Option) (*Series, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if isNaN := nanTester(arr); isNaN != nil {
		for i := range arr.Len() {
			if arr.IsValid(i) && isNaN(i) {
				return s.Gather([]int{i}, opts...)
			}
		}
	}
	idx, err := s.argExtreme(isMax)
	if err != nil {
		return nil, err
	}
	return s.Gather([]int{idx}, opts...)
}

// MaxValue returns the maximum (NaN ignored unless all NaN) keeping the
// input dtype. Works for every orderable dtype including strings.
func (s *Series) MaxValue(opts ...Option) (*Series, error) {
	idx, err := s.argExtreme(true)
	if err != nil {
		return nil, err
	}
	return s.Gather([]int{idx}, opts...)
}

// MinValue is the minimum counterpart of MaxValue.
func (s *Series) MinValue(opts ...Option) (*Series, error) {
	idx, err := s.argExtreme(false)
	if err != nil {
		return nil, err
	}
	return s.Gather([]int{idx}, opts...)
}

// Implode wraps the whole Series into a single list value.
func (s *Series) Implode(opts ...Option) (*Series, error) {
	return ListFromInt32Offsets(s.name, s, []int32{0, int32(s.Len())}, nil, opts...)
}

// LowerBound returns the smallest representable value of the dtype as
// a length-1 Series. Mirrors polars' lower_bound.
func (s *Series) LowerBound(opts ...Option) (*Series, error) { return s.bound(false, opts) }

// UpperBound returns the largest representable value of the dtype.
func (s *Series) UpperBound(opts ...Option) (*Series, error) { return s.bound(true, opts) }

func (s *Series) bound(upper bool, opts []Option) (*Series, error) {
	dt := s.data.DataType()
	var v any
	switch dt.ID() {
	case arrow.INT8:
		v = pick(upper, int64(math.MaxInt8), int64(math.MinInt8))
	case arrow.INT16:
		v = pick(upper, int64(math.MaxInt16), int64(math.MinInt16))
	case arrow.INT32:
		v = pick(upper, int64(math.MaxInt32), int64(math.MinInt32))
	case arrow.INT64:
		v = pick(upper, int64(math.MaxInt64), int64(math.MinInt64))
	case arrow.UINT8:
		v = pick(upper, uint64(math.MaxUint8), uint64(0))
	case arrow.UINT16:
		v = pick(upper, uint64(math.MaxUint16), uint64(0))
	case arrow.UINT32:
		v = pick(upper, uint64(math.MaxUint32), uint64(0))
	case arrow.UINT64:
		v = pick(upper, uint64(math.MaxUint64), uint64(0))
	case arrow.FLOAT32, arrow.FLOAT64:
		v = pick(upper, math.Inf(1), math.Inf(-1))
	default:
		return nil, fmt.Errorf("series: cannot determine bound for dtype %s", s.DType())
	}
	return FromValues(s.name, dt, []any{v}, opts...)
}

func pick[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// QuantileMethod values accepted by QuantileSeries.
const (
	QuantileNearest  = "nearest"
	QuantileLinear   = "linear"
	QuantileLower    = "lower"
	QuantileHigher   = "higher"
	QuantileMidpoint = "midpoint"
	QuantileEquiprob = "equiprobable"
)

// sortedFloats returns the non-null values sorted ascending with NaN
// last (polars' total order).
func sortedFloats(arr arrow.Array) ([]float64, error) {
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	if arr.NullN() > 0 {
		k := 0
		for i, v := range vals {
			if arr.IsValid(i) {
				vals[k] = v
				k++
			}
		}
		vals = vals[:k]
	}
	// Move NaNs to the end, then use the typed sort on the rest; this is
	// several times faster than a comparator sort.
	k := 0
	for _, v := range vals {
		if v == v {
			vals[k] = v
			k++
		}
	}
	for i := k; i < len(vals); i++ {
		vals[i] = math.NaN()
	}
	slices.Sort(vals[:k])
	return vals, nil
}

// quantileSorted computes the q-th quantile of ascending data.
func quantileSorted(vals []float64, q float64, method string) (float64, error) {
	n := len(vals)
	if n == 0 {
		return 0, nil
	}
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("series: quantile must be in [0, 1], got %v", q)
	}
	pos := float64(q * float64(n-1)) // conversion blocks FMA fusion below
	switch method {
	case QuantileNearest, "":
		// polars rounds half away from zero on the float index.
		return vals[int(math.Round(pos))], nil
	case QuantileLower:
		return vals[int(math.Floor(pos))], nil
	case QuantileHigher:
		return vals[int(math.Ceil(pos))], nil
	case QuantileMidpoint:
		lo, hi := vals[int(math.Floor(pos))], vals[int(math.Ceil(pos))]
		if lo == hi {
			return lo, nil
		}
		return (lo + hi) / 2, nil
	case QuantileLinear:
		lo := int(math.Floor(pos))
		hi := int(math.Ceil(pos))
		if lo == hi {
			return vals[lo], nil
		}
		frac := pos - float64(lo)
		// The explicit conversion blocks FMA fusion so results round like polars.
		return float64(frac*(vals[hi]-vals[lo])) + vals[lo], nil
	case QuantileEquiprob:
		idx := int(math.Max(math.Ceil(float64(n)*q)-1, 0))
		return vals[idx], nil
	}
	return 0, fmt.Errorf("series: unknown quantile interpolation %q", method)
}

// QuantileSeries returns the q-th quantile as a length-1 float Series
// (f32 for f32 input, f64 otherwise). Nulls are skipped, NaN sorts
// last. Empty or all-null input gives null. Mirrors polars'
// Series.quantile; polars' default method is nearest.
func (s *Series) QuantileSeries(q float64, method string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	vals, err := sortedFloats(arr)
	if err != nil {
		return nil, err
	}
	v, err := quantileSorted(vals, q, method)
	if err != nil {
		return nil, err
	}
	return scalarFloat(s.name, v, len(vals) > 0, arr.DataType().ID() == arrow.FLOAT32, cfg.alloc)
}

// MedianSeries is QuantileSeries(0.5, "linear").
func (s *Series) MedianSeries(opts ...Option) (*Series, error) {
	return s.QuantileSeries(0.5, QuantileLinear, opts...)
}

// VarSeries returns the variance with the given ddof as a length-1
// float Series. Fewer than ddof+1 values give null; NaN propagates.
func (s *Series) VarSeries(ddof int, opts ...Option) (*Series, error) {
	return s.moment(ddof, false, opts)
}

// StdSeries returns the standard deviation with the given ddof.
func (s *Series) StdSeries(ddof int, opts ...Option) (*Series, error) {
	return s.moment(ddof, true, opts)
}

func (s *Series) moment(ddof int, sqrt bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	var n, mean, m2 float64
	for i, v := range vals {
		if arr.NullN() > 0 && arr.IsNull(i) {
			continue
		}
		// Welford keeps the variance stable for large offsets.
		n++
		d := v - mean
		mean += d / n
		m2 += d * (v - mean)
	}
	f32 := arr.DataType().ID() == arrow.FLOAT32
	if n <= float64(ddof) {
		return scalarFloat(s.name, 0, false, f32, cfg.alloc)
	}
	v := m2 / (n - float64(ddof))
	if sqrt {
		v = math.Sqrt(v)
	}
	return scalarFloat(s.name, v, true, f32, cfg.alloc)
}

// EntropyWith computes -sum(p * ln p) / ln(base) treating the values as
// a distribution. With normalize, p is value / sum(values). Nulls are
// skipped. Mirrors polars' Series.entropy.
func (s *Series) EntropyWith(base float64, normalize bool) (float64, error) {
	arr, err := s.single(nil)
	if err != nil {
		return 0, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) {
		return 0, fmt.Errorf("series: entropy expects numeric input, got %s", arr.DataType())
	}
	vals, err := floatVals(arr)
	if err != nil {
		return 0, err
	}
	var sum float64
	if normalize {
		for i, v := range vals {
			if arr.IsValid(i) {
				sum += v
			}
		}
	}
	var h float64
	for i, v := range vals {
		if !arr.IsValid(i) {
			continue
		}
		p := v
		if normalize {
			p = v / sum
		}
		if p != 0 {
			h -= p * math.Log(p)
		}
	}
	if base != math.E {
		h /= math.Log(base)
	}
	return h, nil
}

// Dot returns sum(s[i] * other[i]) over rows where both are non-null.
// Integer inputs give i64, otherwise f64. Mirrors polars' dot.
func (s *Series) Dot(other *Series, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if s.Len() != other.Len() {
		return nil, fmt.Errorf("series: dot length mismatch %d vs %d", s.Len(), other.Len())
	}
	a, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	b, err := other.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer b.Release()
	if isIntType(a.DataType()) && isIntType(b.DataType()) {
		x, err := intVals(a)
		if err != nil {
			return nil, err
		}
		y, err := intVals(b)
		if err != nil {
			return nil, err
		}
		var acc int64
		for i := range x {
			if a.IsValid(i) && b.IsValid(i) {
				acc += x[i] * y[i]
			}
		}
		return int64Series(s.name, []int64{acc}, nil, cfg.alloc)
	}
	x, err := floatVals(a)
	if err != nil {
		return nil, err
	}
	y, err := floatVals(b)
	if err != nil {
		return nil, err
	}
	var acc float64
	for i := range x {
		if a.IsValid(i) && b.IsValid(i) {
			acc += x[i] * y[i]
		}
	}
	return scalarFloat(s.name, acc, true, false, cfg.alloc)
}

// BitwiseAnd reduces with bitwise AND (logical AND for booleans),
// skipping nulls. Empty or all-null input gives null.
func (s *Series) BitwiseAnd(opts ...Option) (*Series, error) { return s.bitReduce('&', opts) }

// BitwiseOr reduces with bitwise OR.
func (s *Series) BitwiseOr(opts ...Option) (*Series, error) { return s.bitReduce('|', opts) }

// BitwiseXor reduces with bitwise XOR.
func (s *Series) BitwiseXor(opts ...Option) (*Series, error) { return s.bitReduce('^', opts) }

func (s *Series) bitReduce(op byte, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	dt := arr.DataType()
	if dt.ID() != arrow.BOOL && !isIntType(dt) {
		return nil, fmt.Errorf("series: bitwise reduction needs integer or bool input, got %s", dt)
	}
	vals, err := intVals(arr)
	if err != nil {
		return nil, err
	}
	var acc uint64
	seen := false
	for i, v := range vals {
		if !arr.IsValid(i) {
			continue
		}
		u := uint64(v)
		if !seen {
			acc, seen = u, true
			continue
		}
		switch op {
		case '&':
			acc &= u
		case '|':
			acc |= u
		case '^':
			acc ^= u
		}
	}
	var out []any
	switch {
	case !seen:
		out = []any{nil}
	case dt.ID() == arrow.BOOL:
		out = []any{acc&1 == 1}
	case dt.ID() == arrow.UINT64:
		out = []any{acc}
	default:
		out = []any{int64(acc)}
	}
	return FromValues(s.name, dt, out, opts...)
}

// bitWidth reports the bit width of an integer or bool dtype.
func bitWidth(dt arrow.DataType) int {
	if dt.ID() == arrow.BOOL {
		return 1
	}
	return fixedWidthBytes(dt) * 8
}

// BitCount computes a per-value bit statistic as a u32 Series. kind is
// one of "count_ones", "count_zeros", "leading_ones", "leading_zeros",
// "trailing_ones", "trailing_zeros". Mirrors polars' bitwise_* methods.
func (s *Series) BitCount(kind string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	dt := arr.DataType()
	if dt.ID() != arrow.BOOL && !isIntType(dt) {
		return nil, fmt.Errorf("series: bitwise_%s needs integer or bool input, got %s", kind, dt)
	}
	w := bitWidth(dt)
	vals, err := intVals(arr)
	if err != nil {
		return nil, err
	}
	mask := uint64(math.MaxUint64)
	if w < 64 {
		mask = 1<<uint(w) - 1
	}
	out := make([]int, len(vals))
	for i, v := range vals {
		u := uint64(v) & mask
		switch kind {
		case "count_ones":
			out[i] = bits.OnesCount64(u)
		case "count_zeros":
			out[i] = w - bits.OnesCount64(u)
		case "leading_zeros":
			out[i] = bits.LeadingZeros64(u) - (64 - w)
		case "leading_ones":
			out[i] = bits.LeadingZeros64((^u)&mask) - (64 - w)
		case "trailing_zeros":
			out[i] = min(bits.TrailingZeros64(u), w)
		case "trailing_ones":
			out[i] = min(bits.TrailingZeros64(^u), w)
		default:
			return nil, fmt.Errorf("series: unknown bit statistic %q", kind)
		}
	}
	return u32Series(s.name, out, validSlice(arr), cfg.alloc)
}

// IndexOfValue returns the first index of v (nil finds the first null)
// as a u32 scalar Series, null when absent. NaN matches NaN. Mirrors
// polars' index_of.
func (s *Series) IndexOfValue(v any, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	for i := range arr.Len() {
		x := ValueAt(arr, i)
		if v == nil {
			if x == nil {
				return ScalarUint32(s.name, i, true, opts...)
			}
			continue
		}
		if x != nil && looseEqual(x, v) {
			return ScalarUint32(s.name, i, true, opts...)
		}
	}
	return ScalarUint32(s.name, 0, false, opts...)
}

// looseEqual compares a stored value with a user-supplied Go value,
// allowing numeric kinds to differ. NaN equals NaN.
func looseEqual(stored, want any) bool {
	if fs, ok := numericAsFloat(stored); ok {
		fw, ok := anyToFloat64(want)
		if !ok {
			return false
		}
		if fs != fs && fw != fw {
			return true
		}
		if is, ok := stored.(int64); ok {
			if iw, ok := anyToInt64(want); ok {
				return is == iw
			}
		}
		return fs == fw
	}
	return valuesEqual(stored, want, true)
}
