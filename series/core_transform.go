package series

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// DropNans removes NaN rows from float columns; other dtypes are
// returned unchanged. Nulls are kept. Mirrors polars' drop_nans.
func (s *Series) DropNans(opts ...Option) (*Series, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	isNaN := nanTester(arr)
	if isNaN == nil {
		return s.Clone(), nil
	}
	idx := make([]int, 0, arr.Len())
	for i := range arr.Len() {
		if arr.IsValid(i) && isNaN(i) {
			continue
		}
		idx = append(idx, i)
	}
	return s.Gather(idx, opts...)
}

// GatherEvery takes every n-th row starting at offset. Mirrors polars'
// gather_every.
func (s *Series) GatherEvery(n, offset int, opts ...Option) (*Series, error) {
	if n <= 0 {
		return nil, fmt.Errorf("series: gather_every step must be positive, got %d", n)
	}
	if offset < 0 {
		return nil, fmt.Errorf("series: gather_every offset must be non-negative")
	}
	var idx []int
	for i := offset; i < s.Len(); i += n {
		idx = append(idx, i)
	}
	return s.Gather(idx, opts...)
}

// GatherSigned gathers with polars' index rules: negative indices count
// from the end and a null index gives a null. Out-of-bounds indices are
// an error.
func (s *Series) GatherSigned(indices *Series, opts ...Option) (*Series, error) {
	arr, err := indices.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isIntType(arr.DataType()) && arr.DataType().ID() != arrow.NULL {
		return nil, fmt.Errorf("series: gather indices must be integers, got %s", indices.DType())
	}
	raw, err := intVals(arr)
	if err != nil {
		return nil, err
	}
	n := s.Len()
	idx := make([]int, len(raw))
	for i, v := range raw {
		if arr.IsNull(i) {
			idx[i] = -1
			continue
		}
		if v < 0 {
			v += int64(n)
		}
		if v < 0 || v >= int64(n) {
			return nil, fmt.Errorf("series: gather indices are out of bounds")
		}
		idx[i] = int(v)
	}
	return s.Gather(idx, opts...)
}

// SortByKeys sorts s by the key Series. descending holds one flag for
// all keys or one per key; nulls in keys go first unless nullsLast.
func (s *Series) SortByKeys(keys []*Series, descending []bool, nullsLast bool, opts ...Option) (*Series, error) {
	idx, err := argSortKeys(keys, descending, nullsLast)
	if err != nil {
		return nil, err
	}
	return s.Gather(idx, opts...)
}

func argSortKeys(keys []*Series, descending []bool, nullsLast bool) ([]int, error) {
	so := make([]SortKey, len(keys))
	for i := range so {
		switch {
		case len(descending) == 1:
			so[i].Descending = descending[0]
		case i < len(descending):
			so[i].Descending = descending[i]
		}
		so[i].NullsLast = nullsLast
	}
	return ArgSortMulti(keys, so)
}

// ArgSortKeys is the exported form of the multi-key argsort with
// polars' sort_by options.
func ArgSortKeys(keys []*Series, descending []bool, nullsLast bool) ([]int, error) {
	return argSortKeys(keys, descending, nullsLast)
}

// TopKBy returns the rows of s at the k largest rows of by (nulls
// last). reverse flips individual keys. Mirrors polars' top_k_by.
func (s *Series) TopKBy(by []*Series, k int, reverse []bool, opts ...Option) (*Series, error) {
	return s.kBy(by, k, reverse, true, opts)
}

// BottomKBy returns the rows of s at the k smallest rows of by.
func (s *Series) BottomKBy(by []*Series, k int, reverse []bool, opts ...Option) (*Series, error) {
	return s.kBy(by, k, reverse, false, opts)
}

func (s *Series) kBy(by []*Series, k int, reverse []bool, top bool, opts []Option) (*Series, error) {
	if k < 0 {
		return nil, fmt.Errorf("series: k must be non-negative")
	}
	so := make([]SortKey, len(by))
	for i := range so {
		rev := false
		switch {
		case len(reverse) == 1:
			rev = reverse[0]
		case i < len(reverse):
			rev = reverse[i]
		}
		so[i] = SortKey{Descending: top != rev, NullsLast: true}
	}
	idx, err := ArgSortMulti(by, so)
	if err != nil {
		return nil, err
	}
	return s.Gather(idx[:min(k, len(idx))], opts...)
}

// TopKValues returns the k largest values sorted descending (NaN is the
// largest float, nulls come last). When k covers the whole column polars
// skips the sort and only moves nulls to the end; that is mirrored here.
// Mirrors polars' top_k.
func (s *Series) TopKValues(k int, opts ...Option) (*Series, error) {
	return s.kValues(k, true, opts)
}

// BottomKValues returns the k smallest values sorted ascending.
func (s *Series) BottomKValues(k int, opts ...Option) (*Series, error) {
	return s.kValues(k, false, opts)
}

func (s *Series) kValues(k int, top bool, opts []Option) (*Series, error) {
	if k < 0 {
		return nil, fmt.Errorf("series: k must be non-negative")
	}
	n := s.Len()
	if k >= n {
		arr, err := s.single(nil)
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		idx := make([]int, 0, n)
		for i := range n {
			if arr.IsValid(i) {
				idx = append(idx, i)
			}
		}
		for i := range n {
			if arr.IsNull(i) {
				idx = append(idx, i)
			}
		}
		return s.Gather(idx, opts...)
	}
	if s.NullCount() == 0 {
		switch s.data.DataType().ID() {
		case arrow.INT64, arrow.FLOAT64:
			// The heap kernels agree with polars when there are no nulls.
			if top {
				return s.TopK(k, opts...)
			}
			return s.BottomK(k, opts...)
		}
	}
	return s.kBy([]*Series{s}, k, nil, top, opts)
}

// ValueCountsStruct returns one struct row {value, count} per distinct
// value (nulls included). sort orders by descending count (stable, so
// ties keep first-appearance order); normalize replaces the count with
// a proportion. countName defaults to "count" or "proportion".
// Mirrors polars' value_counts.
func (s *Series) ValueCountsStruct(sort, normalize bool, countName string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	ids, err := s.denseIDs()
	if err != nil {
		return nil, err
	}
	counts := make([]int, ids.count())
	for _, id := range ids.ids {
		counts[id]++
	}
	order := make([]int, len(counts))
	for i := range order {
		order[i] = i
	}
	if sort {
		stableSortBy(order, func(a, b int) bool { return counts[a] > counts[b] })
	}
	rows := make([]int, len(order))
	cs := make([]int, len(order))
	for i, id := range order {
		rows[i] = ids.first[id]
		cs[i] = counts[id]
	}
	vals, err := s.Gather(rows, opts...)
	if err != nil {
		return nil, err
	}
	defer vals.Release()
	var cnt *Series
	if normalize {
		if countName == "" {
			countName = "proportion"
		}
		total := float64(s.Len())
		ps := make([]float64, len(cs))
		for i, c := range cs {
			ps[i] = float64(c) / total
		}
		cnt, err = floatSeries(countName, ps, nil, false, cfg.alloc)
	} else {
		if countName == "" {
			countName = "count"
		}
		cnt, err = u32Series(countName, cs, nil, cfg.alloc)
	}
	if err != nil {
		return nil, err
	}
	defer cnt.Release()
	return StructFromSeries(s.name, []*Series{vals, cnt}, nil, opts...)
}

// stableSortBy is an insertion-merge stable sort on ints with a less
// function; the value lists here are distinct-value tables.
func stableSortBy(xs []int, less func(a, b int) bool) {
	if len(xs) < 2 {
		return
	}
	mid := len(xs) / 2
	left := append([]int(nil), xs[:mid]...)
	right := append([]int(nil), xs[mid:]...)
	stableSortBy(left, less)
	stableSortBy(right, less)
	i, j, k := 0, 0, 0
	for i < len(left) && j < len(right) {
		if less(right[j], left[i]) {
			xs[k] = right[j]
			j++
		} else {
			xs[k] = left[i]
			i++
		}
		k++
	}
	for i < len(left) {
		xs[k] = left[i]
		i++
		k++
	}
	for j < len(right) {
		xs[k] = right[j]
		j++
		k++
	}
}

// runStarts returns the first row of every run of equal consecutive
// values (null equals null, NaN equals NaN).
func (s *Series) runStarts() ([]int, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	n := arr.Len()
	if n == 0 {
		return nil, nil
	}
	var c func(i, j int) int
	if arr.DataType().ID() != arrow.NULL {
		c, err = orderCmp(arr)
		if err != nil {
			return nil, err
		}
	}
	starts := []int{0}
	for i := 1; i < n; i++ {
		pn, cn := arr.IsNull(i-1), arr.IsNull(i)
		same := pn && cn || !pn && !cn && c(i-1, i) == 0
		if !same {
			starts = append(starts, i)
		}
	}
	return starts, nil
}

// Rle run-length encodes s into a struct {len u32, value}. Mirrors
// polars' rle.
func (s *Series) Rle(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	starts, err := s.runStarts()
	if err != nil {
		return nil, err
	}
	lens := make([]int, len(starts))
	for i, st := range starts {
		end := s.Len()
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		lens[i] = end - st
	}
	l, err := u32Series("len", lens, nil, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer l.Release()
	v, err := s.Gather(starts, opts...)
	if err != nil {
		return nil, err
	}
	defer v.Release()
	val := v.Rename("value")
	defer val.Release()
	return StructFromSeries(s.name, []*Series{l, val}, nil, opts...)
}

// RleID returns a u32 run id per row that increments whenever the value
// changes. Mirrors polars' rle_id.
func (s *Series) RleID(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	starts, err := s.runStarts()
	if err != nil {
		return nil, err
	}
	ids := make([]int, s.Len())
	run := -1
	next := 0
	for i := range ids {
		if next < len(starts) && starts[next] == i {
			run++
			next++
		}
		ids[i] = run
	}
	return u32Series(s.name, ids, nil, cfg.alloc)
}

// RepeatBy repeats each value by the count in by, giving a list column.
// A null count gives a null list. Mirrors polars' repeat_by.
func (s *Series) RepeatBy(by *Series, opts ...Option) (*Series, error) {
	if by.Len() != s.Len() {
		return nil, fmt.Errorf("series: repeat_by length mismatch %d vs %d", s.Len(), by.Len())
	}
	arr, err := by.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	counts, err := intVals(arr)
	if err != nil {
		return nil, err
	}
	offsets := make([]int32, s.Len()+1)
	var idx []int
	valid := make([]bool, s.Len())
	for i, c := range counts {
		if arr.IsValid(i) {
			valid[i] = true
			for range max(c, 0) {
				idx = append(idx, i)
			}
		}
		offsets[i+1] = int32(len(idx))
	}
	vals, err := s.Gather(idx, opts...)
	if err != nil {
		return nil, err
	}
	defer vals.Release()
	return ListFromInt32Offsets(s.name, vals, offsets, validOrNil(valid), opts...)
}

// Reshape reshapes s. One dimension returns the flat column; two give
// a fixed-size list of width dims[1]. One -1 dimension is inferred.
func (s *Series) Reshape(dims []int, opts ...Option) (*Series, error) {
	n := s.Len()
	switch len(dims) {
	case 1:
		if dims[0] != -1 && dims[0] != n {
			return nil, fmt.Errorf("series: cannot reshape length %d into (%d,)", n, dims[0])
		}
		return s.Clone(), nil
	case 2:
		rows, cols := dims[0], dims[1]
		switch {
		case rows == -1 && cols > 0:
			rows = n / cols
		case cols == -1 && rows > 0:
			cols = n / rows
		}
		if rows*cols != n || cols <= 0 {
			return nil, fmt.Errorf("series: cannot reshape length %d into (%d, %d)", n, dims[0], dims[1])
		}
		return FixedListFromValues(s.name, s, cols, opts...)
	}
	return nil, fmt.Errorf("series: reshape supports one or two dimensions, got %d", len(dims))
}

// Interpolate fills interior nulls. "linear" returns f64 (f32 for f32
// input); "nearest" keeps the dtype and rounds ties up. Leading and
// trailing nulls stay null. Non-numeric columns pass through.
func (s *Series) Interpolate(method string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) {
		return s.Clone(), nil
	}
	n := arr.Len()
	if method == "nearest" {
		idx := make([]int, n)
		for i := range n {
			if arr.IsValid(i) {
				idx[i] = i
			} else {
				idx[i] = -1
			}
		}
		fillNearest(arr, idx)
		if isFloatType(arr.DataType()) {
			return nearestFloat(s.name, arr, idx, cfg)
		}
		return s.Gather(idx, opts...)
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	pos := make([]float64, n)
	for i := range pos {
		pos[i] = float64(i)
	}
	out, valid := interpLinear(arr, vals, pos)
	return floatSeries(s.name, out, valid, arr.DataType().ID() == arrow.FLOAT32, cfg.alloc)
}

// nearestFloat materialises nearest interpolation for floats the way
// polars computes it: taking the right neighbour evaluates
// low + (high - low), so a NaN on either side propagates.
func nearestFloat(name string, arr arrow.Array, idx []int, cfg config) (*Series, error) {
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	f32 := arr.DataType().ID() == arrow.FLOAT32
	n := len(idx)
	out := make([]float64, n)
	valid := make([]bool, n)
	prev := -1
	for i, j := range idx {
		if j < 0 {
			continue
		}
		valid[i] = true
		if arr.IsValid(i) {
			out[i] = vals[i]
			prev = i
			continue
		}
		if j == prev {
			out[i] = vals[prev]
			continue
		}
		low, high := vals[prev], vals[j]
		if f32 {
			l32 := float32(low)
			out[i] = float64(l32 + (float32(high) - l32))
		} else {
			out[i] = low + (high - low)
		}
	}
	return floatSeries(name, out, validOrNil(valid), f32, cfg.alloc)
}

// fillNearest sets idx[i] for interior nulls to the closer valid
// neighbour, preferring the right one on ties.
func fillNearest(arr arrow.Array, idx []int) {
	n := len(idx)
	prev := -1
	for i := 0; i < n; i++ {
		if arr.IsValid(i) {
			prev = i
			continue
		}
		next := i
		for next < n && arr.IsNull(next) {
			next++
		}
		if prev < 0 || next >= n {
			i = next - 1
			continue
		}
		for j := i; j < next; j++ {
			if j-prev < next-j {
				idx[j] = prev
			} else {
				idx[j] = next
			}
		}
		i = next - 1
	}
}

// interpLinear fills nulls between valid neighbours linearly against
// pos (row positions or a by column).
func interpLinear(arr arrow.Array, vals, pos []float64) ([]float64, []bool) {
	n := len(vals)
	out := make([]float64, n)
	valid := make([]bool, n)
	prev := -1
	for i := 0; i < n; i++ {
		if arr.IsValid(i) {
			out[i] = vals[i]
			valid[i] = true
			prev = i
			continue
		}
		next := i
		for next < n && arr.IsNull(next) {
			next++
		}
		if prev >= 0 && next < n {
			x0, x1 := pos[prev], pos[next]
			y0, y1 := vals[prev], vals[next]
			// polars computes low + step*slope; the conversion blocks FMA.
			slope := (y1 - y0) / (x1 - x0)
			for j := i; j < next; j++ {
				out[j] = y0 + float64((pos[j]-x0)*slope)
				valid[j] = true
			}
		}
		i = next - 1
	}
	return out, validOrNil(valid)
}

// InterpolateBy fills nulls by linear interpolation against by. Rows
// are visited in ascending order of by; nulls at either end of that
// order stay null. Output is f64 (f32 for f32 input).
func (s *Series) InterpolateBy(by *Series, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if by.Len() != s.Len() {
		return nil, fmt.Errorf("series: interpolate_by length mismatch %d vs %d", s.Len(), by.Len())
	}
	order, err := by.ArgSortWith(false, true)
	if err != nil {
		return nil, err
	}
	sortedS, err := s.Gather(order)
	if err != nil {
		return nil, err
	}
	defer sortedS.Release()
	sortedBy, err := by.Gather(order)
	if err != nil {
		return nil, err
	}
	defer sortedBy.Release()
	arr, err := sortedS.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	byArr, err := sortedBy.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer byArr.Release()
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	pos, err := floatVals(byArr)
	if err != nil {
		return nil, err
	}
	sv, svalid := interpLinear(arr, vals, pos)
	n := len(order)
	out := make([]float64, n)
	var valid []bool
	if svalid != nil {
		valid = make([]bool, n)
	}
	for k, row := range order {
		out[row] = sv[k]
		if valid != nil {
			valid[row] = svalid[k]
		}
	}
	return floatSeries(s.name, out, valid, arr.DataType().ID() == arrow.FLOAT32, cfg.alloc)
}

// Append returns s followed by other (same dtype). Mirrors polars'
// append; Extend is the same operation.
func (s *Series) Append(other *Series, opts ...Option) (*Series, error) {
	return ConcatSeries(s.name, []*Series{s, other}, opts...)
}

// Extend is an alias for Append.
func (s *Series) Extend(other *Series, opts ...Option) (*Series, error) {
	return s.Append(other, opts...)
}

// ZipWith takes s where mask is true and other elsewhere (a null mask
// picks other). Mirrors polars' zip_with.
func (s *Series) ZipWith(mask, other *Series, opts ...Option) (*Series, error) {
	n := s.Len()
	if mask.Len() != n || other.Len() != n {
		return nil, fmt.Errorf("series: zip_with length mismatch")
	}
	m, err := mask.single(nil)
	if err != nil {
		return nil, err
	}
	defer m.Release()
	b, ok := m.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("series: zip_with mask must be bool, got %s", mask.DType())
	}
	pool, err := ConcatSeries(s.name, []*Series{s, other}, opts...)
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	idx := make([]int, n)
	for i := range idx {
		if b.IsValid(i) && b.Value(i) {
			idx[i] = i
		} else {
			idx[i] = n + i
		}
	}
	return pool.Gather(idx, opts...)
}

// Scatter returns a copy of s with values written at the given
// positions (negative positions count from the end). values must have
// one row per index or a single row to broadcast. Mirrors polars'
// scatter (which mutates in place; golars returns a new Series).
func (s *Series) Scatter(indices []int, values *Series, opts ...Option) (*Series, error) {
	n := s.Len()
	if values.Len() != len(indices) && values.Len() != 1 {
		return nil, fmt.Errorf("series: scatter needs %d values, got %d", len(indices), values.Len())
	}
	pool, err := ConcatSeries(s.name, []*Series{s, values}, opts...)
	if err != nil {
		return nil, err
	}
	defer pool.Release()
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	for k, p := range indices {
		if p < 0 {
			p += n
		}
		if p < 0 || p >= n {
			return nil, fmt.Errorf("series: scatter index %d out of bounds for length %d", indices[k], n)
		}
		if values.Len() == 1 {
			idx[p] = n
		} else {
			idx[p] = n + k
		}
	}
	return pool.Gather(idx, opts...)
}

// ShrinkDtype returns s cast to the smallest integer dtype that holds
// every value (unsigned inputs stay unsigned), and floats to f32, which
// is what polars' Series.shrink_dtype does.
func (s *Series) ShrinkDtype(opts ...Option) (*Series, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	dt := arr.DataType()
	if isFloatType(dt) {
		if dt.ID() == arrow.FLOAT32 {
			return s.Clone(), nil
		}
		vals, _ := floatVals(arr)
		return floatSeries(s.name, vals, validSlice(arr), true, resolve(opts).alloc)
	}
	if !isIntType(dt) {
		return s.Clone(), nil
	}
	unsigned := dt.ID() == arrow.UINT8 || dt.ID() == arrow.UINT16 || dt.ID() == arrow.UINT32 || dt.ID() == arrow.UINT64
	vals, _ := intVals(arr)
	lo, hi := int64(0), int64(0)
	for i, v := range vals {
		if arr.IsNull(i) {
			continue
		}
		lo, hi = min(lo, v), max(hi, v)
	}
	var to arrow.DataType
	switch {
	case unsigned && hi <= math.MaxUint8:
		to = arrow.PrimitiveTypes.Uint8
	case unsigned && hi <= math.MaxUint16:
		to = arrow.PrimitiveTypes.Uint16
	case unsigned && hi <= math.MaxUint32:
		to = arrow.PrimitiveTypes.Uint32
	case unsigned:
		to = arrow.PrimitiveTypes.Uint64
	case lo >= math.MinInt8 && hi <= math.MaxInt8:
		to = arrow.PrimitiveTypes.Int8
	case lo >= math.MinInt16 && hi <= math.MaxInt16:
		to = arrow.PrimitiveTypes.Int16
	case lo >= math.MinInt32 && hi <= math.MaxInt32:
		to = arrow.PrimitiveTypes.Int32
	default:
		to = arrow.PrimitiveTypes.Int64
	}
	if arrow.TypeEqual(to, dt) {
		return s.Clone(), nil
	}
	return intSeriesAs(s.name, vals, validSlice(arr), to, resolve(opts).alloc)
}
