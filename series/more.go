package series

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RankMethod controls how ties are broken in Rank. Polars-compatible
// values: Average (default), Min, Max, Dense, Ordinal.
type RankMethod int

const (
	// RankAverage assigns the mean of the tied ranks.
	RankAverage RankMethod = iota
	// RankMin assigns the lowest of the tied ranks.
	RankMin
	// RankMax assigns the highest of the tied ranks.
	RankMax
	// RankDense assigns consecutive ranks with no gaps.
	RankDense
	// RankOrdinal breaks ties by position (first tied value gets the
	// lowest rank).
	RankOrdinal
)

// Rank returns a float64 Series of per-row ranks. Ascending by
// default; tie handling follows `method`. Nulls stay null.
func (s *Series) Rank(method RankMethod, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	idx, err := s.ArgSort()
	if err != nil {
		return nil, err
	}
	n := s.Len()
	chunk := s.Chunk(0)
	// Fast paths: no-null inputs walk raw values directly instead of
	// the dispatched eqAt comparator, and skip the validity bitmap.
	if chunk.NullN() == 0 {
		switch a := chunk.(type) {
		case *array.Int64:
			return rankSortedInt64(s.Name(), a.Int64Values(), idx, n, method, cfg.alloc)
		case *array.Float64:
			return rankSortedFloat64(s.Name(), a.Float64Values(), idx, n, method, cfg.alloc)
		}
	}
	ranks := make([]float64, n)
	valid := make([]bool, n)
	hasNulls := chunk.NullN() > 0
	// Walk the sorted permutation; within ties (determined by comparing
	// raw values), assign rank per `method`. Dense ranks count distinct
	// groups with a running counter (linear, not quadratic).
	dense := 0
	i := 0
	for i < n {
		// Skip any nulls at the tail of the sorted order.
		if hasNulls && !chunk.IsValid(idx[i]) {
			i++
			continue
		}
		j := i + 1
		for j < n {
			if hasNulls && !chunk.IsValid(idx[j]) {
				break
			}
			if !eqAt(chunk, idx[i], idx[j]) {
				break
			}
			j++
		}
		fillRanks(ranks, valid, idx, i, j, dense, method)
		dense++
		i = j
	}
	return FromFloat64(s.Name(), ranks, validOrNil(valid), WithAllocator(cfg.alloc))
}

// fillRanks writes one tie group's ranks. The method switch runs once
// per group rather than once per row.
func fillRanks(ranks []float64, valid []bool, idx []int, i, j, dense int, method RankMethod) {
	if method == RankOrdinal {
		for k := i; k < j; k++ {
			pos := idx[k]
			ranks[pos] = float64(k + 1)
			valid[pos] = true
		}
		return
	}
	var rv float64
	switch method {
	case RankAverage:
		// +1 so ranks are 1-based (polars default).
		rv = float64(i+1) + float64(j-i-1)/2
	case RankMin:
		rv = float64(i + 1)
	case RankMax:
		rv = float64(j)
	default: // RankDense: distinct-value counter.
		rv = float64(dense + 1)
	}
	for k := i; k < j; k++ {
		pos := idx[k]
		ranks[pos] = rv
		valid[pos] = true
	}
}

// rankSortedInt64 assigns ranks over a no-null int64 column given its
// ascending stable permutation. Output has no validity bitmap.
func rankSortedInt64(name string, vals []int64, idx []int, n int, method RankMethod, alloc memory.Allocator) (*Series, error) {
	ranks := make([]float64, n)
	dense := 0
	i := 0
	for i < n {
		j := i + 1
		for j < n && vals[idx[j]] == vals[idx[i]] {
			j++
		}
		if method == RankOrdinal {
			for k := i; k < j; k++ {
				ranks[idx[k]] = float64(k + 1)
			}
		} else {
			var rv float64
			switch method {
			case RankAverage:
				rv = float64(i+1) + float64(j-i-1)/2
			case RankMin:
				rv = float64(i + 1)
			case RankMax:
				rv = float64(j)
			default:
				rv = float64(dense + 1)
			}
			for k := i; k < j; k++ {
				ranks[idx[k]] = rv
			}
		}
		dense++
		i = j
	}
	return FromFloat64(name, ranks, nil, WithAllocator(alloc))
}

// rankSortedFloat64 mirrors rankSortedInt64 with NaNs tied together.
func rankSortedFloat64(name string, vals []float64, idx []int, n int, method RankMethod, alloc memory.Allocator) (*Series, error) {
	ranks := make([]float64, n)
	dense := 0
	i := 0
	for i < n {
		vi := vals[idx[i]]
		j := i + 1
		for j < n {
			vj := vals[idx[j]]
			if math.IsNaN(vi) || math.IsNaN(vj) {
				if !(math.IsNaN(vi) && math.IsNaN(vj)) {
					break
				}
			} else if vj != vi {
				break
			}
			j++
		}
		if method == RankOrdinal {
			for k := i; k < j; k++ {
				ranks[idx[k]] = float64(k + 1)
			}
		} else {
			var rv float64
			switch method {
			case RankAverage:
				rv = float64(i+1) + float64(j-i-1)/2
			case RankMin:
				rv = float64(i + 1)
			case RankMax:
				rv = float64(j)
			default:
				rv = float64(dense + 1)
			}
			for k := i; k < j; k++ {
				ranks[idx[k]] = rv
			}
		}
		dense++
		i = j
	}
	return FromFloat64(name, ranks, nil, WithAllocator(alloc))
}

// eqAt compares the value at positions a and b of chunk. Used by Rank
// to find tie groups under the sorted permutation.
func eqAt(chunk any, a, b int) bool {
	switch arr := chunk.(type) {
	case *array.Int64:
		return arr.Value(a) == arr.Value(b)
	case *array.Int32:
		return arr.Value(a) == arr.Value(b)
	case *array.Float64:
		va, vb := arr.Value(a), arr.Value(b)
		// NaN != NaN; treat all NaNs as equal for tie-grouping purposes.
		if math.IsNaN(va) && math.IsNaN(vb) {
			return true
		}
		return va == vb
	case *array.Float32:
		va, vb := arr.Value(a), arr.Value(b)
		fa, fb := float64(va), float64(vb)
		if math.IsNaN(fa) && math.IsNaN(fb) {
			return true
		}
		return va == vb
	case *array.String:
		return arr.Value(a) == arr.Value(b)
	case *array.Boolean:
		return arr.Value(a) == arr.Value(b)
	}
	return false
}

// SearchSorted returns the insertion index for v such that the
// resulting slice would remain sorted (ascending). The Series is
// assumed to already be sorted. Mirrors polars' Series.search_sorted
// with "left" side semantics: the first position i where s[i] >= v.
func (s *Series) SearchSorted(v any) (int, error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		want, ok := toInt64(v)
		if !ok {
			return 0, fmt.Errorf("series.SearchSorted: int64 series needs int value, got %T", v)
		}
		return sort.Search(n, func(i int) bool { return a.Value(i) >= want }), nil
	case *array.Float64:
		want, ok := toFloat64(v)
		if !ok {
			return 0, fmt.Errorf("series.SearchSorted: float64 series needs numeric value, got %T", v)
		}
		return sort.Search(n, func(i int) bool { return a.Value(i) >= want }), nil
	case *array.String:
		want, ok := v.(string)
		if !ok {
			return 0, fmt.Errorf("series.SearchSorted: string series needs string value, got %T", v)
		}
		return sort.Search(n, func(i int) bool { return a.Value(i) >= want }), nil
	}
	return 0, fmt.Errorf("series.SearchSorted: unsupported dtype %s", s.DType())
}

// IndexOf returns the first index where the Series equals v, or -1 if
// no such position exists. Non-matching dtypes return an error.
func (s *Series) IndexOf(v any) (int, error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		want, ok := toInt64(v)
		if !ok {
			return -1, fmt.Errorf("series.IndexOf: int64 series needs int value, got %T", v)
		}
		for i := range n {
			if a.IsValid(i) && a.Value(i) == want {
				return i, nil
			}
		}
		return -1, nil
	case *array.Float64:
		want, ok := toFloat64(v)
		if !ok {
			return -1, fmt.Errorf("series.IndexOf: float64 series needs numeric value, got %T", v)
		}
		for i := range n {
			if a.IsValid(i) && a.Value(i) == want {
				return i, nil
			}
		}
		return -1, nil
	case *array.String:
		want, ok := v.(string)
		if !ok {
			return -1, fmt.Errorf("series.IndexOf: string series needs string value, got %T", v)
		}
		for i := range n {
			if a.IsValid(i) && a.Value(i) == want {
				return i, nil
			}
		}
		return -1, nil
	case *array.Boolean:
		want, ok := v.(bool)
		if !ok {
			return -1, fmt.Errorf("series.IndexOf: bool series needs bool value, got %T", v)
		}
		for i := range n {
			if a.IsValid(i) && a.Value(i) == want {
				return i, nil
			}
		}
		return -1, nil
	}
	return -1, fmt.Errorf("series.IndexOf: unsupported dtype %s", s.DType())
}

// IsUnique returns a boolean Series where true marks rows whose value
// occurs exactly once in the input. Nulls form their own equivalence
// class: multiple nulls mark each other as non-unique.
func (s *Series) IsUnique(opts ...Option) (*Series, error) {
	return s.occurrenceMask(true, opts)
}

// IsDuplicated is the complement of IsUnique: true where a value
// occurs more than once.
func (s *Series) IsDuplicated(opts ...Option) (*Series, error) {
	return s.occurrenceMask(false, opts)
}

// occurrenceMask powers IsUnique/IsDuplicated. Unique==true returns a
// mask where true marks singleton occurrences; unique==false returns
// the complement.
func (s *Series) occurrenceMask(unique bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]bool, n)
	// Count occurrences into a dtype-specific map.
	switch a := chunk.(type) {
	case *array.Int64:
		counts := map[int64]int{}
		nullC := 0
		for i := range n {
			if !a.IsValid(i) {
				nullC++
				continue
			}
			counts[a.Value(i)]++
		}
		for i := range n {
			if !a.IsValid(i) {
				out[i] = (nullC == 1) == unique
				continue
			}
			out[i] = (counts[a.Value(i)] == 1) == unique
		}
	case *array.Float64:
		counts := map[uint64]int{}
		nullC := 0
		for i := range n {
			if !a.IsValid(i) {
				nullC++
				continue
			}
			// Treat NaN payloads as one equivalence class via canonical
			// bit pattern.
			v := a.Value(i)
			var key uint64
			if math.IsNaN(v) {
				key = 0x7FF8000000000001
			} else {
				key = math.Float64bits(v)
			}
			counts[key]++
		}
		for i := range n {
			if !a.IsValid(i) {
				out[i] = (nullC == 1) == unique
				continue
			}
			v := a.Value(i)
			var key uint64
			if math.IsNaN(v) {
				key = 0x7FF8000000000001
			} else {
				key = math.Float64bits(v)
			}
			out[i] = (counts[key] == 1) == unique
		}
	case *array.String:
		counts := map[string]int{}
		nullC := 0
		for i := range n {
			if !a.IsValid(i) {
				nullC++
				continue
			}
			counts[a.Value(i)]++
		}
		for i := range n {
			if !a.IsValid(i) {
				out[i] = (nullC == 1) == unique
				continue
			}
			out[i] = (counts[a.Value(i)] == 1) == unique
		}
	case *array.Boolean:
		countT, countF, nullC := 0, 0, 0
		for i := range n {
			if !a.IsValid(i) {
				nullC++
				continue
			}
			if a.Value(i) {
				countT++
			} else {
				countF++
			}
		}
		for i := range n {
			if !a.IsValid(i) {
				out[i] = (nullC == 1) == unique
				continue
			}
			if a.Value(i) {
				out[i] = (countT == 1) == unique
			} else {
				out[i] = (countF == 1) == unique
			}
		}
	default:
		return nil, fmt.Errorf("series.IsUnique/IsDuplicated: unsupported dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, nil, WithAllocator(cfg.alloc))
}

// IsFirstDistinct returns a boolean mask where true marks the first
// occurrence of each distinct value. Polars-compatible.
func (s *Series) IsFirstDistinct(opts ...Option) (*Series, error) {
	return s.firstOrLastDistinct(true, opts)
}

// IsLastDistinct marks the last occurrence.
func (s *Series) IsLastDistinct(opts ...Option) (*Series, error) {
	return s.firstOrLastDistinct(false, opts)
}

func (s *Series) firstOrLastDistinct(first bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]bool, n)

	switch a := chunk.(type) {
	case *array.Int64:
		seen := map[int64]struct{}{}
		seenNull := false
		iterate(n, first, func(i int) {
			if !a.IsValid(i) {
				if !seenNull {
					out[i] = true
					seenNull = true
				}
				return
			}
			v := a.Value(i)
			if _, ok := seen[v]; !ok {
				out[i] = true
				seen[v] = struct{}{}
			}
		})
	case *array.String:
		seen := map[string]struct{}{}
		seenNull := false
		iterate(n, first, func(i int) {
			if !a.IsValid(i) {
				if !seenNull {
					out[i] = true
					seenNull = true
				}
				return
			}
			v := a.Value(i)
			if _, ok := seen[v]; !ok {
				out[i] = true
				seen[v] = struct{}{}
			}
		})
	case *array.Float64:
		seen := map[uint64]struct{}{}
		seenNull := false
		iterate(n, first, func(i int) {
			if !a.IsValid(i) {
				if !seenNull {
					out[i] = true
					seenNull = true
				}
				return
			}
			v := a.Value(i)
			var key uint64
			if math.IsNaN(v) {
				key = 0x7FF8000000000001
			} else {
				key = math.Float64bits(v)
			}
			if _, ok := seen[key]; !ok {
				out[i] = true
				seen[key] = struct{}{}
			}
		})
	default:
		return nil, fmt.Errorf("series: IsFirstDistinct/IsLastDistinct unsupported for dtype %s", s.DType())
	}
	return FromBool(s.Name(), out, nil, WithAllocator(cfg.alloc))
}

// iterate walks [0, n) forward when forward==true, reverse otherwise.
func iterate(n int, forward bool, fn func(i int)) {
	if forward {
		for i := range n {
			fn(i)
		}
		return
	}
	for i := n - 1; i >= 0; i-- {
		fn(i)
	}
}

// ExtendConstant returns a Series with n copies of v appended at the
// tail. The result dtype is unchanged. Mirrors polars' Series.extend_
// constant.
func (s *Series) ExtendConstant(v any, n int, opts ...Option) (*Series, error) {
	if n < 0 {
		return nil, fmt.Errorf("series.ExtendConstant: n must be >= 0, got %d", n)
	}
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	m := chunk.Len()
	total := m + n
	switch a := chunk.(type) {
	case *array.Int64:
		val, ok := toInt64(v)
		if !ok {
			return nil, fmt.Errorf("series.ExtendConstant: int64 series wants int value, got %T", v)
		}
		out := make([]int64, total)
		copy(out, a.Int64Values())
		for i := m; i < total; i++ {
			out[i] = val
		}
		return FromInt64(s.Name(), out, extendValid(a, n, true), WithAllocator(cfg.alloc))
	case *array.Float64:
		val, ok := toFloat64(v)
		if !ok {
			return nil, fmt.Errorf("series.ExtendConstant: float64 series wants numeric value, got %T", v)
		}
		out := make([]float64, total)
		copy(out, a.Float64Values())
		for i := m; i < total; i++ {
			out[i] = val
		}
		return FromFloat64(s.Name(), out, extendValid(a, n, true), WithAllocator(cfg.alloc))
	case *array.String:
		val, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("series.ExtendConstant: string series wants string value, got %T", v)
		}
		out := make([]string, total)
		for i := range m {
			out[i] = a.Value(i)
		}
		for i := m; i < total; i++ {
			out[i] = val
		}
		return FromString(s.Name(), out, extendValid(a, n, true), WithAllocator(cfg.alloc))
	case *array.Boolean:
		val, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("series.ExtendConstant: bool series wants bool value, got %T", v)
		}
		out := make([]bool, total)
		for i := range m {
			out[i] = a.Value(i)
		}
		for i := m; i < total; i++ {
			out[i] = val
		}
		return FromBool(s.Name(), out, extendValid(a, n, true), WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series.ExtendConstant: unsupported dtype %s", s.DType())
}

// extendValid builds the validity slice for ExtendConstant: copies the
// source validity for the first m entries, then n copies of extendValid
// (true when the constant is non-null, false when it is nil). Returns
// nil when no nulls are present.
func extendValid(a interface {
	Len() int
	NullN() int
	IsValid(int) bool
}, n int, constantValid bool) []bool {
	m := a.Len()
	if a.NullN() == 0 && constantValid {
		return nil
	}
	valid := make([]bool, m+n)
	for i := range m {
		valid[i] = a.IsValid(i)
	}
	for i := m; i < m+n; i++ {
		valid[i] = constantValid
	}
	return valid
}

// Hash returns a uint64 Series of FNV-64a hashes of each non-null
// value. Null positions are preserved as null in the output. Deterministic
// but not cryptographic. Matches polars' Series.hash in spirit; the
// exact hash value differs since polars uses xxhash.
func (s *Series) Hash(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]uint64, n)
	valid := validFromChunk(chunk)
	h := fnv.New64a()
	write := func(b []byte) uint64 {
		h.Reset()
		h.Write(b)
		return h.Sum64()
	}
	switch a := chunk.(type) {
	case *array.Int64:
		for i, v := range a.Int64Values() {
			if valid != nil && !valid[i] {
				continue
			}
			var b [8]byte
			for k := range 8 {
				b[k] = byte(v >> (k * 8))
			}
			out[i] = write(b[:])
		}
	case *array.String:
		for i := range n {
			if valid != nil && !valid[i] {
				continue
			}
			out[i] = write([]byte(a.Value(i)))
		}
	case *array.Float64:
		for i, v := range a.Float64Values() {
			if valid != nil && !valid[i] {
				continue
			}
			bits := math.Float64bits(v)
			if math.IsNaN(v) {
				bits = 0x7FF8000000000001
			}
			var b [8]byte
			for k := range 8 {
				b[k] = byte(bits >> (k * 8))
			}
			out[i] = write(b[:])
		}
	case *array.Boolean:
		for i := range n {
			if valid != nil && !valid[i] {
				continue
			}
			if a.Value(i) {
				out[i] = write([]byte{1})
			} else {
				out[i] = write([]byte{0})
			}
		}
	default:
		return nil, fmt.Errorf("series.Hash: unsupported dtype %s", s.DType())
	}
	return FromUint64(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// FirstNonNull returns the first non-null value in the Series and its
// index, or (nil, -1, nil) when the series is all-null or empty.
func (s *Series) FirstNonNull() (value any, index int, err error) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	for i := range n {
		if !chunk.IsValid(i) {
			continue
		}
		switch a := chunk.(type) {
		case *array.Int64:
			return a.Value(i), i, nil
		case *array.Float64:
			return a.Value(i), i, nil
		case *array.Int32:
			return a.Value(i), i, nil
		case *array.Float32:
			return a.Value(i), i, nil
		case *array.Boolean:
			return a.Value(i), i, nil
		case *array.String:
			return a.Value(i), i, nil
		default:
			return nil, -1, fmt.Errorf("series.FirstNonNull: unsupported dtype %s", s.DType())
		}
	}
	return nil, -1, nil
}

// Apply maps a user function over non-null values, producing a new
// Series of the same dtype. Null positions pass through unchanged.
// The function is called dtype-specifically: an Int64 Series gets
// func(int64) int64, a String Series func(string) string, etc.
//
// Pass the right-typed applier via ApplyInt64/ApplyFloat64/ApplyString
// /ApplyBool. This avoids an `any` boxing per row on hot paths.
func (s *Series) ApplyInt64(fn func(int64) int64, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, ok := s.Chunk(0).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("series.ApplyInt64: requires int64 dtype, got %s", s.DType())
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]int64, n)
	raw := a.Int64Values()
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = fn(raw[i])
	}
	return FromInt64(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// ApplyFloat64 is the f64 counterpart.
func (s *Series) ApplyFloat64(fn func(float64) float64, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, ok := s.Chunk(0).(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("series.ApplyFloat64: requires float64 dtype, got %s", s.DType())
	}
	n := a.Len()
	valid := validFromChunk(a)
	return BuildFloat64Direct(s.Name(), n, cfg.alloc, func(out []float64) {
		raw := a.Float64Values()
		for i := range n {
			if valid != nil && !valid[i] {
				continue
			}
			out[i] = fn(raw[i])
		}
	})
}

// ApplyString is the utf8 counterpart.
func (s *Series) ApplyString(fn func(string) string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, ok := s.Chunk(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("series.ApplyString: requires string dtype, got %s", s.DType())
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]string, n)
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = fn(a.Value(i))
	}
	return FromString(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// ApplyBool is the bool counterpart.
func (s *Series) ApplyBool(fn func(bool) bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, ok := s.Chunk(0).(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("series.ApplyBool: requires bool dtype, got %s", s.DType())
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]bool, n)
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = fn(a.Value(i))
	}
	return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// SampleFrac returns a Series of floor(fraction * Len()) rows drawn
// without replacement by default. Mirrors the polars sample(fraction=)
// convenience.
func (s *Series) SampleFrac(fraction float64, withReplacement bool, seed uint64, opts ...Option) (*Series, error) {
	if fraction < 0 {
		return nil, fmt.Errorf("series.SampleFrac: fraction must be >= 0, got %v", fraction)
	}
	if !withReplacement && fraction > 1 {
		return nil, fmt.Errorf("series.SampleFrac: fraction > 1 requires withReplacement")
	}
	n := int(float64(s.Len()) * fraction)
	return s.Sample(n, withReplacement, seed, opts...)
}
