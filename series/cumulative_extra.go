package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CumMin returns a running minimum: out[i] = min(src[0..=i]).
// Nulls are skipped (a null row stays null and does not reset the
// running minimum), and NaN is ignored unless every value so far is
// NaN, matching polars. The dtype is kept. Empty input gives empty
// output.
func (s *Series) CumMin(opts ...Option) (*Series, error) {
	return s.cumExtreme(opts, true, "CumMin")
}

// CumMax is the symmetric counterpart of CumMin.
func (s *Series) CumMax(opts ...Option) (*Series, error) {
	return s.cumExtreme(opts, false, "CumMax")
}

func (s *Series) cumExtreme(opts []Option, isMin bool, name string) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	var out arrow.Array
	switch a := arr.(type) {
	case *array.Int8:
		out = cumExtremeNum(a, a.Int8Values(), isMin, cfg.alloc)
	case *array.Int16:
		out = cumExtremeNum(a, a.Int16Values(), isMin, cfg.alloc)
	case *array.Int32:
		out = cumExtremeNum(a, a.Int32Values(), isMin, cfg.alloc)
	case *array.Int64:
		out = cumExtremeNum(a, a.Int64Values(), isMin, cfg.alloc)
	case *array.Uint8:
		out = cumExtremeNum(a, a.Uint8Values(), isMin, cfg.alloc)
	case *array.Uint16:
		out = cumExtremeNum(a, a.Uint16Values(), isMin, cfg.alloc)
	case *array.Uint32:
		out = cumExtremeNum(a, a.Uint32Values(), isMin, cfg.alloc)
	case *array.Uint64:
		out = cumExtremeNum(a, a.Uint64Values(), isMin, cfg.alloc)
	case *array.Float32:
		out = cumExtremeNum(a, a.Float32Values(), isMin, cfg.alloc)
	case *array.Float64:
		out = cumExtremeNum(a, a.Float64Values(), isMin, cfg.alloc)
	default:
		return nil, fmt.Errorf("series: %s unsupported for dtype %s", name, s.DType())
	}
	return New(s.Name(), out)
}

func cumExtremeNum[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64](
	a arrow.Array, raw []T, isMin bool, mem memory.Allocator,
) arrow.Array {
	out := make([]T, len(raw))
	var valid []bool
	if a.NullN() > 0 {
		valid = make([]bool, len(raw))
	}
	var cur T
	seen := false
	for i, v := range raw {
		if valid != nil {
			if a.IsNull(i) {
				continue
			}
			valid[i] = true
		}
		switch {
		case !seen, cur != cur: // first value, or only NaN so far
			cur = v
		case v != v: // NaN never replaces a number
		case isMin && v < cur, !isMin && v > cur:
			cur = v
		}
		seen = true
		out[i] = cur
	}
	b := array.NewBuilder(mem, a.DataType())
	defer b.Release()
	switch bb := b.(type) {
	case *array.Int8Builder:
		bb.AppendValues(any(out).([]int8), valid)
	case *array.Int16Builder:
		bb.AppendValues(any(out).([]int16), valid)
	case *array.Int32Builder:
		bb.AppendValues(any(out).([]int32), valid)
	case *array.Int64Builder:
		bb.AppendValues(any(out).([]int64), valid)
	case *array.Uint8Builder:
		bb.AppendValues(any(out).([]uint8), valid)
	case *array.Uint16Builder:
		bb.AppendValues(any(out).([]uint16), valid)
	case *array.Uint32Builder:
		bb.AppendValues(any(out).([]uint32), valid)
	case *array.Uint64Builder:
		bb.AppendValues(any(out).([]uint64), valid)
	case *array.Float32Builder:
		bb.AppendValues(any(out).([]float32), valid)
	case *array.Float64Builder:
		bb.AppendValues(any(out).([]float64), valid)
	}
	return b.NewArray()
}

// CumProd returns a running product. Polars widens to the input's
// numeric dtype; we emit float64 for simplicity and overflow safety.
func (s *Series) CumProd(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	cur := 1.0
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			cur *= float64(raw[i])
			out[i] = cur
			valid[i] = true
		}
	case *array.Float64:
		raw := a.Float64Values()
		for i := range n {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			cur *= raw[i]
			out[i] = cur
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("series: CumProd unsupported for dtype %s", s.DType())
	}
	return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
}

// CumCount returns a running count of non-null values. Output is an
// int64 Series with no nulls.
func (s *Series) CumCount(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]int64, n)
	count := int64(0)
	for i := range n {
		if chunk.NullN() == 0 || chunk.IsValid(i) {
			count++
		}
		out[i] = count
	}
	return FromInt64(s.Name(), out, nil, WithAllocator(cfg.alloc))
}

// PctChange returns the percent change between each row and the
// previous row (or between row i and row i-periods for non-default
// periods). Output is float64; leading rows are null.
func (s *Series) PctChange(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]float64, n)
	valid := make([]bool, n)
	toFloat := func(i int) (float64, bool) {
		switch a := chunk.(type) {
		case *array.Int64:
			if a.NullN() > 0 && !a.IsValid(i) {
				return 0, false
			}
			return float64(a.Value(i)), true
		case *array.Float64:
			if a.NullN() > 0 && !a.IsValid(i) {
				return 0, false
			}
			return a.Value(i), true
		}
		return 0, false
	}
	for i := range n {
		j := i - periods
		if j < 0 || j >= n {
			continue
		}
		cur, okC := toFloat(i)
		prev, okP := toFloat(j)
		if !okC || !okP || prev == 0 {
			continue
		}
		out[i] = cur/prev - 1
		valid[i] = true
	}
	return FromFloat64(s.Name(), out, validOrNil(valid), WithAllocator(cfg.alloc))
}

// Mode returns the most frequent value(s). Ties produce multiple
// rows. Nulls are counted only if all non-null values would tie with
// the null count. Empty input returns an empty Series of the same
// dtype.
func (s *Series) Mode(opts ...Option) (*Series, error) {
	u, err := s.Unique(opts...)
	if err != nil {
		return nil, err
	}
	defer u.Release()
	// Build a frequency table by scanning the original.
	chunk := s.Chunk(0)
	switch a := chunk.(type) {
	case *array.Int64:
		counts := map[int64]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopInt64(s.Name(), counts, opts)
	case *array.Float64:
		counts := map[float64]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopFloat64(s.Name(), counts, opts)
	case *array.String:
		counts := map[string]int{}
		for i := range a.Len() {
			if a.NullN() > 0 && !a.IsValid(i) {
				continue
			}
			counts[a.Value(i)]++
		}
		return modeTopString(s.Name(), counts, opts)
	}
	return nil, fmt.Errorf("series: Mode unsupported for dtype %s", s.DType())
}

// modeTop* pick every key that ties for the max count.
func modeTopInt64(name string, counts map[int64]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []int64
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromInt64(name, out, nil, opts...)
}

func modeTopFloat64(name string, counts map[float64]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []float64
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromFloat64(name, out, nil, opts...)
}

func modeTopString(name string, counts map[string]int, opts []Option) (*Series, error) {
	maxN := 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
	}
	var out []string
	for k, c := range counts {
		if c == maxN {
			out = append(out, k)
		}
	}
	return FromString(name, out, nil, opts...)
}

// validOrNil returns valid if any slot is false, otherwise nil (the
// conventional "no nulls" marker accepted by From* constructors).
func validOrNil(valid []bool) []bool {
	for _, v := range valid {
		if !v {
			return valid
		}
	}
	return nil
}
