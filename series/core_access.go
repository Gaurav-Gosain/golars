package series

import (
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ValueAt returns row i of an arrow array as a plain Go value, or nil
// for a null. Signed integers come back as int64, unsigned as uint64,
// floats as float64. Dates and datetimes become time.Time (UTC),
// durations and times of day become time.Duration, lists become []any
// and structs become map[string]any.
func ValueAt(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch x := a.(type) {
	case *array.Boolean:
		return x.Value(i)
	case *array.Int8:
		return int64(x.Value(i))
	case *array.Int16:
		return int64(x.Value(i))
	case *array.Int32:
		return int64(x.Value(i))
	case *array.Int64:
		return x.Value(i)
	case *array.Uint8:
		return uint64(x.Value(i))
	case *array.Uint16:
		return uint64(x.Value(i))
	case *array.Uint32:
		return uint64(x.Value(i))
	case *array.Uint64:
		return x.Value(i)
	case *array.Float16:
		return float64(x.Value(i).Float32())
	case *array.Float32:
		return float64(x.Value(i))
	case *array.Float64:
		return x.Value(i)
	case *array.String:
		return x.Value(i)
	case *array.LargeString:
		return x.Value(i)
	case *array.Binary:
		return append([]byte(nil), x.Value(i)...)
	case *array.LargeBinary:
		return append([]byte(nil), x.Value(i)...)
	case *array.Date32:
		return time.Unix(int64(x.Value(i))*86400, 0).UTC()
	case *array.Date64:
		return time.UnixMilli(int64(x.Value(i))).UTC()
	case *array.Timestamp:
		unit := x.DataType().(*arrow.TimestampType).Unit
		return time.Unix(0, int64(x.Value(i))*int64(unit.Multiplier())).UTC()
	case *array.Duration:
		unit := x.DataType().(*arrow.DurationType).Unit
		return time.Duration(int64(x.Value(i)) * int64(unit.Multiplier()))
	case *array.Time32:
		unit := x.DataType().(*arrow.Time32Type).Unit
		return time.Duration(int64(x.Value(i)) * int64(unit.Multiplier()))
	case *array.Time64:
		unit := x.DataType().(*arrow.Time64Type).Unit
		return time.Duration(int64(x.Value(i)) * int64(unit.Multiplier()))
	case *array.List:
		start, end := x.ValueOffsets(i)
		child := x.ListValues()
		out := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, ValueAt(child, int(j)))
		}
		return out
	case *array.LargeList:
		start, end := x.ValueOffsets(i)
		child := x.ListValues()
		out := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, ValueAt(child, int(j)))
		}
		return out
	case *array.FixedSizeList:
		size := int(x.DataType().(*arrow.FixedSizeListType).Len())
		child := x.ListValues()
		base := (x.Data().Offset() + i) * size
		out := make([]any, size)
		for j := range size {
			out[j] = ValueAt(child, base+j)
		}
		return out
	case *array.Struct:
		st := x.DataType().(*arrow.StructType)
		out := make(map[string]any, st.NumFields())
		for f := range st.NumFields() {
			out[st.Field(f).Name] = ValueAt(x.Field(f), i)
		}
		return out
	}
	return a.ValueStr(i)
}

// ToList returns every value as a plain Go value (see ValueAt for the
// mapping). Nulls are nil. Mirrors polars' Series.to_list.
func (s *Series) ToList() []any {
	out := make([]any, 0, s.Len())
	for _, c := range s.Chunks() {
		for i := range c.Len() {
			out = append(out, ValueAt(c, i))
		}
	}
	return out
}

// Get returns the value at row i (negative i counts from the end), or
// nil for a null. Mirrors polars' Series.item(index) and s[i].
func (s *Series) Get(i int) (any, error) {
	n := s.Len()
	if i < 0 {
		i += n
	}
	if i < 0 || i >= n {
		return nil, fmt.Errorf("series: index %d out of bounds for length %d", i, n)
	}
	for _, c := range s.Chunks() {
		if i < c.Len() {
			return ValueAt(c, i), nil
		}
		i -= c.Len()
	}
	return nil, fmt.Errorf("series: index out of bounds")
}

// Item returns the single value of a length-1 Series. Any other length
// is an error. Mirrors polars' Series.item().
func (s *Series) Item() (any, error) {
	if s.Len() != 1 {
		return nil, fmt.Errorf("series: item() requires length 1, got %d", s.Len())
	}
	return s.Get(0)
}

// First returns the first value (nil when the Series is empty or the
// first value is null). Mirrors polars' Series.first().
func (s *Series) First() any {
	if s.Len() == 0 {
		return nil
	}
	v, _ := s.Get(0)
	return v
}

// Last returns the last value (nil when empty or null). Mirrors
// polars' Series.last().
func (s *Series) Last() any {
	if s.Len() == 0 {
		return nil
	}
	v, _ := s.Get(-1)
	return v
}

// Shape returns the one-element shape tuple (length,). Mirrors polars'
// Series.shape.
func (s *Series) Shape() [1]int { return [1]int{s.Len()} }

// NChunks is the polars-named alias of NumChunks.
func (s *Series) NChunks() int { return s.NumChunks() }

// ChunkLengths returns the length of every chunk. Mirrors polars'
// Series.chunk_lengths.
func (s *Series) ChunkLengths() []int {
	chunks := s.Chunks()
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = c.Len()
	}
	return out
}

// Clear returns an all-null Series of the same name and dtype with n
// rows (n=0 gives an empty Series). Mirrors polars' Series.clear.
func (s *Series) Clear(n int, opts ...Option) (*Series, error) {
	if n < 0 {
		return nil, fmt.Errorf("series: clear length must be non-negative, got %d", n)
	}
	return FullNull(s.name, s.data.DataType(), n, opts...)
}

// NewFromIndex returns a Series of length n where every value is the
// value at row index. Mirrors polars' Series.new_from_index.
func (s *Series) NewFromIndex(index, n int, opts ...Option) (*Series, error) {
	if index < 0 || index >= s.Len() {
		return nil, fmt.Errorf("series: index %d out of bounds for length %d", index, s.Len())
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = index
	}
	return s.Gather(idx, opts...)
}

// EqualsOptions tunes Equals. The zero value matches polars' defaults:
// names and dtypes are not compared, nulls compare equal.
type EqualsOptions struct {
	CheckNames  bool
	CheckDTypes bool
	// NullUnequal makes two nulls compare unequal (polars null_equal=False).
	NullUnequal bool
}

// Equals reports whether s and other hold the same values. Nulls compare
// equal to nulls and NaN compares equal to NaN. Numeric values compare
// by value across dtypes unless CheckDTypes is set. Mirrors polars'
// Series.equals.
func (s *Series) Equals(other *Series, opt ...EqualsOptions) bool {
	var o EqualsOptions
	if len(opt) > 0 {
		o = opt[0]
	}
	if s.Len() != other.Len() {
		return false
	}
	if o.CheckNames && s.Name() != other.Name() {
		return false
	}
	if o.CheckDTypes && !s.DType().Equal(other.DType()) {
		return false
	}
	a := s.ToList()
	b := other.ToList()
	for i := range a {
		if !valuesEqual(a[i], b[i], !o.NullUnequal) {
			return false
		}
	}
	return true
}

func valuesEqual(a, b any, nullEqual bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil && nullEqual
	}
	if fa, ok := numericAsFloat(a); ok {
		fb, ok := numericAsFloat(b)
		if !ok {
			return false
		}
		if math.IsNaN(fa) && math.IsNaN(fb) {
			return true
		}
		if ia, ok := a.(int64); ok {
			if ib, ok := b.(int64); ok {
				return ia == ib
			}
		}
		return fa == fb
	}
	switch x := a.(type) {
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !valuesEqual(x[i], y[i], true) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			if !valuesEqual(v, y[k], true) {
				return false
			}
		}
		return true
	case []byte:
		y, ok := b.([]byte)
		return ok && string(x) == string(y)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	}
	return a == b
}

func numericAsFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
