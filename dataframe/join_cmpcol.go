package dataframe

import (
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// cmpKind is the comparison domain a column is mapped into for the
// sort-based joins (join_asof and join_where).
type cmpKind uint8

const (
	cmpInt cmpKind = iota
	cmpFloat
	cmpStr
)

// cmpCol is a read-only view of one column in its comparison domain.
// Integer, boolean and temporal columns become int64 values, floats
// become float64 values and strings are read in place. Building a view
// copies at most one value slice; nothing is allocated per row later.
type cmpCol struct {
	kind   cmpKind
	ints   []int64
	floats []float64
	strs   stringValuer
	arr    arrow.Array // validity source; nil for literals
	nulls  bool
	dt     arrow.DataType
}

type stringValuer interface {
	Value(int) string
}

func (c *cmpCol) isNull(i int) bool {
	return c.nulls && c.arr.IsNull(i)
}

// newCmpCol builds a comparison view over s. The returned view borrows
// s's buffers, so s must outlive it.
func newCmpCol(s *series.Series) (cmpCol, error) {
	arr := chunk0(s)
	c := cmpCol{arr: arr, nulls: arr.NullN() > 0, dt: arr.DataType()}
	n := arr.Len()
	switch a := arr.(type) {
	case *array.Int64:
		c.ints = a.Int64Values()
	case *array.Int32:
		c.ints = widen(a.Int32Values())
	case *array.Int16:
		c.ints = widen(a.Int16Values())
	case *array.Int8:
		c.ints = widen(a.Int8Values())
	case *array.Uint64:
		c.ints = widen(a.Uint64Values())
	case *array.Uint32:
		c.ints = widen(a.Uint32Values())
	case *array.Uint16:
		c.ints = widen(a.Uint16Values())
	case *array.Uint8:
		c.ints = widen(a.Uint8Values())
	case *array.Timestamp:
		c.ints = widen(a.TimestampValues())
	case *array.Date32:
		c.ints = widen(a.Date32Values())
	case *array.Date64:
		c.ints = widen(a.Date64Values())
	case *array.Duration:
		c.ints = widen(a.DurationValues())
	case *array.Time32:
		c.ints = widen(a.Time32Values())
	case *array.Time64:
		c.ints = widen(a.Time64Values())
	case *array.Boolean:
		c.ints = make([]int64, n)
		for i := range n {
			if a.Value(i) {
				c.ints[i] = 1
			}
		}
	case *array.Float64:
		c.kind = cmpFloat
		c.floats = a.Float64Values()
	case *array.Float32:
		c.kind = cmpFloat
		c.floats = widenFloat(a.Float32Values())
	case *array.String:
		c.kind = cmpStr
		c.strs = a
	case *array.LargeString:
		c.kind = cmpStr
		c.strs = a
	default:
		return cmpCol{}, fmt.Errorf("dataframe: unsupported comparison dtype %s", s.DType())
	}
	return c, nil
}

// chunk0 returns the single consolidated chunk of s, or an empty array
// for a Series without chunks (the shape DataFrame.Clear produces).
func chunk0(s *series.Series) arrow.Array {
	if s.NumChunks() == 0 {
		return array.MakeArrayOfNull(memory.DefaultAllocator, s.DType().Arrow(), 0)
	}
	return s.Chunk(0)
}

type intLike interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func widen[T intLike](src []T) []int64 {
	out := make([]int64, len(src))
	for i, v := range src {
		out[i] = int64(v)
	}
	return out
}

func widenFloat(src []float32) []float64 {
	out := make([]float64, len(src))
	for i, v := range src {
		out[i] = float64(v)
	}
	return out
}

// litCmpCol wraps a scalar literal as a one-row comparison view.
func litCmpCol(v any) (cmpCol, error) {
	switch x := v.(type) {
	case int:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case int8:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case int16:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case int32:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case int64:
		return cmpCol{kind: cmpInt, ints: []int64{x}}, nil
	case uint8:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case uint16:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case uint32:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case uint64:
		return cmpCol{kind: cmpInt, ints: []int64{int64(x)}}, nil
	case float32:
		return cmpCol{kind: cmpFloat, floats: []float64{float64(x)}}, nil
	case float64:
		return cmpCol{kind: cmpFloat, floats: []float64{x}}, nil
	case bool:
		b := int64(0)
		if x {
			b = 1
		}
		return cmpCol{kind: cmpInt, ints: []int64{b}}, nil
	case string:
		return cmpCol{kind: cmpStr, strs: constString(x)}, nil
	}
	return cmpCol{}, fmt.Errorf("dataframe: unsupported literal %v (%T) in join predicate", v, v)
}

type constString string

func (c constString) Value(int) string { return string(c) }

// floatAt reads a numeric view as float64 so int and float operands can
// be compared against literals.
func (c *cmpCol) floatAt(i int) float64 {
	if c.kind == cmpFloat {
		return c.floats[i]
	}
	return float64(c.ints[i])
}

// compareTotal orders two non-null values. Floats use polars' total
// order: NaN compares equal to NaN and greater than every other value.
func compareTotal(a *cmpCol, i int, b *cmpCol, j int) int {
	switch {
	case a.kind == cmpInt && b.kind == cmpInt:
		x, y := a.ints[i], b.ints[j]
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
		return 0
	case a.kind == cmpStr && b.kind == cmpStr:
		return strings.Compare(a.strs.Value(i), b.strs.Value(j))
	}
	return compareFloatTotal(a.floatAt(i), b.floatAt(j))
}

func compareFloatTotal(x, y float64) int {
	xn, yn := math.IsNaN(x), math.IsNaN(y)
	switch {
	case xn && yn:
		return 0
	case xn:
		return 1
	case yn:
		return -1
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}
