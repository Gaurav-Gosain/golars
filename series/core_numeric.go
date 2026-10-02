package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// isIntType reports whether dt is a signed or unsigned integer.
func isIntType(dt arrow.DataType) bool { return arrow.IsInteger(dt.ID()) }

// isFloatType reports whether dt is f32 or f64.
func isFloatType(dt arrow.DataType) bool {
	return dt.ID() == arrow.FLOAT32 || dt.ID() == arrow.FLOAT64
}

// isNumericType reports whether dt is an integer or float.
func isNumericType(dt arrow.DataType) bool { return isIntType(dt) || isFloatType(dt) }

// floatVals copies the values of a numeric or boolean array into a
// []float64. Null slots hold 0; consult arr for validity.
func floatVals(arr arrow.Array) ([]float64, error) {
	n := arr.Len()
	out := make([]float64, n)
	switch a := arr.(type) {
	case *array.Float64:
		copy(out, a.Float64Values())
		return out, nil
	case *array.Float32:
		for i, v := range a.Float32Values() {
			out[i] = float64(v)
		}
		return out, nil
	case *array.Boolean:
		for i := range n {
			if a.Value(i) {
				out[i] = 1
			}
		}
		return out, nil
	}
	switch arr.DataType().ID() {
	case arrow.INT8:
		convertInto(out, fixedValues[int8](arr))
	case arrow.INT16:
		convertInto(out, fixedValues[int16](arr))
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		convertInto(out, fixedValues[int32](arr))
	case arrow.INT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		convertInto(out, fixedValues[int64](arr))
	case arrow.UINT8:
		convertInto(out, fixedValues[uint8](arr))
	case arrow.UINT16:
		convertInto(out, fixedValues[uint16](arr))
	case arrow.UINT32:
		convertInto(out, fixedValues[uint32](arr))
	case arrow.UINT64:
		convertInto(out, fixedValues[uint64](arr))
	case arrow.NULL:
	default:
		return nil, fmt.Errorf("series: expected numeric input, got %s", arr.DataType())
	}
	return out, nil
}

func convertInto[T fixedWidth](dst []float64, src []T) {
	for i, v := range src {
		dst[i] = float64(v)
	}
}

// intVals copies an integer or boolean array into []int64. Unsigned
// values above MaxInt64 wrap, which callers avoid by checking dtypes.
func intVals(arr arrow.Array) ([]int64, error) {
	n := arr.Len()
	out := make([]int64, n)
	if a, ok := arr.(*array.Boolean); ok {
		for i := range n {
			if a.Value(i) {
				out[i] = 1
			}
		}
		return out, nil
	}
	switch arr.DataType().ID() {
	case arrow.INT8:
		intInto(out, fixedValues[int8](arr))
	case arrow.INT16:
		intInto(out, fixedValues[int16](arr))
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		intInto(out, fixedValues[int32](arr))
	case arrow.INT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		copy(out, fixedValues[int64](arr))
	case arrow.UINT8:
		intInto(out, fixedValues[uint8](arr))
	case arrow.UINT16:
		intInto(out, fixedValues[uint16](arr))
	case arrow.UINT32:
		intInto(out, fixedValues[uint32](arr))
	case arrow.UINT64:
		intInto(out, fixedValues[uint64](arr))
	case arrow.NULL:
	default:
		return nil, fmt.Errorf("series: expected integer input, got %s", arr.DataType())
	}
	return out, nil
}

func intInto[T fixedWidth](dst []int64, src []T) {
	for i, v := range src {
		dst[i] = int64(v)
	}
}

// validSlice returns a []bool validity mask for arr, or nil when arr has
// no nulls.
func validSlice(arr arrow.Array) []bool {
	if arr.NullN() == 0 {
		return nil
	}
	out := make([]bool, arr.Len())
	for i := range out {
		out[i] = arr.IsValid(i)
	}
	return out
}

// andValid combines two validity masks (nil means all valid).
func andValid(a, b []bool) []bool {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	out := make([]bool, len(a))
	for i := range out {
		out[i] = a[i] && b[i]
	}
	return out
}

// floatSeries builds an f64 (or f32 when asF32) Series from values.
func floatSeries(name string, vals []float64, valid []bool, asF32 bool, mem memory.Allocator) (*Series, error) {
	if asF32 {
		return buildFixedValid(name, arrow.PrimitiveTypes.Float32, len(vals), valid, mem, func(out []float32) {
			for i, v := range vals {
				out[i] = float32(v)
			}
		})
	}
	return buildFixedValid(name, arrow.PrimitiveTypes.Float64, len(vals), valid, mem, func(out []float64) {
		copy(out, vals)
	})
}

// scalarFloat builds a length-1 float Series; ok=false gives a null.
func scalarFloat(name string, v float64, ok bool, asF32 bool, mem memory.Allocator) (*Series, error) {
	var valid []bool
	if !ok {
		valid = []bool{false}
	}
	return floatSeries(name, []float64{v}, valid, asF32, mem)
}

// ScalarUint32 builds a length-1 u32 Series; ok=false gives a null.
func ScalarUint32(name string, v int, ok bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	var valid []bool
	if !ok {
		valid = []bool{false}
	}
	return buildFixedValid(name, arrow.PrimitiveTypes.Uint32, 1, valid, cfg.alloc, func(out []uint32) {
		out[0] = uint32(v)
	})
}

// ScalarBool builds a length-1 bool Series.
func ScalarBool(name string, v bool, opts ...Option) (*Series, error) {
	return FromBool(name, []bool{v}, nil, opts...)
}

// u32Series builds a u32 Series from ints with an optional mask.
func u32Series(name string, vals []int, valid []bool, mem memory.Allocator) (*Series, error) {
	return buildFixedValid(name, arrow.PrimitiveTypes.Uint32, len(vals), valid, mem, func(out []uint32) {
		for i, v := range vals {
			out[i] = uint32(v)
		}
	})
}

// int64Series builds an i64 Series with an optional mask.
func int64Series(name string, vals []int64, valid []bool, mem memory.Allocator) (*Series, error) {
	return buildFixedValid(name, arrow.PrimitiveTypes.Int64, len(vals), valid, mem, func(out []int64) {
		copy(out, vals)
	})
}

// Float64Values copies a numeric or boolean Series into []float64 and
// returns the validity mask (nil when there are no nulls).
func Float64Values(s *Series) ([]float64, []bool, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, nil, err
	}
	defer arr.Release()
	v, err := floatVals(arr)
	return v, validSlice(arr), err
}

// Int64Values copies an integer or boolean Series into []int64 and
// returns the validity mask (nil when there are no nulls).
func Int64Values(s *Series) ([]int64, []bool, error) {
	arr, err := s.single(nil)
	if err != nil {
		return nil, nil, err
	}
	defer arr.Release()
	v, err := intVals(arr)
	return v, validSlice(arr), err
}
