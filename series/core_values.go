package series

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// InferValuesDType picks the arrow dtype polars would infer for a list
// of Go values: integers become i64, any float makes the list f64,
// then bool and string. An all-nil list is the Null dtype.
func InferValuesDType(vals []any) arrow.DataType {
	var sawInt, sawFloat, sawBool, sawString, sawUint bool
	for _, v := range vals {
		switch v.(type) {
		case nil:
		case int, int8, int16, int32, int64:
			sawInt = true
		case uint, uint8, uint16, uint32, uint64:
			sawUint = true
		case float32, float64:
			sawFloat = true
		case bool:
			sawBool = true
		case string:
			sawString = true
		case time.Time:
			return arrow.FixedWidthTypes.Timestamp_us
		case time.Duration:
			return arrow.FixedWidthTypes.Duration_us
		}
	}
	switch {
	case sawString:
		return arrow.BinaryTypes.String
	case sawFloat:
		return arrow.PrimitiveTypes.Float64
	case sawInt:
		return arrow.PrimitiveTypes.Int64
	case sawUint:
		return arrow.PrimitiveTypes.Uint64
	case sawBool:
		return arrow.FixedWidthTypes.Boolean
	}
	return arrow.Null
}

// FromValues builds a Series of dtype dt from plain Go values. nil is a
// null. Numeric values convert to the target numeric type; a value that
// cannot be represented is an error.
func FromValues(name string, dt arrow.DataType, vals []any, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if dt.ID() == arrow.NULL {
		return New(name, array.MakeArrayOfNull(cfg.alloc, dt, len(vals)))
	}
	b := array.NewBuilder(cfg.alloc, dt)
	defer b.Release()
	b.Reserve(len(vals))
	for _, v := range vals {
		if err := appendValue(b, v); err != nil {
			return nil, err
		}
	}
	return New(name, b.NewArray())
}

func appendValue(b array.Builder, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch bb := b.(type) {
	case *array.Int8Builder:
		x, ok := anyToInt64(v)
		if !ok || x < math.MinInt8 || x > math.MaxInt8 {
			return fmt.Errorf("series: cannot store %v as i8", v)
		}
		bb.Append(int8(x))
	case *array.Int16Builder:
		x, ok := anyToInt64(v)
		if !ok || x < math.MinInt16 || x > math.MaxInt16 {
			return fmt.Errorf("series: cannot store %v as i16", v)
		}
		bb.Append(int16(x))
	case *array.Int32Builder:
		x, ok := anyToInt64(v)
		if !ok || x < math.MinInt32 || x > math.MaxInt32 {
			return fmt.Errorf("series: cannot store %v as i32", v)
		}
		bb.Append(int32(x))
	case *array.Int64Builder:
		x, ok := anyToInt64(v)
		if !ok {
			return fmt.Errorf("series: cannot store %v as i64", v)
		}
		bb.Append(x)
	case *array.Uint8Builder:
		x, ok := anyToInt64(v)
		if !ok || x < 0 || x > math.MaxUint8 {
			return fmt.Errorf("series: cannot store %v as u8", v)
		}
		bb.Append(uint8(x))
	case *array.Uint16Builder:
		x, ok := anyToInt64(v)
		if !ok || x < 0 || x > math.MaxUint16 {
			return fmt.Errorf("series: cannot store %v as u16", v)
		}
		bb.Append(uint16(x))
	case *array.Uint32Builder:
		x, ok := anyToInt64(v)
		if !ok || x < 0 || x > math.MaxUint32 {
			return fmt.Errorf("series: cannot store %v as u32", v)
		}
		bb.Append(uint32(x))
	case *array.Uint64Builder:
		if u, ok := v.(uint64); ok {
			bb.Append(u)
			return nil
		}
		x, ok := anyToInt64(v)
		if !ok || x < 0 {
			return fmt.Errorf("series: cannot store %v as u64", v)
		}
		bb.Append(uint64(x))
	case *array.Float32Builder:
		x, ok := anyToFloat64(v)
		if !ok {
			return fmt.Errorf("series: cannot store %v as f32", v)
		}
		bb.Append(float32(x))
	case *array.Float64Builder:
		x, ok := anyToFloat64(v)
		if !ok {
			return fmt.Errorf("series: cannot store %v as f64", v)
		}
		bb.Append(x)
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("series: cannot store %v as bool", v)
		}
		bb.Append(x)
	case *array.StringBuilder:
		x, ok := v.(string)
		if !ok {
			return fmt.Errorf("series: cannot store %v as str", v)
		}
		bb.Append(x)
	case *array.TimestampBuilder:
		unit := bb.Type().(*arrow.TimestampType).Unit
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Timestamp(x.UnixNano() / int64(unit.Multiplier())))
		default:
			i, ok := anyToInt64(v)
			if !ok {
				return fmt.Errorf("series: cannot store %v as datetime", v)
			}
			bb.Append(arrow.Timestamp(i))
		}
	case *array.DurationBuilder:
		unit := bb.Type().(*arrow.DurationType).Unit
		switch x := v.(type) {
		case time.Duration:
			bb.Append(arrow.Duration(int64(x) / int64(unit.Multiplier())))
		default:
			i, ok := anyToInt64(v)
			if !ok {
				return fmt.Errorf("series: cannot store %v as duration", v)
			}
			bb.Append(arrow.Duration(i))
		}
	case *array.Date32Builder:
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Date32FromTime(x))
		default:
			i, ok := anyToInt64(v)
			if !ok {
				return fmt.Errorf("series: cannot store %v as date", v)
			}
			bb.Append(arrow.Date32(i))
		}
	default:
		return fmt.Errorf("series: cannot build values of dtype %s", b.Type())
	}
	return nil
}

func anyToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > math.MaxInt64 {
			return 0, false
		}
		return int64(x), true
	case float64:
		if x != math.Trunc(x) || math.IsInf(x, 0) || math.IsNaN(x) {
			return 0, false
		}
		return int64(x), true
	case float32:
		f := float64(x)
		if f != math.Trunc(f) || math.IsInf(f, 0) || math.IsNaN(f) {
			return 0, false
		}
		return int64(f), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func anyToFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case uint64:
		return float64(x), true
	}
	if i, ok := anyToInt64(v); ok {
		return float64(i), true
	}
	return 0, false
}

// MapValue is the set of Go value types MapValues can read and write.
type MapValue interface {
	~int64 | ~int32 | ~uint32 | ~uint64 | ~float64 | ~float32 | ~bool | ~string
}

// MapValues applies fn to every non-null value. The input column's
// physical type must match In (int64 for i64, float64 for f64, and so
// on; integer columns may also be read as float64). The output dtype
// follows Out. Nulls stay null.
func MapValues[In, Out MapValue](s *Series, fn func(In) Out, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	read, err := readerFor[In](arr)
	if err != nil {
		return nil, err
	}
	n := arr.Len()
	out := make([]Out, n)
	var valid []bool
	if arr.NullN() > 0 {
		valid = make([]bool, n)
	}
	for i := range n {
		if valid != nil {
			if arr.IsNull(i) {
				continue
			}
			valid[i] = true
		}
		out[i] = fn(read(i))
	}
	return fromSlice(s.name, out, valid, cfg.alloc)
}

// readerFor returns a per-row accessor converting arr's values to T.
func readerFor[T MapValue](arr arrow.Array) (func(int) T, error) {
	var zero T
	switch any(zero).(type) {
	case int64:
		switch a := arr.(type) {
		case *array.Int64:
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		case *array.Int32:
			return func(i int) T { return bitCast[T](int64(a.Value(i))) }, nil
		case *array.Int16:
			return func(i int) T { return bitCast[T](int64(a.Value(i))) }, nil
		case *array.Int8:
			return func(i int) T { return bitCast[T](int64(a.Value(i))) }, nil
		case *array.Uint32:
			return func(i int) T { return bitCast[T](int64(a.Value(i))) }, nil
		}
	case int32:
		if a, ok := arr.(*array.Int32); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	case uint32:
		if a, ok := arr.(*array.Uint32); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	case uint64:
		if a, ok := arr.(*array.Uint64); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	case float64:
		switch a := arr.(type) {
		case *array.Float64:
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		case *array.Float32:
			return func(i int) T { return bitCast[T](float64(a.Value(i))) }, nil
		case *array.Int64:
			return func(i int) T { return bitCast[T](float64(a.Value(i))) }, nil
		case *array.Int32:
			return func(i int) T { return bitCast[T](float64(a.Value(i))) }, nil
		}
	case float32:
		if a, ok := arr.(*array.Float32); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	case bool:
		if a, ok := arr.(*array.Boolean); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	case string:
		if a, ok := arr.(*array.String); ok {
			return func(i int) T { return bitCast[T](a.Value(i)) }, nil
		}
	}
	return nil, fmt.Errorf("series: cannot read %s values as %T", arr.DataType(), zero)
}

// fromSlice wraps a typed slice as a Series of the matching dtype.
func fromSlice[T MapValue](name string, vals []T, valid []bool, mem memory.Allocator) (*Series, error) {
	var zero T
	switch any(zero).(type) {
	case int64:
		return buildFixedValid(name, arrow.PrimitiveTypes.Int64, len(vals), valid, mem, func(out []int64) {
			for i, v := range vals {
				out[i] = bitCast[int64](v)
			}
		})
	case int32:
		return buildFixedValid(name, arrow.PrimitiveTypes.Int32, len(vals), valid, mem, func(out []int32) {
			for i, v := range vals {
				out[i] = bitCast[int32](v)
			}
		})
	case uint32:
		return buildFixedValid(name, arrow.PrimitiveTypes.Uint32, len(vals), valid, mem, func(out []uint32) {
			for i, v := range vals {
				out[i] = bitCast[uint32](v)
			}
		})
	case uint64:
		return buildFixedValid(name, arrow.PrimitiveTypes.Uint64, len(vals), valid, mem, func(out []uint64) {
			for i, v := range vals {
				out[i] = bitCast[uint64](v)
			}
		})
	case float64:
		return buildFixedValid(name, arrow.PrimitiveTypes.Float64, len(vals), valid, mem, func(out []float64) {
			for i, v := range vals {
				out[i] = bitCast[float64](v)
			}
		})
	case float32:
		return buildFixedValid(name, arrow.PrimitiveTypes.Float32, len(vals), valid, mem, func(out []float32) {
			for i, v := range vals {
				out[i] = bitCast[float32](v)
			}
		})
	case bool:
		bs := make([]bool, len(vals))
		for i, v := range vals {
			bs[i] = bitCast[bool](v)
		}
		return FromBool(name, bs, valid, WithAllocator(mem))
	case string:
		ss := make([]string, len(vals))
		for i, v := range vals {
			ss[i] = bitCast[string](v)
		}
		return FromString(name, ss, valid, WithAllocator(mem))
	}
	return nil, fmt.Errorf("series: unsupported output type %T", zero)
}

// bitCast reinterprets v as T. Callers guarantee T and U share one
// underlying type (checked by a type switch on the zero value).
func bitCast[T, U any](v U) T { return *(*T)(unsafe.Pointer(&v)) }
