package dataframe

import (
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// cellValue returns element i of a as a Go value, or nil for null.
// Integers keep their width (int8, uint16, ...), floats are float32 or
// float64, strings are string, binaries are []byte, dates and datetimes
// are time.Time in UTC, durations are time.Duration, times are
// time.Duration since midnight, lists are []any and structs are
// map[string]any.
func cellValue(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch v := a.(type) {
	case *array.Boolean:
		return v.Value(i)
	case *array.Int8:
		return v.Value(i)
	case *array.Int16:
		return v.Value(i)
	case *array.Int32:
		return v.Value(i)
	case *array.Int64:
		return v.Value(i)
	case *array.Uint8:
		return v.Value(i)
	case *array.Uint16:
		return v.Value(i)
	case *array.Uint32:
		return v.Value(i)
	case *array.Uint64:
		return v.Value(i)
	case *array.Float32:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.String:
		return v.Value(i)
	case *array.LargeString:
		return v.Value(i)
	case *array.Binary:
		return append([]byte(nil), v.Value(i)...)
	case *array.LargeBinary:
		return append([]byte(nil), v.Value(i)...)
	case *array.Date32:
		return time.Unix(int64(v.Value(i))*86400, 0).UTC()
	case *array.Date64:
		return time.UnixMilli(int64(v.Value(i))).UTC()
	case *array.Timestamp:
		unit := v.DataType().(*arrow.TimestampType).Unit
		return v.Value(i).ToTime(unit)
	case *array.Duration:
		unit := v.DataType().(*arrow.DurationType).Unit
		return time.Duration(int64(v.Value(i)) * int64(unit.Multiplier()))
	case *array.Time32:
		unit := v.DataType().(*arrow.Time32Type).Unit
		return time.Duration(int64(v.Value(i)) * int64(unit.Multiplier()))
	case *array.Time64:
		unit := v.DataType().(*arrow.Time64Type).Unit
		return time.Duration(int64(v.Value(i)) * int64(unit.Multiplier()))
	case array.ListLike:
		start, end := v.ValueOffsets(i)
		inner := v.ListValues()
		out := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, cellValue(inner, int(j)))
		}
		return out
	case *array.Struct:
		st := v.DataType().(*arrow.StructType)
		out := make(map[string]any, st.NumFields())
		for f := range st.NumFields() {
			out[st.Field(f).Name] = cellValue(v.Field(f), i)
		}
		return out
	}
	return a.ValueStr(i)
}

// appendGoValue appends v to b converting between Go and arrow types the
// way a polars literal would be cast to the column dtype. nil appends a
// null. Lossy conversions (a fractional float into an integer column, an
// out-of-range integer) return an error.
func appendGoValue(b array.Builder, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("cannot use %T as bool", v)
		}
		bb.Append(x)
		return nil
	case *array.StringBuilder:
		x, ok := v.(string)
		if !ok {
			return fmt.Errorf("cannot use %T as str", v)
		}
		bb.Append(x)
		return nil
	case *array.LargeStringBuilder:
		x, ok := v.(string)
		if !ok {
			return fmt.Errorf("cannot use %T as str", v)
		}
		bb.Append(x)
		return nil
	case *array.BinaryBuilder:
		switch x := v.(type) {
		case []byte:
			bb.Append(x)
		case string:
			bb.AppendString(x)
		default:
			return fmt.Errorf("cannot use %T as binary", v)
		}
		return nil
	case *array.Float32Builder:
		f, ok := goFloat(v)
		if !ok {
			return fmt.Errorf("cannot use %T as f32", v)
		}
		bb.Append(float32(f))
		return nil
	case *array.Float64Builder:
		f, ok := goFloat(v)
		if !ok {
			return fmt.Errorf("cannot use %T as f64", v)
		}
		bb.Append(f)
		return nil
	case *array.Date32Builder:
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Date32FromTime(x))
			return nil
		}
	case *array.TimestampBuilder:
		if x, ok := v.(time.Time); ok {
			unit := bb.Type().(*arrow.TimestampType).Unit
			ts, err := arrow.TimestampFromTime(x, unit)
			if err != nil {
				return err
			}
			bb.Append(ts)
			return nil
		}
	case *array.DurationBuilder:
		if x, ok := v.(time.Duration); ok {
			unit := bb.Type().(*arrow.DurationType).Unit
			bb.Append(arrow.Duration(int64(x) / int64(unit.Multiplier())))
			return nil
		}
	}
	iv, uv, isUnsigned, ok := goInteger(v)
	if !ok {
		return fmt.Errorf("cannot use %T as %s", v, b.Type())
	}
	fits := func(lo, hi int64) bool {
		if isUnsigned {
			return uv <= uint64(hi)
		}
		return iv >= lo && iv <= hi
	}
	switch bb := b.(type) {
	case *array.Int8Builder:
		if !fits(math.MinInt8, math.MaxInt8) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(int8(iv))
	case *array.Int16Builder:
		if !fits(math.MinInt16, math.MaxInt16) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(int16(iv))
	case *array.Int32Builder:
		if !fits(math.MinInt32, math.MaxInt32) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(int32(iv))
	case *array.Int64Builder:
		if isUnsigned && uv > math.MaxInt64 {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(iv)
	case *array.Uint8Builder:
		if !fits(0, math.MaxUint8) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(uint8(uv))
	case *array.Uint16Builder:
		if !fits(0, math.MaxUint16) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(uint16(uv))
	case *array.Uint32Builder:
		if !fits(0, math.MaxUint32) {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(uint32(uv))
	case *array.Uint64Builder:
		if !isUnsigned && iv < 0 {
			return errOutOfRange(v, b.Type())
		}
		bb.Append(uv)
	case *array.Date32Builder:
		bb.Append(arrow.Date32(int32(iv)))
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(iv))
	case *array.DurationBuilder:
		bb.Append(arrow.Duration(iv))
	case *array.Time32Builder:
		bb.Append(arrow.Time32(int32(iv)))
	case *array.Time64Builder:
		bb.Append(arrow.Time64(iv))
	default:
		return fmt.Errorf("cannot use %T as %s", v, b.Type())
	}
	return nil
}

func errOutOfRange(v any, dt arrow.DataType) error {
	return fmt.Errorf("value %v does not fit in %s", v, dt)
}

// goInteger normalises Go integer kinds. For unsigned inputs uv holds the
// value and iv its int64 reinterpretation; for signed inputs both hold
// the value (uv reinterpreted).
func goInteger(v any) (iv int64, uv uint64, unsigned bool, ok bool) {
	switch x := v.(type) {
	case int:
		return int64(x), uint64(x), false, true
	case int8:
		return int64(x), uint64(x), false, true
	case int16:
		return int64(x), uint64(x), false, true
	case int32:
		return int64(x), uint64(x), false, true
	case int64:
		return x, uint64(x), false, true
	case uint:
		return int64(x), uint64(x), true, true
	case uint8:
		return int64(x), uint64(x), true, true
	case uint16:
		return int64(x), uint64(x), true, true
	case uint32:
		return int64(x), uint64(x), true, true
	case uint64:
		return int64(x), x, true, true
	case bool:
		if x {
			return 1, 1, false, true
		}
		return 0, 0, false, true
	}
	return 0, 0, false, false
}

func goFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	}
	if iv, uv, unsigned, ok := goInteger(v); ok {
		if unsigned {
			return float64(uv), true
		}
		return float64(iv), true
	}
	return 0, false
}

// constantArray returns an array of n copies of v with dtype dt. A nil v
// yields an all-null array.
func constantArray(dt arrow.DataType, v any, n int, mem memory.Allocator) (arrow.Array, error) {
	if v == nil || dt.ID() == arrow.NULL {
		return array.MakeArrayOfNull(mem, dt, n), nil
	}
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.Reserve(n)
	if n == 0 {
		// Validate the value even for empty outputs so errors surface
		// consistently.
		tmp := array.NewBuilder(mem, dt)
		defer tmp.Release()
		if err := appendGoValue(tmp, v); err != nil {
			return nil, err
		}
		return b.NewArray(), nil
	}
	for range n {
		if err := appendGoValue(b, v); err != nil {
			return nil, err
		}
	}
	return b.NewArray(), nil
}
