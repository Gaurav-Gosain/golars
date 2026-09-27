package dataframe_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// fxCol builds a Series of dtype dt from Go values; nil means null. It
// accepts ints for every integer, date (days), datetime and duration
// dtype, float64 for floats, string, bool, time.Time for dates and
// datetimes, and []any for list[inner] dtypes.
func fxCol(t testing.TB, mem memory.Allocator, name string, dt arrow.DataType, vals ...any) *series.Series {
	t.Helper()
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for _, v := range vals {
		fxAppend(t, b, v)
	}
	arr := b.NewArray()
	s, err := series.New(name, arr)
	if err != nil {
		arr.Release()
		t.Fatal(err)
	}
	return s
}

func fxAppend(t testing.TB, b array.Builder, v any) {
	t.Helper()
	if v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.Int8Builder:
		bb.Append(int8(v.(int)))
	case *array.Int16Builder:
		bb.Append(int16(v.(int)))
	case *array.Int32Builder:
		bb.Append(int32(v.(int)))
	case *array.Int64Builder:
		bb.Append(int64(v.(int)))
	case *array.Uint8Builder:
		bb.Append(uint8(v.(int)))
	case *array.Uint16Builder:
		bb.Append(uint16(v.(int)))
	case *array.Uint32Builder:
		bb.Append(uint32(v.(int)))
	case *array.Uint64Builder:
		bb.Append(uint64(v.(int)))
	case *array.Float32Builder:
		bb.Append(float32(v.(float64)))
	case *array.Float64Builder:
		bb.Append(v.(float64))
	case *array.StringBuilder:
		bb.Append(v.(string))
	case *array.BooleanBuilder:
		bb.Append(v.(bool))
	case *array.Date32Builder:
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Date32FromTime(x))
		default:
			bb.Append(arrow.Date32(int32(v.(int))))
		}
	case *array.TimestampBuilder:
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Timestamp(x.UnixMicro()))
		default:
			bb.Append(arrow.Timestamp(int64(v.(int))))
		}
	case *array.DurationBuilder:
		switch x := v.(type) {
		case time.Duration:
			bb.Append(arrow.Duration(x.Microseconds()))
		default:
			bb.Append(arrow.Duration(int64(v.(int))))
		}
	case *array.NullBuilder:
		bb.AppendNull()
	case *array.ListBuilder:
		bb.Append(true)
		for _, x := range v.([]any) {
			fxAppend(t, bb.ValueBuilder(), x)
		}
	default:
		t.Fatalf("fxAppend: unsupported builder %T", b)
	}
}

// fxFrame assembles a DataFrame from columns and fails the test on error.
func fxFrame(t testing.TB, cols ...*series.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func fxDay(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

var (
	fxI8   = arrow.PrimitiveTypes.Int8
	fxI16  = arrow.PrimitiveTypes.Int16
	fxI32  = arrow.PrimitiveTypes.Int32
	fxI64  = arrow.PrimitiveTypes.Int64
	fxU8   = arrow.PrimitiveTypes.Uint8
	fxU16  = arrow.PrimitiveTypes.Uint16
	fxU32  = arrow.PrimitiveTypes.Uint32
	fxU64  = arrow.PrimitiveTypes.Uint64
	fxF32  = arrow.PrimitiveTypes.Float32
	fxF64  = arrow.PrimitiveTypes.Float64
	fxStr  = arrow.BinaryTypes.String
	fxBool = arrow.FixedWidthTypes.Boolean
	fxDate = arrow.FixedWidthTypes.Date32
	fxDT   = &arrow.TimestampType{Unit: arrow.Microsecond}
	fxDur  = &arrow.DurationType{Unit: arrow.Microsecond}
	fxNull = arrow.Null
)
