package compute

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// wrapInt64To narrows an i64 Series to the integer dtype to with two's
// complement wrapping, the way polars integer arithmetic overflows
// (u8 1 - 2 is 255). Nulls are kept.
func wrapInt64To(s *series.Series, to arrow.DataType, name string, mem memory.Allocator) (*series.Series, error) {
	arr, err := extractChunk(s, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	src, ok := arr.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("compute: wrapInt64To needs i64 input, got %s", arr.DataType())
	}
	b := array.NewBuilder(mem, to)
	defer b.Release()
	b.Reserve(src.Len())
	return wrapInt64Generic(src, b, name)
}

func wrapInt64Generic(src *array.Int64, b array.Builder, name string) (*series.Series, error) {
	vals := src.Int64Values()
	for i, v := range vals {
		if !src.IsValid(i) {
			b.AppendNull()
			continue
		}
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(int8(v))
		case *array.Int16Builder:
			bb.Append(int16(v))
		case *array.Int32Builder:
			bb.Append(int32(v))
		case *array.Int64Builder:
			bb.Append(v)
		case *array.Uint8Builder:
			bb.Append(uint8(v))
		case *array.Uint16Builder:
			bb.Append(uint16(v))
		case *array.Uint32Builder:
			bb.Append(uint32(v))
		case *array.Uint64Builder:
			bb.Append(uint64(v))
		default:
			return nil, fmt.Errorf("compute: cannot wrap i64 to %s", b.Type())
		}
	}
	out := b.NewArray()
	s, err := series.New(name, out)
	if err != nil {
		out.Release()
	}
	return s, err
}
