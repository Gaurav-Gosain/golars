package difftest

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// valueAt decodes element i of arr into the canonical representation.
func valueAt(arr arrow.Array, i int) any {
	if arr.IsNull(i) {
		return nil
	}
	switch a := arr.(type) {
	case *array.Null:
		return nil
	case *array.Boolean:
		return a.Value(i)
	case *array.Int8:
		return int64(a.Value(i))
	case *array.Int16:
		return int64(a.Value(i))
	case *array.Int32:
		return int64(a.Value(i))
	case *array.Int64:
		return a.Value(i)
	case *array.Uint8:
		return uint64(a.Value(i))
	case *array.Uint16:
		return uint64(a.Value(i))
	case *array.Uint32:
		return uint64(a.Value(i))
	case *array.Uint64:
		return a.Value(i)
	case *array.Float16:
		return float64(a.Value(i).Float32())
	case *array.Float32:
		return float64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.String:
		return strings.Clone(a.Value(i))
	case *array.LargeString:
		return strings.Clone(a.Value(i))
	case *array.StringView:
		return strings.Clone(a.Value(i))
	case *array.Binary:
		return string(a.Value(i))
	case *array.LargeBinary:
		return string(a.Value(i))
	case *array.BinaryView:
		return string(a.Value(i))
	case *array.Date32:
		return int64(a.Value(i))
	case *array.Date64:
		return int64(a.Value(i)) / 86400000
	case *array.Time32:
		return int64(a.Value(i))
	case *array.Time64:
		return int64(a.Value(i))
	case *array.Timestamp:
		return int64(a.Value(i))
	case *array.Duration:
		return int64(a.Value(i))
	case *array.List:
		start, end := a.ValueOffsets(i)
		return sliceValues(a.ListValues(), int(start), int(end))
	case *array.LargeList:
		start, end := a.ValueOffsets(i)
		return sliceValues(a.ListValues(), int(start), int(end))
	case *array.FixedSizeList:
		start, end := a.ValueOffsets(i)
		return sliceValues(a.ListValues(), int(start), int(end))
	case *array.Struct:
		out := make([]any, a.NumField())
		for f := range out {
			out[f] = valueAt(a.Field(f), i)
		}
		return out
	case *array.Dictionary:
		return valueAt(a.Dictionary(), a.GetValueIndex(i))
	}
	return fmt.Sprintf("<unsupported %s>", arr.DataType())
}

func sliceValues(arr arrow.Array, start, end int) []any {
	out := make([]any, end-start)
	for j := range out {
		out[j] = valueAt(arr, start+j)
	}
	return out
}
