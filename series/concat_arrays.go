package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ConcatArrays concatenates chunks of one dtype into a single array.
// Unlike array.Concatenate it accepts categorical chunks whose
// dictionaries differ (a frame assembled from pieces): the
// dictionaries are unified and the indices remapped first. The caller
// owns the result.
func ConcatArrays(chunks []arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if len(chunks) > 1 && chunks[0].DataType().ID() == arrow.DICTIONARY {
		ch := arrow.NewChunked(chunks[0].DataType(), chunks)
		defer ch.Release()
		unified, err := array.UnifyChunkedDicts(mem, ch)
		if err != nil {
			return nil, err
		}
		defer unified.Release()
		return array.Concatenate(unified.Chunks(), mem)
	}
	return array.Concatenate(chunks, mem)
}
