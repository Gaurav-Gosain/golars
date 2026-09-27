package compute

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// catTake gathers rows of a dictionary array (Categorical or Enum). Only
// the uint32 codes move; the output shares the source dictionary.
func catTake(name string, src arrow.Array, indices []int, mem memory.Allocator) (*series.Series, error) {
	d := src.(*array.Dictionary)
	idx := d.Indices()
	if _, ok := idx.(*array.Uint32); !ok {
		// Foreign index width (for example from another arrow producer):
		// gather through the slower builder path, which emits uint32 codes.
		return catTakeGeneric(name, d, indices, mem)
	}
	codes, err := takeNumeric("codes", idx, uint32Values(idx), indices, mem, fromUint32Result)
	if err != nil {
		return nil, err
	}
	defer codes.Release()
	out := array.NewDictionaryArray(d.DataType(), codes.Chunk(0), d.Dictionary())
	return series.New(name, out)
}

// catTakeGeneric handles dictionaries with a non-uint32 index type
// (for example from a foreign IPC file) by building the gathered codes
// with an arrow builder.
func catTakeGeneric(name string, d *array.Dictionary, indices []int, mem memory.Allocator) (*series.Series, error) {
	b := array.NewUint32Builder(mem)
	defer b.Release()
	b.Reserve(len(indices))
	for _, i := range indices {
		if d.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.UnsafeAppend(uint32(d.GetValueIndex(i)))
	}
	codes := b.NewUint32Array()
	defer codes.Release()
	dt := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint32,
		ValueType: d.Dictionary().DataType(),
		Ordered:   d.DataType().(*arrow.DictionaryType).Ordered,
	}
	return series.New(name, array.NewDictionaryArray(dt, codes, d.Dictionary()))
}
