package series

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// Categorical and Enum columns are arrow dictionary arrays with uint32
// codes and a utf8 dictionary (see dtype.Categorical). The helpers in
// this file convert between plain string arrays and dictionary arrays
// without per-row allocations: codes and offsets are written straight
// into allocator-backed buffers.

// CatEncodeStrings builds a Categorical dictionary array from a utf8
// array. Codes are assigned in order of first appearance, matching
// polars. Nulls stay null and do not create a category.
func CatEncodeStrings(a *array.String, mem memory.Allocator) *array.Dictionary {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	n := a.Len()
	codesBuf := memory.NewResizableBuffer(mem)
	codesBuf.Resize(n * 4)
	codes := arrow.Uint32Traits.CastFromBytes(codesBuf.Bytes())
	lookup := make(map[string]uint32)
	var uniq []string
	hasNulls := a.NullN() > 0
	for i := range n {
		if hasNulls && a.IsNull(i) {
			codes[i] = 0
			continue
		}
		v := a.Value(i)
		c, ok := lookup[v]
		if !ok {
			c = uint32(len(uniq))
			lookup[v] = c
			uniq = append(uniq, v)
		}
		codes[i] = c
	}
	dict := catStringArray(uniq, mem)
	defer dict.Release()
	return catAssemble(dtype.Categorical().Arrow(), a, codesBuf, dict, mem)
}

// CatEncodeEnum builds an Enum dictionary array from a utf8 array and
// the enum's fixed categories. Values missing from categories are
// returned in unknown (deduplicated, first-appearance order) together
// with their row count; the array is nil in that case.
func CatEncodeEnum(a *array.String, categories []string, mem memory.Allocator) (out *array.Dictionary, unknown []string, nUnknown int) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	lookup := make(map[string]uint32, len(categories))
	for i, c := range categories {
		lookup[c] = uint32(i)
	}
	n := a.Len()
	codesBuf := memory.NewResizableBuffer(mem)
	codesBuf.Resize(n * 4)
	codes := arrow.Uint32Traits.CastFromBytes(codesBuf.Bytes())
	var seenUnknown map[string]struct{}
	hasNulls := a.NullN() > 0
	for i := range n {
		if hasNulls && a.IsNull(i) {
			codes[i] = 0
			continue
		}
		v := a.Value(i)
		c, ok := lookup[v]
		if !ok {
			nUnknown++
			if seenUnknown == nil {
				seenUnknown = map[string]struct{}{}
			}
			if _, dup := seenUnknown[v]; !dup {
				seenUnknown[v] = struct{}{}
				unknown = append(unknown, strings.Clone(v))
			}
			continue
		}
		codes[i] = c
	}
	if nUnknown > 0 {
		codesBuf.Release()
		return nil, unknown, nUnknown
	}
	dict := catStringArray(categories, mem)
	defer dict.Release()
	return catAssemble(dtype.Enum(categories).Arrow(), a, codesBuf, dict, mem), nil, 0
}

// CatDecode materialises a dictionary array with a utf8 dictionary as
// a plain utf8 array. Null codes and null dictionary entries both
// produce nulls.
func CatDecode(d *array.Dictionary, mem memory.Allocator) (*array.String, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	dict, ok := d.Dictionary().(*array.String)
	if !ok {
		return catDecodeGeneric(d, mem)
	}
	codes, err := catCodes(d)
	if err != nil {
		return nil, err
	}
	n := d.Len()
	dictOffs := dict.ValueOffsets()
	dictBytes := dict.ValueBytes()
	base := int32(0)
	if len(dictOffs) > 0 {
		base = dictOffs[0]
	}
	dictHasNulls := dict.NullN() > 0
	rowValid := func(i int) bool {
		if d.IsNull(i) {
			return false
		}
		return !dictHasNulls || dict.IsValid(int(codes[i]))
	}

	total := 0
	nulls := 0
	for i := range n {
		if !rowValid(i) {
			nulls++
			continue
		}
		c := codes[i]
		total += int(dictOffs[c+1] - dictOffs[c])
	}

	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * 4)
	offs := arrow.Int32Traits.CastFromBytes(offBuf.Bytes())
	valBuf := memory.NewResizableBuffer(mem)
	valBuf.Resize(total)
	vals := valBuf.Bytes()
	var nullBuf *memory.Buffer
	var bits []byte
	if nulls > 0 {
		nullBuf = memory.NewResizableBuffer(mem)
		nullBuf.Resize((n + 7) / 8)
		bits = nullBuf.Bytes()
		clear(bits)
	}
	pos := int32(0)
	offs[0] = 0
	for i := range n {
		if nulls > 0 && !rowValid(i) {
			offs[i+1] = pos
			continue
		}
		c := codes[i]
		s, e := dictOffs[c]-base, dictOffs[c+1]-base
		pos += int32(copy(vals[pos:], dictBytes[s:e]))
		offs[i+1] = pos
		if bits != nil {
			bits[i>>3] |= 1 << uint(i&7)
		}
	}
	data := array.NewData(arrow.BinaryTypes.String, n,
		[]*memory.Buffer{nullBuf, offBuf, valBuf}, nil, nulls, 0)
	out := array.NewStringData(data)
	data.Release()
	offBuf.Release()
	valBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return out, nil
}

// catDecodeGeneric handles dictionaries whose values are not plain
// utf8 (large_string or string_view, as written by other arrow
// producers). It is the slow path; golars itself always writes utf8.
func catDecodeGeneric(d *array.Dictionary, mem memory.Allocator) (*array.String, error) {
	type stringer interface {
		Value(int) string
		IsNull(int) bool
	}
	dict, ok := d.Dictionary().(stringer)
	if !ok {
		return nil, fmt.Errorf("series: dictionary values must be strings, got %s", d.Dictionary().DataType())
	}
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Reserve(d.Len())
	for i := range d.Len() {
		if d.IsNull(i) {
			b.AppendNull()
			continue
		}
		c := d.GetValueIndex(i)
		if dict.IsNull(c) {
			b.AppendNull()
			continue
		}
		b.Append(dict.Value(c))
	}
	return b.NewStringArray(), nil
}

// CatWithDictionary returns a dictionary array that keeps the codes and
// validity of d but swaps in a new dictionary and dictionary type.
// Codes are shared, not copied.
func CatWithDictionary(d *array.Dictionary, dt arrow.DataType, dict arrow.Array) *array.Dictionary {
	idx := d.Indices()
	return array.NewDictionaryArray(dt, idx, dict)
}

// CatRemap rewrites the codes of d through lut (old code -> new code)
// and attaches dict as the new dictionary. lut entries of -1 turn the
// row into a null.
func CatRemap(d *array.Dictionary, lut []int64, dt arrow.DataType, dict arrow.Array, mem memory.Allocator) (*array.Dictionary, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	codes, err := catCodes(d)
	if err != nil {
		return nil, err
	}
	n := d.Len()
	codesBuf := memory.NewResizableBuffer(mem)
	codesBuf.Resize(n * 4)
	out := arrow.Uint32Traits.CastFromBytes(codesBuf.Bytes())
	nullBuf := memory.NewResizableBuffer(mem)
	nullBuf.Resize((n + 7) / 8)
	bits := nullBuf.Bytes()
	clear(bits)
	nulls := 0
	for i := range n {
		if d.IsNull(i) {
			nulls++
			continue
		}
		m := lut[codes[i]]
		if m < 0 {
			nulls++
			continue
		}
		out[i] = uint32(m)
		bits[i>>3] |= 1 << uint(i&7)
	}
	if nulls == 0 {
		nullBuf.Release()
		nullBuf = nil
	}
	idxData := array.NewData(arrow.PrimitiveTypes.Uint32, n,
		[]*memory.Buffer{nullBuf, codesBuf}, nil, nulls, 0)
	idx := array.NewUint32Data(idxData)
	idxData.Release()
	codesBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	defer idx.Release()
	return array.NewDictionaryArray(dt, idx, dict), nil
}

// catAssemble wraps codes plus the validity of src into a dictionary
// array of type dt. It takes ownership of codesBuf.
func catAssemble(dt arrow.DataType, src arrow.Array, codesBuf *memory.Buffer, dict arrow.Array, mem memory.Allocator) *array.Dictionary {
	n := src.Len()
	nullBuf := CopyValidityBitmap(src, mem)
	idxData := array.NewData(arrow.PrimitiveTypes.Uint32, n,
		[]*memory.Buffer{nullBuf, codesBuf}, nil, src.NullN(), 0)
	idx := array.NewUint32Data(idxData)
	idxData.Release()
	codesBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	defer idx.Release()
	return array.NewDictionaryArray(dt, idx, dict)
}

// catStringArray builds a utf8 array holding vals, with no nulls.
func catStringArray(vals []string, mem memory.Allocator) *array.String {
	total := 0
	for _, v := range vals {
		total += len(v)
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((len(vals) + 1) * 4)
	offs := arrow.Int32Traits.CastFromBytes(offBuf.Bytes())
	valBuf := memory.NewResizableBuffer(mem)
	valBuf.Resize(total)
	b := valBuf.Bytes()
	pos := 0
	offs[0] = 0
	for i, v := range vals {
		pos += copy(b[pos:], v)
		offs[i+1] = int32(pos)
	}
	data := array.NewData(arrow.BinaryTypes.String, len(vals),
		[]*memory.Buffer{nil, offBuf, valBuf}, nil, 0, 0)
	out := array.NewStringData(data)
	data.Release()
	offBuf.Release()
	valBuf.Release()
	return out
}

// catCodes returns the codes of d widened to uint32 values. For the
// uint32 index type (the only one golars writes) this is a zero-copy
// view.
func catCodes(d *array.Dictionary) ([]uint32, error) {
	idx := d.Indices()
	switch a := idx.(type) {
	case *array.Uint32:
		return a.Uint32Values(), nil
	case *array.Int32:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Uint8:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Int8:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Uint16:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Int16:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Int64:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	case *array.Uint64:
		return catWiden(a.Len(), func(i int) uint32 { return uint32(a.Value(i)) }), nil
	}
	return nil, fmt.Errorf("series: unsupported dictionary index type %s", idx.DataType())
}

func catWiden(n int, at func(int) uint32) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = at(i)
	}
	return out
}
