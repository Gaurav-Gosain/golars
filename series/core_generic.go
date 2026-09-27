package series

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fixedWidth is the set of Go types that back arrow fixed-width
// primitive buffers. Temporal types reuse the int32/int64 members.
type fixedWidth interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// fixedValues returns a zero-copy view of the value buffer of a
// fixed-width array, with the array offset applied. The caller must keep
// a reference to a while the view is in use.
func fixedValues[T fixedWidth](a arrow.Array) []T {
	n := a.Len()
	if n == 0 {
		return nil
	}
	buf := a.Data().Buffers()[1]
	if buf == nil || buf.Len() == 0 {
		return nil
	}
	off := a.Data().Offset()
	return unsafe.Slice((*T)(unsafe.Pointer(&buf.Bytes()[0])), off+n)[off:]
}

// buildFixedValid allocates an n-element fixed-width buffer of dtype dt, hands
// it to fill, and wraps it as a Series. valid may be nil (all valid).
func buildFixedValid[T fixedWidth](name string, dt arrow.DataType, n int, valid []bool, mem memory.Allocator, fill func(out []T)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var zero T
	size := int(unsafe.Sizeof(zero))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * size)
	defer buf.Release()
	if n > 0 {
		fill(unsafe.Slice((*T)(unsafe.Pointer(&buf.Bytes()[0])), n))
	}
	var validBuf *memory.Buffer
	nulls := 0
	if valid != nil {
		validBuf, nulls = packValidity(valid, mem)
		defer validBuf.Release()
		if nulls == 0 {
			validBuf = nil
		}
	}
	data := array.NewData(dt, n, []*memory.Buffer{validBuf, buf}, nil, nulls, 0)
	defer data.Release()
	return New(name, array.MakeFromData(data))
}

// buildFixedBits is buildFixedValid with a pre-packed validity bitmap (nil
// means all valid). The bitmap is retained by the output.
func buildFixedBits[T fixedWidth](name string, dt arrow.DataType, n int, validBits *memory.Buffer, nulls int, mem memory.Allocator, fill func(out []T)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var zero T
	size := int(unsafe.Sizeof(zero))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * size)
	defer buf.Release()
	if n > 0 {
		fill(unsafe.Slice((*T)(unsafe.Pointer(&buf.Bytes()[0])), n))
	}
	if nulls == 0 {
		validBits = nil
	}
	data := array.NewData(dt, n, []*memory.Buffer{validBits, buf}, nil, nulls, 0)
	defer data.Release()
	return New(name, array.MakeFromData(data))
}

// fixedWidthBytes reports the element width of a fixed-width primitive
// or temporal arrow type, or 0 when the type is not fixed width in the
// simple one-buffer sense.
func fixedWidthBytes(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.INT8, arrow.UINT8:
		return 1
	case arrow.INT16, arrow.UINT16, arrow.FLOAT16:
		return 2
	case arrow.INT32, arrow.UINT32, arrow.FLOAT32, arrow.DATE32, arrow.TIME32:
		return 4
	case arrow.INT64, arrow.UINT64, arrow.FLOAT64, arrow.DATE64, arrow.TIME64,
		arrow.TIMESTAMP, arrow.DURATION:
		return 8
	}
	return 0
}

// single returns one contiguous array for s. The caller owns the
// returned reference.
func (s *Series) single(mem memory.Allocator) (arrow.Array, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return extractSingleChunk(s, mem)
}

// Gather returns a Series built from the rows at the given indices.
// An index of -1 produces a null. Any other out-of-range index is an
// error. Every dtype is supported: fixed-width, boolean and string
// types take a direct path; nested types fall back to slice-and-concat.
// Mirrors polars' Series.gather.
func (s *Series) Gather(indices []int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	n := arr.Len()
	for _, i := range indices {
		if i < -1 || i >= n {
			return nil, fmt.Errorf("series: gather index %d out of bounds for length %d", i, n)
		}
	}
	out, err := gatherArray(arr, indices, cfg.alloc)
	if err != nil {
		return nil, err
	}
	return New(s.name, out)
}

// gatherArray is the array-level worker behind Gather. Indices must be
// validated by the caller (-1 means null).
func gatherArray(arr arrow.Array, indices []int, mem memory.Allocator) (arrow.Array, error) {
	dt := arr.DataType()
	m := len(indices)
	switch w := fixedWidthBytes(dt); w {
	case 1:
		return gatherFixed[uint8](arr, indices, mem), nil
	case 2:
		return gatherFixed[uint16](arr, indices, mem), nil
	case 4:
		return gatherFixed[uint32](arr, indices, mem), nil
	case 8:
		return gatherFixed[uint64](arr, indices, mem), nil
	}
	switch dt.ID() {
	case arrow.NULL:
		return array.MakeArrayOfNull(mem, dt, m), nil
	case arrow.BOOL:
		return gatherBool(arr, indices, mem), nil
	case arrow.STRING:
		return gatherString(arr.(*array.String), indices, mem), nil
	case arrow.BINARY:
		return gatherBinary(arr.(*array.Binary), indices, mem), nil
	}
	return gatherSlices(arr, indices, mem)
}

// gatherValidity builds the validity bitmap for a gather and returns it
// with the null count. It returns (nil, 0) when every output row is valid.
func gatherValidity(arr arrow.Array, indices []int, mem memory.Allocator) (*memory.Buffer, int) {
	hasSrcNulls := arr.NullN() > 0
	hasIdxNulls := false
	for _, i := range indices {
		if i < 0 {
			hasIdxNulls = true
			break
		}
	}
	if !hasSrcNulls && !hasIdxNulls {
		return nil, 0
	}
	m := len(indices)
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(m))))
	bits := buf.Bytes()
	for i := range bits {
		bits[i] = 0
	}
	nulls := 0
	for j, i := range indices {
		if i >= 0 && (!hasSrcNulls || arr.IsValid(i)) {
			bitutil.SetBit(bits, j)
		} else {
			nulls++
		}
	}
	if nulls == 0 {
		buf.Release()
		return nil, 0
	}
	return buf, nulls
}

func gatherFixed[T fixedWidth](arr arrow.Array, indices []int, mem memory.Allocator) arrow.Array {
	src := fixedValues[T](arr)
	m := len(indices)
	var zero T
	size := int(unsafe.Sizeof(zero))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(m * size)
	defer buf.Release()
	if m > 0 {
		out := unsafe.Slice((*T)(unsafe.Pointer(&buf.Bytes()[0])), m)
		for j, i := range indices {
			if i >= 0 {
				out[j] = src[i]
			} else {
				out[j] = zero
			}
		}
	}
	validBuf, nulls := gatherValidity(arr, indices, mem)
	if validBuf != nil {
		defer validBuf.Release()
	}
	data := array.NewData(arr.DataType(), m, []*memory.Buffer{validBuf, buf}, nil, nulls, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func gatherBool(arr arrow.Array, indices []int, mem memory.Allocator) arrow.Array {
	a := arr.(*array.Boolean)
	m := len(indices)
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(m))))
	defer buf.Release()
	bits := buf.Bytes()
	for i := range bits {
		bits[i] = 0
	}
	for j, i := range indices {
		if i >= 0 && a.Value(i) {
			bitutil.SetBit(bits, j)
		}
	}
	validBuf, nulls := gatherValidity(arr, indices, mem)
	if validBuf != nil {
		defer validBuf.Release()
	}
	data := array.NewData(arr.DataType(), m, []*memory.Buffer{validBuf, buf}, nil, nulls, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func gatherString(a *array.String, indices []int, mem memory.Allocator) arrow.Array {
	total := 0
	for _, i := range indices {
		if i >= 0 && a.IsValid(i) {
			total += a.ValueLen(i)
		}
	}
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Reserve(len(indices))
	b.ReserveData(total)
	for _, i := range indices {
		if i < 0 || a.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(a.Value(i))
	}
	return b.NewArray()
}

func gatherBinary(a *array.Binary, indices []int, mem memory.Allocator) arrow.Array {
	b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Reserve(len(indices))
	for _, i := range indices {
		if i < 0 || a.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(a.Value(i))
	}
	return b.NewArray()
}

// gatherSlices handles nested and other exotic dtypes by concatenating
// zero-copy slices of contiguous index runs. Null indices become
// all-null runs.
func gatherSlices(arr arrow.Array, indices []int, mem memory.Allocator) (arrow.Array, error) {
	if len(indices) == 0 {
		return array.NewSlice(arr, 0, 0), nil
	}
	var parts []arrow.Array
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	j := 0
	for j < len(indices) {
		start := indices[j]
		k := j + 1
		if start < 0 {
			for k < len(indices) && indices[k] < 0 {
				k++
			}
			parts = append(parts, array.MakeArrayOfNull(mem, arr.DataType(), k-j))
		} else {
			for k < len(indices) && indices[k] == start+(k-j) {
				k++
			}
			parts = append(parts, array.NewSlice(arr, int64(start), int64(start+k-j)))
		}
		j = k
	}
	if len(parts) == 1 {
		parts[0].Retain()
		return parts[0], nil
	}
	return array.Concatenate(parts, mem)
}

// ConcatSeries concatenates the given Series (which must share one
// dtype) into a single-chunk Series named name. An empty input list is
// an error because the dtype would be unknown.
func ConcatSeries(name string, parts []*Series, opts ...Option) (*Series, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("series: ConcatSeries needs at least one input")
	}
	cfg := resolve(opts)
	var chunks []arrow.Array
	for _, p := range parts {
		for _, c := range p.Chunks() {
			if c.Len() > 0 {
				chunks = append(chunks, c)
			}
		}
	}
	if len(chunks) == 0 {
		return New(name, array.MakeArrayOfNull(cfg.alloc, parts[0].data.DataType(), 0))
	}
	dt := parts[0].data.DataType()
	for _, c := range chunks {
		if !arrow.TypeEqual(c.DataType(), dt) {
			return nil, fmt.Errorf("series: cannot concat %s with %s", dt, c.DataType())
		}
	}
	if len(chunks) == 1 {
		chunks[0].Retain()
		return New(name, chunks[0])
	}
	out, err := array.Concatenate(chunks, cfg.alloc)
	if err != nil {
		return nil, err
	}
	return New(name, out)
}

// FullNull returns an all-null Series of the given arrow dtype.
func FullNull(name string, dt arrow.DataType, n int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	return New(name, nullArray(cfg.alloc, dt, n))
}

// nullArray builds an all-null array. arrow's MakeArrayOfNull assumes
// freshly allocated buffers are zeroed; a recycling allocator returns
// dirty memory, which left garbage offsets in string and list columns
// (found by internal/difftest), so a builder is used instead.
func nullArray(alloc memory.Allocator, dt arrow.DataType, n int) arrow.Array {
	b := array.NewBuilder(alloc, dt)
	defer b.Release()
	b.AppendNulls(n)
	return b.NewArray()
}

// Broadcast repeats a length-1 Series n times. A Series that is already
// length n is returned as a clone.
func (s *Series) Broadcast(n int, opts ...Option) (*Series, error) {
	if s.Len() == n {
		return s.Clone(), nil
	}
	if s.Len() != 1 {
		return nil, fmt.Errorf("series: cannot broadcast length %d to %d", s.Len(), n)
	}
	idx := make([]int, n)
	return s.Gather(idx, opts...)
}

// ListFromInt32Offsets wraps values as a list Series whose row i spans
// values[offsets[i]:offsets[i+1]]. valid may be nil (every row valid).
// len(offsets) must be rows+1.
func ListFromInt32Offsets(name string, values *Series, offsets []int32, valid []bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	child, err := values.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	rows := len(offsets) - 1
	if rows < 0 {
		return nil, fmt.Errorf("series: ListFromInt32Offsets needs at least one offset")
	}
	offBuf := memory.NewResizableBuffer(cfg.alloc)
	offBuf.Resize(len(offsets) * arrow.Int32SizeBytes)
	defer offBuf.Release()
	copy(unsafe.Slice((*int32)(unsafe.Pointer(&offBuf.Bytes()[0])), len(offsets)), offsets)
	var validBuf *memory.Buffer
	nulls := 0
	if valid != nil {
		validBuf, nulls = packValidity(valid, cfg.alloc)
		defer validBuf.Release()
		if nulls == 0 {
			validBuf = nil
		}
	}
	dt := arrow.ListOf(child.DataType())
	data := array.NewData(dt, rows, []*memory.Buffer{validBuf, offBuf},
		[]arrow.ArrayData{child.Data()}, nulls, 0)
	defer data.Release()
	return New(name, array.MakeFromData(data))
}
