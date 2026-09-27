package series

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// nsbuild.go holds the direct-buffer builders shared by the expression
// namespace kernels (str, list, arr, struct). They write offsets,
// values and validity straight into allocator-owned buffers with
// amortised growth, so kernels never allocate per row and never go
// through a []string or []bool intermediate.

// growBytes is an allocator-backed byte buffer with amortised growth.
type growBytes struct {
	buf *memory.Buffer
	b   []byte // full-capacity view of buf
	n   int    // bytes in use
}

func (g *growBytes) reserve(mem memory.Allocator, extra int) {
	need := g.n + extra
	if need <= len(g.b) {
		return
	}
	if g.buf == nil {
		g.buf = memory.NewResizableBuffer(mem)
	}
	newCap := max(need, 2*len(g.b), 64)
	g.buf.Reserve(newCap)
	g.b = g.buf.Buf()
}

// finish sets the logical length and hands the buffer to the caller.
// The caller owns one reference.
func (g *growBytes) finish(mem memory.Allocator) *memory.Buffer {
	if g.buf == nil {
		g.buf = memory.NewResizableBuffer(mem)
	}
	g.buf.ResizeNoShrink(g.n)
	out := g.buf
	g.buf, g.b, g.n = nil, nil, 0
	return out
}

func (g *growBytes) release() {
	if g.buf != nil {
		g.buf.Release()
		g.buf, g.b, g.n = nil, nil, 0
	}
}

// growValidity tracks a validity bitmap that is only materialised once
// the first null arrives.
type growValidity struct {
	bits  growBytes
	n     int
	nulls int
}

func (v *growValidity) append(mem memory.Allocator, valid bool) {
	if valid && v.nulls == 0 {
		v.n++
		return
	}
	if v.nulls == 0 && v.bits.buf == nil {
		// First null: back-fill every earlier row as valid.
		nb := int(bitutil.BytesForBits(int64(v.n + 1)))
		v.bits.reserve(mem, nb)
		for i := range nb {
			v.bits.b[i] = 0
		}
		v.bits.n = nb
		bitutil.SetBitsTo(v.bits.b, 0, int64(v.n), true)
	}
	nb := int(bitutil.BytesForBits(int64(v.n + 1)))
	if nb > v.bits.n {
		v.bits.reserve(mem, nb-v.bits.n)
		for i := v.bits.n; i < nb; i++ {
			v.bits.b[i] = 0
		}
		v.bits.n = nb
	}
	if valid {
		bitutil.SetBit(v.bits.b, v.n)
	} else {
		v.nulls++
	}
	v.n++
}

// finish returns the bitmap (nil when no nulls) and the null count.
func (v *growValidity) finish(mem memory.Allocator) (*memory.Buffer, int) {
	if v.nulls == 0 {
		v.bits.release()
		return nil, 0
	}
	nulls := v.nulls
	v.nulls, v.n = 0, 0
	return v.bits.finish(mem), nulls
}

func (v *growValidity) release() { v.bits.release() }

// strColBuilder appends utf8 (or binary) values into offsets and data
// buffers directly.
type strColBuilder struct {
	mem    memory.Allocator
	dt     arrow.DataType
	offs   growBytes // int32 offsets
	data   growBytes
	valid  growValidity
	length int
}

func newStrColBuilder(mem memory.Allocator, rows, bytesHint int) *strColBuilder {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	b := &strColBuilder{mem: mem, dt: arrow.BinaryTypes.String}
	b.offs.reserve(mem, (rows+1)*4)
	b.offs.n = 4
	*(*int32)(unsafe.Pointer(&b.offs.b[0])) = 0
	if bytesHint > 0 {
		b.data.reserve(mem, bytesHint)
	}
	return b
}

// newBinColBuilder is strColBuilder emitting arrow Binary.
func newBinColBuilder(mem memory.Allocator, rows, bytesHint int) *strColBuilder {
	b := newStrColBuilder(mem, rows, bytesHint)
	b.dt = arrow.BinaryTypes.Binary
	return b
}

func (b *strColBuilder) pushOffset() {
	b.offs.reserve(b.mem, 4)
	*(*int32)(unsafe.Pointer(&b.offs.b[b.offs.n])) = int32(b.data.n)
	b.offs.n += 4
	b.length++
}

func (b *strColBuilder) appendString(s string) {
	if len(s) > 0 {
		b.data.reserve(b.mem, len(s))
		copy(b.data.b[b.data.n:], s)
		b.data.n += len(s)
	}
	b.pushOffset()
	b.valid.append(b.mem, true)
}

func (b *strColBuilder) appendBytes(p []byte) {
	if len(p) > 0 {
		b.data.reserve(b.mem, len(p))
		copy(b.data.b[b.data.n:], p)
		b.data.n += len(p)
	}
	b.pushOffset()
	b.valid.append(b.mem, true)
}

// extend writes raw bytes for the current (open) value without closing
// it. Pair with closeValue.
func (b *strColBuilder) extend(s string) {
	if len(s) == 0 {
		return
	}
	b.data.reserve(b.mem, len(s))
	copy(b.data.b[b.data.n:], s)
	b.data.n += len(s)
}

func (b *strColBuilder) extendBytes(p []byte) {
	if len(p) == 0 {
		return
	}
	b.data.reserve(b.mem, len(p))
	copy(b.data.b[b.data.n:], p)
	b.data.n += len(p)
}

func (b *strColBuilder) extendRune(r rune) {
	b.data.reserve(b.mem, 4)
	b.data.n += encodeRune(b.data.b[b.data.n:], r)
}

// closeValue finishes the value assembled via extend calls.
func (b *strColBuilder) closeValue() {
	b.pushOffset()
	b.valid.append(b.mem, true)
}

func (b *strColBuilder) appendNull() {
	b.pushOffset()
	b.valid.append(b.mem, false)
}

// newArray finishes the builder into an arrow array. The builder must
// not be used afterwards.
func (b *strColBuilder) newArray() arrow.Array {
	nullBuf, nulls := b.valid.finish(b.mem)
	offBuf := b.offs.finish(b.mem)
	dataBuf := b.data.finish(b.mem)
	data := array.NewData(b.dt, b.length,
		[]*memory.Buffer{nullBuf, offBuf, dataBuf}, nil, nulls, 0)
	arr := array.MakeFromData(data)
	data.Release()
	offBuf.Release()
	dataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return arr
}

func (b *strColBuilder) finish(name string) (*Series, error) {
	return New(name, b.newArray())
}

func (b *strColBuilder) release() {
	b.offs.release()
	b.data.release()
	b.valid.release()
}

// encodeRune is utf8.EncodeRune without the import in hot helpers.
func encodeRune(p []byte, r rune) int {
	switch {
	case r >= 0 && r < 0x80:
		p[0] = byte(r)
		return 1
	case r < 0x800:
		p[0] = 0xC0 | byte(r>>6)
		p[1] = 0x80 | byte(r)&0x3F
		return 2
	case r > 0x10FFFF, r >= 0xD800 && r <= 0xDFFF:
		r = 0xFFFD
		fallthrough
	case r < 0x10000:
		p[0] = 0xE0 | byte(r>>12)
		p[1] = 0x80 | byte(r>>6)&0x3F
		p[2] = 0x80 | byte(r)&0x3F
		return 3
	default:
		p[0] = 0xF0 | byte(r>>18)
		p[1] = 0x80 | byte(r>>12)&0x3F
		p[2] = 0x80 | byte(r>>6)&0x3F
		p[3] = 0x80 | byte(r)&0x3F
		return 4
	}
}

// listOffsetsBuilder tracks the outer offsets and validity of a List
// array whose child is built separately.
type listOffsetsBuilder struct {
	mem    memory.Allocator
	offs   growBytes
	valid  growValidity
	length int
}

func newListOffsetsBuilder(mem memory.Allocator, rows int) *listOffsetsBuilder {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	b := &listOffsetsBuilder{mem: mem}
	b.offs.reserve(mem, (rows+1)*4)
	b.offs.n = 4
	*(*int32)(unsafe.Pointer(&b.offs.b[0])) = 0
	return b
}

// closeRow records that the child now holds childLen elements and the
// row is valid (or null).
func (b *listOffsetsBuilder) closeRow(childLen int, valid bool) {
	b.offs.reserve(b.mem, 4)
	*(*int32)(unsafe.Pointer(&b.offs.b[b.offs.n])) = int32(childLen)
	b.offs.n += 4
	b.length++
	b.valid.append(b.mem, valid)
}

// newListArray assembles a List array around child. It takes over the
// caller's reference to child.
func (b *listOffsetsBuilder) newListArray(child arrow.Array) arrow.Array {
	nullBuf, nulls := b.valid.finish(b.mem)
	offBuf := b.offs.finish(b.mem)
	dt := arrow.ListOfField(arrow.Field{Name: "item", Type: child.DataType(), Nullable: true})
	data := array.NewData(dt, b.length,
		[]*memory.Buffer{nullBuf, offBuf}, []arrow.ArrayData{child.Data()}, nulls, 0)
	arr := array.NewListData(data)
	data.Release()
	offBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	child.Release()
	return arr
}

func (b *listOffsetsBuilder) release() {
	b.offs.release()
	b.valid.release()
}

// packBoolsCounted packs a []bool validity slice into an allocator
// buffer. Returns (nil, 0) for a nil slice.
func packBoolsCounted(valid []bool, mem memory.Allocator) (*memory.Buffer, int) {
	if valid == nil {
		return nil, 0
	}
	buf, nulls := packValidity(valid, mem)
	if nulls == 0 {
		buf.Release()
		return nil, 0
	}
	return buf, nulls
}

// stringArrayOf returns s as a single *array.String (retained; caller
// releases). Empty and multi-chunk inputs are handled.
func stringArrayOf(s *Series, op string) (*array.String, error) {
	if !s.DType().IsString() || s.DType().ID() != arrow.STRING {
		return nil, fmt.Errorf("series.str.%s: requires string dtype, got %s", op, s.DType())
	}
	a, err := s.Consolidated()
	if err != nil {
		return nil, err
	}
	return a.(*array.String), nil
}

// structFromChildren builds a struct array. The struct takes over the
// caller's references on cols. validity may be nil.
func structFromChildren(cols []arrow.Array, names []string, length int, validity *memory.Buffer, nulls int) (arrow.Array, error) {
	fields := make([]arrow.Field, len(cols))
	children := make([]arrow.ArrayData, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{Name: names[i], Type: c.DataType(), Nullable: true}
		children[i] = c.Data()
	}
	if validity == nil {
		nulls = 0
	}
	data := array.NewData(arrow.StructOf(fields...), length, []*memory.Buffer{validity}, children, nulls, 0)
	arr := array.NewStructData(data)
	data.Release()
	for _, c := range cols {
		c.Release()
	}
	return arr, nil
}

// lastOffset returns the end offset of the last closed value, which is
// where an abandoned in-progress value should be rewound to.
func (b *strColBuilder) lastOffset() int32 {
	return *(*int32)(unsafe.Pointer(&b.offs.b[b.offs.n-4]))
}
