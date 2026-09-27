package series

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// BinOps is the namespace for operations over binary Series. Reach it
// via s.Bin(). Mirrors polars' `s.bin.*` surface. Every method returns
// a fresh Series; the receiver is not modified.
type BinOps struct{ s *Series }

// Bin returns the binary namespace. Methods fail with a clear error
// when the Series is not binary-typed.
func (s *Series) Bin() BinOps { return BinOps{s: s} }

// FromBinary builds a binary Series from a [][]byte. valid may be nil
// to mark every value as valid; otherwise valid[i] false marks row i
// null and values[i] is ignored.
func FromBinary(name string, values [][]byte, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	out := newBinOut(cfg.alloc, len(values), 0)
	for i, v := range values {
		if valid != nil && !valid[i] {
			out.appendNull()
			continue
		}
		out.appendBytes(v)
		out.endRow()
	}
	return New(name, out.finish(arrow.BinaryTypes.Binary))
}

// binView is a uniform zero-copy accessor over String and Binary
// arrays. Row bytes alias the array's values buffer; callers must not
// mutate or retain them past the array's lifetime.
type binView struct {
	arr  arrow.Array
	offs []int32
	vals []byte
	base int32
}

func (v binView) Len() int          { return v.arr.Len() }
func (v binView) IsNull(i int) bool { return v.arr.IsNull(i) }
func (v binView) row(i int) []byte {
	return v.vals[v.offs[i]-v.base : v.offs[i+1]-v.base]
}

// binViewOf builds a binView over a String or Binary array.
func binViewOf(a arrow.Array) (binView, bool) {
	var offs []int32
	var vals []byte
	switch t := a.(type) {
	case *array.String:
		offs, vals = t.ValueOffsets(), t.ValueBytes()
	case *array.Binary:
		offs, vals = t.ValueOffsets(), t.ValueBytes()
	default:
		return binView{}, false
	}
	var base int32
	if len(offs) > 0 {
		base = offs[0]
	}
	return binView{arr: a, offs: offs, vals: vals, base: base}, true
}

// binChunk returns a single contiguous chunk of s with its own
// reference. Zero-chunk Series produce an empty array.
func binChunk(s *Series, alloc memory.Allocator) arrow.Array {
	if s.NumChunks() == 0 {
		return array.MakeArrayOfNull(alloc, s.DType().Arrow(), 0)
	}
	a := s.Chunk(0)
	a.Retain()
	return a
}

// binaryArr returns the backing binary chunk plus a view over it. The
// caller must Release the returned array.
func (o BinOps) binaryArr(op string, alloc memory.Allocator) (arrow.Array, binView, error) {
	if o.s.DType().ID() != arrow.BINARY {
		return nil, binView{}, fmt.Errorf("series.bin.%s: %q has dtype %s (need binary)",
			op, o.s.Name(), o.s.DType())
	}
	a := binChunk(o.s, alloc)
	v, ok := binViewOf(a)
	if !ok {
		a.Release()
		return nil, binView{}, fmt.Errorf("series.bin.%s: unsupported array type %T", op, a)
	}
	return a, v, nil
}

// binOut builds a String or Binary array directly into allocator
// buffers: one offsets buffer sized up front, one values buffer grown
// geometrically, and a validity bitmap. No per-row allocation.
type binOut struct {
	alloc   memory.Allocator
	n       int
	row     int
	offsets *memory.Buffer
	offs    []int32
	values  *memory.Buffer
	used    int
	valid   *memory.Buffer
	bits    []byte
	nulls   int
}

func newBinOut(alloc memory.Allocator, n, sizeHint int) *binOut {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	b := &binOut{alloc: alloc, n: n}
	b.offsets = memory.NewResizableBuffer(alloc)
	b.offsets.Resize((n + 1) * 4)
	b.offs = arrow.Int32Traits.CastFromBytes(b.offsets.Bytes())
	b.offs[0] = 0
	b.values = memory.NewResizableBuffer(alloc)
	if sizeHint > 0 {
		b.values.Reserve(sizeHint)
	}
	b.valid = memory.NewResizableBuffer(alloc)
	b.valid.Resize(int(bitutil.BytesForBits(int64(n))))
	b.bits = b.valid.Bytes()
	for i := range b.bits {
		b.bits[i] = 0
	}
	return b
}

// grow reserves k bytes at the end of the values buffer and returns
// them for the caller to fill.
func (b *binOut) grow(k int) []byte {
	need := b.used + k
	if need > b.values.Cap() {
		b.values.Reserve(max(need, 2*b.values.Cap(), 64))
	}
	dst := b.values.Buf()[b.used:need]
	b.used = need
	return dst
}

func (b *binOut) appendBytes(p []byte) { copy(b.grow(len(p)), p) }

func (b *binOut) appendString(s string) { copy(b.grow(len(s)), s) }

// truncate drops bytes written for the current row (used when a row
// turns out to be null halfway through).
func (b *binOut) truncate() { b.used = int(b.offs[b.row]) }

// endRow closes the current row as valid.
func (b *binOut) endRow() {
	b.bits[b.row>>3] |= 1 << uint(b.row&7)
	b.row++
	b.offs[b.row] = int32(b.used)
}

// appendNull closes the current row as null, discarding any bytes
// already written for it.
func (b *binOut) appendNull() {
	b.truncate()
	b.nulls++
	b.row++
	b.offs[b.row] = int32(b.used)
}

// release frees every buffer without building an array (error path).
func (b *binOut) release() {
	b.offsets.Release()
	b.values.Release()
	b.valid.Release()
}

// finish wraps the buffers into an array of type dt (String or
// Binary). binOut must not be used afterwards.
func (b *binOut) finish(dt arrow.DataType) arrow.Array {
	b.values.Resize(b.used)
	validBuf := b.valid
	if b.nulls == 0 {
		validBuf.Release()
		validBuf = nil
	}
	data := array.NewData(dt, b.n, []*memory.Buffer{validBuf, b.offsets, b.values}, nil, b.nulls, 0)
	arr := array.MakeFromData(data)
	data.Release()
	b.offsets.Release()
	b.values.Release()
	if validBuf != nil {
		validBuf.Release()
	}
	return arr
}

// binBoolResult evaluates pred on every non-null row and builds a
// boolean Series that keeps the input's null pattern.
func binBoolResult(name string, v binView, alloc memory.Allocator, pred func([]byte) bool) (*Series, error) {
	n := v.Len()
	dataBuf := memory.NewResizableBuffer(alloc)
	dataBuf.Resize(int(bitutil.BytesForBits(int64(n))))
	bits := dataBuf.Bytes()
	for i := range bits {
		bits[i] = 0
	}
	nulls := v.arr.NullN()
	for i := range n {
		if nulls > 0 && v.IsNull(i) {
			continue
		}
		if pred(v.row(i)) {
			bits[i>>3] |= 1 << uint(i&7)
		}
	}
	nullBuf := CopyValidityBitmap(v.arr, alloc)
	arr := array.NewBoolean(n, dataBuf, nullBuf, nulls)
	dataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// Contains reports whether each value contains the byte sequence
// needle. Null rows stay null.
func (o BinOps) Contains(needle []byte, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("contains", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binBoolResult(o.s.Name(), v, cfg.alloc, func(b []byte) bool { return bytes.Contains(b, needle) })
}

// StartsWith reports whether each value begins with prefix.
func (o BinOps) StartsWith(prefix []byte, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("starts_with", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binBoolResult(o.s.Name(), v, cfg.alloc, func(b []byte) bool { return bytes.HasPrefix(b, prefix) })
}

// EndsWith reports whether each value ends with suffix.
func (o BinOps) EndsWith(suffix []byte, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("ends_with", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binBoolResult(o.s.Name(), v, cfg.alloc, func(b []byte) bool { return bytes.HasSuffix(b, suffix) })
}

// Encode renders each value as text with the given transfer encoding
// ("hex" or "base64"). The output dtype is String.
func (o BinOps) Encode(encoding string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("encode", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binEncode(o.s.Name(), v, encoding, cfg.alloc)
}

// Decode parses each value as text in the given transfer encoding
// ("hex" or "base64") and returns the raw bytes as a Binary Series.
// With strict set, invalid input is an error; otherwise invalid rows
// become null.
func (o BinOps) Decode(encoding string, strict bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("decode", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	return binDecode(o.s.Name(), v, encoding, strict, cfg.alloc)
}

// binEncode is shared by BinOps.Encode and StrOps.Encode.
func binEncode(name string, v binView, encoding string, alloc memory.Allocator) (*Series, error) {
	var encLen func(int) int
	var enc func(dst, src []byte)
	switch encoding {
	case "hex":
		encLen, enc = hex.EncodedLen, func(dst, src []byte) { hex.Encode(dst, src) }
	case "base64":
		encLen, enc = base64.StdEncoding.EncodedLen, base64.StdEncoding.Encode
	default:
		return nil, fmt.Errorf("series: unknown encoding %q (want hex or base64)", encoding)
	}
	n := v.Len()
	out := newBinOut(alloc, n, encLen(len(v.vals)))
	for i := range n {
		if v.IsNull(i) {
			out.appendNull()
			continue
		}
		src := v.row(i)
		enc(out.grow(encLen(len(src))), src)
		out.endRow()
	}
	return New(name, out.finish(arrow.BinaryTypes.String))
}

// binDecode is shared by BinOps.Decode and StrOps.Decode.
func binDecode(name string, v binView, encoding string, strict bool, alloc memory.Allocator) (*Series, error) {
	var decLen func(int) int
	var dec func(dst, src []byte) (int, error)
	switch encoding {
	case "hex":
		decLen, dec = hex.DecodedLen, hex.Decode
	case "base64":
		decLen, dec = base64.StdEncoding.DecodedLen, base64.StdEncoding.Strict().Decode
	default:
		return nil, fmt.Errorf("series: unknown encoding %q (want hex or base64)", encoding)
	}
	n := v.Len()
	out := newBinOut(alloc, n, decLen(len(v.vals)))
	for i := range n {
		if v.IsNull(i) {
			out.appendNull()
			continue
		}
		src := v.row(i)
		start := out.used
		dst := out.grow(decLen(len(src)))
		k, err := dec(dst, src)
		// Go's base64 decoder skips CR and LF; polars rejects them.
		if err == nil && encoding == "base64" && bytes.ContainsAny(src, "\r\n") {
			err = base64.CorruptInputError(0)
		}
		if err != nil {
			if strict {
				out.release()
				return nil, fmt.Errorf("invalid `%s` encoding found; try setting `strict=false` to ignore", encoding)
			}
			out.appendNull()
			continue
		}
		out.used = start + k
		out.endRow()
	}
	return New(name, out.finish(arrow.BinaryTypes.Binary))
}

// Size returns the byte length of each value. unit "b" or "bytes"
// yields UInt32; "kb", "mb", "gb", "tb" (or their long forms) yield
// Float64 scaled by powers of 1024, as in polars.
func (o BinOps) Size(unit string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	var div float64
	switch unit {
	case "", "b", "bytes":
	case "kb", "kilobytes":
		div = 1 << 10
	case "mb", "megabytes":
		div = 1 << 20
	case "gb", "gigabytes":
		div = 1 << 30
	case "tb", "terabytes":
		div = 1 << 40
	default:
		return nil, fmt.Errorf("series.bin.size: unknown unit %q", unit)
	}
	a, v, err := o.binaryArr("size", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := v.Len()
	valid := validFromChunk(a)
	if div == 0 {
		out := make([]uint32, n)
		for i := range n {
			out[i] = uint32(v.offs[i+1] - v.offs[i])
		}
		return FromUint32(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	out := make([]float64, n)
	for i := range n {
		out[i] = float64(v.offs[i+1]-v.offs[i]) / div
	}
	return FromFloat64(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// Get returns the byte at position idx of each value as UInt8.
// Negative idx counts from the end. An out-of-range index is an error
// unless nullOnOOB is set, in which case that row becomes null.
func (o BinOps) Get(idx int, nullOnOOB bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr("get", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := v.Len()
	b := array.NewUint8Builder(cfg.alloc)
	defer b.Release()
	for i := range n {
		if v.IsNull(i) {
			b.AppendNull()
			continue
		}
		row := v.row(i)
		j := idx
		if j < 0 {
			j += len(row)
		}
		if j < 0 || j >= len(row) {
			if !nullOnOOB {
				return nil, fmt.Errorf("series.bin.get: get index is out of bounds")
			}
			b.AppendNull()
			continue
		}
		b.Append(row[j])
	}
	return New(o.s.Name(), b.NewArray())
}

// Head returns the first n bytes of each value. Negative n keeps all
// but the last |n| bytes.
func (o BinOps) Head(n int, opts ...Option) (*Series, error) {
	return o.sliceBy("head", opts, func(l int) (int, int) {
		if n >= 0 {
			return 0, min(n, l)
		}
		return 0, max(l+n, 0)
	})
}

// Tail returns the last n bytes of each value. Negative n drops the
// first |n| bytes.
func (o BinOps) Tail(n int, opts ...Option) (*Series, error) {
	return o.sliceBy("tail", opts, func(l int) (int, int) {
		if n >= 0 {
			k := min(n, l)
			return l - k, l
		}
		return min(-n, l), l
	})
}

// Slice returns length bytes starting at offset for each value.
// Negative offset counts from the end; length < 0 means "to the end".
// Bounds are clamped the way polars clamps them.
func (o BinOps) Slice(offset, length int, opts ...Option) (*Series, error) {
	return o.sliceBy("slice", opts, func(l int) (int, int) {
		return binSliceBounds(offset, length, l)
	})
}

// binSliceBounds mirrors polars' slice_offsets: the window is
// computed in signed space and then clamped to [0, l].
func binSliceBounds(offset, length, l int) (int, int) {
	start := offset
	if start < 0 {
		start += l
	}
	stop := l
	if length >= 0 {
		stop = start + length
	}
	start = min(max(start, 0), l)
	stop = min(max(stop, 0), l)
	if stop < start {
		stop = start
	}
	return start, stop
}

func (o BinOps) sliceBy(op string, opts []Option, bounds func(l int) (int, int)) (*Series, error) {
	cfg := resolve(opts)
	a, v, err := o.binaryArr(op, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := v.Len()
	out := newBinOut(cfg.alloc, n, len(v.vals))
	for i := range n {
		if v.IsNull(i) {
			out.appendNull()
			continue
		}
		row := v.row(i)
		s, e := bounds(len(row))
		out.appendBytes(row[s:e])
		out.endRow()
	}
	return New(o.s.Name(), out.finish(arrow.BinaryTypes.Binary))
}

// Reinterpret reads each value as one fixed-width number of dtype to.
// Values whose byte length differs from the dtype's width become null.
// littleEndian selects the byte order. Supported targets: every
// integer and float dtype.
func (o BinOps) Reinterpret(to dtype.DType, littleEndian bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	width := 0
	switch to.ID() {
	case arrow.INT8, arrow.UINT8:
		width = 1
	case arrow.INT16, arrow.UINT16:
		width = 2
	case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
		width = 4
	case arrow.INT64, arrow.UINT64, arrow.FLOAT64:
		width = 8
	default:
		return nil, fmt.Errorf("series.bin.reinterpret: unsupported dtype %s", to)
	}
	a, v, err := o.binaryArr("reinterpret", cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	var order binary.ByteOrder = binary.LittleEndian
	if !littleEndian {
		order = binary.BigEndian
	}
	n := v.Len()
	bld := array.NewBuilder(cfg.alloc, to.Arrow())
	defer bld.Release()
	bld.Reserve(n)
	for i := range n {
		row := v.row(i)
		if v.IsNull(i) || len(row) != width {
			bld.AppendNull()
			continue
		}
		switch b := bld.(type) {
		case *array.Int8Builder:
			b.Append(int8(row[0]))
		case *array.Uint8Builder:
			b.Append(row[0])
		case *array.Int16Builder:
			b.Append(int16(order.Uint16(row)))
		case *array.Uint16Builder:
			b.Append(order.Uint16(row))
		case *array.Int32Builder:
			b.Append(int32(order.Uint32(row)))
		case *array.Uint32Builder:
			b.Append(order.Uint32(row))
		case *array.Float32Builder:
			b.Append(math.Float32frombits(order.Uint32(row)))
		case *array.Int64Builder:
			b.Append(int64(order.Uint64(row)))
		case *array.Uint64Builder:
			b.Append(order.Uint64(row))
		case *array.Float64Builder:
			b.Append(math.Float64frombits(order.Uint64(row)))
		}
	}
	return New(o.s.Name(), bld.NewArray())
}
