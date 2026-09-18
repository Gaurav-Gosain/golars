package series

import (
	"math/bits"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BuildInt64Direct constructs an Int64 Series by allocating an
// allocator-tracked buffer once and handing it to the caller as []int64 to
// fill. This is the direct-write path mimicked from polars'
// MutablePrimitiveArray and arrow-go's array.NewData constructor: it avoids
// the memcpy that FromInt64 / AppendValues incur, cutting the output-cost
// floor of binary kernels and Filter roughly in half.
//
// fill receives a []int64 of length n aliasing the underlying buffer.
// Every slot must be written; unwritten slots contain arbitrary memory.
// The returned Series owns its reference and can be Released normally.
func BuildInt64Direct(name string, n int, mem memory.Allocator, fill func(out []int64)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Int64SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*int64)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	data := array.NewData(arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{nil, buf}, nil, 0, 0)
	defer data.Release()
	arr := array.NewInt64Data(data)
	return New(name, arr)
}

// BuildTypedInt64Direct is BuildInt64Direct that preserves a caller-supplied
// DataType. Used to gather values into int64-backed temporal dtypes
// (TIMESTAMP, DATE64, TIME64, DURATION) without collapsing them to plain
// Int64. valid is optional; nil means all values are valid.
func BuildTypedInt64Direct(name string, n int, mem memory.Allocator, dt arrow.DataType, valid []bool, fill func(out []int64)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Int64SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*int64)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	var validBuf *memory.Buffer
	nullCount := 0
	if valid != nil {
		validBuf, nullCount = packValidity(valid, mem)
		defer validBuf.Release()
	}
	data := array.NewData(dt, n,
		[]*memory.Buffer{validBuf, buf}, nil, nullCount, 0)
	defer data.Release()
	arr := array.MakeFromData(data)
	return New(name, arr)
}

// BuildTypedInt32Direct is the int32-backed sibling for DATE32 / TIME32.
func BuildTypedInt32Direct(name string, n int, mem memory.Allocator, dt arrow.DataType, valid []bool, fill func(out []int32)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Int32SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*int32)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	var validBuf *memory.Buffer
	nullCount := 0
	if valid != nil {
		validBuf, nullCount = packValidity(valid, mem)
		defer validBuf.Release()
	}
	data := array.NewData(dt, n,
		[]*memory.Buffer{validBuf, buf}, nil, nullCount, 0)
	defer data.Release()
	arr := array.MakeFromData(data)
	return New(name, arr)
}

// BuildInt32DirectFused is the int32 counterpart of BuildInt64DirectFused.
func BuildInt32DirectFused(name string, n int, mem memory.Allocator, fill func(out []int32, validBits []byte) int) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * arrow.Int32SizeBytes)
	defer data.Release()

	validBuf := memory.NewResizableBuffer(mem)
	nBytes := bitutil.BytesForBits(int64(n))
	validBuf.Resize(int(nBytes))
	defer validBuf.Release()
	validBytes := validBuf.Bytes()

	nullCount := n
	if n > 0 {
		view := unsafe.Slice((*int32)(unsafe.Pointer(&data.Bytes()[0])), n)
		nullCount = fill(view, validBytes)
	}

	ad := array.NewData(arrow.PrimitiveTypes.Int32, n,
		[]*memory.Buffer{validBuf, data}, nil, nullCount, 0)
	defer ad.Release()
	arr := array.NewInt32Data(ad)
	return New(name, arr)
}

// BuildInt64DirectFused is the fused-fill nullable variant: the callback
// receives both the output value slice AND the packed bitmap buffer, and
// returns the null count. This lets the caller write to values and the
// validity bitmap in a single pass, saving an n-sized []bool allocation
// plus a second pass to pack it. Mirrors BuildFloat64DirectFused.
func BuildInt64DirectFused(name string, n int, mem memory.Allocator, fill func(out []int64, validBits []byte) int) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * arrow.Int64SizeBytes)
	defer data.Release()

	validBuf := memory.NewResizableBuffer(mem)
	nBytes := bitutil.BytesForBits(int64(n))
	validBuf.Resize(int(nBytes))
	defer validBuf.Release()
	// mallocgc zeroes buffers; bitmap starts all-invalid.
	validBytes := validBuf.Bytes()

	nullCount := n
	if n > 0 {
		view := unsafe.Slice((*int64)(unsafe.Pointer(&data.Bytes()[0])), n)
		nullCount = fill(view, validBytes)
	}

	ad := array.NewData(arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{validBuf, data}, nil, nullCount, 0)
	defer ad.Release()
	arr := array.NewInt64Data(ad)
	return New(name, arr)
}

// BuildFloat64DirectFused is the float64 counterpart of BuildInt64DirectFused.
func BuildFloat64DirectFused(name string, n int, mem memory.Allocator, fill func(out []float64, validBits []byte) int) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * arrow.Float64SizeBytes)
	defer data.Release()

	validBuf := memory.NewResizableBuffer(mem)
	nBytes := bitutil.BytesForBits(int64(n))
	validBuf.Resize(int(nBytes))
	defer validBuf.Release()
	validBytes := validBuf.Bytes()

	nullCount := n
	if n > 0 {
		view := unsafe.Slice((*float64)(unsafe.Pointer(&data.Bytes()[0])), n)
		nullCount = fill(view, validBytes)
	}

	ad := array.NewData(arrow.PrimitiveTypes.Float64, n,
		[]*memory.Buffer{validBuf, data}, nil, nullCount, 0)
	defer ad.Release()
	arr := array.NewFloat64Data(ad)
	return New(name, arr)
}

// BuildInt64DirectNullable is the nullable-output variant. valid is a []bool
// of length n where true marks a valid value. A nil valid slice means every
// value is valid (in which case BuildInt64Direct is preferred).
func BuildInt64DirectNullable(name string, n int, mem memory.Allocator, fill func(out []int64), valid []bool) (*Series, error) {
	if valid == nil {
		return BuildInt64Direct(name, n, mem, fill)
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * arrow.Int64SizeBytes)
	defer data.Release()

	validBuf, nullCount := packValidity(valid, mem)
	defer validBuf.Release()

	if n > 0 {
		view := unsafe.Slice((*int64)(unsafe.Pointer(&data.Bytes()[0])), n)
		fill(view)
	}

	ad := array.NewData(arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{validBuf, data}, nil, nullCount, 0)
	defer ad.Release()
	arr := array.NewInt64Data(ad)
	return New(name, arr)
}

// BuildFloat64Direct is the float64 variant of BuildInt64Direct.
func BuildFloat64Direct(name string, n int, mem memory.Allocator, fill func(out []float64)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * arrow.Float64SizeBytes)
	defer buf.Release()
	if n > 0 {
		view := unsafe.Slice((*float64)(unsafe.Pointer(&buf.Bytes()[0])), n)
		fill(view)
	}
	data := array.NewData(arrow.PrimitiveTypes.Float64, n,
		[]*memory.Buffer{nil, buf}, nil, 0, 0)
	defer data.Release()
	arr := array.NewFloat64Data(data)
	return New(name, arr)
}

// BuildFloat64DirectNullable is the nullable-output variant for float64.
func BuildFloat64DirectNullable(name string, n int, mem memory.Allocator, fill func(out []float64), valid []bool) (*Series, error) {
	if valid == nil {
		return BuildFloat64Direct(name, n, mem, fill)
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * arrow.Float64SizeBytes)
	defer data.Release()

	validBuf, nullCount := packValidity(valid, mem)
	defer validBuf.Release()

	if n > 0 {
		view := unsafe.Slice((*float64)(unsafe.Pointer(&data.Bytes()[0])), n)
		fill(view)
	}

	ad := array.NewData(arrow.PrimitiveTypes.Float64, n,
		[]*memory.Buffer{validBuf, data}, nil, nullCount, 0)
	defer ad.Release()
	arr := array.NewFloat64Data(ad)
	return New(name, arr)
}

// BuildBoolDirect constructs a Boolean Series with a pre-packed bitmap.
// bits is the caller-supplied bit buffer, little-endian bit order, length
// bitutil.CeilByte(n)/8 bytes. This avoids the per-bit BuilderAppend loop.
func BuildBoolDirect(name string, n int, mem memory.Allocator, fill func(bits []byte)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	nb := bitutil.BytesForBits(int64(n))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(nb))
	defer buf.Release()
	if nb > 0 {
		// Zero out the buffer so unset bits read as 0 / false.
		bytes := buf.Bytes()
		for i := range bytes {
			bytes[i] = 0
		}
		fill(bytes)
	}
	data := array.NewData(arrow.FixedWidthTypes.Boolean, n,
		[]*memory.Buffer{nil, buf}, nil, 0, 0)
	defer data.Release()
	arr := array.NewBooleanData(data)
	return New(name, arr)
}

// packValidity converts a []bool validity slice to a packed little-endian
// bitmap buffer and returns the buffer plus the null count. The buffer is
// allocated via mem; the caller owns one reference.
//
// Uses unsafe to view []bool as []byte (Go guarantees bool is a single byte
// with value 0 or 1), then processes 8 bytes at a time: load as uint64,
// pack each byte's low bit into a byte output via a bit-spread multiply.
// 8x faster than the per-bit scalar loop.
func packValidity(valid []bool, mem memory.Allocator) (*memory.Buffer, int) {
	n := len(valid)
	nb := bitutil.BytesForBits(int64(n))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(nb))
	bytes := buf.Bytes()
	if n == 0 {
		return buf, 0
	}
	// Alias []bool as []byte safely: len(valid) bytes of 0/1 values.
	b8 := unsafe.Slice((*byte)(unsafe.Pointer(&valid[0])), n)

	nullCount := 0
	i := 0
	// Process 8 bools → 1 bitmap byte using bit-spread trick. The
	// multiplication by 0x0102040810204080 distributes each byte's low bit
	// into a distinct bit of the top byte; we then shift right 56 to land
	// those 8 bits in the low byte.
	const bitSpread uint64 = 0x0102040810204080
	for ; i+8 <= n; i += 8 {
		w := *(*uint64)(unsafe.Pointer(&b8[i]))
		// Keep only the low bit of each byte (bools are 0 or 1, but defend
		// against anything else).
		w &= 0x0101010101010101
		packed := byte((w * bitSpread) >> 56)
		bytes[i>>3] = packed
		nullCount += 8 - bits.OnesCount8(packed)
	}
	// Tail.
	if i < n {
		var packed byte
		for j := i; j < n; j++ {
			if valid[j] {
				packed |= 1 << (j - i)
			} else {
				nullCount++
			}
		}
		bytes[i>>3] = packed
	}
	return buf, nullCount
}
