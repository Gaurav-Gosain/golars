package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// strfast.go holds direct-buffer helpers used by hot string kernels.
// The theme is: skip the []bool / []string intermediate the FromBool /
// FromString constructors need, and build Arrow buffers in one pass.
//
// Two families of helpers:
//
//   * boolResultFromStr    - predicate kernels like Contains, StartsWith:
//                            materialise a boolean array directly, one
//                            bit at a time into the result bitmap. Null
//                            inputs produce null outputs, matching the
//                            mapBool behaviour we replace.
//
//   * stringResultSameSize - transforms that don't change byte length
//                            (ASCII case fold, reverse of a uniform-
//                            length string). Copy the offsets, allocate
//                            one values buffer of the same size, fill
//                            it byte-for-byte. Single alloc for the
//                            output values; no []string intermediate.
//
// These keep null-bitmap passthrough cheap: when the input array has
// no nulls we skip allocating a validity bitmap entirely.

// boolResultFromStr builds a boolean Series where out[i] = pred(a.Value(i))
// when a.IsValid(i), and null otherwise. The output validity bitmap is
// the input's, byte-copied (nil when the input has no nulls).
//
// Hot-path notes:
//   - The per-row loop is allocation-free: a.Value(i) aliases into the
//     array's backing string (arrow-go's []int32 offsets + string
//     values buffer are not copied per read).
//   - We build the result bitmap byte-at-a-time: accumulate 8 bits in a
//     local uint8, write it out on every wrap-around.
func boolResultFromStr(name string, a *array.String, alloc memory.Allocator, pred func(string) bool) (*Series, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	dataBuf := memory.NewResizableBuffer(alloc)
	dataBuf.Resize((n + 7) / 8)
	bits := dataBuf.Bytes()
	for i := range bits {
		bits[i] = 0
	}
	var nullBuf *memory.Buffer
	if a.NullN() == 0 {
		for i := range n {
			if pred(a.Value(i)) {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
	} else {
		for i := range n {
			if !a.IsValid(i) {
				continue
			}
			if pred(a.Value(i)) {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
		nullBuf = copyValidityBitmap(a, alloc)
	}
	// NewBoolean takes buffers and retains them internally. Drop our
	// refs after constructing; the array then holds the only ref.
	arr := array.NewBoolean(n, dataBuf, nullBuf, a.NullN())
	dataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// int64ResultFromStr is the int64 analogue of boolResultFromStr, for
// kernels like LenBytes / CountMatches / Find that emit one int per
// row. fn is called only for valid rows; null inputs stay null.
func int64ResultFromStr(name string, a *array.String, alloc memory.Allocator, fn func(string) int64) (*Series, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	dataBuf := memory.NewResizableBuffer(alloc)
	dataBuf.Resize(n * 8)
	values := arrow.Int64Traits.CastFromBytes(dataBuf.Bytes())
	var nullBuf *memory.Buffer
	if a.NullN() == 0 {
		for i := range n {
			values[i] = fn(a.Value(i))
		}
	} else {
		for i := range n {
			if !a.IsValid(i) {
				continue
			}
			values[i] = fn(a.Value(i))
		}
		nullBuf = copyValidityBitmap(a, alloc)
	}
	data := array.NewData(
		arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{nullBuf, dataBuf}, nil, a.NullN(), 0,
	)
	arr := array.NewInt64Data(data)
	data.Release()
	dataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// copyValidityBitmap is the package-local shim for the string kernels.
// All of its work is done by CopyValidityBitmap in bitmap.go now; the
// alias stays so the string kernels read unchanged.
func copyValidityBitmap(a arrow.Array, alloc memory.Allocator) *memory.Buffer {
	return CopyValidityBitmap(a, alloc)
}

// stringResultSameSize builds a string Series where every row has the
// same byte length as the input (typical of ASCII case folding or
// in-place reversal). We copy the input's offsets buffer verbatim and
// allocate one values buffer of the same byte size; transform writes
// each row's bytes into the right slice in one pass.
//
// transform is given (src, dst) byte slices of equal length for each
// valid row. Nulls are skipped (no write performed; the bytes in dst
// at that position are left zero-valued, which is acceptable because
// Arrow consumers read through the validity bitmap).
func stringResultSameSize(name string, a *array.String, alloc memory.Allocator, transform func(src, dst []byte)) (*Series, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	srcOffsets := a.ValueOffsets()
	srcValues := a.ValueBytes() // may be nil for empty arrays

	offsetsBuf := memory.NewResizableBuffer(alloc)
	offsetsBuf.Resize(len(srcOffsets) * 4)
	dstOffsets := arrow.Int32Traits.CastFromBytes(offsetsBuf.Bytes())
	// When the slice is a zero-offset view (the usual case), we can
	// copy offsets verbatim. Otherwise rebase to 0.
	if len(srcOffsets) > 0 && srcOffsets[0] == 0 {
		copy(dstOffsets, srcOffsets)
	} else if len(srcOffsets) > 0 {
		base := srcOffsets[0]
		for i, off := range srcOffsets {
			dstOffsets[i] = off - base
		}
	}

	totalBytes := 0
	if len(srcOffsets) > 0 {
		totalBytes = int(srcOffsets[len(srcOffsets)-1] - srcOffsets[0])
	}
	valuesBuf := memory.NewResizableBuffer(alloc)
	valuesBuf.Resize(totalBytes)
	dstValues := valuesBuf.Bytes()

	// Walk once. srcValues is a re-slice into the backing buffer that
	// already starts at srcOffsets[0]; rebase indices accordingly.
	base := int32(0)
	if len(srcOffsets) > 0 {
		base = srcOffsets[0]
	}
	if a.NullN() == 0 {
		for i := range n {
			s, e := srcOffsets[i]-base, srcOffsets[i+1]-base
			transform(srcValues[s:e], dstValues[s:e])
		}
	} else {
		for i := range n {
			if !a.IsValid(i) {
				continue
			}
			s, e := srcOffsets[i]-base, srcOffsets[i+1]-base
			transform(srcValues[s:e], dstValues[s:e])
		}
	}

	nullBuf := copyValidityBitmap(a, alloc)
	data := array.NewData(
		arrow.BinaryTypes.String, n,
		[]*memory.Buffer{nullBuf, offsetsBuf, valuesBuf},
		nil, a.NullN(), 0,
	)
	arr := array.NewStringData(data)
	data.Release()
	offsetsBuf.Release()
	valuesBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}
