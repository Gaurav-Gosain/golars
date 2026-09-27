package dataframe

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// takeOptional gathers src at idx where a negative index produces a null
// output row. It supports every arrow dtype: fixed-width primitives
// (including temporal types), booleans and 32/64-bit offset strings and
// binaries use direct buffer builds; nested types fall back to a
// slice-and-concatenate path that is correct but slower.
//
// When idx has no negative entries and the dtype is supported by
// compute.Take, that kernel is used instead.
func takeOptional(ctx context.Context, src *series.Series, idx []int, mem memory.Allocator) (*series.Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	hasMiss := false
	for _, i := range idx {
		if i < 0 {
			hasMiss = true
			break
		}
	}
	if !hasMiss && takeSupported(src.DType().ID()) {
		return compute.Take(ctx, src, idx, compute.WithAllocator(mem))
	}
	arr, err := src.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	out, err := takeArrayOptional(arr, idx, mem)
	if err != nil {
		return nil, err
	}
	return seriesFromArray(src.Name(), out)
}

// firstChunk returns the single chunk of s. A Series without chunks (as
// produced for empty frames) yields a zero-length array; that array is
// backed by the Go allocator and left to the garbage collector.
func firstChunk(s *series.Series) arrow.Array {
	if s.NumChunks() == 0 {
		return array.MakeArrayOfNull(memory.DefaultAllocator, s.DType().Arrow(), 0)
	}
	return s.Chunk(0)
}

// seriesFromArray wraps arr in a Series. The caller's reference to arr
// is consumed whether or not an error is returned.
func seriesFromArray(name string, arr arrow.Array) (*series.Series, error) {
	s, err := series.New(name, arr)
	if err != nil {
		arr.Release()
		return nil, err
	}
	return s, nil
}

func takeSupported(id arrow.Type) bool {
	switch id {
	case arrow.INT32, arrow.INT64, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.BOOL, arrow.STRING,
		arrow.TIMESTAMP, arrow.DATE64, arrow.TIME64, arrow.DURATION,
		arrow.DATE32, arrow.TIME32:
		return true
	}
	return false
}

// takeArrayOptional is the arrow-level worker behind takeOptional. The
// caller owns the returned array.
func takeArrayOptional(arr arrow.Array, idx []int, mem memory.Allocator) (arrow.Array, error) {
	n := len(idx)
	dt := arr.DataType()
	if dt.ID() == arrow.NULL {
		return array.MakeArrayOfNull(mem, dt, n), nil
	}
	data := arr.Data()
	srcOff := data.Offset()
	srcHasNulls := arr.NullN() > 0

	// Validity bitmap for the output; also counts nulls.
	validBuf := memory.NewResizableBuffer(mem)
	validBuf.Resize(int(bitutil.BytesForBits(int64(n))))
	vb := validBuf.Bytes()
	for i := range vb {
		vb[i] = 0
	}
	nulls := 0
	for j, i := range idx {
		if i < 0 || (srcHasNulls && arr.IsNull(i)) {
			nulls++
			continue
		}
		bitutil.SetBit(vb, j)
	}

	switch t := dt.(type) {
	case *arrow.BooleanType:
		vals := memory.NewResizableBuffer(mem)
		vals.Resize(int(bitutil.BytesForBits(int64(n))))
		out := vals.Bytes()
		for i := range out {
			out[i] = 0
		}
		srcBits := data.Buffers()[1].Bytes()
		for j, i := range idx {
			if i >= 0 && bitutil.BitIsSet(srcBits, srcOff+i) {
				bitutil.SetBit(out, j)
			}
		}
		return finishTake(dt, n, []*memory.Buffer{validBuf, vals}, nulls), nil
	case arrow.FixedWidthDataType:
		width := t.BitWidth() / 8
		if t.BitWidth()%8 != 0 || width == 0 {
			break
		}
		vals := memory.NewResizableBuffer(mem)
		vals.Resize(n * width)
		out := vals.Bytes()
		src := data.Buffers()[1].Bytes()
		switch width {
		case 8:
			for j, i := range idx {
				if i < 0 {
					clear(out[j*8 : j*8+8])
					continue
				}
				s := (srcOff + i) * 8
				copy(out[j*8:j*8+8], src[s:s+8])
			}
		case 4:
			for j, i := range idx {
				if i < 0 {
					clear(out[j*4 : j*4+4])
					continue
				}
				s := (srcOff + i) * 4
				copy(out[j*4:j*4+4], src[s:s+4])
			}
		default:
			for j, i := range idx {
				if i < 0 {
					clear(out[j*width : j*width+width])
					continue
				}
				s := (srcOff + i) * width
				copy(out[j*width:j*width+width], src[s:s+width])
			}
		}
		return finishTake(dt, n, []*memory.Buffer{validBuf, vals}, nulls), nil
	}

	switch a := arr.(type) {
	case *array.String:
		return takeVarBinary(dt, n, idx, validBuf, nulls, mem, func(i int) []byte {
			return unsafeStringBytes(a.Value(i))
		}), nil
	case *array.Binary:
		return takeVarBinary(dt, n, idx, validBuf, nulls, mem, a.Value), nil
	case *array.LargeString:
		return takeLargeVarBinary(dt, n, idx, validBuf, nulls, mem, func(i int) []byte {
			return unsafeStringBytes(a.Value(i))
		}), nil
	case *array.LargeBinary:
		return takeLargeVarBinary(dt, n, idx, validBuf, nulls, mem, a.Value), nil
	}
	validBuf.Release()
	return takeBySlices(arr, idx, mem)
}

func finishTake(dt arrow.DataType, n int, bufs []*memory.Buffer, nulls int) arrow.Array {
	if nulls == 0 {
		bufs[0].Release()
		bufs[0] = nil
	}
	d := array.NewData(dt, n, bufs, nil, nulls, 0)
	for _, b := range bufs {
		if b != nil {
			b.Release()
		}
	}
	defer d.Release()
	return array.MakeFromData(d)
}

// unsafeStringBytes views s as bytes without copying. The result must not
// be mutated.
func unsafeStringBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func takeVarBinary(dt arrow.DataType, n int, idx []int, validBuf *memory.Buffer, nulls int, mem memory.Allocator, value func(int) []byte) arrow.Array {
	offs := memory.NewResizableBuffer(mem)
	offs.Resize((n + 1) * 4)
	ov := arrow.Int32Traits.CastFromBytes(offs.Bytes())
	total := 0
	vb := validBuf.Bytes()
	for j, i := range idx {
		ov[j] = int32(total)
		if bitutil.BitIsSet(vb, j) {
			total += len(value(i))
		}
	}
	ov[n] = int32(total)
	body := memory.NewResizableBuffer(mem)
	body.Resize(total)
	bb := body.Bytes()
	for j, i := range idx {
		if bitutil.BitIsSet(vb, j) {
			copy(bb[ov[j]:], value(i))
		}
	}
	return finishTake(dt, n, []*memory.Buffer{validBuf, offs, body}, nulls)
}

func takeLargeVarBinary(dt arrow.DataType, n int, idx []int, validBuf *memory.Buffer, nulls int, mem memory.Allocator, value func(int) []byte) arrow.Array {
	offs := memory.NewResizableBuffer(mem)
	offs.Resize((n + 1) * 8)
	ov := arrow.Int64Traits.CastFromBytes(offs.Bytes())
	total := int64(0)
	vb := validBuf.Bytes()
	for j, i := range idx {
		ov[j] = total
		if bitutil.BitIsSet(vb, j) {
			total += int64(len(value(i)))
		}
	}
	ov[n] = total
	body := memory.NewResizableBuffer(mem)
	body.Resize(int(total))
	bb := body.Bytes()
	for j, i := range idx {
		if bitutil.BitIsSet(vb, j) {
			copy(bb[ov[j]:], value(i))
		}
	}
	return finishTake(dt, n, []*memory.Buffer{validBuf, offs, body}, nulls)
}

// takeBySlices handles nested dtypes. Runs of consecutive source indices
// become one zero-copy slice and runs of misses become one null array,
// then everything is concatenated.
func takeBySlices(arr arrow.Array, idx []int, mem memory.Allocator) (arrow.Array, error) {
	n := len(idx)
	if n == 0 {
		return array.MakeArrayOfNull(mem, arr.DataType(), 0), nil
	}
	var parts []arrow.Array
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	for j := 0; j < n; {
		k := j + 1
		if idx[j] < 0 {
			for k < n && idx[k] < 0 {
				k++
			}
			parts = append(parts, array.MakeArrayOfNull(mem, arr.DataType(), k-j))
		} else {
			for k < n && idx[k] == idx[k-1]+1 {
				k++
			}
			start := idx[j]
			if start+(k-j) > arr.Len() {
				return nil, fmt.Errorf("dataframe: gather index %d out of range for length %d", idx[k-1], arr.Len())
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

// filterColumn is compute.Filter with a fallback for dtypes the kernel
// does not cover (lists, structs, small integers, ...): the kept row
// positions are gathered with takeOptional instead.
func filterColumn(ctx context.Context, c, mask *series.Series, mem memory.Allocator) (*series.Series, error) {
	out, err := compute.Filter(ctx, c, mask, compute.WithAllocator(mem))
	if err == nil || !errors.Is(err, compute.ErrUnsupportedDType) {
		return out, err
	}
	m := firstChunk(mask).(*array.Boolean)
	idx := make([]int, 0, m.Len())
	for i := range m.Len() {
		if m.IsValid(i) && m.Value(i) {
			idx = append(idx, i)
		}
	}
	return takeOptional(ctx, c, idx, mem)
}
