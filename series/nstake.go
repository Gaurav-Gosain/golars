package series

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// takeArrow gathers src[idx[i]] into a new array of the same dtype. An
// index of -1 produces a null. It handles every layout the nested
// namespaces meet: fixed-width primitives, booleans, (large) strings
// and binaries, (large) lists, fixed-size lists, structs, dictionaries
// and the null type. The list and array kernels reduce most of their
// work to computing idx and calling this once.
func takeArrow(src arrow.Array, idx []int, mem memory.Allocator) (arrow.Array, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	n := len(idx)
	validBuf, nulls := takeValidity(src, idx, mem)
	release := func() {
		if validBuf != nil {
			validBuf.Release()
		}
	}
	dt := src.DataType()
	switch a := src.(type) {
	case *array.Null:
		release()
		return array.NewNull(n), nil
	case *array.Boolean:
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(int(bitutil.BytesForBits(int64(n))))
		bits := buf.Bytes()
		clear(bits)
		for i, j := range idx {
			if j >= 0 && a.Value(j) {
				bitutil.SetBit(bits, i)
			}
		}
		data := array.NewData(dt, n, []*memory.Buffer{validBuf, buf}, nil, nulls, 0)
		out := array.MakeFromData(data)
		data.Release()
		buf.Release()
		release()
		return out, nil
	case *array.String:
		return takeVarBinary32(dt, a.ValueOffsets(), a.ValueBytes(), idx, validBuf, nulls, mem), nil
	case *array.Binary:
		return takeVarBinary32(dt, a.ValueOffsets(), a.ValueBytes(), idx, validBuf, nulls, mem), nil
	case *array.LargeString:
		return takeVarBinary64(dt, a.ValueOffsets(), a.ValueBytes(), idx, validBuf, nulls, mem), nil
	case *array.LargeBinary:
		return takeVarBinary64(dt, a.ValueOffsets(), a.ValueBytes(), idx, validBuf, nulls, mem), nil
	case *array.List, *array.LargeList:
		return takeList(a.(array.ListLike), idx, validBuf, nulls, mem)
	case *array.FixedSizeList:
		size := int(dt.(*arrow.FixedSizeListType).Len())
		childIdx := make([]int, 0, n*size)
		for _, j := range idx {
			if j < 0 {
				for range size {
					childIdx = append(childIdx, -1)
				}
				continue
			}
			start, _ := a.ValueOffsets(j)
			for k := range size {
				childIdx = append(childIdx, int(start)+k)
			}
		}
		child, err := takeArrow(a.ListValues(), childIdx, mem)
		if err != nil {
			release()
			return nil, err
		}
		data := array.NewData(dt, n, []*memory.Buffer{validBuf}, []arrow.ArrayData{child.Data()}, nulls, 0)
		out := array.MakeFromData(data)
		data.Release()
		child.Release()
		release()
		return out, nil
	case *array.Struct:
		st := dt.(*arrow.StructType)
		children := make([]arrow.ArrayData, st.NumFields())
		owned := make([]arrow.Array, st.NumFields())
		for f := range st.NumFields() {
			c, err := takeArrow(a.Field(f), idx, mem)
			if err != nil {
				for _, o := range owned[:f] {
					o.Release()
				}
				release()
				return nil, err
			}
			owned[f] = c
			children[f] = c.Data()
		}
		data := array.NewData(dt, n, []*memory.Buffer{validBuf}, children, nulls, 0)
		out := array.MakeFromData(data)
		data.Release()
		for _, o := range owned {
			o.Release()
		}
		release()
		return out, nil
	case *array.Dictionary:
		codes, err := takeArrow(a.Indices(), idx, mem)
		if err != nil {
			release()
			return nil, err
		}
		release()
		out := array.NewDictionaryArray(dt, codes, a.Dictionary())
		codes.Release()
		return out, nil
	}
	if fw, ok := dt.(arrow.FixedWidthDataType); ok && fw.BitWidth()%8 == 0 {
		width := fw.BitWidth() / 8
		d := src.Data()
		raw := d.Buffers()[1].Bytes()[d.Offset()*width:]
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(n * width)
		dst := buf.Bytes()
		switch width {
		case 8:
			takeFixed[uint64](dst, raw, idx)
		case 4:
			takeFixed[uint32](dst, raw, idx)
		case 2:
			takeFixed[uint16](dst, raw, idx)
		case 1:
			takeFixed[uint8](dst, raw, idx)
		default:
			for i, j := range idx {
				if j >= 0 {
					copy(dst[i*width:(i+1)*width], raw[j*width:(j+1)*width])
				} else {
					clear(dst[i*width : (i+1)*width])
				}
			}
		}
		data := array.NewData(dt, n, []*memory.Buffer{validBuf, buf}, nil, nulls, 0)
		out := array.MakeFromData(data)
		data.Release()
		buf.Release()
		release()
		return out, nil
	}
	release()
	return nil, fmt.Errorf("series: gather not supported for dtype %s", dt)
}

func takeFixed[T uint8 | uint16 | uint32 | uint64](dst, raw []byte, idx []int) {
	size := int(unsafe.Sizeof(T(0)))
	var out, in []T
	if len(dst) > 0 {
		out = unsafe.Slice((*T)(unsafe.Pointer(&dst[0])), len(dst)/size)
	}
	if len(raw) >= size {
		in = unsafe.Slice((*T)(unsafe.Pointer(&raw[0])), len(raw)/size)
	}
	for i, j := range idx {
		if j >= 0 {
			out[i] = in[j]
		} else {
			out[i] = 0
		}
	}
}

// takeValidity builds the output validity for a gather: bit i is set
// when idx[i] >= 0 and src is valid there.
func takeValidity(src arrow.Array, idx []int, mem memory.Allocator) (*memory.Buffer, int) {
	n := len(idx)
	srcNulls := src.NullN() > 0
	if _, isNull := src.(*array.Null); isNull {
		srcNulls = true
	}
	anyNeg := false
	for _, j := range idx {
		if j < 0 {
			anyNeg = true
			break
		}
	}
	if !srcNulls && !anyNeg {
		return nil, 0
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	bits := buf.Bytes()
	clear(bits)
	nulls := 0
	for i, j := range idx {
		if j >= 0 && src.IsValid(j) {
			bitutil.SetBit(bits, i)
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

func takeVarBinary32(dt arrow.DataType, offs []int32, data []byte, idx []int, validBuf *memory.Buffer, nulls int, mem memory.Allocator) arrow.Array {
	n := len(idx)
	total := 0
	for _, j := range idx {
		if j >= 0 {
			total += int(offs[j+1] - offs[j])
		}
	}
	base := int32(0)
	if len(offs) > 0 {
		base = offs[0]
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * 4)
	outOffs := unsafe.Slice((*int32)(unsafe.Pointer(&offBuf.Bytes()[0])), n+1)
	dataBuf := memory.NewResizableBuffer(mem)
	dataBuf.Resize(total)
	dst := dataBuf.Bytes()
	pos := int32(0)
	outOffs[0] = 0
	for i, j := range idx {
		if j >= 0 {
			s, e := offs[j]-base, offs[j+1]-base
			pos += int32(copy(dst[pos:], data[s:e]))
		}
		outOffs[i+1] = pos
	}
	d := array.NewData(dt, n, []*memory.Buffer{validBuf, offBuf, dataBuf}, nil, nulls, 0)
	out := array.MakeFromData(d)
	d.Release()
	offBuf.Release()
	dataBuf.Release()
	if validBuf != nil {
		validBuf.Release()
	}
	return out
}

func takeVarBinary64(dt arrow.DataType, offs []int64, data []byte, idx []int, validBuf *memory.Buffer, nulls int, mem memory.Allocator) arrow.Array {
	n := len(idx)
	total := int64(0)
	for _, j := range idx {
		if j >= 0 {
			total += offs[j+1] - offs[j]
		}
	}
	base := int64(0)
	if len(offs) > 0 {
		base = offs[0]
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * 8)
	outOffs := unsafe.Slice((*int64)(unsafe.Pointer(&offBuf.Bytes()[0])), n+1)
	dataBuf := memory.NewResizableBuffer(mem)
	dataBuf.Resize(int(total))
	dst := dataBuf.Bytes()
	pos := int64(0)
	outOffs[0] = 0
	for i, j := range idx {
		if j >= 0 {
			s, e := offs[j]-base, offs[j+1]-base
			pos += int64(copy(dst[pos:], data[s:e]))
		}
		outOffs[i+1] = pos
	}
	d := array.NewData(dt, n, []*memory.Buffer{validBuf, offBuf, dataBuf}, nil, nulls, 0)
	out := array.MakeFromData(d)
	d.Release()
	offBuf.Release()
	dataBuf.Release()
	if validBuf != nil {
		validBuf.Release()
	}
	return out
}

func takeList(a array.ListLike, idx []int, validBuf *memory.Buffer, nulls int, mem memory.Allocator) (arrow.Array, error) {
	n := len(idx)
	dt := a.DataType()
	childIdx := make([]int, 0, n)
	large := dt.ID() == arrow.LARGE_LIST
	width := 4
	if large {
		width = 8
	}
	offBuf := memory.NewResizableBuffer(mem)
	offBuf.Resize((n + 1) * width)
	var o32 []int32
	var o64 []int64
	if large {
		o64 = unsafe.Slice((*int64)(unsafe.Pointer(&offBuf.Bytes()[0])), n+1)
	} else {
		o32 = unsafe.Slice((*int32)(unsafe.Pointer(&offBuf.Bytes()[0])), n+1)
	}
	for i, j := range idx {
		if j >= 0 && a.IsValid(j) {
			start, end := a.ValueOffsets(j)
			for k := start; k < end; k++ {
				childIdx = append(childIdx, int(k))
			}
		}
		if large {
			o64[i+1] = int64(len(childIdx))
		} else {
			o32[i+1] = int32(len(childIdx))
		}
	}
	if large {
		o64[0] = 0
	} else {
		o32[0] = 0
	}
	child, err := takeArrow(a.ListValues(), childIdx, mem)
	if err != nil {
		offBuf.Release()
		if validBuf != nil {
			validBuf.Release()
		}
		return nil, err
	}
	data := array.NewData(dt, n, []*memory.Buffer{validBuf, offBuf}, []arrow.ArrayData{child.Data()}, nulls, 0)
	out := array.MakeFromData(data)
	data.Release()
	child.Release()
	offBuf.Release()
	if validBuf != nil {
		validBuf.Release()
	}
	return out, nil
}
