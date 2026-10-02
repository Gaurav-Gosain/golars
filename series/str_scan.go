package series

import (
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// str_scan.go holds whole-buffer string kernels. Instead of calling a
// search per row, they run the (SIMD-accelerated) substring search over
// the entire values buffer and map each hit back to its row by walking
// the offsets forward. Rows without a hit cost nothing, so sparse
// matches run at memchr speed.

// containsWholeBuffer marks row i when needle occurs inside value i.
func containsWholeBuffer(name string, a *array.String, needle string, alloc memory.Allocator) (*Series, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	n := a.Len()
	offs := a.ValueOffsets()
	data := unsafe.String(unsafe.SliceData(a.ValueBytes()), len(a.ValueBytes()))
	base := 0
	if len(offs) > 0 {
		base = int(offs[0])
	}
	buf := memory.NewResizableBuffer(alloc)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	bits := buf.Bytes()
	clear(bits)
	row, pos := 0, 0
	for row < n {
		k := strings.Index(data[pos:], needle)
		if k < 0 {
			break
		}
		m := pos + k
		for row < n && int(offs[row+1])-base <= m {
			row++
		}
		if row >= n {
			break
		}
		rowEnd := int(offs[row+1]) - base
		if m+len(needle) <= rowEnd {
			bits[row>>3] |= 1 << uint(row&7)
			// One hit per row is enough: jump to the next row.
			pos = rowEnd
			row++
			continue
		}
		// The hit straddles a row boundary; resume just after its start.
		pos = m + 1
	}
	var nullBuf *memory.Buffer
	if a.NullN() > 0 {
		nullBuf = copyValidityBitmap(a, alloc)
	}
	arr := array.NewBoolean(n, buf, nullBuf, a.NullN())
	buf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

// splitByteWholeBuffer is Split for a one-byte separator (not
// inclusive). It counts pieces up front so the child offsets and data
// are allocated exactly once, then fills them in a single pass.
func splitByteWholeBuffer(name string, a *array.String, sep byte, mem memory.Allocator) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	n := a.Len()
	offs := a.ValueOffsets()
	raw := a.ValueBytes()
	base := int32(0)
	if len(offs) > 0 {
		base = offs[0]
	}
	// Pieces: one per valid row plus one per separator inside a valid
	// row. Separator bytes are dropped from the child data.
	pieces, seps := 0, 0
	sepStr := string([]byte{sep})
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		s, e := offs[i]-base, offs[i+1]-base
		c := strings.Count(unsafe.String(unsafe.SliceData(raw[s:]), int(e-s)), sepStr)
		seps += c
		pieces += c + 1
	}
	childBytes := 0
	for i := range n {
		if a.IsValid(i) {
			childBytes += int(offs[i+1] - offs[i])
		}
	}
	childBytes -= seps

	listOffBuf := memory.NewResizableBuffer(mem)
	listOffBuf.Resize((n + 1) * 4)
	listOffs := arrow.Int32Traits.CastFromBytes(listOffBuf.Bytes())
	childOffBuf := memory.NewResizableBuffer(mem)
	childOffBuf.Resize((pieces + 1) * 4)
	childOffs := arrow.Int32Traits.CastFromBytes(childOffBuf.Bytes())
	childDataBuf := memory.NewResizableBuffer(mem)
	childDataBuf.Resize(childBytes)
	dst := childDataBuf.Bytes()

	p, w := 0, 0
	childOffs[0] = 0
	listOffs[0] = 0
	for i := range n {
		if a.IsValid(i) {
			row := raw[offs[i]-base : offs[i+1]-base]
			for {
				k := indexByte(row, sep)
				if k < 0 {
					w += copy(dst[w:], row)
					p++
					childOffs[p] = int32(w)
					break
				}
				w += copy(dst[w:], row[:k])
				p++
				childOffs[p] = int32(w)
				row = row[k+1:]
			}
		}
		listOffs[i+1] = int32(p)
	}

	childData := array.NewData(arrow.BinaryTypes.String, pieces,
		[]*memory.Buffer{nil, childOffBuf, childDataBuf}, nil, 0, 0)
	var nullBuf *memory.Buffer
	if a.NullN() > 0 {
		nullBuf = copyValidityBitmap(a, mem)
	}
	listData := array.NewData(arrow.ListOf(arrow.BinaryTypes.String), n,
		[]*memory.Buffer{nullBuf, listOffBuf}, []arrow.ArrayData{childData}, a.NullN(), 0)
	arr := array.NewListData(listData)
	listData.Release()
	childData.Release()
	listOffBuf.Release()
	childOffBuf.Release()
	childDataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}

func indexByte(b []byte, c byte) int {
	return strings.IndexByte(unsafe.String(unsafe.SliceData(b), len(b)), c)
}
