package series

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"sync"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)

// Whole-buffer string kernels. These treat the values buffer of a
// string array as one flat byte slice instead of visiting rows one at
// a time, and split large inputs across goroutines.

// strParallelMinBytes is the values-buffer size above which the
// whole-buffer kernels fan out. Below it goroutine start-up costs more
// than the scan.
const strParallelMinBytes = 1 << 20

// strWorkers picks a worker count for work proportional to size bytes.
func strWorkers(size int) int {
	if size < strParallelMinBytes {
		return 1
	}
	return max(1, min(runtime.GOMAXPROCS(0), size/(strParallelMinBytes/4)))
}

// parallelRange splits [0, n) into at most workers contiguous ranges
// whose starts are multiples of align and runs fn on each.
func parallelRange(n, workers, align int, fn func(lo, hi int)) {
	if workers <= 1 || n <= align {
		fn(0, n)
		return
	}
	chunk := (n + workers - 1) / workers
	chunk = (chunk + align - 1) / align * align
	var wg sync.WaitGroup
	for lo := 0; lo < n; lo += chunk {
		hi := min(lo+chunk, n)
		wg.Go(func() { fn(lo, hi) })
	}
	wg.Wait()
}

const (
	swarOnes = 0x0101010101010101
	swarHigh = 0x8080808080808080
)

// foldAsciiSWAR writes src to dst with ASCII letters case folded, eight
// bytes per step. It reports false, leaving dst partly written, when
// src holds any byte >= 0x80; callers then take the Unicode path.
//
// For a word whose bytes are all < 0x80, adding (0x80 - lo) to each
// byte sets its high bit exactly when byte >= lo, with no carry into
// the next byte because 0x7f + 0x80 - lo stays below 0x100 for the
// bounds used here.
func foldAsciiSWAR(src, dst []byte, upper bool) bool {
	lo, hi := uint64('a'), uint64('z')
	if !upper {
		lo, hi = 'A', 'Z'
	}
	addLo := (0x80 - lo) * swarOnes
	addHi := (0x80 - hi - 1) * swarOnes
	_ = dst[:len(src)]
	i := 0
	for ; i+8 <= len(src); i += 8 {
		w := binary.LittleEndian.Uint64(src[i:])
		if w&swarHigh != 0 {
			return false
		}
		geLo := (w + addLo) & swarHigh
		gtHi := (w + addHi) & swarHigh
		mask := (geLo &^ gtHi) >> 2 // 0x20 in every letter byte
		binary.LittleEndian.PutUint64(dst[i:], w^mask)
	}
	for ; i < len(src); i++ {
		b := src[i]
		if b >= 0x80 {
			return false
		}
		if uint64(b) >= lo && uint64(b) <= hi {
			b ^= 0x20
		}
		dst[i] = b
	}
	return true
}

// caseFoldAsciiParallel folds src into dst on several goroutines.
// Returns false when any byte is non-ASCII.
func caseFoldAsciiParallel(src, dst []byte, upper bool) bool {
	workers := strWorkers(len(src))
	if workers <= 1 {
		return foldAsciiSWAR(src, dst, upper)
	}
	var mu sync.Mutex
	ok := true
	parallelRange(len(src), workers, 64, func(lo, hi int) {
		if !foldAsciiSWAR(src[lo:hi], dst[lo:hi], upper) {
			mu.Lock()
			ok = false
			mu.Unlock()
		}
	})
	return ok
}

// caseFoldAscii is the ASCII case-fold kernel: offsets are copied, the
// values buffer is folded in one flat pass. ok=false means the input
// was not pure ASCII and nothing was returned.
func caseFoldAscii(name string, a *array.String, alloc memory.Allocator, upper bool) (*Series, bool, error) {
	alloc = mempool.Pooling(alloc)
	n := a.Len()
	src := a.ValueBytes()

	valuesBuf := memory.NewResizableBuffer(alloc)
	valuesBuf.Resize(len(src))
	if !caseFoldAsciiParallel(src, valuesBuf.Bytes(), upper) {
		valuesBuf.Release()
		return nil, false, nil
	}
	offsetsBuf := rebasedOffsets(a, alloc)
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
	s, err := New(name, arr)
	return s, true, err
}

// rebasedOffsets copies a's offsets into a new buffer starting at 0.
func rebasedOffsets(a *array.String, alloc memory.Allocator) *memory.Buffer {
	src := a.ValueOffsets()
	buf := memory.NewResizableBuffer(alloc)
	buf.Resize(len(src) * 4)
	dst := arrow.Int32Traits.CastFromBytes(buf.Bytes())
	if len(src) == 0 {
		return buf
	}
	if src[0] == 0 {
		copy(dst, src)
		return buf
	}
	base := src[0]
	for i, off := range src {
		dst[i] = off - base
	}
	return buf
}

// int64FromOffsets builds the int64 result of a per-row count kernel.
// fill writes values for rows [lo, hi); it runs on several goroutines
// for large inputs. Null rows keep whatever fill wrote; the validity
// bitmap masks them.
func int64FromOffsets(name string, a *array.String, alloc memory.Allocator, fill func(dst []int64, lo, hi int)) (*Series, error) {
	alloc = mempool.Pooling(alloc)
	n := a.Len()
	dataBuf := memory.NewResizableBuffer(alloc)
	dataBuf.Resize(n * 8)
	values := arrow.Int64Traits.CastFromBytes(dataBuf.Bytes())
	workers := strWorkers(n * 8)
	parallelRange(n, workers, 8, func(lo, hi int) { fill(values, lo, hi) })

	nullBuf := copyValidityBitmap(a, alloc)
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

// lenBytesFill writes offset differences, the byte length of each row.
func lenBytesFill(offsets []int32) func(dst []int64, lo, hi int) {
	return func(dst []int64, lo, hi int) {
		off := offsets[lo : hi+1]
		out := dst[lo:hi]
		for i := range out {
			out[i] = int64(off[i+1] - off[i])
		}
	}
}

// lenCharsKernel counts runes per row. A pure-ASCII values buffer makes
// the rune count equal the byte count, so the kernel reads only the
// offsets. Otherwise each row is counted with utf8.RuneCount, which
// treats each invalid byte as one rune exactly like the per-row
// RuneCountInString it replaces.
func lenCharsKernel(name string, a *array.String, alloc memory.Allocator) (*Series, error) {
	offsets := a.ValueOffsets()
	values := a.ValueBytes()
	if isAsciiParallel(values) {
		return int64FromOffsets(name, a, alloc, lenBytesFill(offsets))
	}
	base := int32(0)
	if len(offsets) > 0 {
		base = offsets[0]
	}
	return int64FromOffsets(name, a, alloc, func(dst []int64, lo, hi int) {
		for i := lo; i < hi; i++ {
			s, e := offsets[i]-base, offsets[i+1]-base
			dst[i] = int64(utf8.RuneCount(values[s:e]))
		}
	})
}

// isAsciiParallel reports whether b has no byte >= 0x80.
func isAsciiParallel(b []byte) bool {
	workers := strWorkers(len(b))
	if workers <= 1 {
		return isAsciiSWAR(b)
	}
	var mu sync.Mutex
	ok := true
	parallelRange(len(b), workers, 64, func(lo, hi int) {
		if !isAsciiSWAR(b[lo:hi]) {
			mu.Lock()
			ok = false
			mu.Unlock()
		}
	})
	return ok
}

func isAsciiSWAR(b []byte) bool {
	i := 0
	var acc uint64
	for ; i+32 <= len(b); i += 32 {
		acc |= binary.LittleEndian.Uint64(b[i:]) |
			binary.LittleEndian.Uint64(b[i+8:]) |
			binary.LittleEndian.Uint64(b[i+16:]) |
			binary.LittleEndian.Uint64(b[i+24:])
		if acc&swarHigh != 0 {
			return false
		}
	}
	for ; i+8 <= len(b); i += 8 {
		acc |= binary.LittleEndian.Uint64(b[i:])
	}
	if acc&swarHigh != 0 {
		return false
	}
	for ; i < len(b); i++ {
		if b[i] >= 0x80 {
			return false
		}
	}
	return true
}

// containsLiteralKernel sets out[i] when row i contains needle
// (len(needle) >= 1). Rather than calling a search per row, it runs
// bytes.Index over the flat values buffer and maps each hit back to its
// row by walking the offsets forward. A hit that straddles a row
// boundary is not a match; the search resumes one byte later. After a
// real match the search jumps to the next row start, so each row costs
// at most one reported hit.
func containsLiteralKernel(name string, a *array.String, alloc memory.Allocator, needle []byte) (*Series, error) {
	alloc = mempool.Pooling(alloc)
	n := a.Len()
	offsets := a.ValueOffsets()
	values := a.ValueBytes()
	base := int32(0)
	if len(offsets) > 0 {
		base = offsets[0]
	}

	dataBuf := memory.NewResizableBuffer(alloc)
	dataBuf.Resize((n + 7) / 8)
	bits := dataBuf.Bytes()
	clear(bits)

	workers := strWorkers(len(values))
	// Ranges start on multiples of 8 rows so no two workers write the
	// same bitmap byte.
	parallelRange(n, workers, 8, func(lo, hi int) {
		start := int(offsets[lo] - base)
		end := int(offsets[hi] - base)
		row := lo
		pos := start
		for pos < end {
			j := bytes.Index(values[pos:end], needle)
			if j < 0 {
				break
			}
			hit := pos + j
			for int(offsets[row+1]-base) <= hit {
				row++
			}
			rowEnd := int(offsets[row+1] - base)
			if hit+len(needle) <= rowEnd {
				bits[row>>3] |= 1 << uint(row&7)
				pos = rowEnd
				row++
			} else {
				pos = hit + 1
			}
		}
	})

	nullBuf := copyValidityBitmap(a, alloc)
	if nullBuf != nil {
		// Null rows may hold bytes; keep their value bits clear.
		valid := nullBuf.Bytes()
		for i := range bits {
			bits[i] &= valid[i]
		}
	}
	arr := array.NewBoolean(n, dataBuf, nullBuf, a.NullN())
	dataBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(name, arr)
}
