package series

import (
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Head returns a Series holding the first n characters of each string.
// n is counted in runes (to match polars str.head semantics) for
// non-ASCII input; ASCII-only columns short-circuit to byte-level.
// Negative n means "all but the last |n| characters".
//
// ASCII fast path: since each UTF-8 code point is exactly one byte in
// ASCII, rune count equals byte count. We can slice offsets directly
// without decoding.
func (o StrOps) Head(n int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("Head")
	if err != nil {
		return nil, err
	}
	if isAsciiSWAR(a.ValueBytes()) {
		return headTailAsciiSlice(o.s.Name(), a, cfg.alloc, n, true)
	}
	return o.mapString("Head", func(s string) string {
		if n >= 0 {
			return headRunes(s, n)
		}
		return headRunesAllBut(s, -n)
	}, opts)
}

// Tail returns the last n characters of each string. Symmetric to
// Head. Negative n means "all but the first |n| characters".
func (o StrOps) Tail(n int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("Tail")
	if err != nil {
		return nil, err
	}
	if isAsciiSWAR(a.ValueBytes()) {
		return headTailAsciiSlice(o.s.Name(), a, cfg.alloc, n, false)
	}
	return o.mapString("Tail", func(s string) string {
		if n >= 0 {
			return tailRunes(s, n)
		}
		return tailRunesAllBut(s, -n)
	}, opts)
}

// Find returns the byte offset of the first occurrence of needle in
// each string as u32, or null when needle is not found. Null inputs
// stay null. Matches polars str.find (byte-indexed, literal needle).
func (o StrOps) Find(needle string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("Find")
	if err != nil {
		return nil, err
	}
	index := func(s string) int { return strings.Index(s, needle) }
	if len(needle) == 1 {
		b := needle[0]
		index = func(s string) int { return strings.IndexByte(s, b) }
	}
	n := a.Len()
	out := make([]uint32, n)
	valid := make([]bool, n)
	for i := range n {
		if !a.IsValid(i) {
			continue
		}
		if at := index(a.Value(i)); at >= 0 {
			out[i] = uint32(at)
			valid[i] = true
		}
	}
	return FromUint32(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// headTailAsciiSlice is the ASCII-only same-buffer slice. We rebuild
// offsets so each row holds n bytes of the original (or fewer when a
// row is shorter than n). Values buffer is copied in one pass - no
// per-row allocations.
func headTailAsciiSlice(name string, a *array.String, alloc memory.Allocator, n int, head bool) (*Series, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	rows := a.Len()
	srcOffsets := a.ValueOffsets()
	srcValues := a.ValueBytes()
	base := int32(0)
	if len(srcOffsets) > 0 {
		base = srcOffsets[0]
	}

	// First pass: compute per-row slice bounds [lo, hi) in source
	// coordinates and the total output byte count. A second pass
	// copies bytes into the pre-sized values buffer.
	los := make([]int32, rows)
	his := make([]int32, rows)
	var total int32
	for i := range rows {
		rowStart := srcOffsets[i] - base
		rowEnd := srcOffsets[i+1] - base
		rowLen := rowEnd - rowStart
		var take int32
		switch {
		case n >= 0:
			take = min(int32(n), rowLen)
		default:
			// Negative: "all but |n| from the other end".
			drop := int32(-n)
			if drop >= rowLen {
				take = 0
			} else {
				take = rowLen - drop
			}
		}
		if head {
			los[i] = rowStart
			his[i] = rowStart + take
		} else {
			los[i] = rowEnd - take
			his[i] = rowEnd
		}
		total += take
	}

	offsetsBuf := memory.NewResizableBuffer(alloc)
	offsetsBuf.Resize((rows + 1) * 4)
	dstOffsets := arrow.Int32Traits.CastFromBytes(offsetsBuf.Bytes())
	valuesBuf := memory.NewResizableBuffer(alloc)
	valuesBuf.Resize(int(total))
	dstValues := valuesBuf.Bytes()

	var cursor int32
	dstOffsets[0] = 0
	for i := range rows {
		lo, hi := los[i], his[i]
		length := hi - lo
		if length > 0 {
			copy(dstValues[cursor:cursor+length], srcValues[lo:hi])
			cursor += length
		}
		dstOffsets[i+1] = cursor
	}

	var nullBuf *memory.Buffer
	if a.NullN() > 0 {
		nullBuf = copyValidityBitmap(a, alloc)
	}
	data := array.NewData(
		arrow.BinaryTypes.String, rows,
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

// headRunes is the rune-aware fallback for non-ASCII columns.
func headRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	i, cnt := 0, 0
	for cnt < n && i < len(s) {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
		cnt++
	}
	return s[:i]
}

func headRunesAllBut(s string, drop int) string {
	total := utf8.RuneCountInString(s)
	keep := total - drop
	if keep <= 0 {
		return ""
	}
	return headRunes(s, keep)
}

func tailRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	total := utf8.RuneCountInString(s)
	if n >= total {
		return s
	}
	skip := total - n
	i, cnt := 0, 0
	for cnt < skip && i < len(s) {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
		cnt++
	}
	return s[i:]
}

func tailRunesAllBut(s string, drop int) string {
	total := utf8.RuneCountInString(s)
	keep := total - drop
	if keep <= 0 {
		return ""
	}
	return tailRunes(s, keep)
}
