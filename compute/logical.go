package compute

import (
	"context"
	"encoding/binary"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// The logical kernels work on packed bitmaps 64 rows at a time. A
// boolean column is two bitmaps: values and validity. Kleene logic is
// expressed with two derived masks per side, "known true"
// (T = value & valid) and "known false" (F = ^value & valid):
//
//	AND: T = Ta & Tb, F = Fa | Fb
//	OR:  T = Ta | Tb, F = Fa & Fb
//
// and the result is value = T, valid = T | F. Without nulls on either
// side the validity bitmap is skipped and value = a op b directly.
// Word-wise loops are memory bound (a few hundred microseconds for
// tens of millions of rows) so they run serially.

// And returns a boolean Series a AND b with Kleene three-valued logic:
//   - true AND null = null
//   - false AND null = false
//   - null AND null = null
func And(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runKleene(ctx, a, b, opts, "And", true)
}

// Or returns a boolean Series a OR b with Kleene three-valued logic:
//   - false OR null = null
//   - true OR null = true
//   - null OR null = null
func Or(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runKleene(ctx, a, b, opts, "Or", false)
}

// Not returns a boolean Series NOT a. Nulls propagate.
func Not(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	if !s.DType().IsBool() {
		return nil, isUnsupported("Not", s.DType())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	n := arr.Len()
	vals := valueBits(arr)
	valid := validityBits(arr)
	return buildBoolBitmaps(cfg.outName(s.Name()), n, cfg.alloc, valid != nil,
		func(out, outValid []byte) {
			nb := len(out)
			if valid == nil {
				notWords(out, vals, nb)
				return
			}
			// Null slots get value 0 so the output is canonical.
			for i := 0; i+8 <= nb; i += 8 {
				v := le64(vals[i:])
				m := le64(valid[i:])
				put64(out[i:], ^v&m)
				put64(outValid[i:], m)
			}
			for i := nb &^ 7; i < nb; i++ {
				out[i] = ^vals[i] & valid[i]
				outValid[i] = valid[i]
			}
		})
}

// runKleene implements AND/OR with three-valued logic. isAnd selects the op.
func runKleene(ctx context.Context, a, b *series.Series, opts []Option, kernel string, isAnd bool) (*series.Series, error) {
	if err := checkBinary(a, b); err != nil {
		return nil, err
	}
	if !a.DType().IsBool() {
		return nil, isUnsupported(kernel, a.DType())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)

	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	bArr, err := extractChunk(b, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bArr.Release()

	n := aArr.Len()
	av, bv := valueBits(aArr), valueBits(bArr)
	am, bm := validityBits(aArr), validityBits(bArr)
	hasNulls := am != nil || bm != nil

	return buildBoolBitmaps(cfg.outName(a.Name()), n, cfg.alloc, hasNulls,
		func(out, outValid []byte) {
			nb := len(out)
			if !hasNulls {
				if isAnd {
					andWords(out, av, bv, nb)
				} else {
					orWords(out, av, bv, nb)
				}
				return
			}
			kleeneWords(out, outValid, av, am, bv, bm, nb, isAnd)
		})
}

// kleeneWords evaluates Kleene AND/OR over nb bytes. A nil validity
// bitmap means all valid.
func kleeneWords(out, outValid, av, am, bv, bm []byte, nb int, isAnd bool) {
	word := func(bits []byte, i int) uint64 {
		if bits == nil {
			return ^uint64(0)
		}
		return le64(bits[i:])
	}
	for i := 0; i+8 <= nb; i += 8 {
		am64, bm64 := word(am, i), word(bm, i)
		a64, b64 := le64(av[i:]), le64(bv[i:])
		ta, fa := a64&am64, ^a64&am64
		tb, fb := b64&bm64, ^b64&bm64
		var t, f uint64
		if isAnd {
			t, f = ta&tb, fa|fb
		} else {
			t, f = ta|tb, fa&fb
		}
		put64(out[i:], t)
		put64(outValid[i:], t|f)
	}
	byteOf := func(bits []byte, i int) byte {
		if bits == nil {
			return 0xff
		}
		return bits[i]
	}
	for i := nb &^ 7; i < nb; i++ {
		amb, bmb := byteOf(am, i), byteOf(bm, i)
		ta, fa := av[i]&amb, ^av[i]&amb
		tb, fb := bv[i]&bmb, ^bv[i]&bmb
		var t, f byte
		if isAnd {
			t, f = ta&tb, fa|fb
		} else {
			t, f = ta|tb, fa&fb
		}
		out[i] = t
		outValid[i] = t | f
	}
}

func andWords(out, a, b []byte, nb int) {
	for i := 0; i+8 <= nb; i += 8 {
		put64(out[i:], le64(a[i:])&le64(b[i:]))
	}
	for i := nb &^ 7; i < nb; i++ {
		out[i] = a[i] & b[i]
	}
}

func orWords(out, a, b []byte, nb int) {
	for i := 0; i+8 <= nb; i += 8 {
		put64(out[i:], le64(a[i:])|le64(b[i:]))
	}
	for i := nb &^ 7; i < nb; i++ {
		out[i] = a[i] | b[i]
	}
}

func notWords(out, a []byte, nb int) {
	for i := 0; i+8 <= nb; i += 8 {
		put64(out[i:], ^le64(a[i:]))
	}
	for i := nb &^ 7; i < nb; i++ {
		out[i] = ^a[i]
	}
}

func le64(b []byte) uint64     { return binary.LittleEndian.Uint64(b) }
func put64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }

// valueBits returns the boolean value bitmap of arr starting at bit 0.
// Offsets that are a whole number of bytes are sliced in place; other
// offsets are copied into a fresh aligned bitmap.
func valueBits(arr arrow.Array) []byte {
	bufs := arr.Data().Buffers()
	if len(bufs) < 2 || bufs[1] == nil {
		return make([]byte, bitutil.BytesForBits(int64(arr.Len())))
	}
	return alignBits(bufs[1].Bytes(), arr.Data().Offset(), arr.Len())
}

// validityBits returns arr's validity bitmap starting at bit 0, or nil
// when arr has no nulls.
func validityBits(arr arrow.Array) []byte {
	if arr.NullN() == 0 {
		return nil
	}
	bufs := arr.Data().Buffers()
	if len(bufs) == 0 || bufs[0] == nil {
		// Every slot is null (for example a null-typed cast result).
		return make([]byte, bitutil.BytesForBits(int64(arr.Len())))
	}
	return alignBits(bufs[0].Bytes(), arr.Data().Offset(), arr.Len())
}

func alignBits(bits []byte, offset, n int) []byte {
	nb := int(bitutil.BytesForBits(int64(n)))
	if offset%8 == 0 {
		start := offset / 8
		return bits[start : start+nb]
	}
	out := make([]byte, nb)
	bitutil.CopyBitmap(bits, offset, n, out, 0)
	return out
}

// buildBoolBitmaps allocates the value bitmap (and a validity bitmap
// when withValidity) of an n-row boolean array, lets fill write them,
// and wraps the result in a Series. Bits past n in the last byte are
// ignored by arrow; the null count only counts the first n bits.
func buildBoolBitmaps(name string, n int, mem memory.Allocator, withValidity bool, fill func(out, outValid []byte)) (*series.Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	nb := int(bitutil.BytesForBits(int64(n)))
	valBuf := memory.NewResizableBuffer(mem)
	valBuf.Resize(nb)
	defer valBuf.Release()
	var validBuf *memory.Buffer
	var outValid []byte
	if withValidity {
		validBuf = memory.NewResizableBuffer(mem)
		validBuf.Resize(nb)
		defer validBuf.Release()
		outValid = validBuf.Bytes()
	}
	if nb > 0 {
		fill(valBuf.Bytes(), outValid)
	}
	nulls := 0
	if withValidity {
		nulls = n - bitutil.CountSetBits(outValid, 0, n)
		if nulls == 0 {
			validBuf = nil
		}
	}
	data := array.NewData(arrow.FixedWidthTypes.Boolean, n,
		[]*memory.Buffer{validBuf, valBuf}, nil, nulls, 0)
	defer data.Release()
	return series.New(name, array.NewBooleanData(data))
}

// IsNull returns a boolean Series where out[i] is true iff s[i] is null. The
// result itself has no nulls.
func IsNull(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	return nullMask(ctx, s, opts, true)
}

// IsNotNull returns a boolean Series where out[i] is true iff s[i] is valid.
func IsNotNull(ctx context.Context, s *series.Series, opts ...Option) (*series.Series, error) {
	return nullMask(ctx, s, opts, false)
}

// nullMask turns the validity bitmap of s into a non-null boolean
// column: the bitmap itself for IsNotNull, its complement for IsNull.
func nullMask(ctx context.Context, s *series.Series, opts []Option, isNull bool) (*series.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := resolve(opts)
	name := cfg.outName(s.Name())
	n := s.Len()
	return buildBoolBitmaps(name, n, cfg.alloc, false, func(out, _ []byte) {
		pos := 0
		for _, c := range s.Chunks() {
			l := c.Len()
			if l == 0 {
				continue
			}
			valid := validityBits(c)
			switch {
			case valid == nil && isNull:
				bitutil.SetBitsTo(out, int64(pos), int64(l), false)
			case valid == nil:
				bitutil.SetBitsTo(out, int64(pos), int64(l), true)
			case isNull:
				tmp := make([]byte, len(valid))
				notWords(tmp, valid, len(valid))
				bitutil.CopyBitmap(tmp, 0, l, out, pos)
			default:
				bitutil.CopyBitmap(valid, 0, l, out, pos)
			}
			pos += l
		}
	})
}
