package compute

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// dateComparePair compares two single-chunk Date columns of the same
// length row by row on their int32 day numbers (TPC-H Q4 and Q12 test
// l_commitdate < l_receiptdate over the whole lineitem table). The
// general temporal path widens both to int64 and calls a closure per
// row. ok is false when the shape does not match.
func dateComparePair(a, b *series.Series, cfg *config, op compareOp) (*series.Series, bool, error) {
	if a.DType().ID() != arrow.DATE32 || b.DType().ID() != arrow.DATE32 ||
		a.Len() != b.Len() || a.NumChunks() != 1 || b.NumChunks() != 1 {
		return nil, false, nil
	}
	x, ok := a.Chunk(0).(*array.Date32)
	if !ok {
		return nil, false, nil
	}
	y, ok := b.Chunk(0).(*array.Date32)
	if !ok {
		return nil, false, nil
	}
	xv, yv := x.Date32Values(), y.Date32Values()
	n := len(xv)
	mem := cfg.alloc
	valid, nulls := andValidity(x, y, n, mem)
	out, err := series.BuildBoolDirectWithValidity(cfg.outName(a.Name()), n, mem, func(bits []byte) {
		fillDatePairCmp(bits, xv, yv, op)
	}, valid, nulls)
	return out, true, err
}

// andValidity returns the combined validity bitmap of x and y (nil when
// neither has nulls) and its null count.
func andValidity(x, y arrow.Array, n int, mem memory.Allocator) (*memory.Buffer, int) {
	switch {
	case x.NullN() == 0 && y.NullN() == 0:
		return nil, 0
	case y.NullN() == 0:
		return series.CopyValidityBitmap(x, mem), x.NullN()
	case x.NullN() == 0:
		return series.CopyValidityBitmap(y, mem), y.NullN()
	}
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	bits := buf.Bytes()
	clear(bits)
	nulls := 0
	for i := range n {
		if x.IsValid(i) && y.IsValid(i) {
			bitutil.SetBit(bits, i)
		} else {
			nulls++
		}
	}
	return buf, nulls
}

// fillDatePairCmp sets bit i when xv[i] op yv[i] holds, eight rows per
// output byte; the other operators invert whole bytes as in
// fillDateCmp.
func fillDatePairCmp(bits []byte, xv, yv []arrow.Date32, op compareOp) {
	var inv byte
	switch op {
	case opLt:
		datePairLt(bits, xv, yv)
	case opGe:
		datePairLt(bits, xv, yv)
		inv = 0xFF
	case opGt:
		datePairLt(bits, yv, xv)
	case opLe:
		datePairLt(bits, yv, xv)
		inv = 0xFF
	case opEq:
		datePairEq(bits, xv, yv)
	case opNe:
		datePairEq(bits, xv, yv)
		inv = 0xFF
	}
	if inv == 0 || len(xv) == 0 {
		return
	}
	for i := range bits {
		bits[i] ^= inv
	}
	if r := len(xv) & 7; r != 0 {
		bits[len(bits)-1] &= byte(1)<<uint(r) - 1
	}
}

func datePairLt(bits []byte, xv, yv []arrow.Date32) {
	yv = yv[:len(xv)]
	full := len(xv) / 8
	for k := range full {
		a := xv[k*8 : k*8+8 : k*8+8]
		b := yv[k*8 : k*8+8 : k*8+8]
		bits[k] = b2u8(a[0] < b[0]) | b2u8(a[1] < b[1])<<1 | b2u8(a[2] < b[2])<<2 | b2u8(a[3] < b[3])<<3 |
			b2u8(a[4] < b[4])<<4 | b2u8(a[5] < b[5])<<5 | b2u8(a[6] < b[6])<<6 | b2u8(a[7] < b[7])<<7
	}
	for i := full * 8; i < len(xv); i++ {
		bits[i>>3] |= b2u8(xv[i] < yv[i]) << uint(i&7)
	}
}

func datePairEq(bits []byte, xv, yv []arrow.Date32) {
	yv = yv[:len(xv)]
	full := len(xv) / 8
	for k := range full {
		a := xv[k*8 : k*8+8 : k*8+8]
		b := yv[k*8 : k*8+8 : k*8+8]
		bits[k] = b2u8(a[0] == b[0]) | b2u8(a[1] == b[1])<<1 | b2u8(a[2] == b[2])<<2 | b2u8(a[3] == b[3])<<3 |
			b2u8(a[4] == b[4])<<4 | b2u8(a[5] == b[5])<<5 | b2u8(a[6] == b[6])<<6 | b2u8(a[7] == b[7])<<7
	}
	for i := full * 8; i < len(xv); i++ {
		bits[i>>3] |= b2u8(xv[i] == yv[i]) << uint(i&7)
	}
}
