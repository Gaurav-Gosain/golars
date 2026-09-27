package compute

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// dateCompareScalar compares a single-chunk Date column with a non-null
// Date scalar on the int32 day numbers directly. The general temporal
// path widens both sides to int64 (a full copy of the column) and calls
// an ordering closure per row; date range filters (TPC-H Q1, Q6, Q12)
// hit this shape constantly. ok is false when the shape does not match.
func dateCompareScalar(a, b *series.Series, cfg *config, op compareOp) (*series.Series, bool, error) {
	if a.DType().ID() != arrow.DATE32 || b.DType().ID() != arrow.DATE32 ||
		b.Len() != 1 || b.NullCount() != 0 || a.NumChunks() != 1 || b.NumChunks() != 1 {
		return nil, false, nil
	}
	arr, ok := a.Chunk(0).(*array.Date32)
	if !ok {
		return nil, false, nil
	}
	lit, ok := b.Chunk(0).(*array.Date32)
	if !ok {
		return nil, false, nil
	}
	vals := arr.Date32Values()
	y := lit.Value(0)
	mem := cfg.alloc
	var valid = series.CopyValidityBitmap(arr, mem)
	out, err := series.BuildBoolDirectWithValidity(cfg.outName(a.Name()), len(vals), mem, func(bits []byte) {
		fillDateCmp(bits, vals, y, op)
	}, valid, arr.NullN())
	return out, true, err
}

// fillDateCmp sets bit i when vals[i] op y holds. Three byte-at-a-time
// loops (less, greater, equal) cover the six operators, the others by
// inverting whole bytes; bits past the end stay zero.
func fillDateCmp(bits []byte, vals []arrow.Date32, y arrow.Date32, op compareOp) {
	var inv byte
	switch op {
	case opLt:
		dateLtBits(bits, vals, y)
	case opGe:
		dateLtBits(bits, vals, y)
		inv = 0xFF
	case opGt:
		dateGtBits(bits, vals, y)
	case opLe:
		dateGtBits(bits, vals, y)
		inv = 0xFF
	case opEq:
		dateEqBits(bits, vals, y)
	case opNe:
		dateEqBits(bits, vals, y)
		inv = 0xFF
	}
	if inv == 0 || len(vals) == 0 {
		return
	}
	for i := range bits {
		bits[i] ^= inv
	}
	if r := len(vals) & 7; r != 0 {
		bits[len(bits)-1] &= byte(1)<<uint(r) - 1
	}
}

func b2u8(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func dateLtBits(bits []byte, vals []arrow.Date32, y arrow.Date32) {
	full := len(vals) / 8
	for k := range full {
		c := vals[k*8 : k*8+8 : k*8+8]
		bits[k] = b2u8(c[0] < y) | b2u8(c[1] < y)<<1 | b2u8(c[2] < y)<<2 | b2u8(c[3] < y)<<3 |
			b2u8(c[4] < y)<<4 | b2u8(c[5] < y)<<5 | b2u8(c[6] < y)<<6 | b2u8(c[7] < y)<<7
	}
	for i := full * 8; i < len(vals); i++ {
		bits[i>>3] |= b2u8(vals[i] < y) << uint(i&7)
	}
}

func dateGtBits(bits []byte, vals []arrow.Date32, y arrow.Date32) {
	full := len(vals) / 8
	for k := range full {
		c := vals[k*8 : k*8+8 : k*8+8]
		bits[k] = b2u8(c[0] > y) | b2u8(c[1] > y)<<1 | b2u8(c[2] > y)<<2 | b2u8(c[3] > y)<<3 |
			b2u8(c[4] > y)<<4 | b2u8(c[5] > y)<<5 | b2u8(c[6] > y)<<6 | b2u8(c[7] > y)<<7
	}
	for i := full * 8; i < len(vals); i++ {
		bits[i>>3] |= b2u8(vals[i] > y) << uint(i&7)
	}
}

func dateEqBits(bits []byte, vals []arrow.Date32, y arrow.Date32) {
	full := len(vals) / 8
	for k := range full {
		c := vals[k*8 : k*8+8 : k*8+8]
		bits[k] = b2u8(c[0] == y) | b2u8(c[1] == y)<<1 | b2u8(c[2] == y)<<2 | b2u8(c[3] == y)<<3 |
			b2u8(c[4] == y)<<4 | b2u8(c[5] == y)<<5 | b2u8(c[6] == y)<<6 | b2u8(c[7] == y)<<7
	}
	for i := full * 8; i < len(vals); i++ {
		bits[i>>3] |= b2u8(vals[i] == y) << uint(i&7)
	}
}
