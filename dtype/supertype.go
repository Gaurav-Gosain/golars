package dtype

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
)

// NumericSupertype returns the dtype two numeric (or bool) operands are
// cast to before a binary operation, following the polars supertype
// table:
//
//   - equal dtypes stay as they are;
//   - two signed or two unsigned integers take the wider one;
//   - a signed and an unsigned integer take the signed type that holds
//     both (i16 for i8 and u8, i64 for i32 and u32), and f64 when the
//     unsigned side is u64;
//   - f32 holds i8, i16, u8 and u16; any other integer with a float
//     gives f64;
//   - bool takes the other side's dtype.
//
// ok is false when either side is not numeric or bool.
func NumericSupertype(a, b DType) (DType, bool) {
	if !numOrBool(a) || !numOrBool(b) {
		return DType{}, false
	}
	if a.Equal(b) {
		return a, true
	}
	if a.IsBool() {
		return b, true
	}
	if b.IsBool() {
		return a, true
	}
	if a.IsFloating() || b.IsFloating() {
		if a.ID() == arrow.FLOAT64 || b.ID() == arrow.FLOAT64 {
			return Float64(), true
		}
		// One side is f32, the other an integer.
		other := a
		if a.ID() == arrow.FLOAT32 {
			other = b
		}
		if intBits(other) <= 16 {
			return Float32(), true
		}
		return Float64(), true
	}
	as, bs := isSigned(a), isSigned(b)
	ab, bb := intBits(a), intBits(b)
	switch {
	case as == bs:
		if ab >= bb {
			return a, true
		}
		return b, true
	case as: // a signed, b unsigned
		return mixedSign(ab, bb), true
	default:
		return mixedSign(bb, ab), true
	}
}

// mixedSign is the supertype of a signed integer of sBits and an
// unsigned integer of uBits.
func mixedSign(sBits, uBits int) DType {
	if uBits < sBits {
		return signedOf(sBits)
	}
	if uBits == 64 {
		return Float64()
	}
	return signedOf(uBits * 2)
}

// DynIntTarget returns the dtype an untyped integer literal v takes
// when combined with an operand of dtype other, as polars does for
// pl.lit(v): the literal adopts other when v fits in it, else the
// smallest wider type that holds both. ok is false when other is not
// numeric; the literal then keeps its default dtype
// (see DefaultIntLiteral).
func DynIntTarget(v int64, other DType) (DType, bool) {
	switch {
	case other.IsFloating():
		return other, true
	case !other.IsInteger():
		return DType{}, false
	}
	bits := intBits(other)
	if isSigned(other) {
		for b := bits; b <= 64; b *= 2 {
			if fitsSigned(v, b) {
				return signedOf(b), true
			}
		}
		return Int64(), true
	}
	if v >= 0 {
		for b := bits; b <= 64; b *= 2 {
			if uint64(v) <= maxUnsigned(b) {
				return unsignedOf(b), true
			}
		}
		return Uint64(), true
	}
	for b := min(bits*2, 64); b <= 64; b *= 2 {
		if fitsSigned(v, b) {
			return signedOf(b), true
		}
	}
	return Int64(), true
}

// DynFloatTarget returns the dtype an untyped float literal takes when
// combined with an operand of dtype other: f32 next to an f32 operand,
// f64 otherwise.
func DynFloatTarget(other DType) DType {
	if other.ID() == arrow.FLOAT32 {
		return Float32()
	}
	return Float64()
}

// DefaultIntLiteral is the dtype an untyped integer literal
// materializes as on its own: i32 when v fits, else i64 (polars).
func DefaultIntLiteral(v int64) DType {
	if v >= math.MinInt32 && v <= math.MaxInt32 {
		return Int32()
	}
	return Int64()
}

func numOrBool(d DType) bool { return d.IsNumeric() || d.IsBool() }

func isSigned(d DType) bool {
	switch d.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	}
	return false
}

func intBits(d DType) int {
	switch d.ID() {
	case arrow.INT8, arrow.UINT8:
		return 8
	case arrow.INT16, arrow.UINT16:
		return 16
	case arrow.INT32, arrow.UINT32:
		return 32
	case arrow.INT64, arrow.UINT64:
		return 64
	case arrow.BOOL:
		return 1
	}
	return 64
}

func fitsSigned(v int64, bits int) bool {
	if bits >= 64 {
		return true
	}
	lim := int64(1) << (bits - 1)
	return v >= -lim && v < lim
}

func maxUnsigned(bits int) uint64 {
	if bits >= 64 {
		return math.MaxUint64
	}
	return 1<<bits - 1
}

func signedOf(bits int) DType {
	switch bits {
	case 8:
		return Int8()
	case 16:
		return Int16()
	case 32:
		return Int32()
	}
	return Int64()
}

func unsignedOf(bits int) DType {
	switch bits {
	case 8:
		return Uint8()
	case 16:
		return Uint16()
	case 32:
		return Uint32()
	}
	return Uint64()
}
