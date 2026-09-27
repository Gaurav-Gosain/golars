//go:build arm64 && !noasm

package compute

import "unsafe"

// simd_arm64.go is the entry point for ARM64 builds (Apple Silicon,
// AWS Graviton, Ampere, Raspberry Pi 5, ...). NEON is baseline on
// AArch64 so we don't gate on CPU features or GOEXPERIMENT; every
// kernel ships hand-written Plan 9 asm in simd_arm64.s and
// reduce_arm64.s.
//
// Build-tag layout:
//
//	simd_amd64.go     (goexperiment.simd && amd64)     AVX2/AVX-512
//	simd_arm64.go     (arm64 && !noasm)                NEON (this file)
//	simd_fallback.go  (neither)                        pure scalar
//
// -tags noasm falls every target through to simd_fallback.go; useful
// when bisecting a codegen bug.
//
// Each Go-level simdXxx function here is a thin dispatcher: tiny
// inputs run a scalar loop, larger ones call the NEON kernel, and the
// dispatcher (or its caller, for kernels that return a count) finishes
// the ragged tail.

const simdAvailable = true

// hasSIMDInt64 reports whether the int64 fast paths route through
// NEON. NEON is mandatory on AArch64 so this is always true.
func hasSIMDInt64() bool { return true }

// --- Assembly stubs ----------------------------------------------

// Reductions (reduce_arm64.s).
func simdSumInt64NEON(a []int64) int64
func simdSumFloat64NEON(a []float64) float64
func simdMinFloat64NEON(vals []float64) (best float64, hasNaN bool)
func simdMaxFloat64NEON(vals []float64) (best float64, hasNaN bool)
func simdMinInt64NEON(vals []int64) int64
func simdMaxInt64NEON(vals []int64) int64

// Elementwise arithmetic (simd_arm64.s). Each returns the number of
// leading elements it wrote. The op codes are the arithOp values.
func neonBinInt64(a, b, out []int64, op int) int
func neonBinFloat64(a, b, out []float64, op int) int
func neonLitInt64(src []int64, lit int64, out []int64, op int) int
func neonLitFloat64(src []float64, lit float64, out []float64, op int) int

// Compares into a packed bitmap, 16 rows (two bytes) per step.
func neonCmpInt64(a, b []int64, bits []byte, kind, invert int) int
func neonCmpFloat64(a, b []float64, bits []byte, kind, invert int) int
func neonCmpInt64Lit(a []int64, lit int64, bits []byte, kind, invert int) int
func neonCmpFloat64Lit(a []float64, lit float64, bits []byte, kind, invert int) int

// Bitmap-driven select of raw 64-bit lanes, 8 rows per step.
func neonBlend64(condBits []byte, aVals, bVals, out []uint64) int

// Compare kinds understood by the neonCmp kernels.
const (
	neonCmpGt   = 0 // x > y
	neonCmpGe   = 1 // x >= y
	neonCmpEq   = 2 // x == y
	neonCmpLtRv = 3 // literal kernels only: lit > x
	neonCmpLeRv = 4 // literal kernels only: lit >= x
)

// --- Reductions ---------------------------------------------------

func simdSumInt64(a []int64) int64 {
	if len(a) < 16 {
		var total int64
		for _, v := range a {
			total += v
		}
		return total
	}
	return simdSumInt64NEON(a)
}

func simdSumFloat64(a []float64) float64 {
	if len(a) < 16 {
		var total float64
		for _, v := range a {
			total += v
		}
		return total
	}
	return simdSumFloat64NEON(a)
}

func simdMinFloat64(vals []float64) (float64, bool) {
	if len(vals) < 16 {
		best := vals[0]
		hasNaN := best != best
		for _, v := range vals[1:] {
			if v != v {
				hasNaN = true
				continue
			}
			if v < best {
				best = v
			}
		}
		return best, hasNaN
	}
	return simdMinFloat64NEON(vals)
}

func simdMaxFloat64(vals []float64) (float64, bool) {
	if len(vals) < 16 {
		best := vals[0]
		hasNaN := best != best
		for _, v := range vals[1:] {
			if v != v {
				hasNaN = true
				continue
			}
			if v > best {
				best = v
			}
		}
		return best, hasNaN
	}
	return simdMaxFloat64NEON(vals)
}

// --- Compares -----------------------------------------------------
//
// The dispatchers return how many leading rows they wrote (a multiple
// of 16); callers finish the tail byte by byte. Lt and Le swap the
// operands, Ne inverts Eq. For integer literals, Lt and Le invert Ge
// and Gt, which is exact for totally ordered values.

func simdCompareInt64(av, bv []int64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	switch op {
	case opEq:
		return neonCmpInt64(av, bv, bits, neonCmpEq, 0)
	case opNe:
		return neonCmpInt64(av, bv, bits, neonCmpEq, -1)
	case opLt:
		return neonCmpInt64(bv, av, bits, neonCmpGt, 0)
	case opLe:
		return neonCmpInt64(bv, av, bits, neonCmpGe, 0)
	case opGt:
		return neonCmpInt64(av, bv, bits, neonCmpGt, 0)
	case opGe:
		return neonCmpInt64(av, bv, bits, neonCmpGe, 0)
	}
	return 0
}

// simdCompareFloat64 uses IEEE compares: a NaN operand makes every op
// false except Ne. Lt and Le swap operands rather than invert, which
// keeps NaN rows false.
func simdCompareFloat64(av, bv []float64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	switch op {
	case opEq:
		return neonCmpFloat64(av, bv, bits, neonCmpEq, 0)
	case opNe:
		return neonCmpFloat64(av, bv, bits, neonCmpEq, -1)
	case opLt:
		return neonCmpFloat64(bv, av, bits, neonCmpGt, 0)
	case opLe:
		return neonCmpFloat64(bv, av, bits, neonCmpGe, 0)
	case opGt:
		return neonCmpFloat64(av, bv, bits, neonCmpGt, 0)
	case opGe:
		return neonCmpFloat64(av, bv, bits, neonCmpGe, 0)
	}
	return 0
}

func simdCompareInt64Lit(av []int64, lit int64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	switch op {
	case opEq:
		return neonCmpInt64Lit(av, lit, bits, neonCmpEq, 0)
	case opNe:
		return neonCmpInt64Lit(av, lit, bits, neonCmpEq, -1)
	case opLt:
		return neonCmpInt64Lit(av, lit, bits, neonCmpGe, -1)
	case opLe:
		return neonCmpInt64Lit(av, lit, bits, neonCmpGt, -1)
	case opGt:
		return neonCmpInt64Lit(av, lit, bits, neonCmpGt, 0)
	case opGe:
		return neonCmpInt64Lit(av, lit, bits, neonCmpGe, 0)
	}
	return 0
}

func simdCompareFloat64Lit(av []float64, lit float64, bits []byte, op compareOp) int {
	if len(av) < 16 {
		return 0
	}
	switch op {
	case opEq:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpEq, 0)
	case opNe:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpEq, -1)
	case opLt:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpLtRv, 0)
	case opLe:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpLeRv, 0)
	case opGt:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpGt, 0)
	case opGe:
		return neonCmpFloat64Lit(av, lit, bits, neonCmpGe, 0)
	}
	return 0
}

// --- Arithmetic with a literal -------------------------------------

func simdAddLitInt64(src []int64, lit int64, out []int64) int {
	return neonLitInt64(src, lit, out, int(opAdd))
}

func simdSubLitInt64(src []int64, lit int64, out []int64) int {
	return neonLitInt64(src, lit, out, int(opSub))
}

func simdAddLitFloat64(src []float64, lit float64, out []float64) int {
	return neonLitFloat64(src, lit, out, int(opAdd))
}

func simdSubLitFloat64(src []float64, lit float64, out []float64) int {
	return neonLitFloat64(src, lit, out, int(opSub))
}

func simdMulLitFloat64(src []float64, lit float64, out []float64) int {
	return neonLitFloat64(src, lit, out, int(opMul))
}

func simdDivLitFloat64(src []float64, lit float64, out []float64) int {
	return neonLitFloat64(src, lit, out, int(opDiv))
}

// --- Blends -------------------------------------------------------

// simdBlendInt64 / simdBlendFloat64 cover the full length.
func simdBlendInt64(condBits []byte, aVals, bVals, out []int64) {
	i := neonBlend64(condBits, asUint64s(aVals), asUint64s(bVals), asUint64s(out))
	for ; i < len(out); i++ {
		bit := int64(condBits[i>>3]>>uint(i&7)) & 1
		mask := -bit
		out[i] = bVals[i] ^ ((aVals[i] ^ bVals[i]) & mask)
	}
}

func simdBlendFloat64(condBits []byte, aVals, bVals, out []float64) {
	i := neonBlend64(condBits, asUint64s(aVals), asUint64s(bVals), asUint64s(out))
	for ; i < len(out); i++ {
		if condBits[i>>3]&(1<<(i&7)) != 0 {
			out[i] = aVals[i]
		} else {
			out[i] = bVals[i]
		}
	}
}

// asUint64s reinterprets a slice of 64-bit values as []uint64 for the
// type-agnostic blend kernel.
func asUint64s[T int64 | float64](s []T) []uint64 {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(&s[0])), len(s))
}

// --- Vector with vector --------------------------------------------

// simdAddInt64 and simdAddFloat64 mirror the amd64 dispatchers; the
// kernels in arith.go go through vecBinInt64 / vecBinFloat64.
func simdAddInt64(a, b, out []int64) { vecBinInt64(out, a, b, opAdd) }

func simdAddFloat64(a, b, out []float64) { vecBinFloat64(out, a, b, opAdd) }
