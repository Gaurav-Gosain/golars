//go:build arm64 && !noasm

// ARM64 NEON kernels for compute/: elementwise arithmetic, compares
// that write a packed bitmap, and bitmap-driven blends. Reductions
// live in reduce_arm64.s.
//
// Operand order follows the Go assembler: OP Vm, Vn, Vd computes
// Vd = Vn OP Vm, so "VSUB V20.D2, V16.D2, V16.D2" is v16 = v16 - v20.
//
// Every kernel works on whole blocks (8 or 16 elements) and returns
// the number of elements it processed; the Go dispatchers in
// simd_arm64.go finish the ragged tail. Loads and stores use
// post-increment addressing so each loop carries a single counter.
//
// Register use:
//   R0..R3   data pointers
//   V16..V23 first operand block (8 x int64 / float64)
//   V24..V31 second operand block, or V24 = broadcast literal
//   V0..V3   compare narrowing scratch
//   V4       bitmap bit weights {1, 2, 4, ..., 128} per byte
//   V5       all-zeros or all-ones, XORed into compare masks

#include "textflag.h"

#define LOADA8 \
	VLD1.P	64(R0), [V16.D2, V17.D2, V18.D2, V19.D2]; \
	VLD1.P	64(R0), [V20.D2, V21.D2, V22.D2, V23.D2]

#define LOADB8 \
	VLD1.P	64(R1), [V24.D2, V25.D2, V26.D2, V27.D2]; \
	VLD1.P	64(R1), [V28.D2, V29.D2, V30.D2, V31.D2]

// ---------------------------------------------------------------------
// Binary arithmetic, vector with vector
// ---------------------------------------------------------------------

// BIN4(op) computes V16..V19 = V16..V19 op V20..V23.
#define BIN4(op) \
	op	V20.D2, V16.D2, V16.D2; \
	op	V21.D2, V17.D2, V17.D2; \
	op	V22.D2, V18.D2, V18.D2; \
	op	V23.D2, V19.D2, V19.D2

#define BINLOOP(label, op) \
label: \
	VLD1.P	64(R0), [V16.D2, V17.D2, V18.D2, V19.D2]; \
	VLD1.P	64(R1), [V20.D2, V21.D2, V22.D2, V23.D2]; \
	BIN4(op); \
	VST1.P	[V16.D2, V17.D2, V18.D2, V19.D2], 64(R2); \
	SUBS	$1, R4, R4; \
	BNE	label

// func neonBinInt64(a, b, out []int64, op int) int
//
// op: 0 add, 1 sub. Processes len(a) rounded down to a multiple of 8
// and returns that count.
TEXT ·neonBinInt64(SB), NOSPLIT, $0-88
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	b_base+24(FP), R1
	MOVD	out_base+48(FP), R2
	MOVD	op+72(FP), R5
	LSR	$3, R3, R4
	LSL	$3, R4, R6
	MOVD	R6, ret+80(FP)
	CBZ	R4, bini64_done
	CBNZ	R5, bini64_sub
	BINLOOP(bini64_add, VADD)
	RET
bini64_sub:
	BINLOOP(bini64_subl, VSUB)
bini64_done:
	RET

// func neonBinFloat64(a, b, out []float64, op int) int
//
// op: 0 add, 1 sub, 2 mul, 3 div. IEEE semantics throughout.
TEXT ·neonBinFloat64(SB), NOSPLIT, $0-88
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	b_base+24(FP), R1
	MOVD	out_base+48(FP), R2
	MOVD	op+72(FP), R5
	LSR	$3, R3, R4
	LSL	$3, R4, R6
	MOVD	R6, ret+80(FP)
	CBZ	R4, binf64_done
	CMP	$1, R5
	BEQ	binf64_sub
	CMP	$2, R5
	BEQ	binf64_mul
	CMP	$3, R5
	BEQ	binf64_div
	BINLOOP(binf64_add, VFADD)
	RET
binf64_sub:
	BINLOOP(binf64_subl, VFSUB)
	RET
binf64_mul:
	BINLOOP(binf64_mull, VFMUL)
	RET
binf64_div:
	BINLOOP(binf64_divl, VFDIV)
binf64_done:
	RET

// ---------------------------------------------------------------------
// Binary arithmetic, vector with broadcast literal
// ---------------------------------------------------------------------

// LIT8(op) computes V16..V23 = V16..V23 op V24.
#define LIT8(op) \
	op	V24.D2, V16.D2, V16.D2; \
	op	V24.D2, V17.D2, V17.D2; \
	op	V24.D2, V18.D2, V18.D2; \
	op	V24.D2, V19.D2, V19.D2; \
	op	V24.D2, V20.D2, V20.D2; \
	op	V24.D2, V21.D2, V21.D2; \
	op	V24.D2, V22.D2, V22.D2; \
	op	V24.D2, V23.D2, V23.D2

#define LITLOOP(label, op) \
label: \
	LOADA8; \
	LIT8(op); \
	VST1.P	[V16.D2, V17.D2, V18.D2, V19.D2], 64(R2); \
	VST1.P	[V20.D2, V21.D2, V22.D2, V23.D2], 64(R2); \
	SUBS	$1, R4, R4; \
	BNE	label

// func neonLitInt64(src []int64, lit int64, out []int64, op int) int
//
// op: 0 add, 1 sub (src - lit). Processes len(src) rounded down to a
// multiple of 16 and returns that count.
TEXT ·neonLitInt64(SB), NOSPLIT, $0-72
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R3
	MOVD	lit+24(FP), R1
	MOVD	out_base+32(FP), R2
	MOVD	op+56(FP), R5
	VDUP	R1, V24.D2
	LSR	$4, R3, R4
	LSL	$4, R4, R6
	MOVD	R6, ret+64(FP)
	CBZ	R4, liti64_done
	CBNZ	R5, liti64_sub
	LITLOOP(liti64_add, VADD)
	RET
liti64_sub:
	LITLOOP(liti64_subl, VSUB)
liti64_done:
	RET

// func neonLitFloat64(src []float64, lit float64, out []float64, op int) int
//
// op: 0 add, 1 sub, 2 mul, 3 div (src op lit).
TEXT ·neonLitFloat64(SB), NOSPLIT, $0-72
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R3
	MOVD	lit+24(FP), R1
	MOVD	out_base+32(FP), R2
	MOVD	op+56(FP), R5
	VDUP	R1, V24.D2
	LSR	$4, R3, R4
	LSL	$4, R4, R6
	MOVD	R6, ret+64(FP)
	CBZ	R4, litf64_done
	CMP	$1, R5
	BEQ	litf64_sub
	CMP	$2, R5
	BEQ	litf64_mul
	CMP	$3, R5
	BEQ	litf64_div
	LITLOOP(litf64_add, VFADD)
	RET
litf64_sub:
	LITLOOP(litf64_subl, VFSUB)
	RET
litf64_mul:
	LITLOOP(litf64_mull, VFMUL)
	RET
litf64_div:
	LITLOOP(litf64_divl, VFDIV)
litf64_done:
	RET

// ---------------------------------------------------------------------
// Compare to packed bitmap
// ---------------------------------------------------------------------
//
// Each iteration compares 16 elements into eight 2 x 64-bit masks,
// narrows them to 16 byte masks in element order with three rounds of
// UZP1, optionally inverts them (V5), keeps one weighted bit per byte
// (V4) and folds each group of eight bytes into one bitmap byte with
// three pairwise adds. The two bitmap bytes are stored as a halfword.

#define CMPSETUP \
	MOVD	$0x8040201008040201, R8; \
	VDUP	R8, V4.D2; \
	VDUP	R5, V5.D2

#define PACKSTORE \
	VUZP1	V17.S4, V16.S4, V0.S4; \
	VUZP1	V19.S4, V18.S4, V1.S4; \
	VUZP1	V21.S4, V20.S4, V2.S4; \
	VUZP1	V23.S4, V22.S4, V3.S4; \
	VUZP1	V1.H8, V0.H8, V0.H8; \
	VUZP1	V3.H8, V2.H8, V2.H8; \
	VUZP1	V2.B16, V0.B16, V0.B16; \
	VEOR	V5.B16, V0.B16, V0.B16; \
	VAND	V4.B16, V0.B16, V0.B16; \
	VADDP	V0.B16, V0.B16, V0.B16; \
	VADDP	V0.B16, V0.B16, V0.B16; \
	VADDP	V0.B16, V0.B16, V0.B16; \
	VMOV	V0.H[0], R8; \
	MOVH.P	R8, 2(R2)

// CMPVV(op): V16+k = V16+k op V24+k (a op b).
#define CMPVV(op) \
	op	V24.D2, V16.D2, V16.D2; \
	op	V25.D2, V17.D2, V17.D2; \
	op	V26.D2, V18.D2, V18.D2; \
	op	V27.D2, V19.D2, V19.D2; \
	op	V28.D2, V20.D2, V20.D2; \
	op	V29.D2, V21.D2, V21.D2; \
	op	V30.D2, V22.D2, V22.D2; \
	op	V31.D2, V23.D2, V23.D2

// CMPLIT(op): V16+k = V16+k op V24 (x op lit).
#define CMPLIT(op) \
	op	V24.D2, V16.D2, V16.D2; \
	op	V24.D2, V17.D2, V17.D2; \
	op	V24.D2, V18.D2, V18.D2; \
	op	V24.D2, V19.D2, V19.D2; \
	op	V24.D2, V20.D2, V20.D2; \
	op	V24.D2, V21.D2, V21.D2; \
	op	V24.D2, V22.D2, V22.D2; \
	op	V24.D2, V23.D2, V23.D2

// CMPLITREV(op): V16+k = V24 op V16+k (lit op x).
#define CMPLITREV(op) \
	op	V16.D2, V24.D2, V16.D2; \
	op	V17.D2, V24.D2, V17.D2; \
	op	V18.D2, V24.D2, V18.D2; \
	op	V19.D2, V24.D2, V19.D2; \
	op	V20.D2, V24.D2, V20.D2; \
	op	V21.D2, V24.D2, V21.D2; \
	op	V22.D2, V24.D2, V22.D2; \
	op	V23.D2, V24.D2, V23.D2

#define CMPLOOPVV(label, op) \
label: \
	LOADA8; \
	LOADB8; \
	CMPVV(op); \
	PACKSTORE; \
	SUBS	$1, R6, R6; \
	BNE	label

#define CMPLOOPLIT(label, cmp8) \
label: \
	LOADA8; \
	cmp8; \
	PACKSTORE; \
	SUBS	$1, R6, R6; \
	BNE	label

// func neonCmpInt64(a, b []int64, bits []byte, kind, invert int) int
//
// kind: 0 a > b, 1 a >= b, 2 a == b. invert is 0 or -1; -1 flips every
// result bit. Processes len(a) rounded down to a multiple of 16 and
// returns that count; bits must hold count/8 bytes.
TEXT ·neonCmpInt64(SB), NOSPLIT, $0-96
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	b_base+24(FP), R1
	MOVD	bits_base+48(FP), R2
	MOVD	kind+72(FP), R4
	MOVD	invert+80(FP), R5
	CMPSETUP
	LSR	$4, R3, R6
	LSL	$4, R6, R7
	MOVD	R7, ret+88(FP)
	CBZ	R6, cmpi64_done
	CMP	$1, R4
	BEQ	cmpi64_ge
	CMP	$2, R4
	BEQ	cmpi64_eq
	CMPLOOPVV(cmpi64_gtl, VCMGT)
	RET
cmpi64_ge:
	CMPLOOPVV(cmpi64_gel, VCMGE)
	RET
cmpi64_eq:
	CMPLOOPVV(cmpi64_eql, VCMEQ)
cmpi64_done:
	RET

// func neonCmpFloat64(a, b []float64, bits []byte, kind, invert int) int
//
// Same contract as neonCmpInt64 with IEEE compares: any NaN operand
// gives false before the optional inversion.
TEXT ·neonCmpFloat64(SB), NOSPLIT, $0-96
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	b_base+24(FP), R1
	MOVD	bits_base+48(FP), R2
	MOVD	kind+72(FP), R4
	MOVD	invert+80(FP), R5
	CMPSETUP
	LSR	$4, R3, R6
	LSL	$4, R6, R7
	MOVD	R7, ret+88(FP)
	CBZ	R6, cmpf64_done
	CMP	$1, R4
	BEQ	cmpf64_ge
	CMP	$2, R4
	BEQ	cmpf64_eq
	CMPLOOPVV(cmpf64_gtl, VFCMGT)
	RET
cmpf64_ge:
	CMPLOOPVV(cmpf64_gel, VFCMGE)
	RET
cmpf64_eq:
	CMPLOOPVV(cmpf64_eql, VFCMEQ)
cmpf64_done:
	RET

// func neonCmpInt64Lit(a []int64, lit int64, bits []byte, kind, invert int) int
//
// kind: 0 x > lit, 1 x >= lit, 2 x == lit, 3 lit > x, 4 lit >= x.
TEXT ·neonCmpInt64Lit(SB), NOSPLIT, $0-80
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	lit+24(FP), R1
	MOVD	bits_base+32(FP), R2
	MOVD	kind+56(FP), R4
	MOVD	invert+64(FP), R5
	CMPSETUP
	VDUP	R1, V24.D2
	LSR	$4, R3, R6
	LSL	$4, R6, R7
	MOVD	R7, ret+72(FP)
	CBZ	R6, cmpli64_done
	CMP	$1, R4
	BEQ	cmpli64_ge
	CMP	$2, R4
	BEQ	cmpli64_eq
	CMP	$3, R4
	BEQ	cmpli64_lt
	CMP	$4, R4
	BEQ	cmpli64_le
	CMPLOOPLIT(cmpli64_gtl, CMPLIT(VCMGT))
	RET
cmpli64_ge:
	CMPLOOPLIT(cmpli64_gel, CMPLIT(VCMGE))
	RET
cmpli64_eq:
	CMPLOOPLIT(cmpli64_eql, CMPLIT(VCMEQ))
	RET
cmpli64_lt:
	CMPLOOPLIT(cmpli64_ltl, CMPLITREV(VCMGT))
	RET
cmpli64_le:
	CMPLOOPLIT(cmpli64_lel, CMPLITREV(VCMGE))
cmpli64_done:
	RET

// func neonCmpFloat64Lit(a []float64, lit float64, bits []byte, kind, invert int) int
//
// Same kinds as neonCmpInt64Lit with IEEE compares.
TEXT ·neonCmpFloat64Lit(SB), NOSPLIT, $0-80
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R3
	MOVD	lit+24(FP), R1
	MOVD	bits_base+32(FP), R2
	MOVD	kind+56(FP), R4
	MOVD	invert+64(FP), R5
	CMPSETUP
	VDUP	R1, V24.D2
	LSR	$4, R3, R6
	LSL	$4, R6, R7
	MOVD	R7, ret+72(FP)
	CBZ	R6, cmplf64_done
	CMP	$1, R4
	BEQ	cmplf64_ge
	CMP	$2, R4
	BEQ	cmplf64_eq
	CMP	$3, R4
	BEQ	cmplf64_lt
	CMP	$4, R4
	BEQ	cmplf64_le
	CMPLOOPLIT(cmplf64_gtl, CMPLIT(VFCMGT))
	RET
cmplf64_ge:
	CMPLOOPLIT(cmplf64_gel, CMPLIT(VFCMGE))
	RET
cmplf64_eq:
	CMPLOOPLIT(cmplf64_eql, CMPLIT(VFCMEQ))
	RET
cmplf64_lt:
	CMPLOOPLIT(cmplf64_ltl, CMPLITREV(VFCMGT))
	RET
cmplf64_le:
	CMPLOOPLIT(cmplf64_lel, CMPLITREV(VFCMGE))
cmplf64_done:
	RET

// ---------------------------------------------------------------------
// Bitmap blend
// ---------------------------------------------------------------------

// func neonBlend64(condBits []byte, aVals, bVals, out []uint64) int
//
// out[i] = aVals[i] where bit i of condBits is set, else bVals[i].
// Works on raw 64-bit lanes, so it serves int64 and float64 alike.
// Each iteration reads one bitmap byte, broadcasts it and tests it
// against {1,2}, {4,8}, {16,32}, {64,128} to get four lane masks, then
// bit-inserts a into b. Processes len(out) rounded down to a multiple
// of 8 and returns that count.
TEXT ·neonBlend64(SB), NOSPLIT, $0-104
	MOVD	condBits_base+0(FP), R0
	MOVD	aVals_base+24(FP), R1
	MOVD	bVals_base+48(FP), R3
	MOVD	out_base+72(FP), R2
	MOVD	out_len+80(FP), R4
	LSR	$3, R4, R6
	LSL	$3, R6, R7
	MOVD	R7, ret+96(FP)
	CBZ	R6, blend_done
	MOVD	$1, R8
	VMOV	R8, V26.D[0]
	MOVD	$2, R8
	VMOV	R8, V26.D[1]
	VSHL	$2, V26.D2, V27.D2
	VSHL	$4, V26.D2, V28.D2
	VSHL	$6, V26.D2, V29.D2
blend_loop:
	MOVBU.P	1(R0), R8
	VDUP	R8, V8.D2
	VCMTST	V26.D2, V8.D2, V9.D2
	VCMTST	V27.D2, V8.D2, V10.D2
	VCMTST	V28.D2, V8.D2, V11.D2
	VCMTST	V29.D2, V8.D2, V12.D2
	VLD1.P	64(R1), [V16.D2, V17.D2, V18.D2, V19.D2]
	VLD1.P	64(R3), [V20.D2, V21.D2, V22.D2, V23.D2]
	VBIT	V9.B16, V16.B16, V20.B16
	VBIT	V10.B16, V17.B16, V21.B16
	VBIT	V11.B16, V18.B16, V22.B16
	VBIT	V12.B16, V19.B16, V23.B16
	VST1.P	[V20.D2, V21.D2, V22.D2, V23.D2], 64(R2)
	SUBS	$1, R6, R6
	BNE	blend_loop
blend_done:
	RET
