//go:build arm64 && !noasm

// ARM64 NEON kernels for compute/. Go Plan 9 asm syntax.
//
// Lane annotations: V0.D2 means "V0 as 2 x int64" (128-bit split into
// 2 lanes of 64 bits). Integer vector mnemonics (VADD, VSUB, VCMGT,
// VCMEQ, VCMGE, VAND, VORR, VEOR, VMVN, VMOV, VDUP, VLD1, VST1, VBIT)
// are all accepted by Go's arm64 assembler. Vector *floating-point*
// mnemonics (FADD.2D, FCMEQ.2D, FMINNM.2D, ...) are not, so we emit
// the raw ARMv8 word encodings via WORD. Each such line has a
// comment showing the equivalent `fadd vd.2d, vn.2d, vm.2d` form.
//
// ABI: $0-N declares 0 bytes of local stack, N bytes of args+return.
// Slice = 24 bytes on arm64 (ptr + len + cap). NOSPLIT skips the
// stack-growth preamble; every kernel below is a leaf.
//
// Register convention:
//   R0..R4   argument pointers / lengths / literals
//   R5..R12  scratch
//   V0..V7   primary and unrolled data lanes
//   V16      broadcast literal (Lit kernels)
//   V8       mask vector (Blend kernels)

#include "textflag.h"

// =====================================================================
// Reductions
// =====================================================================

// func simdSumInt64NEON(a []int64) int64
//
// 4-way unrolled with 4 parallel accumulators to expose ILP. Loads 8
// int64s per iteration (128 bytes across 4 vector registers), adds
// each into its own accumulator, then folds at the end. The parallel
// accumulator chains let the M-series cores hide vector-add latency
// and approach peak throughput.
TEXT ·simdSumInt64NEON(SB), NOSPLIT, $0-32
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1

	VEOR	V0.B16, V0.B16, V0.B16
	VEOR	V5.B16, V5.B16, V5.B16
	VEOR	V6.B16, V6.B16, V6.B16
	VEOR	V7.B16, V7.B16, V7.B16
	MOVD	ZR, R2

	CMP	$8, R1
	BLT	sumi64_partial
sumi64_loop8:
	VLD1	(R0), [V1.D2, V2.D2, V3.D2, V4.D2]	// 8 int64s
	VADD	V0.D2, V1.D2, V0.D2
	VADD	V5.D2, V2.D2, V5.D2
	VADD	V6.D2, V3.D2, V6.D2
	VADD	V7.D2, V4.D2, V7.D2
	ADD	$64, R0, R0
	SUB	$8, R1, R1
	CMP	$8, R1
	BGE	sumi64_loop8
	// Fold V5, V6, V7 into V0.
	VADD	V0.D2, V5.D2, V0.D2
	VADD	V0.D2, V6.D2, V0.D2
	VADD	V0.D2, V7.D2, V0.D2

sumi64_partial:
	CMP	$2, R1
	BLT	sumi64_tail
sumi64_loop2:
	VLD1	(R0), [V1.D2]
	VADD	V0.D2, V1.D2, V0.D2
	ADD	$16, R0, R0
	SUB	$2, R1, R1
	CMP	$2, R1
	BGE	sumi64_loop2

sumi64_tail:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	ADD	R3, R2, R2
	ADD	R4, R2, R2
	CBZ	R1, sumi64_done
	MOVD	(R0), R3
	ADD	R3, R2, R2
sumi64_done:
	MOVD	R2, ret+24(FP)
	RET

// func simdSumFloat64NEON(a []float64) float64
//
// 4-way unrolled float64 sum. Same structure as simdSumInt64NEON but
// with FADD.2D word encodings.
TEXT ·simdSumFloat64NEON(SB), NOSPLIT, $0-32
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1

	VEOR	V0.B16, V0.B16, V0.B16
	VEOR	V5.B16, V5.B16, V5.B16
	VEOR	V6.B16, V6.B16, V6.B16
	VEOR	V7.B16, V7.B16, V7.B16

	CMP	$8, R1
	BLT	sumf64_partial
sumf64_loop8:
	VLD1	(R0), [V1.D2, V2.D2, V3.D2, V4.D2]
	WORD	$0x4E61D400		// fadd v0.2d, v0.2d, v1.2d
	WORD	$0x4E62D4A5		// fadd v5.2d, v5.2d, v2.2d
	WORD	$0x4E63D4C6		// fadd v6.2d, v6.2d, v3.2d
	WORD	$0x4E64D4E7		// fadd v7.2d, v7.2d, v4.2d
	ADD	$64, R0, R0
	SUB	$8, R1, R1
	CMP	$8, R1
	BGE	sumf64_loop8
	// Fold V5, V6, V7 into V0.
	WORD	$0x4E65D400		// fadd v0.2d, v0.2d, v5.2d
	WORD	$0x4E66D400		// fadd v0.2d, v0.2d, v6.2d
	WORD	$0x4E67D400		// fadd v0.2d, v0.2d, v7.2d

sumf64_partial:
	CMP	$2, R1
	BLT	sumf64_tail
sumf64_loop2:
	VLD1	(R0), [V1.D2]
	WORD	$0x4E61D400		// fadd v0.2d, v0.2d, v1.2d
	ADD	$16, R0, R0
	SUB	$2, R1, R1
	CMP	$2, R1
	BGE	sumf64_loop2

sumf64_tail:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	FMOVD	R3, F0
	FMOVD	R4, F1
	FADDD	F1, F0, F0
	CBZ	R1, sumf64_done
	FMOVD	(R0), F1
	FADDD	F1, F0, F0
sumf64_done:
	FMOVD	F0, ret+24(FP)
	RET

// func simdMinFloat64NEON(vals []float64) (best float64, hasNaN bool)
//
// 4-accumulator FMIN reduction with +Inf seed. FMIN (unlike FMINNM)
// propagates NaN, so NaN tracking is free: the accumulator becomes
// NaN on first NaN input and stays there. Caller detects via
// (val != val) at the end and falls back to a scalar NaN-aware
// rescan of the partition. Hot loop is LD1 + 4x FMIN per 64 bytes
// with zero extra ops - pure memory bandwidth.
TEXT ·simdMinFloat64NEON(SB), NOSPLIT, $0-33
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1

	// Seed = +Inf = 0x7FF0000000000000. FMIN(+Inf, x) = x for finite x,
	// so a +Inf-seeded reduction over non-NaN inputs yields the true min.
	MOVD	$0x7FF0000000000000, R14
	VDUP	R14, V0.D2
	VMOV	V0.B16, V5.B16
	VMOV	V0.B16, V6.B16
	VMOV	V0.B16, V7.B16

	CMP	$8, R1
	BLT	minf64_fold

minf64_loop8:
	VLD1	(R0), [V1.D2, V2.D2, V3.D2, V4.D2]	// 8 doubles
	WORD	$0x4EE1F400		// fmin v0.2d, v0.2d, v1.2d
	WORD	$0x4EE2F4A5		// fmin v5.2d, v5.2d, v2.2d
	WORD	$0x4EE3F4C6		// fmin v6.2d, v6.2d, v3.2d
	WORD	$0x4EE4F4E7		// fmin v7.2d, v7.2d, v4.2d
	ADD	$64, R0, R0
	SUB	$8, R1, R1
	CMP	$8, R1
	BGE	minf64_loop8

minf64_fold:
	// Fold V5, V6, V7 into V0. No-op when loop skipped (seeds are +Inf).
	WORD	$0x4EE5F400		// fmin v0.2d, v0.2d, v5.2d
	WORD	$0x4EE7F4C6		// fmin v6.2d, v6.2d, v7.2d
	WORD	$0x4EE6F400		// fmin v0.2d, v0.2d, v6.2d

	CMP	$4, R1
	BLT	minf64_partial2
	VLD1	(R0), [V1.D2, V2.D2]
	WORD	$0x4EE1F400		// fmin v0.2d, v0.2d, v1.2d
	WORD	$0x4EE2F400		// fmin v0.2d, v0.2d, v2.2d
	ADD	$32, R0, R0
	SUB	$4, R1, R1

minf64_partial2:
	CMP	$2, R1
	BLT	minf64_lanes
	VLD1	(R0), [V1.D2]
	WORD	$0x4EE1F400		// fmin v0.2d, v0.2d, v1.2d
	ADD	$16, R0, R0
	SUB	$2, R1, R1

minf64_lanes:
	// Reduce the two lanes of V0 to scalar F0.
	VMOV	V0.D[0], R6
	VMOV	V0.D[1], R7
	FMOVD	R6, F0
	FMOVD	R7, F1
	FMIND	F1, F0, F0

minf64_tail:
	CBZ	R1, minf64_done
	FMOVD	(R0), F1
	FMIND	F1, F0, F0
	ADD	$8, R0, R0
	SUB	$1, R1, R1
	B	minf64_tail

minf64_done:
	// hasNaN via self-compare: FCMPD sets V flag when either operand is NaN.
	FCMPD	F0, F0
	CSET	VS, R6
	FMOVD	F0, best+24(FP)
	MOVB	R6, hasNaN+32(FP)
	RET

// func simdMaxFloat64NEON(vals []float64) (best float64, hasNaN bool)
//
// Mirror of simdMinFloat64NEON using FMAX with -Inf seed.
TEXT ·simdMaxFloat64NEON(SB), NOSPLIT, $0-33
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1

	// Seed = -Inf = 0xFFF0000000000000.
	MOVD	$0xFFF0000000000000, R14
	VDUP	R14, V0.D2
	VMOV	V0.B16, V5.B16
	VMOV	V0.B16, V6.B16
	VMOV	V0.B16, V7.B16

	CMP	$8, R1
	BLT	maxf64_fold

maxf64_loop8:
	VLD1	(R0), [V1.D2, V2.D2, V3.D2, V4.D2]	// 8 doubles
	WORD	$0x4E61F400		// fmax v0.2d, v0.2d, v1.2d
	WORD	$0x4E62F4A5		// fmax v5.2d, v5.2d, v2.2d
	WORD	$0x4E63F4C6		// fmax v6.2d, v6.2d, v3.2d
	WORD	$0x4E64F4E7		// fmax v7.2d, v7.2d, v4.2d
	ADD	$64, R0, R0
	SUB	$8, R1, R1
	CMP	$8, R1
	BGE	maxf64_loop8

maxf64_fold:
	WORD	$0x4E65F400		// fmax v0.2d, v0.2d, v5.2d
	WORD	$0x4E67F4C6		// fmax v6.2d, v6.2d, v7.2d
	WORD	$0x4E66F400		// fmax v0.2d, v0.2d, v6.2d

	CMP	$4, R1
	BLT	maxf64_partial2
	VLD1	(R0), [V1.D2, V2.D2]
	WORD	$0x4E61F400		// fmax v0.2d, v0.2d, v1.2d
	WORD	$0x4E62F400		// fmax v0.2d, v0.2d, v2.2d
	ADD	$32, R0, R0
	SUB	$4, R1, R1

maxf64_partial2:
	CMP	$2, R1
	BLT	maxf64_lanes
	VLD1	(R0), [V1.D2]
	WORD	$0x4E61F400		// fmax v0.2d, v0.2d, v1.2d
	ADD	$16, R0, R0
	SUB	$2, R1, R1

maxf64_lanes:
	VMOV	V0.D[0], R6
	VMOV	V0.D[1], R7
	FMOVD	R6, F0
	FMOVD	R7, F1
	FMAXD	F1, F0, F0

maxf64_tail:
	CBZ	R1, maxf64_done
	FMOVD	(R0), F1
	FMAXD	F1, F0, F0
	ADD	$8, R0, R0
	SUB	$1, R1, R1
	B	maxf64_tail

maxf64_done:
	FCMPD	F0, F0
	CSET	VS, R6
	FMOVD	F0, best+24(FP)
	MOVB	R6, hasNaN+32(FP)
	RET

// =====================================================================
// Vector binary ops
// =====================================================================

// func simdAddInt64NEON(a, b, out []int64) int
//
// Layout: V0/V1 hold 4 int64s of a, V2/V3 hold 4 int64s of b,
// V4/V5 hold 4 int64s of out. One iteration = 4 elements.
TEXT ·simdAddInt64NEON(SB), NOSPLIT, $0-80
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1
	MOVD	b_base+24(FP), R2
	MOVD	out_base+48(FP), R3

	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, addi64_done
addi64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	VLD1	(R2), [V2.D2, V3.D2]
	VADD	V0.D2, V2.D2, V4.D2
	VADD	V1.D2, V3.D2, V5.D2
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R2, R2
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	addi64_loop
addi64_done:
	MOVD	R5, ret+72(FP)
	RET

// func simdAddFloat64NEON(a, b, out []float64) int
TEXT ·simdAddFloat64NEON(SB), NOSPLIT, $0-80
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1
	MOVD	b_base+24(FP), R2
	MOVD	out_base+48(FP), R3

	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, addf64_done
addf64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	VLD1	(R2), [V2.D2, V3.D2]
	WORD	$0x4E62D404		// fadd v4.2d, v0.2d, v2.2d
	WORD	$0x4E63D425		// fadd v5.2d, v1.2d, v3.2d
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R2, R2
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	addf64_loop
addf64_done:
	MOVD	R5, ret+72(FP)
	RET

// =====================================================================
// Vector + scalar literal
// =====================================================================

// func simdAddLitInt64NEON(src []int64, lit int64, out []int64) int
TEXT ·simdAddLitInt64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, adli64_done
adli64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	VADD	V16.D2, V0.D2, V4.D2
	VADD	V16.D2, V1.D2, V5.D2
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	adli64_loop
adli64_done:
	MOVD	R5, ret+56(FP)
	RET

// func simdSubLitInt64NEON(src []int64, lit int64, out []int64) int
TEXT ·simdSubLitInt64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, subli64_done
subli64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	VSUB	V16.D2, V0.D2, V4.D2	// V4 = V0 - V16
	VSUB	V16.D2, V1.D2, V5.D2
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	subli64_loop
subli64_done:
	MOVD	R5, ret+56(FP)
	RET

// func simdAddLitFloat64NEON(src []float64, lit float64, out []float64) int
TEXT ·simdAddLitFloat64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, adlf64_done
adlf64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	WORD	$0x4E70D404		// fadd v4.2d, v0.2d, v16.2d
	WORD	$0x4E70D425		// fadd v5.2d, v1.2d, v16.2d
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	adlf64_loop
adlf64_done:
	MOVD	R5, ret+56(FP)
	RET

// func simdSubLitFloat64NEON(src []float64, lit float64, out []float64) int
TEXT ·simdSubLitFloat64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, sublf64_done
sublf64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	WORD	$0x4EF0D404		// fsub v4.2d, v0.2d, v16.2d
	WORD	$0x4EF0D425		// fsub v5.2d, v1.2d, v16.2d
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	sublf64_loop
sublf64_done:
	MOVD	R5, ret+56(FP)
	RET

// func simdMulLitFloat64NEON(src []float64, lit float64, out []float64) int
TEXT ·simdMulLitFloat64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, mullf64_done
mullf64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	WORD	$0x6E70DC04		// fmul v4.2d, v0.2d, v16.2d
	WORD	$0x6E70DC25		// fmul v5.2d, v1.2d, v16.2d
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	mullf64_loop
mullf64_done:
	MOVD	R5, ret+56(FP)
	RET

// func simdDivLitFloat64NEON(src []float64, lit float64, out []float64) int
TEXT ·simdDivLitFloat64NEON(SB), NOSPLIT, $0-64
	MOVD	src_base+0(FP), R0
	MOVD	src_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	out_base+32(FP), R3

	VDUP	R2, V16.D2
	AND	$-4, R1, R4
	MOVD	ZR, R5
	CBZ	R4, divlf64_done
divlf64_loop:
	VLD1	(R0), [V0.D2, V1.D2]
	WORD	$0x6E70FC04		// fdiv v4.2d, v0.2d, v16.2d
	WORD	$0x6E70FC25		// fdiv v5.2d, v1.2d, v16.2d
	VST1	[V4.D2, V5.D2], (R3)
	ADD	$32, R0, R0
	ADD	$32, R3, R3
	ADD	$4, R5, R5
	CMP	R4, R5
	BLT	divlf64_loop
divlf64_done:
	MOVD	R5, ret+56(FP)
	RET

// =====================================================================
// Comparisons -> bitmap
// =====================================================================
//
// Each outer iteration processes 8 elements and stores one byte of
// bitmap. 4 inner NEON compares (2 lanes each) build up a byte via
// scalar extract + shift + OR.

// func simdCompareInt64NEON(av, bv []int64, bits []byte, op compareOp) int
TEXT ·simdCompareInt64NEON(SB), NOSPLIT, $0-88
	MOVD	av_base+0(FP), R0
	MOVD	av_len+8(FP), R1
	MOVD	bv_base+24(FP), R2
	MOVD	bits_base+48(FP), R3
	MOVD	op+72(FP), R4

	// V7 = all-ones, used by the Ne inversion in the op dispatcher.
	MOVD	$-1, R14
	VDUP	R14, V7.D2

	AND	$-8, R1, R9
	MOVD	ZR, R5
	CBZ	R9, cmpi64_done

cmpi64_outer:
	MOVD	ZR, R8

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpi64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	ORR	R6, R8, R8
	LSL	$1, R7, R7
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpi64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$2, R6, R6
	LSL	$3, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpi64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$4, R6, R6
	LSL	$5, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpi64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$6, R6, R6
	LSL	$7, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	MOVB	R8, (R3)
	ADD	$1, R3, R3
	ADD	$8, R5, R5
	CMP	R9, R5
	BLT	cmpi64_outer

cmpi64_done:
	MOVD	R5, ret+80(FP)
	RET

// cmpi64_op: compare V0 vs V1 per op in R4, write per-lane mask to V4.
// op: 0=Eq, 1=Ne, 2=Lt, 3=Le, 4=Gt, 5=Ge.
// Caller must have V7 preloaded with all-1s for the Ne inversion.
TEXT cmpi64_op<>(SB), NOSPLIT, $0-0
	CMP	$0, R4
	BEQ	cmpi64_op_eq
	CMP	$1, R4
	BEQ	cmpi64_op_ne
	CMP	$2, R4
	BEQ	cmpi64_op_lt
	CMP	$3, R4
	BEQ	cmpi64_op_le
	CMP	$4, R4
	BEQ	cmpi64_op_gt
cmpi64_op_ge:
	WORD	$0x4EE13C04		// cmge v4.2d, v0.2d, v1.2d
	RET
cmpi64_op_eq:
	VCMEQ	V0.D2, V1.D2, V4.D2
	RET
cmpi64_op_ne:
	VCMEQ	V0.D2, V1.D2, V4.D2
	VEOR	V7.B16, V4.B16, V4.B16	// V4 = ~V4 using preloaded all-1s
	RET
cmpi64_op_lt:
	WORD	$0x4EE03424		// cmgt v4.2d, v1.2d, v0.2d (a < b iff b > a)
	RET
cmpi64_op_le:
	WORD	$0x4EE03C24		// cmge v4.2d, v1.2d, v0.2d
	RET
cmpi64_op_gt:
	WORD	$0x4EE13404		// cmgt v4.2d, v0.2d, v1.2d
	RET

// func simdCompareInt64LitNEON(av []int64, lit int64, bits []byte, op compareOp) int
TEXT ·simdCompareInt64LitNEON(SB), NOSPLIT, $0-72
	MOVD	av_base+0(FP), R0
	MOVD	av_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	bits_base+32(FP), R3
	MOVD	op+56(FP), R4

	VDUP	R2, V16.D2
	MOVD	$-1, R14
	VDUP	R14, V7.D2

	AND	$-8, R1, R9
	MOVD	ZR, R5
	CBZ	R9, cmpli64_done

cmpli64_outer:
	MOVD	ZR, R8

	VLD1	(R0), [V0.D2]
	BL	cmpli64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	ORR	R6, R8, R8
	LSL	$1, R7, R7
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmpli64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$2, R6, R6
	LSL	$3, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmpli64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$4, R6, R6
	LSL	$5, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmpli64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$6, R6, R6
	LSL	$7, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	MOVB	R8, (R3)
	ADD	$1, R3, R3
	ADD	$8, R5, R5
	CMP	R9, R5
	BLT	cmpli64_outer

cmpli64_done:
	MOVD	R5, ret+64(FP)
	RET

// cmpli64_op: compare V0 vs V16 (broadcast lit) per op in R4.
// Caller preloads V7 with all-1s for the Ne inversion.
TEXT cmpli64_op<>(SB), NOSPLIT, $0-0
	CMP	$0, R4
	BEQ	cmpli64_op_eq
	CMP	$1, R4
	BEQ	cmpli64_op_ne
	CMP	$2, R4
	BEQ	cmpli64_op_lt
	CMP	$3, R4
	BEQ	cmpli64_op_le
	CMP	$4, R4
	BEQ	cmpli64_op_gt
cmpli64_op_ge:
	WORD	$0x4EF03C04		// cmge v4.2d, v0.2d, v16.2d
	RET
cmpli64_op_eq:
	VCMEQ	V0.D2, V16.D2, V4.D2
	RET
cmpli64_op_ne:
	VCMEQ	V0.D2, V16.D2, V4.D2
	VEOR	V7.B16, V4.B16, V4.B16
	RET
cmpli64_op_lt:
	WORD	$0x4EE03604		// cmgt v4.2d, v16.2d, v0.2d (a < lit iff lit > a)
	RET
cmpli64_op_le:
	WORD	$0x4EE03E04		// cmge v4.2d, v16.2d, v0.2d
	RET
cmpli64_op_gt:
	WORD	$0x4EF03404		// cmgt v4.2d, v0.2d, v16.2d
	RET

// func simdCompareFloat64NEON(av, bv []float64, bits []byte, op compareOp) int
TEXT ·simdCompareFloat64NEON(SB), NOSPLIT, $0-88
	MOVD	av_base+0(FP), R0
	MOVD	av_len+8(FP), R1
	MOVD	bv_base+24(FP), R2
	MOVD	bits_base+48(FP), R3
	MOVD	op+72(FP), R4

	MOVD	$-1, R14
	VDUP	R14, V7.D2

	AND	$-8, R1, R9
	MOVD	ZR, R5
	CBZ	R9, cmpf64_done

cmpf64_outer:
	MOVD	ZR, R8

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	ORR	R6, R8, R8
	LSL	$1, R7, R7
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$2, R6, R6
	LSL	$3, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$4, R6, R6
	LSL	$5, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	VLD1	(R0), [V0.D2]
	VLD1	(R2), [V1.D2]
	BL	cmpf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$6, R6, R6
	LSL	$7, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0
	ADD	$16, R2, R2

	MOVB	R8, (R3)
	ADD	$1, R3, R3
	ADD	$8, R5, R5
	CMP	R9, R5
	BLT	cmpf64_outer

cmpf64_done:
	MOVD	R5, ret+80(FP)
	RET

// cmpf64_op: FP compare V0 vs V1 per op in R4.
// NaN inputs produce false for all ops except Ne (IEEE 754 semantics,
// which matches polars).
TEXT cmpf64_op<>(SB), NOSPLIT, $0-0
	CMP	$0, R4
	BEQ	cmpf64_op_eq
	CMP	$1, R4
	BEQ	cmpf64_op_ne
	CMP	$2, R4
	BEQ	cmpf64_op_lt
	CMP	$3, R4
	BEQ	cmpf64_op_le
	CMP	$4, R4
	BEQ	cmpf64_op_gt
cmpf64_op_ge:
	WORD	$0x6E61E404		// fcmge v4.2d, v0.2d, v1.2d  (a >= b)
	RET
cmpf64_op_eq:
	WORD	$0x4E61E404		// fcmeq v4.2d, v0.2d, v1.2d
	RET
cmpf64_op_ne:
	WORD	$0x4E61E404		// fcmeq v4.2d, v0.2d, v1.2d
	VEOR	V7.B16, V4.B16, V4.B16
	RET
cmpf64_op_lt:
	WORD	$0x6EE0E424		// fcmgt v4.2d, v1.2d, v0.2d  (a < b iff b > a)
	RET
cmpf64_op_le:
	WORD	$0x6E60E424		// fcmge v4.2d, v1.2d, v0.2d  (a <= b iff b >= a)
	RET
cmpf64_op_gt:
	WORD	$0x6EE1E404		// fcmgt v4.2d, v0.2d, v1.2d  (a > b)
	RET

// func simdCompareFloat64LitNEON(av []float64, lit float64, bits []byte, op compareOp) int
TEXT ·simdCompareFloat64LitNEON(SB), NOSPLIT, $0-72
	MOVD	av_base+0(FP), R0
	MOVD	av_len+8(FP), R1
	MOVD	lit+24(FP), R2
	MOVD	bits_base+32(FP), R3
	MOVD	op+56(FP), R4

	VDUP	R2, V16.D2
	MOVD	$-1, R14
	VDUP	R14, V7.D2

	AND	$-8, R1, R9
	MOVD	ZR, R5
	CBZ	R9, cmplf64_done

cmplf64_outer:
	MOVD	ZR, R8

	VLD1	(R0), [V0.D2]
	BL	cmplf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	ORR	R6, R8, R8
	LSL	$1, R7, R7
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmplf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$2, R6, R6
	LSL	$3, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmplf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$4, R6, R6
	LSL	$5, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	VLD1	(R0), [V0.D2]
	BL	cmplf64_op<>(SB)
	VMOV	V4.D[0], R6
	VMOV	V4.D[1], R7
	AND	$1, R6, R6
	AND	$1, R7, R7
	LSL	$6, R6, R6
	LSL	$7, R7, R7
	ORR	R6, R8, R8
	ORR	R7, R8, R8
	ADD	$16, R0, R0

	MOVB	R8, (R3)
	ADD	$1, R3, R3
	ADD	$8, R5, R5
	CMP	R9, R5
	BLT	cmplf64_outer

cmplf64_done:
	MOVD	R5, ret+64(FP)
	RET

TEXT cmplf64_op<>(SB), NOSPLIT, $0-0
	CMP	$0, R4
	BEQ	cmplf64_op_eq
	CMP	$1, R4
	BEQ	cmplf64_op_ne
	CMP	$2, R4
	BEQ	cmplf64_op_lt
	CMP	$3, R4
	BEQ	cmplf64_op_le
	CMP	$4, R4
	BEQ	cmplf64_op_gt
cmplf64_op_ge:
	WORD	$0x6E70E404		// fcmge v4.2d, v0.2d, v16.2d  (src >= lit)
	RET
cmplf64_op_eq:
	WORD	$0x4E70E404		// fcmeq v4.2d, v0.2d, v16.2d
	RET
cmplf64_op_ne:
	WORD	$0x4E70E404		// fcmeq v4.2d, v0.2d, v16.2d
	VEOR	V7.B16, V4.B16, V4.B16
	RET
cmplf64_op_lt:
	WORD	$0x6EE0E604		// fcmgt v4.2d, v16.2d, v0.2d  (src < lit iff lit > src)
	RET
cmplf64_op_le:
	WORD	$0x6E60E604		// fcmge v4.2d, v16.2d, v0.2d  (src <= lit iff lit >= src)
	RET
cmplf64_op_gt:
	WORD	$0x6EF0E404		// fcmgt v4.2d, v0.2d, v16.2d  (src > lit)
	RET

// =====================================================================
// Blends
// =====================================================================

// func simdBlendInt64NEON(condBits []byte, aVals, bVals, out []int64)
//
// Fast path: process 8 elements (= 1 bitmap byte) per outer iteration
// with a single MOVBU of the byte and 4 inline pair-blends. Only
// scalar tail runs once the index can't accommodate another 8-block.
TEXT ·simdBlendInt64NEON(SB), NOSPLIT, $0-96
	MOVD	condBits_base+0(FP), R0
	MOVD	aVals_base+24(FP), R1
	MOVD	bVals_base+48(FP), R2
	MOVD	out_base+72(FP), R3
	MOVD	out_len+80(FP), R4

	MOVD	ZR, R5
blendi64_outer8:
	// Need R5+8 <= R4 to process a full 8-block.
	ADD	$8, R5, R11
	CMP	R4, R11
	BGT	blendi64_loop2

	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7	// R7 = condBits[R5/8], holds 8 bits

	// --- pair 0: bits 0, 1 of R7 ---
	AND	$1, R7, R8
	LSR	$1, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	// --- pair 1: bits 2, 3 ---
	LSR	$2, R7, R8
	AND	$1, R8, R8
	LSR	$3, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	// --- pair 2: bits 4, 5 ---
	LSR	$4, R7, R8
	AND	$1, R8, R8
	LSR	$5, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	// --- pair 3: bits 6, 7 ---
	LSR	$6, R7, R8
	AND	$1, R8, R8
	LSR	$7, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	ADD	$8, R5, R5
	B	blendi64_outer8

blendi64_loop2:
	// Pair path for the trailing 2, 4, or 6 elements.
	ADD	$2, R5, R11
	CMP	R4, R11
	BGT	blendi64_tail

	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7
	AND	$7, R5, R8
	LSR	R8, R7, R7
	AND	$1, R7, R9

	ADD	$1, R5, R10
	LSR	$3, R10, R11
	MOVBU	(R0)(R11), R12
	AND	$7, R10, R10
	LSR	R10, R12, R12
	AND	$1, R12, R10

	NEG	R9, R9
	NEG	R10, R10
	VMOV	R9, V8.D[0]
	VMOV	R10, V8.D[1]

	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)

	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3
	ADD	$2, R5, R5
	B	blendi64_loop2

blendi64_tail:
	CMP	R4, R5
	BGE	blendi64_done
	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7
	AND	$7, R5, R8
	LSR	R8, R7, R7
	AND	$1, R7, R9
	CBZ	R9, blendi64_use_b
	MOVD	(R1), R7
	B	blendi64_store
blendi64_use_b:
	MOVD	(R2), R7
blendi64_store:
	MOVD	R7, (R3)
	ADD	$8, R1, R1
	ADD	$8, R2, R2
	ADD	$8, R3, R3
	ADD	$1, R5, R5
	B	blendi64_tail

blendi64_done:
	RET

// func simdBlendFloat64NEON(condBits []byte, aVals, bVals, out []float64)
//
// Float64 is bit-identical to Int64 here: we're moving 64-bit values
// based on a mask, no arithmetic involved. Shares the 8-element fast
// path with the int variant.
TEXT ·simdBlendFloat64NEON(SB), NOSPLIT, $0-96
	MOVD	condBits_base+0(FP), R0
	MOVD	aVals_base+24(FP), R1
	MOVD	bVals_base+48(FP), R2
	MOVD	out_base+72(FP), R3
	MOVD	out_len+80(FP), R4

	MOVD	ZR, R5
blendf64_outer8:
	ADD	$8, R5, R11
	CMP	R4, R11
	BGT	blendf64_loop2

	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7

	AND	$1, R7, R8
	LSR	$1, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	LSR	$2, R7, R8
	AND	$1, R8, R8
	LSR	$3, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	LSR	$4, R7, R8
	AND	$1, R8, R8
	LSR	$5, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	LSR	$6, R7, R8
	AND	$1, R8, R8
	LSR	$7, R7, R9
	AND	$1, R9, R9
	NEG	R8, R8
	NEG	R9, R9
	VMOV	R8, V8.D[0]
	VMOV	R9, V8.D[1]
	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)
	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3

	ADD	$8, R5, R5
	B	blendf64_outer8

blendf64_loop2:
	ADD	$2, R5, R11
	CMP	R4, R11
	BGT	blendf64_tail

	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7
	AND	$7, R5, R8
	LSR	R8, R7, R7
	AND	$1, R7, R9

	ADD	$1, R5, R10
	LSR	$3, R10, R11
	MOVBU	(R0)(R11), R12
	AND	$7, R10, R10
	LSR	R10, R12, R12
	AND	$1, R12, R10

	NEG	R9, R9
	NEG	R10, R10
	VMOV	R9, V8.D[0]
	VMOV	R10, V8.D[1]

	VLD1	(R1), [V0.D2]
	VLD1	(R2), [V1.D2]
	VEOR	V0.B16, V1.B16, V9.B16
	VAND	V8.B16, V9.B16, V9.B16
	VEOR	V1.B16, V9.B16, V9.B16
	VST1	[V9.D2], (R3)

	ADD	$16, R1, R1
	ADD	$16, R2, R2
	ADD	$16, R3, R3
	ADD	$2, R5, R5
	B	blendf64_loop2

blendf64_tail:
	CMP	R4, R5
	BGE	blendf64_done
	LSR	$3, R5, R6
	MOVBU	(R0)(R6), R7
	AND	$7, R5, R8
	LSR	R8, R7, R7
	AND	$1, R7, R9
	CBZ	R9, blendf64_use_b
	MOVD	(R1), R7
	B	blendf64_store
blendf64_use_b:
	MOVD	(R2), R7
blendf64_store:
	MOVD	R7, (R3)
	ADD	$8, R1, R1
	ADD	$8, R2, R2
	ADD	$8, R3, R3
	ADD	$1, R5, R5
	B	blendf64_tail

blendf64_done:
	RET
