//go:build arm64 && !noasm

// NEON reduction kernels: sum, min and max over int64 and float64.
//
// Every kernel keeps eight independent 2-lane accumulators so the
// add/compare latency (2 to 3 cycles on M-series cores) is hidden
// behind four vector pipes, and loads 128 bytes per iteration with
// post-increment addressing so the loop carries only one counter.
// Accumulators are folded pairwise at the end, then a 2-element loop
// and a scalar step handle the ragged tail.
//
// Operand order follows the Go assembler: OP Vm, Vn, Vd computes
// Vd = Vn OP Vm.

#include "textflag.h"

#define ZERO8 \
	VEOR	V0.B16, V0.B16, V0.B16; \
	VEOR	V1.B16, V1.B16, V1.B16; \
	VEOR	V2.B16, V2.B16, V2.B16; \
	VEOR	V3.B16, V3.B16, V3.B16; \
	VEOR	V4.B16, V4.B16, V4.B16; \
	VEOR	V5.B16, V5.B16, V5.B16; \
	VEOR	V6.B16, V6.B16, V6.B16; \
	VEOR	V7.B16, V7.B16, V7.B16

#define SPLAT8(src) \
	VMOV	src.B16, V1.B16; \
	VMOV	src.B16, V2.B16; \
	VMOV	src.B16, V3.B16; \
	VMOV	src.B16, V4.B16; \
	VMOV	src.B16, V5.B16; \
	VMOV	src.B16, V6.B16; \
	VMOV	src.B16, V7.B16

#define LOAD16 \
	VLD1.P	64(R0), [V16.D2, V17.D2, V18.D2, V19.D2]; \
	VLD1.P	64(R0), [V20.D2, V21.D2, V22.D2, V23.D2]

// OP8 applies a three-operand op (acc = acc OP x) to all eight
// accumulator/data pairs.
#define OP8(op) \
	op	V16.D2, V0.D2, V0.D2; \
	op	V17.D2, V1.D2, V1.D2; \
	op	V18.D2, V2.D2, V2.D2; \
	op	V19.D2, V3.D2, V3.D2; \
	op	V20.D2, V4.D2, V4.D2; \
	op	V21.D2, V5.D2, V5.D2; \
	op	V22.D2, V6.D2, V6.D2; \
	op	V23.D2, V7.D2, V7.D2

// FOLD8 folds V1..V7 into V0 with a balanced tree.
#define FOLD8(op) \
	op	V4.D2, V0.D2, V0.D2; \
	op	V5.D2, V1.D2, V1.D2; \
	op	V6.D2, V2.D2, V2.D2; \
	op	V7.D2, V3.D2, V3.D2; \
	op	V2.D2, V0.D2, V0.D2; \
	op	V3.D2, V1.D2, V1.D2; \
	op	V1.D2, V0.D2, V0.D2

// Signed int64 min and max: NEON has no 64-bit SMIN/SMAX, so each
// step is a compare into a mask register followed by a bit insert.
// MINSTEP: acc = x where acc > x. MAXSTEP: acc = x where x > acc.
#define MINSTEP(x, acc, m) \
	VCMGT	x.D2, acc.D2, m.D2; \
	VBIT	m.B16, x.B16, acc.B16

#define MAXSTEP(x, acc, m) \
	VCMGT	acc.D2, x.D2, m.D2; \
	VBIT	m.B16, x.B16, acc.B16

// func simdSumInt64NEON(a []int64) int64
TEXT ·simdSumInt64NEON(SB), NOSPLIT, $0-32
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1
	ZERO8
	LSR	$4, R1, R2
	CBZ	R2, sumi64_fold
sumi64_loop16:
	LOAD16
	OP8(VADD)
	SUBS	$1, R2, R2
	BNE	sumi64_loop16
sumi64_fold:
	FOLD8(VADD)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, sumi64_lanes
sumi64_loop2:
	VLD1.P	16(R0), [V16.D2]
	VADD	V16.D2, V0.D2, V0.D2
	SUBS	$1, R2, R2
	BNE	sumi64_loop2
sumi64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	ADD	R4, R3, R3
	TBZ	$0, R1, sumi64_done
	MOVD	(R0), R4
	ADD	R4, R3, R3
sumi64_done:
	MOVD	R3, ret+24(FP)
	RET

// func simdSumFloat64NEON(a []float64) float64
//
// Lane j of accumulator k sums the elements at index 16*i + 2*k + j,
// so the rounding differs from a left-to-right scalar sum. That is
// the same reassociation every vectorised float sum performs.
TEXT ·simdSumFloat64NEON(SB), NOSPLIT, $0-32
	MOVD	a_base+0(FP), R0
	MOVD	a_len+8(FP), R1
	ZERO8
	LSR	$4, R1, R2
	CBZ	R2, sumf64_fold
sumf64_loop16:
	LOAD16
	OP8(VFADD)
	SUBS	$1, R2, R2
	BNE	sumf64_loop16
sumf64_fold:
	FOLD8(VFADD)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, sumf64_lanes
sumf64_loop2:
	VLD1.P	16(R0), [V16.D2]
	VFADD	V16.D2, V0.D2, V0.D2
	SUBS	$1, R2, R2
	BNE	sumf64_loop2
sumf64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	FMOVD	R3, F0
	FMOVD	R4, F1
	FADDD	F1, F0, F0
	TBZ	$0, R1, sumf64_done
	FMOVD	(R0), F1
	FADDD	F1, F0, F0
sumf64_done:
	FMOVD	F0, ret+24(FP)
	RET

// func simdMinFloat64NEON(vals []float64) (best float64, hasNaN bool)
//
// FMIN propagates NaN, so any NaN input leaves NaN in the result and
// hasNaN reports it; callers then rescan with NaN-aware scalar code.
// The +Inf seed is the identity for every non-NaN input.
TEXT ·simdMinFloat64NEON(SB), NOSPLIT, $0-33
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1
	MOVD	$0x7FF0000000000000, R3
	VDUP	R3, V0.D2
	SPLAT8(V0)
	LSR	$4, R1, R2
	CBZ	R2, minf64_fold
minf64_loop16:
	LOAD16
	OP8(VFMIN)
	SUBS	$1, R2, R2
	BNE	minf64_loop16
minf64_fold:
	FOLD8(VFMIN)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, minf64_lanes
minf64_loop2:
	VLD1.P	16(R0), [V16.D2]
	VFMIN	V16.D2, V0.D2, V0.D2
	SUBS	$1, R2, R2
	BNE	minf64_loop2
minf64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	FMOVD	R3, F0
	FMOVD	R4, F1
	FMIND	F1, F0, F0
	TBZ	$0, R1, minf64_done
	FMOVD	(R0), F1
	FMIND	F1, F0, F0
minf64_done:
	FCMPD	F0, F0
	CSET	VS, R3
	FMOVD	F0, best+24(FP)
	MOVB	R3, hasNaN+32(FP)
	RET

// func simdMaxFloat64NEON(vals []float64) (best float64, hasNaN bool)
TEXT ·simdMaxFloat64NEON(SB), NOSPLIT, $0-33
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1
	MOVD	$0xFFF0000000000000, R3
	VDUP	R3, V0.D2
	SPLAT8(V0)
	LSR	$4, R1, R2
	CBZ	R2, maxf64_fold
maxf64_loop16:
	LOAD16
	OP8(VFMAX)
	SUBS	$1, R2, R2
	BNE	maxf64_loop16
maxf64_fold:
	FOLD8(VFMAX)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, maxf64_lanes
maxf64_loop2:
	VLD1.P	16(R0), [V16.D2]
	VFMAX	V16.D2, V0.D2, V0.D2
	SUBS	$1, R2, R2
	BNE	maxf64_loop2
maxf64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	FMOVD	R3, F0
	FMOVD	R4, F1
	FMAXD	F1, F0, F0
	TBZ	$0, R1, maxf64_done
	FMOVD	(R0), F1
	FMAXD	F1, F0, F0
maxf64_done:
	FCMPD	F0, F0
	CSET	VS, R3
	FMOVD	F0, best+24(FP)
	MOVB	R3, hasNaN+32(FP)
	RET

// func simdMinInt64NEON(vals []int64) int64
//
// vals must be non-empty; the accumulators are seeded with vals[0].
TEXT ·simdMinInt64NEON(SB), NOSPLIT, $0-32
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1
	MOVD	(R0), R3
	VDUP	R3, V0.D2
	SPLAT8(V0)
	LSR	$4, R1, R2
	CBZ	R2, mini64_fold
mini64_loop16:
	LOAD16
	MINSTEP(V16, V0, V24)
	MINSTEP(V17, V1, V25)
	MINSTEP(V18, V2, V26)
	MINSTEP(V19, V3, V27)
	MINSTEP(V20, V4, V28)
	MINSTEP(V21, V5, V29)
	MINSTEP(V22, V6, V30)
	MINSTEP(V23, V7, V31)
	SUBS	$1, R2, R2
	BNE	mini64_loop16
mini64_fold:
	MINSTEP(V4, V0, V24)
	MINSTEP(V5, V1, V25)
	MINSTEP(V6, V2, V26)
	MINSTEP(V7, V3, V27)
	MINSTEP(V2, V0, V24)
	MINSTEP(V3, V1, V25)
	MINSTEP(V1, V0, V24)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, mini64_lanes
mini64_loop2:
	VLD1.P	16(R0), [V16.D2]
	MINSTEP(V16, V0, V24)
	SUBS	$1, R2, R2
	BNE	mini64_loop2
mini64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	CMP	R4, R3
	CSEL	GT, R4, R3, R3
	TBZ	$0, R1, mini64_done
	MOVD	(R0), R4
	CMP	R4, R3
	CSEL	GT, R4, R3, R3
mini64_done:
	MOVD	R3, ret+24(FP)
	RET

// func simdMaxInt64NEON(vals []int64) int64
//
// vals must be non-empty; the accumulators are seeded with vals[0].
TEXT ·simdMaxInt64NEON(SB), NOSPLIT, $0-32
	MOVD	vals_base+0(FP), R0
	MOVD	vals_len+8(FP), R1
	MOVD	(R0), R3
	VDUP	R3, V0.D2
	SPLAT8(V0)
	LSR	$4, R1, R2
	CBZ	R2, maxi64_fold
maxi64_loop16:
	LOAD16
	MAXSTEP(V16, V0, V24)
	MAXSTEP(V17, V1, V25)
	MAXSTEP(V18, V2, V26)
	MAXSTEP(V19, V3, V27)
	MAXSTEP(V20, V4, V28)
	MAXSTEP(V21, V5, V29)
	MAXSTEP(V22, V6, V30)
	MAXSTEP(V23, V7, V31)
	SUBS	$1, R2, R2
	BNE	maxi64_loop16
maxi64_fold:
	MAXSTEP(V4, V0, V24)
	MAXSTEP(V5, V1, V25)
	MAXSTEP(V6, V2, V26)
	MAXSTEP(V7, V3, V27)
	MAXSTEP(V2, V0, V24)
	MAXSTEP(V3, V1, V25)
	MAXSTEP(V1, V0, V24)
	AND	$15, R1, R1
	LSR	$1, R1, R2
	CBZ	R2, maxi64_lanes
maxi64_loop2:
	VLD1.P	16(R0), [V16.D2]
	MAXSTEP(V16, V0, V24)
	SUBS	$1, R2, R2
	BNE	maxi64_loop2
maxi64_lanes:
	VMOV	V0.D[0], R3
	VMOV	V0.D[1], R4
	CMP	R4, R3
	CSEL	LT, R4, R3, R3
	TBZ	$0, R1, maxi64_done
	MOVD	(R0), R4
	CMP	R4, R3
	CSEL	LT, R4, R3, R3
maxi64_done:
	MOVD	R3, ret+24(FP)
	RET
