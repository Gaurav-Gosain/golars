//go:build arm64 && !noasm

// func simdFillNullInt64NEON(condBits []byte, aVals []int64, lit int64, out []int64) int
//
// out[i] = aVals[i] where validity bit i is set, else lit. condBits
// bit 0 must correspond to aVals[0].
//
// Each iteration handles 8 rows (one bitmap byte): the byte is
// broadcast to both 64-bit lanes and tested against {1,2}, {4,8},
// {16,32} and {64,128}, giving four lane masks with no scalar bit
// extraction, and BSL picks src or the broadcast literal per lane.
// Returns the number of rows written, len(out) rounded down to a
// multiple of 8; the caller finishes the tail.

#include "textflag.h"

TEXT ·simdFillNullInt64NEON(SB), NOSPLIT, $0-88
	MOVD	condBits_base+0(FP), R0
	MOVD	aVals_base+24(FP), R1
	MOVD	lit+48(FP), R2
	MOVD	out_base+56(FP), R3
	MOVD	out_len+64(FP), R4
	LSR	$3, R4, R6
	LSL	$3, R6, R7
	MOVD	R7, ret+80(FP)
	CBZ	R6, fnull_done
	VDUP	R2, V7.D2
	MOVD	$1, R8
	VMOV	R8, V26.D[0]
	MOVD	$2, R8
	VMOV	R8, V26.D[1]
	VSHL	$2, V26.D2, V27.D2
	VSHL	$4, V26.D2, V28.D2
	VSHL	$6, V26.D2, V29.D2
fnull_loop:
	MOVBU.P	1(R0), R8
	VDUP	R8, V8.D2
	VCMTST	V26.D2, V8.D2, V9.D2
	VCMTST	V27.D2, V8.D2, V10.D2
	VCMTST	V28.D2, V8.D2, V11.D2
	VCMTST	V29.D2, V8.D2, V12.D2
	VLD1.P	64(R1), [V16.D2, V17.D2, V18.D2, V19.D2]
	VBSL	V7.B16, V16.B16, V9.B16
	VBSL	V7.B16, V17.B16, V10.B16
	VBSL	V7.B16, V18.B16, V11.B16
	VBSL	V7.B16, V19.B16, V12.B16
	VST1.P	[V9.D2, V10.D2, V11.D2, V12.D2], 64(R3)
	SUBS	$1, R6, R6
	BNE	fnull_loop
fnull_done:
	RET
