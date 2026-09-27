//go:build arm64 && !noasm

// ARM64 NEON int64 -> float64 cast.
//
// SCVTF.2D converts two lanes per instruction. Each iteration converts
// 16 values with eight independent SCVTFs and post-increment loads and
// stores, so the loop carries one counter. A 2-lane loop and a scalar
// SCVTF finish the tail. SCVTF rounds to nearest even, the same as a
// Go float64(int64) conversion.

#include "textflag.h"

// func simdCastInt64ToFloat64NT(out []float64, src []int64)
TEXT ·simdCastInt64ToFloat64NT(SB), NOSPLIT, $0-48
	MOVD	out_base+0(FP), R0
	MOVD	out_len+8(FP), R1
	MOVD	src_base+24(FP), R2
	LSR	$4, R1, R3
	CBZ	R3, cast64_pairs
cast64_loop16:
	VLD1.P	64(R2), [V16.D2, V17.D2, V18.D2, V19.D2]
	VLD1.P	64(R2), [V20.D2, V21.D2, V22.D2, V23.D2]
	VSCVTF	V16.D2, V16.D2
	VSCVTF	V17.D2, V17.D2
	VSCVTF	V18.D2, V18.D2
	VSCVTF	V19.D2, V19.D2
	VSCVTF	V20.D2, V20.D2
	VSCVTF	V21.D2, V21.D2
	VSCVTF	V22.D2, V22.D2
	VSCVTF	V23.D2, V23.D2
	VST1.P	[V16.D2, V17.D2, V18.D2, V19.D2], 64(R0)
	VST1.P	[V20.D2, V21.D2, V22.D2, V23.D2], 64(R0)
	SUBS	$1, R3, R3
	BNE	cast64_loop16
cast64_pairs:
	AND	$15, R1, R1
	LSR	$1, R1, R3
	CBZ	R3, cast64_last
cast64_loop2:
	VLD1.P	16(R2), [V16.D2]
	VSCVTF	V16.D2, V16.D2
	VST1.P	[V16.D2], 16(R0)
	SUBS	$1, R3, R3
	BNE	cast64_loop2
cast64_last:
	TBZ	$0, R1, cast64_done
	MOVD	(R2), R6
	SCVTFD	R6, F0
	FMOVD	F0, (R0)
cast64_done:
	RET
