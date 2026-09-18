//go:build amd64 && !noasm

// Cast int64 → float64 with regular stores (no streaming), for inputs
// that fit in cache. The streaming-store variant in cast_nt_amd64.s
// pays the SFENCE + write-combining-buffer cost on every call and is
// only a win when the output doesn't fit in L2.
//
// AVX2 lacks a packed int64→double instruction (VCVTQQ2PD is AVX-512
// only), so we do scalar CVTSQ2SD converts and pack pairs with MOVLHPS
// before SSE stores. 4-wide × 2-unroll = 8 elements per iter.
//
// Every floating-point op deliberately uses its LEGACY SSE encoding:
// mixing legacy SSE (CVTSQ2SD/MOVLHPS) with VEX (VINSERTF128/VMOVUPD)
// costs a ~100-cycle SSE-AVX transition penalty per iteration on Intel.
// Pure SSE has identical store traffic (8× 64-bit stores per 8 elems).

#include "textflag.h"

// func simdCastInt64ToFloat64AVX2(out []float64, src []int64)
TEXT ·simdCastInt64ToFloat64AVX2(SB), NOSPLIT, $0-48
	MOVQ out_base+0(FP), DI
	MOVQ src_base+24(FP), SI
	MOVQ out_len+8(FP), CX
	XORQ AX, AX

	// Round down to multiple of 8.
	MOVQ CX, R8
	ANDQ $~7, R8

loop8:
	CMPQ AX, R8
	JGE tail4

	// First 4 lanes.
	MOVQ (SI)(AX*8), R9
	CVTSQ2SD R9, X0
	MOVQ 8(SI)(AX*8), R10
	CVTSQ2SD R10, X1
	MOVLHPS X1, X0
	MOVQ 16(SI)(AX*8), R9
	CVTSQ2SD R9, X2
	MOVQ 24(SI)(AX*8), R10
	CVTSQ2SD R10, X3
	MOVLHPS X3, X2
	MOVLPD X0, (DI)(AX*8)
	MOVHPD X0, 8(DI)(AX*8)
	MOVLPD X2, 16(DI)(AX*8)
	MOVHPD X2, 24(DI)(AX*8)

	// Second 4 lanes, independent data deps so the two iterations can
	// issue in parallel through the two CVT ports on Skylake/Zen.
	MOVQ 32(SI)(AX*8), R9
	CVTSQ2SD R9, X4
	MOVQ 40(SI)(AX*8), R10
	CVTSQ2SD R10, X5
	MOVLHPS X5, X4
	MOVQ 48(SI)(AX*8), R9
	CVTSQ2SD R9, X6
	MOVQ 56(SI)(AX*8), R10
	CVTSQ2SD R10, X7
	MOVLHPS X7, X6
	MOVLPD X4, 32(DI)(AX*8)
	MOVHPD X4, 40(DI)(AX*8)
	MOVLPD X6, 48(DI)(AX*8)
	MOVHPD X6, 56(DI)(AX*8)

	ADDQ $8, AX
	JMP loop8

tail4:
	// Round down to multiple of 4 for the residual.
	MOVQ CX, R8
	ANDQ $~3, R8
	CMPQ AX, R8
	JGE tail1

loop4:
	MOVQ (SI)(AX*8), R9
	CVTSQ2SD R9, X0
	MOVQ 8(SI)(AX*8), R10
	CVTSQ2SD R10, X1
	MOVLHPS X1, X0
	MOVQ 16(SI)(AX*8), R9
	CVTSQ2SD R9, X2
	MOVQ 24(SI)(AX*8), R10
	CVTSQ2SD R10, X3
	MOVLHPS X3, X2
	MOVLPD X0, (DI)(AX*8)
	MOVHPD X0, 8(DI)(AX*8)
	MOVLPD X2, 16(DI)(AX*8)
	MOVHPD X2, 24(DI)(AX*8)

	ADDQ $4, AX
	CMPQ AX, R8
	JL loop4

tail1:
	CMPQ AX, CX
	JGE done

scalar_tail:
	MOVQ (SI)(AX*8), R9
	CVTSQ2SD R9, X0
	MOVSD X0, (DI)(AX*8)
	INCQ AX
	CMPQ AX, CX
	JL scalar_tail

done:
	RET
