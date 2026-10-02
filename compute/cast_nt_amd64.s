//go:build amd64 && !noasm

// Cast int64 → float64 with streaming stores.
// AVX2 has no packed int64→float64 (that's AVX-512 VCVTQQ2PD), so we use
// scalar CVTSQ2SD and pack pairs before two MOVNTPD stores. The stream
// stores bypass write-allocate so the output buffer doesn't pull 8 MB
// of cacheline-ownership traffic at 1 MiB inputs. Everything stays in
// legacy SSE encoding: mixing VEX stores with scalar converts costs an
// SSE-AVX transition penalty per iteration on Intel (see
// cast_i64f64_amd64.s).

#include "textflag.h"

// func simdCastInt64ToFloat64NT(out []float64, src []int64)
TEXT ·simdCastInt64ToFloat64NT(SB), NOSPLIT, $0-48
	MOVQ out_base+0(FP), DI
	MOVQ src_base+24(FP), SI
	MOVQ out_len+8(FP), CX
	XORQ AX, AX

	MOVQ CX, R8
	ANDQ $~3, R8

loop4:
	CMPQ AX, R8
	JGE tail_scalar

	// 4 scalar int64→double converts → pack pairs → two NT stores
	// of 2 doubles each. Same traffic as one 32-byte NT store.

	MOVQ (SI)(AX*8), R9
	CVTSQ2SD R9, X0
	MOVQ 8(SI)(AX*8), R10
	CVTSQ2SD R10, X1
	MOVLHPS X1, X0 // X0 = [double0, double1]

	MOVQ 16(SI)(AX*8), R9
	CVTSQ2SD R9, X2
	MOVQ 24(SI)(AX*8), R10
	CVTSQ2SD R10, X3
	MOVLHPS X3, X2 // X2 = [double2, double3]

	MOVNTPD X0, (DI)(AX*8)
	MOVNTPD X2, 16(DI)(AX*8)

	ADDQ $4, AX
	JMP loop4

tail_scalar:
	SFENCE

	CMPQ AX, CX
	JGE done

tail_loop:
	MOVQ (SI)(AX*8), R9
	CVTSQ2SD R9, X0
	MOVSD X0, (DI)(AX*8)
	INCQ AX
	CMPQ AX, CX
	JL tail_loop

done:
	RET
