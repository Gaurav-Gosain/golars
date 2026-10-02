//go:build arm64 && !noasm

// gather64ARM64 writes src[indices[j]] into out[j] for 8-byte
// elements, eight rows per iteration. Index pairs load with LDP and
// output pairs store with STP, which roughly halves the instruction
// count of the compiled Go loop. Every index is checked against
// len(src) as an unsigned value, so negative indices also fail.
//
// Signature:
//   func gather64ARM64(out, src []uint64, indices []int) int
//
// Returns the number of rows written. It stops at the start of the
// first block of eight that holds an out-of-range index, and always
// stops before the last len(indices)%8 rows. The caller finishes the
// rest in Go, where the normal bounds check raises the panic.
//
// FP layout: out +0/+8/+16, src +24/+32/+40, indices +48/+56/+64,
//   ret +72. Frame size 80 bytes.

#include "textflag.h"

TEXT ·gather64ARM64(SB), NOSPLIT, $0-80
	MOVD out_base+0(FP), R0
	MOVD src_base+24(FP), R1
	MOVD src_len+32(FP), R2
	MOVD indices_base+48(FP), R3
	MOVD indices_len+56(FP), R4

	MOVD ZR, R5 // rows written
	SUB  $8, R4, R6 // last start that still has a full block

loop:
	CMP R6, R5
	BGT done

	LDP 0(R3), (R7, R8)
	LDP 16(R3), (R9, R10)
	LDP 32(R3), (R11, R12)
	LDP 48(R3), (R13, R14)

	CMP R2, R7
	BHS done
	CMP R2, R8
	BHS done
	CMP R2, R9
	BHS done
	CMP R2, R10
	BHS done
	CMP R2, R11
	BHS done
	CMP R2, R12
	BHS done
	CMP R2, R13
	BHS done
	CMP R2, R14
	BHS done

	MOVD (R1)(R7<<3), R7
	MOVD (R1)(R8<<3), R8
	MOVD (R1)(R9<<3), R9
	MOVD (R1)(R10<<3), R10
	MOVD (R1)(R11<<3), R11
	MOVD (R1)(R12<<3), R12
	MOVD (R1)(R13<<3), R13
	MOVD (R1)(R14<<3), R14

	STP (R7, R8), 0(R0)
	STP (R9, R10), 16(R0)
	STP (R11, R12), 32(R0)
	STP (R13, R14), 48(R0)

	ADD $64, R3, R3
	ADD $64, R0, R0
	ADD $8, R5, R5
	B   loop

done:
	MOVD R5, ret+72(FP)
	RET
