//go:build arm64 && !noasm

package compute

// simdCastInt64ToFloat64NT converts int64 values to float64 using
// NEON's SCVTF.2D, 16 values per loop iteration.
//
// Overrides the scalar fallback in add_nt_fallback.go on arm64; the
// build tags in that file exclude arm64 when -tags noasm is absent.
func simdCastInt64ToFloat64NT(out []float64, src []int64)

// simdCastInt64ToFloat64AVX2 is the small-N entry point; on arm64 it is
// the same NEON kernel.
func simdCastInt64ToFloat64AVX2(out []float64, src []int64) {
	simdCastInt64ToFloat64NT(out, src)
}

// hasCastI64ToF64AVX2 gates the small-N kernel in castInt64ToFloat64.
// The name predates the arm64 port; here it selects the NEON SCVTF
// kernel, which is about four times the scalar loop's throughput on
// L1/L2-resident data.
const hasCastI64ToF64AVX2 = true

// castNTThreshold is the size from which castInt64ToFloat64 splits the
// conversion across goroutines. Both paths run the NEON kernel.
const castNTThreshold = 128 * 1024
