//go:build arm64 && !noasm

package compute

// minMaxInt64Kernel returns the minimum (or maximum when isMax) of a
// non-empty no-null int64 slice. Short slices stay scalar because the
// NEON setup and fold cost more than the loop.
func minMaxInt64Kernel(vals []int64, isMax bool) int64 {
	if len(vals) < 32 {
		return minMaxInt64Scalar(vals, isMax)
	}
	if isMax {
		return simdMaxInt64NEON(vals)
	}
	return simdMinInt64NEON(vals)
}
