//go:build !arm64 || noasm

package compute

// minMaxInt64Kernel returns the minimum (or maximum when isMax) of a
// non-empty no-null int64 slice.
func minMaxInt64Kernel(vals []int64, isMax bool) int64 {
	return minMaxInt64Scalar(vals, isMax)
}
