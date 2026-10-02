package compute

// minMaxInt64Scalar is the portable reference for minMaxInt64Kernel.
// vals must be non-empty.
func minMaxInt64Scalar(vals []int64, isMax bool) int64 {
	best := vals[0]
	if isMax {
		for _, v := range vals[1:] {
			best = max(best, v)
		}
		return best
	}
	for _, v := range vals[1:] {
		best = min(best, v)
	}
	return best
}
