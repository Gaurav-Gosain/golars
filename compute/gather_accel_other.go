//go:build !arm64 || noasm

package compute

// gather64Accel has no assembly kernel on this platform.
func gather64Accel(out, src []uint64, indices []int) int { return 0 }
