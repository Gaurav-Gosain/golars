//go:build arm64 && !noasm

package compute

// gather64ARM64 is implemented in gather_arm64.s.
//
//go:noescape
func gather64ARM64(out, src []uint64, indices []int) int

// gather64Accel gathers 8-byte rows with the assembly kernel. It returns
// how many leading rows it wrote; the caller finishes the rest.
func gather64Accel(out, src []uint64, indices []int) int {
	return gather64ARM64(out, src, indices)
}
