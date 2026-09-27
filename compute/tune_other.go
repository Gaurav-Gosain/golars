//go:build !arm64

package compute

// Per-architecture cutoffs. These values were measured on an Intel
// desktop; tune_arm64.go holds the Apple Silicon set.
const (
	// sumSerialCutoff: no-null sums below this many rows run the
	// SIMD kernel on one goroutine.
	sumSerialCutoff = 256 * 1024
	// minMaxSerialCutoff: no-null float min/max below this many rows
	// run on one goroutine.
	minMaxSerialCutoff = minParallelRows
	// elementwiseSerialCutoff: elementwise arithmetic below this many
	// rows runs on one goroutine.
	elementwiseSerialCutoff = 128 * 1024
	// ntStoreCutoff: at or above this many rows add and mul switch to
	// the non-temporal store kernels, which only exist on amd64.
	ntStoreCutoff = 1024 * 1024
)

// elementwiseWorkers caps the goroutines used by an elementwise kernel
// above elementwiseSerialCutoff. par <= 0 means GOMAXPROCS.
func elementwiseWorkers(par int) int { return par }
