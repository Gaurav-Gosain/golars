//go:build arm64

package compute

import "math"

// Per-architecture cutoffs for Apple Silicon class cores, measured on
// an M3 Pro. A single core streams 100+ GB/s from L2 through the NEON
// kernels, so serial runs stay ahead of goroutine fan-out for longer
// than on x86, and the fan-out cost itself dominates at mid sizes.
const (
	sumSerialCutoff         = 256 * 1024
	minMaxSerialCutoff      = 256 * 1024
	elementwiseSerialCutoff = 128 * 1024
	// No non-temporal store kernels on arm64.
	ntStoreCutoff = math.MaxInt
)

// elementwiseWorkers caps the goroutines used by an elementwise kernel
// above elementwiseSerialCutoff. par <= 0 means GOMAXPROCS.
func elementwiseWorkers(par int) int { return par }
