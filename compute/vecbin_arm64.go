//go:build arm64 && !noasm

package compute

// vecBinInt64 writes out[i] = a[i] op b[i] for add, sub and mul. Add
// and sub run on NEON; NEON has no 64-bit integer multiply, so mul
// stays scalar.
func vecBinInt64(out, a, b []int64, op arithOp) {
	start := 0
	if op == opAdd || op == opSub {
		n := len(out)
		start = neonBinInt64(a[:n], b[:n], out, int(op))
	}
	vecBinInt64Scalar(out, a, b, op, start)
}

// vecBinFloat64 writes out[i] = a[i] op b[i] with IEEE semantics.
func vecBinFloat64(out, a, b []float64, op arithOp) {
	n := len(out)
	start := neonBinFloat64(a[:n], b[:n], out, int(op))
	vecBinFloat64Scalar(out, a, b, op, start)
}
