//go:build !arm64 || noasm

package compute

// vecBinInt64 writes out[i] = a[i] op b[i] for add, sub and mul.
func vecBinInt64(out, a, b []int64, op arithOp) {
	vecBinInt64Scalar(out, a, b, op, 0)
}

// vecBinFloat64 writes out[i] = a[i] op b[i] with IEEE semantics.
func vecBinFloat64(out, a, b []float64, op arithOp) {
	vecBinFloat64Scalar(out, a, b, op, 0)
}
