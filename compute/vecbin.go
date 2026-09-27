package compute

// vecBinInt64Scalar writes out[i] = a[i] op b[i] for i >= start. Every
// operand is resliced to len(out) so the loops carry no bounds checks.
// opDiv is not handled here: integer division needs the zero check.
func vecBinInt64Scalar(out, a, b []int64, op arithOp, start int) {
	n := len(out)
	a, b = a[:n], b[:n]
	switch op {
	case opAdd:
		for i := start; i < n; i++ {
			out[i] = a[i] + b[i]
		}
	case opSub:
		for i := start; i < n; i++ {
			out[i] = a[i] - b[i]
		}
	case opMul:
		for i := start; i < n; i++ {
			out[i] = a[i] * b[i]
		}
	}
}

// vecBinFloat64Scalar is the float64 counterpart, including division.
func vecBinFloat64Scalar(out, a, b []float64, op arithOp, start int) {
	n := len(out)
	a, b = a[:n], b[:n]
	switch op {
	case opAdd:
		for i := start; i < n; i++ {
			out[i] = a[i] + b[i]
		}
	case opSub:
		for i := start; i < n; i++ {
			out[i] = a[i] - b[i]
		}
	case opMul:
		for i := start; i < n; i++ {
			out[i] = a[i] * b[i]
		}
	case opDiv:
		for i := start; i < n; i++ {
			out[i] = a[i] / b[i]
		}
	}
}
