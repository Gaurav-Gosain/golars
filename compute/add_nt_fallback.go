//go:build !amd64 || noasm

// Non-temporal stores are an amd64 feature. Elsewhere these entry
// points are plain elementwise kernels: vecBinInt64 / vecBinFloat64
// run NEON on arm64 and scalar loops on other targets.

package compute

// simdAddInt64NT writes out[i] = a[i] + b[i].
func simdAddInt64NT(out, a, b []int64) { vecBinInt64(out, a, b, opAdd) }

// simdAddFloat64NT writes out[i] = a[i] + b[i].
func simdAddFloat64NT(out, a, b []float64) { vecBinFloat64(out, a, b, opAdd) }

// simdMulInt64NT writes out[i] = a[i] * b[i].
func simdMulInt64NT(out, a, b []int64) { vecBinInt64(out, a, b, opMul) }
