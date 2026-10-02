//go:build unix

package parquet_test

import (
	"syscall"
	"testing"
	"time"
)

// cpuTime returns the user plus system CPU time of the process.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// reportCPU records the process CPU time per iteration as cpu-ms/op.
// Wall time is noisy on a shared machine; CPU time shows how much work
// an operation does regardless of how many cores it got.
func reportCPU(b *testing.B) func() {
	start := cpuTime()
	return func() {
		b.ReportMetric(float64(cpuTime()-start)/float64(time.Millisecond)/float64(b.N), "cpu-ms/op")
	}
}
