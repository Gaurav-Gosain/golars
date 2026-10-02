//go:build unix

package dataframe_test

import (
	"syscall"
	"testing"
)

// cpuTime returns the user plus system CPU time of the process in
// nanoseconds.
func cpuTime() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Utime.Nano() + ru.Stime.Nano()
}

// reportCPU records the process CPU time per iteration as cpu-ns/op.
func reportCPU(b *testing.B) func() {
	start := cpuTime()
	return func() {
		b.ReportMetric(float64(cpuTime()-start)/float64(b.N), "cpu-ns/op")
	}
}
