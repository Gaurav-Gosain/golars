package main

// Per-workload memory accounting. After timing a workload, timeNs runs
// it once more between two runtime.ReadMemStats calls and records the
// bytes and objects allocated by that single call. The polars-rs
// harness does the same with a counting global allocator, so the two
// alloc_bytes figures are directly comparable: both count every heap
// allocation made while the operation runs, including memory freed
// again before it returns.
//
// Buffers served from the shared arrow buffer pool (internal/mempool)
// never reach the Go heap, so MemStats misses them; the pool counts
// them while measureAlloc runs and they are added in. The figures
// measure allocation volume per operation, not resident memory;
// compare.py --rss measures peak RSS per workload in separate
// processes.

import (
	"runtime"

	"github.com/Gaurav-Gosain/golars/internal/mempool"
)

type memSample struct {
	allocBytes int64
	allocs     int64
}

// memQueue collects one sample per timeNs call. add and addAll attach
// the samples to the results they return.
var memQueue []memSample

// measureAlloc runs fn once and records how much it allocated. The
// counters are cumulative, so a collection during fn does not change
// them.
func measureAlloc(fn func()) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	mempool.CountPoolHits(true)
	fn()
	pooled := mempool.CountPoolHits(false)
	runtime.ReadMemStats(&after)
	memQueue = append(memQueue, memSample{
		allocBytes: int64(after.TotalAlloc-before.TotalAlloc) + pooled,
		allocs:     int64(after.Mallocs - before.Mallocs),
	})
}

// attachMem copies queued samples onto rs, which must be the results
// produced since the queue was last drained, in order. When the counts
// do not line up the results are left without memory figures rather
// than risk pairing a sample with the wrong workload.
func attachMem(rs []result) {
	if len(memQueue) == len(rs) {
		for i := range rs {
			rs[i].AllocBytes = memQueue[i].allocBytes
			rs[i].Allocs = memQueue[i].allocs
		}
	}
	memQueue = memQueue[:0]
}
