// Package mempool owns the process-global pooled arrow allocator
// shared by hot-loop kernels across compute, series, dataframe, and
// lazy. Putting it in one place keeps the "is this buffer from the
// hot pool" question answerable by reference identity: compare the
// allocator to mempool.Default() or route through Pooling().
//
// The previous home was compute/bufpool.go, scoped to the compute
// package. Moving it out lets series-level kernels (FillNull,
// string case-fold, LenBytes) share the same recycled byte buckets
// - no duplicate pool, no layering dance, one pool to rule them.
package mempool

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// pooledAllocator wraps an underlying arrow memory.Allocator with a
// size-bucketed sync.Pool so back-to-back kernel calls reuse the same
// backing byte slice rather than hitting mallocgc each time. Crucially
// it does NOT zero on Allocate: the hot kernels (Add, Mul, Cast,
// Filter, Gather, FillNull) fully overwrite the output buffer, so a
// zero-init would be wasted work.
//
// Buckets are indexed by ceil(log2(size)). A 1 MB buffer and a 600 KB
// buffer share bucket 20; requests larger than the sampled slice fall
// through to the underlying allocator.
type pooledAllocator struct {
	inner memory.Allocator
	pools [40]sync.Pool
}

func newPooledAllocator(inner memory.Allocator) *pooledAllocator {
	return &pooledAllocator{inner: inner}
}

// log2Ceil returns ⌈log2(max(n, 1))⌉, clamped to [0, 39].
func log2Ceil(n int) int {
	if n <= 1 {
		return 0
	}
	b := 0
	m := n - 1
	for m > 0 {
		m >>= 1
		b++
	}
	if b >= 40 {
		b = 39
	}
	return b
}

func (p *pooledAllocator) Allocate(size int) []byte {
	b := log2Ceil(size)
	if got := p.pools[b].Get(); got != nil {
		buf := got.([]byte)
		if cap(buf) >= size {
			if countHits.Load() {
				hitBytes.Add(int64(size))
			}
			return buf[:size]
		}
		// Under-cap bucket hit - drop and fall through.
		p.pools[b].Put(buf)
	}
	return p.inner.Allocate(size)
}

func (p *pooledAllocator) Reallocate(size int, b []byte) []byte {
	if size <= cap(b) {
		return b[:size]
	}
	out := p.Allocate(size)
	copy(out, b)
	p.Free(b)
	return out
}

func (p *pooledAllocator) Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	bucket := log2Ceil(cap(b))
	p.pools[bucket].Put(b[:cap(b)])
}

// hot is the process-global pooled allocator. Kernels that fully
// overwrite their output buffer should route through Pooling(alloc)
// at their entry points.
var hot = newPooledAllocator(memory.DefaultAllocator)

// Default returns the hot pool as a memory.Allocator. Rarely useful
// directly; prefer Pooling() which respects caller-supplied test
// allocators.
func Default() memory.Allocator { return hot }

// Pooling returns the hot pool when userMem is nil or the arrow
// default allocator; otherwise it returns userMem unchanged. This
// preserves the behaviour tests rely on: a memory.CheckedAllocator
// supplied via WithAllocator is never replaced, so leak detection
// keeps working end-to-end.
func Pooling(userMem memory.Allocator) memory.Allocator {
	if userMem == nil || userMem == memory.DefaultAllocator {
		return hot
	}
	return userMem
}

// Pool-hit accounting for the bench harness (cmd/bench). A buffer
// served from a pool never reaches the Go heap, so runtime.MemStats
// does not see it; with counting on, its size is added to a counter so
// per-operation allocation figures include it. Off by default; the
// cost when off is one atomic load per pool hit.
var (
	countHits atomic.Bool
	hitBytes  atomic.Int64
)

// CountPoolHits turns pool-hit accounting on or off and returns the
// bytes counted since the previous call, resetting the counter.
func CountPoolHits(on bool) int64 {
	countHits.Store(on)
	return hitBytes.Swap(0)
}
