package dataframe

import (
	"math/bits"
	"sync"
)

// scratchPool recycles per-row scratch slices (group ids, key codes)
// between group-by calls. Slices are bucketed by power-of-two capacity
// so a request never receives a slice that is too small, and repeated
// calls at one size reuse the same backing array instead of paying a
// large allocation (and the page faults behind it) every time.
type scratchPool[T any] struct {
	buckets [40]sync.Pool
}

func (p *scratchPool[T]) get(n int) []T {
	if n == 0 {
		return nil
	}
	b := bits.Len(uint(n - 1))
	if v := p.buckets[b].Get(); v != nil {
		s := *(v.(*[]T))
		return s[:n]
	}
	return make([]T, n, 1<<b)
}

// put returns s for reuse. The caller must not touch s afterwards.
func (p *scratchPool[T]) put(s []T) {
	c := cap(s)
	if c == 0 || c&(c-1) != 0 {
		return
	}
	s = s[:0]
	p.buckets[bits.Len(uint(c-1))].Put(&s)
}

var (
	int32Scratch  scratchPool[int32]
	intScratch    scratchPool[int]
	uint64Scratch scratchPool[uint64]
	uint8Scratch  scratchPool[uint8]
)
