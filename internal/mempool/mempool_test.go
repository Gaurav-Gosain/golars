package mempool

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestPoolingPassthrough(t *testing.T) {
	if got := Pooling(nil); got != Default() {
		t.Fatal("nil must resolve to the hot pool")
	}
	if got := Pooling(memory.DefaultAllocator); got != Default() {
		t.Fatal("default allocator must resolve to the hot pool")
	}
	custom := memory.NewCheckedAllocator(memory.NewGoAllocator())
	if got := Pooling(custom); got != memory.Allocator(custom) {
		t.Fatal("custom allocator must pass through untouched")
	}
}

func TestPooledReuse(t *testing.T) {
	a := Default()
	b1 := a.Allocate(100)
	if len(b1) != 100 {
		t.Fatalf("len = %d, want 100", len(b1))
	}
	// Scribble to prove reuse hands back the same backing array.
	for i := range b1 {
		b1[i] = byte(i)
	}
	a.Free(b1)
	b2 := a.Allocate(100)
	for i := range b2 {
		if b2[i] != byte(i) {
			t.Fatal("expected recycled backing array with stale contents")
		}
	}
	a.Free(b2)
}

func TestUnderCapFallsThrough(t *testing.T) {
	a := Default()
	small := a.Allocate(8)
	a.Free(small)
	// Same bucket, bigger request: must not return the small buffer.
	big := a.Allocate(1 << 20)
	if len(big) != 1<<20 {
		t.Fatalf("len = %d", len(big))
	}
	if cap(big) < 1<<20 {
		t.Fatalf("cap = %d", cap(big))
	}
	a.Free(big)
}

func TestZeroSize(t *testing.T) {
	a := Default()
	b := a.Allocate(0)
	if len(b) != 0 {
		t.Fatalf("len = %d", len(b))
	}
	a.Free(b) // must not panic or pollute the pool
}

func TestReallocate(t *testing.T) {
	a := Default()
	b := a.Allocate(16)
	for i := range b {
		b[i] = byte(i + 1)
	}
	// Shrink: same backing, no copy needed.
	s := a.Reallocate(8, b)
	if len(s) != 8 || s[0] != 1 || s[7] != 8 {
		t.Fatalf("shrink broke contents: %v", s)
	}
	// Grow: new buffer with prefix preserved.
	g := a.Reallocate(1024, s)
	if len(g) != 1024 || g[0] != 1 || g[7] != 8 {
		t.Fatal("grow lost contents")
	}
	a.Free(g)
}

func TestConcurrentUse(t *testing.T) {
	a := Default()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				b := a.Allocate(64 + (i*37+w)%1024)
				b[0] = byte(w)
				a.Free(b)
			}
		}(w)
	}
	wg.Wait()
}
