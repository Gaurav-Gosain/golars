package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// dirtyAllocator hands out buffers full of set bits, like a recycled
// buffer from a sync.Pool.
type dirtyAllocator struct{ memory.Allocator }

func (d dirtyAllocator) Allocate(n int) []byte {
	b := d.Allocator.Allocate(n)
	for i := range b {
		b[i] = 0xff
	}
	return b
}

func (d dirtyAllocator) Reallocate(n int, old []byte) []byte {
	b := d.Allocator.Reallocate(n, old)
	for i := range b {
		b[i] = 0xff
	}
	return b
}

// The fused builders' callbacks only set valid bits, so a recycled
// buffer must not leak stale valid bits into the output.
func TestFusedBuildersClearRecycledBitmap(t *testing.T) {
	mem := dirtyAllocator{memory.NewGoAllocator()}
	onlyFirst := func(validBits []byte) int {
		validBits[0] |= 1
		return 9
	}
	i64, err := series.BuildInt64DirectFused("a", 10, mem, func(out []int64, bits []byte) int { out[0] = 1; return onlyFirst(bits) })
	if err != nil {
		t.Fatal(err)
	}
	defer i64.Release()
	i32, err := series.BuildInt32DirectFused("b", 10, mem, func(out []int32, bits []byte) int { out[0] = 1; return onlyFirst(bits) })
	if err != nil {
		t.Fatal(err)
	}
	defer i32.Release()
	f64, err := series.BuildFloat64DirectFused("c", 10, mem, func(out []float64, bits []byte) int { out[0] = 1; return onlyFirst(bits) })
	if err != nil {
		t.Fatal(err)
	}
	defer f64.Release()
	for _, s := range []*series.Series{i64, i32, f64} {
		c := s.Chunk(0)
		for i := 1; i < 10; i++ {
			if c.IsValid(i) {
				t.Fatalf("%s: row %d valid, want null", s.Name(), i)
			}
		}
	}
}
