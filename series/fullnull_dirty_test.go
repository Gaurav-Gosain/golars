package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// dirtyAlloc hands out memory filled with 0xff, like a recycling pool.
type dirtyAlloc struct{ memory.Allocator }

func (d dirtyAlloc) Allocate(n int) []byte {
	b := d.Allocator.Allocate(n)
	for i := range b {
		b[i] = 0xff
	}
	return b
}

func (d dirtyAlloc) Reallocate(n int, b []byte) []byte {
	old := len(b)
	b = d.Allocator.Reallocate(n, b)
	for i := old; i < len(b); i++ {
		b[i] = 0xff
	}
	return b
}

// FullNull must produce valid offsets even when the allocator does not
// zero memory. A left join against an empty right frame produced
// string columns with garbage offsets (found by internal/difftest).
func TestFullNullDirtyAllocator(t *testing.T) {
	mem := dirtyAlloc{memory.NewGoAllocator()}
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int64), arrow.PrimitiveTypes.Int64} {
		s, err := series.FullNull("x", dt, 5, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		a := s.Chunk(0)
		if a.NullN() != 5 {
			t.Fatalf("%s: %d nulls", dt, a.NullN())
		}
		if v, ok := a.(interface{ ValidateFull() error }); ok {
			if err := v.ValidateFull(); err != nil {
				t.Fatalf("%s: %v", dt, err)
			}
		}
		if str, ok := a.(*array.String); ok {
			for i := range 5 {
				if str.Value(i) != "" {
					t.Fatalf("row %d: %q", i, str.Value(i))
				}
			}
		}
		s.Release()
	}
}
