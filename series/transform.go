package series

import (
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Reverse returns a new Series with elements in reverse order. Mirrors
// polars' Series.reverse.
func (s *Series) Reverse(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		for i := range out {
			out[i] = raw[n-1-i]
		}
		return FromInt64(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, n)
		for i := range out {
			out[i] = raw[n-1-i]
		}
		return FromFloat64(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		for i := range out {
			out[i] = raw[n-1-i]
		}
		return FromInt32(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	case *array.Float32:
		raw := a.Float32Values()
		out := make([]float32, n)
		for i := range out {
			out[i] = raw[n-1-i]
		}
		return FromFloat32(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	case *array.String:
		out := make([]string, n)
		for i := range out {
			out[i] = a.Value(n - 1 - i)
		}
		return FromString(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	case *array.Boolean:
		out := make([]bool, n)
		for i := range out {
			out[i] = a.Value(n - 1 - i)
		}
		return FromBool(s.Name(), out, reverseValid(chunk), WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: Reverse unsupported for dtype %s", s.DType())
}

func reverseValid(a interface {
	Len() int
	NullN() int
	IsValid(int) bool
}) []bool {
	if a.NullN() == 0 {
		return nil
	}
	n := a.Len()
	valid := make([]bool, n)
	for i := range n {
		valid[i] = a.IsValid(n - 1 - i)
	}
	return valid
}

// Head returns the first n rows. Negative n is interpreted polars-
// style: keep all rows except the last |n|.
func (s *Series) Head(n int, opts ...Option) (*Series, error) {
	total := s.Len()
	if n < 0 {
		n = max0(total + n)
	}
	if n > total {
		n = total
	}
	if n == 0 {
		return Empty(s.Name(), s.DType()), nil
	}
	_ = opts
	return s.Slice(0, n)
}

// Tail returns the last n rows. Negative n skips the first |n|.
func (s *Series) Tail(n int, opts ...Option) (*Series, error) {
	total := s.Len()
	if n < 0 {
		n = max0(total + n)
	}
	if n > total {
		n = total
	}
	if n == 0 {
		return Empty(s.Name(), s.DType()), nil
	}
	_ = opts
	return s.Slice(total-n, n)
}

func max0(x int) int {
	if x < 0 {
		return 0
	}
	return x
}

// Sample returns n randomly selected rows, with or without replacement.
// seed controls the PRNG; 0 asks for a fresh PCG source.
func (s *Series) Sample(n int, withReplacement bool, seed uint64, opts ...Option) (*Series, error) {
	if n < 0 {
		return nil, fmt.Errorf("series: Sample n must be non-negative")
	}
	total := s.Len()
	if total == 0 || n == 0 {
		return Empty(s.Name(), s.DType()), nil
	}
	r := rand.New(rand.NewPCG(seed, seed+1))
	idx := make([]int, n)
	if withReplacement {
		for i := range idx {
			idx[i] = r.IntN(total)
		}
	} else {
		if n > total {
			return nil, fmt.Errorf("series: Sample n=%d > len=%d without replacement", n, total)
		}
		perm := r.Perm(total)[:n]
		copy(idx, perm)
	}
	return s.takeIndices(idx, opts)
}

// Shuffle returns a Series containing the same values in a random
// order. Equivalent to Sample(Len(), false, seed).
func (s *Series) Shuffle(seed uint64, opts ...Option) (*Series, error) {
	return s.Sample(s.Len(), false, seed, opts...)
}

// TopK returns the k largest non-null elements, sorted descending.
// Stable across ties. Empty/all-null returns an empty Series.
func (s *Series) TopK(k int, opts ...Option) (*Series, error) {
	if k < 0 {
		return nil, fmt.Errorf("series: TopK k must be non-negative")
	}
	idx, err := topKIndices(s, k, true)
	if err != nil {
		return nil, err
	}
	return s.takeIndices(idx, opts)
}

// BottomK returns the k smallest non-null elements, sorted ascending.
func (s *Series) BottomK(k int, opts ...Option) (*Series, error) {
	if k < 0 {
		return nil, fmt.Errorf("series: BottomK k must be non-negative")
	}
	idx, err := topKIndices(s, k, false)
	if err != nil {
		return nil, err
	}
	return s.takeIndices(idx, opts)
}

// Equal reports whether two Series are element-wise equal (names +
// values + null pattern + dtypes). NaN != NaN. Mirrors the polars
// `series_equal` function.
func (s *Series) Equal(other *Series) bool {
	if s == nil || other == nil {
		return s == other
	}
	if s.Name() != other.Name() {
		return false
	}
	if !s.DType().Equal(other.DType()) {
		return false
	}
	if s.Len() != other.Len() {
		return false
	}
	if s.NullCount() != other.NullCount() {
		return false
	}
	a := s.Chunk(0)
	b := other.Chunk(0)
	n := a.Len()
	for i := range n {
		va := a.IsValid(i)
		vb := b.IsValid(i)
		if va != vb {
			return false
		}
		if !va {
			continue
		}
		switch aa := a.(type) {
		case *array.Int64:
			if aa.Value(i) != b.(*array.Int64).Value(i) {
				return false
			}
		case *array.Int32:
			if aa.Value(i) != b.(*array.Int32).Value(i) {
				return false
			}
		case *array.Float64:
			if aa.Value(i) != b.(*array.Float64).Value(i) {
				return false
			}
		case *array.Float32:
			if aa.Value(i) != b.(*array.Float32).Value(i) {
				return false
			}
		case *array.Boolean:
			if aa.Value(i) != b.(*array.Boolean).Value(i) {
				return false
			}
		case *array.String:
			if aa.Value(i) != b.(*array.String).Value(i) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// takeIndices is the primitive behind TopK/BottomK/Sample/Shuffle.
// Out-of-bounds indices panic; callers guarantee correctness.
func (s *Series) takeIndices(idx []int, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	chunk := s.Chunk(0)
	n := len(idx)
	switch a := chunk.(type) {
	case *array.Int64:
		raw := a.Int64Values()
		out := make([]int64, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = raw[k]
		}
		return FromInt64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float64:
		raw := a.Float64Values()
		out := make([]float64, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = raw[k]
		}
		return FromFloat64(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Int32:
		raw := a.Int32Values()
		out := make([]int32, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = raw[k]
		}
		return FromInt32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Float32:
		raw := a.Float32Values()
		out := make([]float32, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = raw[k]
		}
		return FromFloat32(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.Boolean:
		out := make([]bool, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = a.Value(k)
		}
		return FromBool(s.Name(), out, valid, WithAllocator(cfg.alloc))
	case *array.String:
		out := make([]string, n)
		valid := takeValid(a, idx)
		for i, k := range idx {
			out[i] = a.Value(k)
		}
		return FromString(s.Name(), out, valid, WithAllocator(cfg.alloc))
	}
	return nil, fmt.Errorf("series: take unsupported for dtype %s", s.DType())
}

func takeValid(a interface {
	NullN() int
	IsValid(int) bool
}, idx []int) []bool {
	if a.NullN() == 0 {
		return nil
	}
	valid := make([]bool, len(idx))
	for i, k := range idx {
		valid[i] = a.IsValid(k)
	}
	return valid
}

// ensure sort is still referenced (used in ArgSort).
var _ = sort.SliceStable
