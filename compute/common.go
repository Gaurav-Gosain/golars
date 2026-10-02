// Package compute holds the vectorized kernel library. Each kernel takes one
// or more Series and returns a new Series (or a scalar). Kernels preserve the
// input Series' lifecycle; callers are responsible for releasing inputs and
// the returned result.
//
// Null semantics. Kernels propagate nulls per polars' defaults. For
// arithmetic and comparison, if any input at position i is null, the output
// at i is null. Logical kernels use Kleene three-valued logic. Aggregates
// skip nulls by default.
//
// Parallelism. Kernels use internal/pool.ParallelFor over the row range.
// Row-range parallelism is disabled for small inputs (see minParallelRows) to
// avoid goroutine overhead.
package compute

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Sentinel errors returned by compute kernels.
var (
	ErrDTypeMismatch    = errors.New("compute: dtype mismatch")
	ErrLengthMismatch   = errors.New("compute: length mismatch")
	ErrUnsupportedDType = errors.New("compute: unsupported dtype for kernel")
	ErrDivisionByZero   = errors.New("compute: integer division by zero")
)

// minParallelRows is the threshold below which kernels run serially. Below
// this size, goroutine overhead exceeds the parallelism benefit for simple
// per-element ops.
const minParallelRows = 16 * 1024

// Option configures a kernel call.
type Option func(*config)

type config struct {
	alloc       memory.Allocator
	parallelism int
	name        string
	nameSet     bool
}

func resolve(opts []Option) config {
	c := config{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the memory allocator used by a kernel.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *config) { c.alloc = alloc }
}

// WithParallelism caps the number of goroutines used by a kernel. Zero or
// negative values use the process default (GOMAXPROCS).
func WithParallelism(n int) Option {
	return func(c *config) { c.parallelism = n }
}

// WithName overrides the name of the output Series. By default, binary
// kernels inherit the name of the left operand.
func WithName(name string) Option {
	return func(c *config) {
		c.name = name
		c.nameSet = true
	}
}

func (c *config) outName(fallback string) string {
	if c.nameSet {
		return c.name
	}
	return fallback
}

// checkBinary validates that a and b can be used as inputs to a binary
// kernel: same length, same dtype.
func checkBinary(a, b *series.Series) error {
	if a.Len() != b.Len() {
		return fmt.Errorf("%w: %d vs %d", ErrLengthMismatch, a.Len(), b.Len())
	}
	if !a.DType().Equal(b.DType()) {
		return fmt.Errorf("%w: %s vs %s", ErrDTypeMismatch, a.DType(), b.DType())
	}
	return nil
}

// extractChunk flattens a Series to a single arrow.Array. The returned array
// carries one reference that the caller must release. If the Series is
// already a single chunk, the chunk is retained and returned directly.
func extractChunk(s *series.Series, mem memory.Allocator) (arrow.Array, error) {
	chunks := s.Chunks()
	if len(chunks) > 1 {
		// arrow's Concatenate fails on some empty chunks (an empty
		// dictionary chunk panics), and they add nothing.
		chunks = nonEmptyChunks(chunks)
	}
	switch len(chunks) {
	case 0:
		return array.MakeArrayOfNull(mem, s.DType().Arrow(), 0), nil
	case 1:
		chunks[0].Retain()
		return chunks[0], nil
	default:
		return series.ConcatArrays(chunks, mem)
	}
}

// nonEmptyChunks drops zero-length chunks, keeping one when all are
// empty so the dtype survives.
func nonEmptyChunks(chunks []arrow.Array) []arrow.Array {
	out := make([]arrow.Array, 0, len(chunks))
	for _, c := range chunks {
		if c.Len() > 0 {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return chunks[:1]
	}
	return out
}

// inferParallelism picks a row-level parallelism bound. For small n we force
// serial execution to avoid goroutine overhead.
func inferParallelism(cfg config, n int) int {
	if n < minParallelRows {
		return 1
	}
	if cfg.parallelism > 0 {
		return cfg.parallelism
	}
	return 0 // pool.ParallelFor resolves to GOMAXPROCS
}

// numericValues extracts a []T accessor from a typed arrow array. It returns
// the slice in logical (offset-adjusted) position. Panics if arr is not the
// expected concrete type. Only the widths referenced by the public API
// (int32/int64/uint32/uint64/float32/float64) are defined; other widths
// are promoted by the caller when needed.
func int32Values(arr arrow.Array) []int32   { return arr.(*array.Int32).Int32Values() }
func int64Values(arr arrow.Array) []int64   { return arr.(*array.Int64).Int64Values() }
func uint32Values(arr arrow.Array) []uint32 { return arr.(*array.Uint32).Uint32Values() }
func uint64Values(arr arrow.Array) []uint64 { return arr.(*array.Uint64).Uint64Values() }
func float32Values(arr arrow.Array) []float32 {
	return arr.(*array.Float32).Float32Values()
}
func float64Values(arr arrow.Array) []float64 {
	return arr.(*array.Float64).Float64Values()
}

// isUnsupported returns a descriptive ErrUnsupportedDType for the kernel.
func isUnsupported(kernel string, dt dtype.DType) error {
	return fmt.Errorf("%w: %s does not accept %s", ErrUnsupportedDType, kernel, dt)
}

// fromSlice wraps a []T into a Series of the appropriate dtype using the
// allocator in cfg. This is the standard output construction path.
func fromInt64Result(name string, out []int64, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromInt64(name, out, valid, series.WithAllocator(mem))
}
func fromUint32Result(name string, out []uint32, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromUint32(name, out, valid, series.WithAllocator(mem))
}
func fromUint64Result(name string, out []uint64, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromUint64(name, out, valid, series.WithAllocator(mem))
}
func fromFloat64Result(name string, out []float64, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromFloat64(name, out, valid, series.WithAllocator(mem))
}
func fromFloat32Result(name string, out []float32, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromFloat32(name, out, valid, series.WithAllocator(mem))
}
func fromInt32Result(name string, out []int32, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromInt32(name, out, valid, series.WithAllocator(mem))
}
func fromBoolResult(name string, out []bool, valid []bool, mem memory.Allocator) (*series.Series, error) {
	return series.FromBool(name, out, valid, series.WithAllocator(mem))
}

// buildValidityAnd constructs a valid[] slice where valid[i] = a.IsValid(i)
// AND b.IsValid(i). Returns nil if neither input has nulls, matching the
// arrow "all valid" convention.
func buildValidityAnd(a, b arrow.Array) []bool {
	if a.NullN() == 0 && b.NullN() == 0 {
		return nil
	}
	n := a.Len()
	v := make([]bool, n)
	for i := range v {
		v[i] = a.IsValid(i) && b.IsValid(i)
	}
	return v
}

// buildValidityCopy returns a copy of a's validity as []bool. Returns nil if
// a has no nulls.
func buildValidityCopy(a arrow.Array) []bool {
	if a.NullN() == 0 {
		return nil
	}
	v := make([]bool, a.Len())
	for i := range v {
		v[i] = a.IsValid(i)
	}
	return v
}

var _ = context.Background // keep context import in scope for kernels
