// Package series defines Series, a named, chunked, nullable column.
//
// A Series is the building block of a DataFrame. It wraps a chunked arrow
// array with a column name and exposes dtype-aware accessors.
//
// Reference counting. A Series owns one reference to its underlying
// arrow.Chunked. Constructors consume the arrow.Array references passed in:
// after a successful call, the caller must not release the input arrays.
// Methods that return a new Series (Rename, Slice, Clone) share the underlying
// buffers through refcounting and do not copy data. Call Release when you are
// done with a Series to drop its reference.
package series

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// Sentinel errors returned by Series operations.
var (
	ErrEmptyChunks        = errors.New("series: at least one chunk required")
	ErrChunkDTypeMismatch = errors.New("series: chunks have mismatched dtypes")
	ErrSliceOutOfBounds   = errors.New("series: slice out of bounds")
	ErrLengthMismatch     = errors.New("series: values and valid mask have different lengths")
)

// Option configures an allocating constructor.
type Option func(*config)

type config struct {
	alloc memory.Allocator
}

func resolve(opts []Option) config {
	c := config{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the memory allocator used by a constructor.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *config) { c.alloc = alloc }
}

// Series is a named column backed by a chunked arrow array.
type Series struct {
	name string
	data *arrow.Chunked
}

// New builds a Series from one or more arrow.Array chunks of the same dtype.
// On success New consumes the caller's references to the input arrays; callers
// must not use or release them afterward. On error the caller retains
// ownership.
func New(name string, chunks ...arrow.Array) (*Series, error) {
	if len(chunks) == 0 {
		return nil, ErrEmptyChunks
	}
	dt := chunks[0].DataType()
	for _, c := range chunks[1:] {
		if !arrow.TypeEqual(c.DataType(), dt) {
			return nil, fmt.Errorf("%w: %s vs %s", ErrChunkDTypeMismatch, c.DataType(), dt)
		}
	}
	chunked := arrow.NewChunked(dt, chunks) // retains each chunk
	for _, c := range chunks {
		c.Release() // release caller's original reference
	}
	return &Series{name: name, data: chunked}, nil
}

// FromChunked wraps an existing *arrow.Chunked. The function retains a
// reference; the caller's reference is unaffected.
func FromChunked(name string, chunked *arrow.Chunked) *Series {
	chunked.Retain()
	return &Series{name: name, data: chunked}
}

// Empty returns an empty Series of the given dtype.
func Empty(name string, dt dtype.DType) *Series {
	return &Series{name: name, data: arrow.NewChunked(dt.Arrow(), nil)}
}

// FromInt64 builds a Series from a []int64. valid may be nil to mark every
// value as valid; otherwise len(valid) must equal len(values) and valid[i]
// true means values[i] is non-null.
func FromInt64(name string, values []int64, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewInt64Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromUint32 builds a Series from a []uint32.
func FromUint32(name string, values []uint32, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewUint32Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromUint64 builds a Series from a []uint64.
func FromUint64(name string, values []uint64, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewUint64Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromInt32 builds a Series from a []int32.
func FromInt32(name string, values []int32, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewInt32Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromFloat64 builds a Series from a []float64.
func FromFloat64(name string, values []float64, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewFloat64Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromFloat32 builds a Series from a []float32.
func FromFloat32(name string, values []float32, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewFloat32Builder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromString builds a Series from a []string.
func FromString(name string, values []string, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewStringBuilder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// FromTime builds a time-of-day Series. The values carry a count of
// the dtype's own unit since midnight (e.g. for Time(Microsecond),
// value 3600*1_000_000 means 01:00:00.000000). Unit must agree with
// dt.Arrow()'s underlying Time32/Time64 resolution.
func FromTime(name string, values []int64, valid []bool, dt dtype.DType, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	switch t := dt.Arrow().(type) {
	case *arrow.Time32Type:
		b := array.NewTime32Builder(cfg.alloc, t)
		defer b.Release()
		out := make([]arrow.Time32, len(values))
		for i, v := range values {
			out[i] = arrow.Time32(v)
		}
		b.AppendValues(out, valid)
		return New(name, b.NewArray())
	case *arrow.Time64Type:
		b := array.NewTime64Builder(cfg.alloc, t)
		defer b.Release()
		out := make([]arrow.Time64, len(values))
		for i, v := range values {
			out[i] = arrow.Time64(v)
		}
		b.AppendValues(out, valid)
		return New(name, b.NewArray())
	}
	return nil, fmt.Errorf("series.FromTime: %q requires a Time dtype, got %s", name, dt)
}

// FromBool builds a Series from a []bool.
func FromBool(name string, values []bool, valid []bool, opts ...Option) (*Series, error) {
	if valid != nil && len(valid) != len(values) {
		return nil, ErrLengthMismatch
	}
	cfg := resolve(opts)
	b := array.NewBooleanBuilder(cfg.alloc)
	defer b.Release()
	b.AppendValues(values, valid)
	arr := b.NewArray()
	return New(name, arr)
}

// Name returns the column name.
func (s *Series) Name() string { return s.name }

// DType returns the logical dtype.
func (s *Series) DType() dtype.DType { return dtype.FromArrow(s.data.DataType()) }

// Len returns the total number of rows across all chunks.
func (s *Series) Len() int { return s.data.Len() }

// NullCount returns the number of null values across all chunks.
func (s *Series) NullCount() int { return s.data.NullN() }

// NumChunks returns the number of chunks that make up this Series.
func (s *Series) NumChunks() int { return len(s.data.Chunks()) }

// Chunks returns the underlying chunk slice. Do not mutate. The returned
// arrays share ownership with this Series; if you retain them independently,
// call Retain on each.
func (s *Series) Chunks() []arrow.Array { return s.data.Chunks() }

// Chunk returns the chunk at index i.
//
// For a single-chunk Series this is a direct O(1) accessor. For a
// multi-chunk Series (e.g. the output of a zero-copy Shift), Chunk(0)
// transparently consolidates all chunks into one and caches the result
// in s.data so subsequent calls are O(1). This keeps the 300+ legacy
// callers that assume single-chunk behaviour correct when they see a
// Series produced by a multi-chunk path. The consolidation pays the
// O(n) copy that the zero-copy path deferred - same model polars uses
// for its chunked arrays.
//
// Not thread-safe: Series has never been, and adding a mutex would
// burden every single-chunk read. Concurrent Chunk(0) calls on a
// multi-chunk Series are a caller bug; callers that want safety should
// rechunk up-front via s.Consolidated().
func (s *Series) Chunk(i int) arrow.Array {
	if i == 0 {
		switch len(s.data.Chunks()) {
		case 0:
			// A zero-chunk Series (Empty, a zero-length slice) gets one
			// empty chunk so callers that read Chunk(0) never index an
			// empty slice.
			s.installEmptyChunk()
		case 1:
		default:
			s.consolidateInPlace()
		}
	}
	return s.data.Chunk(i)
}

func (s *Series) installEmptyChunk() {
	arr := array.MakeArrayOfNull(memory.DefaultAllocator, s.data.DataType(), 0)
	defer arr.Release()
	newChunked := arrow.NewChunked(s.data.DataType(), []arrow.Array{arr})
	s.data.Release()
	s.data = newChunked
}

// consolidateInPlace replaces s.data with a single-chunk *arrow.Chunked
// that holds the concatenated contents of every existing chunk. No-op
// when already single-chunk.
func (s *Series) consolidateInPlace() {
	chunks := s.data.Chunks()
	if len(chunks) <= 1 {
		return
	}
	arr, err := ConcatArrays(chunks, memory.DefaultAllocator)
	if err != nil {
		// Concatenate only fails on dtype mismatch, which arrow already
		// prevents at NewChunked time; treat as impossible but fall back
		// to the multi-chunk state rather than panicking.
		return
	}
	defer arr.Release()
	newChunked := arrow.NewChunked(s.data.DataType(), []arrow.Array{arr})
	s.data.Release()
	s.data = newChunked
}

// Chunked returns the underlying *arrow.Chunked. The caller must Retain it if
// it intends to outlive the Series.
func (s *Series) Chunked() *arrow.Chunked { return s.data }

// Rechunk returns a single-chunk clone of s. When s already has one
// chunk the call is a retain-only passthrough; otherwise the chunks
// are concatenated via arrow.Concatenate. Matches polars' series
// `rechunk()` and is the common preparatory step before crossing
// the FFI boundary or handing the Series to a kernel that assumes
// a single backing buffer.
func (s *Series) Rechunk() *Series {
	clone := s.Clone()
	clone.consolidateInPlace()
	return clone
}

// nonEmptyChunks drops zero-length chunks from a multi-chunk list,
// keeping one when all are empty so the dtype survives. arrow's
// Concatenate fails on some empty chunks (an empty dictionary chunk
// panics), and they add nothing.
func nonEmptyChunks(chunks []arrow.Array) []arrow.Array {
	if len(chunks) < 2 {
		return chunks
	}
	kept := make([]arrow.Array, 0, len(chunks))
	for _, c := range chunks {
		if c.Len() > 0 {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		return chunks[:1]
	}
	return kept
}

// Consolidated returns an arrow.Array that holds every row of this
// Series in a single contiguous chunk. When the Series already has one
// chunk the call is a retain-only no-op; with multiple chunks it
// concatenates via array.Concatenate. The caller owns the returned
// array and must Release it.
//
// Needed whenever an op can accept a Series that might have been
// produced by a zero-copy multi-chunk path (Shift and friends) and the
// consumer's kernel assumes contiguous values. compute.extractChunk
// already does this internally, so compute kernels don't need to call
// this - it's for external consumers and tests.
func (s *Series) Consolidated() (arrow.Array, error) {
	chunks := nonEmptyChunks(s.data.Chunks())
	switch len(chunks) {
	case 0:
		return array.MakeArrayOfNull(memory.DefaultAllocator, s.data.DataType(), 0), nil
	case 1:
		chunks[0].Retain()
		return chunks[0], nil
	default:
		return ConcatArrays(chunks, memory.DefaultAllocator)
	}
}

// Release drops this Series' reference to its chunked data. After Release the
// Series must not be used.
func (s *Series) Release() {
	if s.data != nil {
		s.data.Release()
		s.data = nil
	}
}

// Retain increments this Series' reference count. Use this when sharing a
// Series across goroutines that each manage their own Release.
func (s *Series) Retain() { s.data.Retain() }

// Clone returns a Series that shares the underlying data with s. Both values
// must be Released independently.
func (s *Series) Clone() *Series {
	s.data.Retain()
	return &Series{name: s.name, data: s.data}
}

// Rename returns a Series with a new name, sharing the underlying data.
func (s *Series) Rename(name string) *Series {
	s.data.Retain()
	return &Series{name: name, data: s.data}
}

// Slice returns a Series containing [offset, offset+length). Data buffers are
// shared; no data is copied.
func (s *Series) Slice(offset, length int) (*Series, error) {
	if offset < 0 || length < 0 || offset+length > s.Len() {
		return nil, fmt.Errorf("%w: offset=%d length=%d len=%d", ErrSliceOutOfBounds, offset, length, s.Len())
	}
	sliced := array.NewChunkedSlice(s.data, int64(offset), int64(offset+length))
	return &Series{name: s.name, data: sliced}, nil
}

// ClampSlice resolves a polars-style slice against a length n. A
// negative offset counts from the end, a negative length means "to the
// end", and the window is clamped to [0, n]. The result is always a
// valid argument pair for Slice.
func ClampSlice(offset, length, n int) (int, int) {
	start := offset
	if start < 0 {
		start += n
	}
	stop := n
	if length >= 0 && start+length < n {
		stop = start + length
	}
	start = min(max(start, 0), n)
	stop = min(max(stop, 0), n)
	return start, max(stop-start, 0)
}

// String returns a short one-line repr: name: dtype [len=N, nulls=M].
func (s *Series) String() string {
	return s.Format(DefaultSeriesFormatOptions())
}

// Summary is the compact one-liner: "name: dtype [len=N, nulls=K]".
func (s *Series) Summary() string {
	return fmt.Sprintf("%s: %s [len=%d, nulls=%d]", s.name, s.DType(), s.Len(), s.NullCount())
}
