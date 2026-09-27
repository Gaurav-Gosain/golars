package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Shift returns a new Series shifted by periods positions. Positive
// periods shift values down (forward in time): out[i] = s[i-periods].
// Negative periods shift up (backward in time).
//
// Positions that fall outside the source become null. The returned
// Series always has the same length as the input. Mirrors polars'
// Series.shift(periods).
//
// Zero-copy implementation: the result is a 2-chunk ChunkedArray
// composed of an all-null prefix/suffix plus a zero-copy arrow slice of
// the source chunk(s). No data is memcpy'd. This matches polars' shift,
// which achieves the same via metadata-only chunk manipulation.
//
// Downstream kernels that need a single contiguous chunk pay an O(n)
// rechunk cost on first access (polars does the same). Kernels that
// iterate via extractChunk or handle chunks natively see no penalty.
func (s *Series) Shift(periods int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	n := s.Len()
	if periods == 0 {
		return s.Clone(), nil
	}
	if periods >= n || -periods >= n {
		return nullSeries(s.Name(), s.data.DataType(), n, cfg.alloc)
	}

	dt := s.data.DataType()
	switch dt.ID() {
	case arrow.INT64, arrow.INT32, arrow.UINT64, arrow.UINT32,
		arrow.FLOAT64, arrow.FLOAT32, arrow.BOOL, arrow.STRING,
		arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64, arrow.DURATION:
		// Supported - fall through to zero-copy construction.
	default:
		return nil, fmt.Errorf("series: Shift unsupported for dtype %s", s.DType())
	}

	// Consolidate to a single arrow.Array so we can slice it cleanly. For
	// an already-single-chunk Series this is a retain-only no-op path
	// (see extractSingleChunk below).
	src, err := extractSingleChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer src.Release()

	var chunks []arrow.Array
	if periods > 0 {
		nullPrefix := array.MakeArrayOfNull(cfg.alloc, dt, periods)
		dataSlice := array.NewSlice(src, 0, int64(n-periods))
		chunks = []arrow.Array{nullPrefix, dataSlice}
	} else {
		off := -periods
		dataSlice := array.NewSlice(src, int64(off), int64(n))
		nullSuffix := array.MakeArrayOfNull(cfg.alloc, dt, off)
		chunks = []arrow.Array{dataSlice, nullSuffix}
	}
	// arrow.NewChunked retains each chunk; release our local references.
	chunked := arrow.NewChunked(dt, chunks)
	for _, c := range chunks {
		c.Release()
	}
	out := &Series{name: s.Name(), data: chunked}
	return out, nil
}

// extractSingleChunk returns a single arrow.Array representing the whole
// Series. For a single-chunk Series this is a cheap retain of the
// existing chunk; for multi-chunk it concatenates via arrow.
func extractSingleChunk(s *Series, mem memory.Allocator) (arrow.Array, error) {
	chunks := s.Chunks()
	switch len(chunks) {
	case 0:
		return array.MakeArrayOfNull(mem, s.data.DataType(), 0), nil
	case 1:
		chunks[0].Retain()
		return chunks[0], nil
	default:
		return array.Concatenate(chunks, mem)
	}
}

// nullSeries returns a new all-null Series of the given dtype and
// length. Used for boundary cases like Shift(n+1). New consumes the
// array reference, so it must not be released here.
func nullSeries(name string, dt arrow.DataType, n int, mem memory.Allocator) (*Series, error) {
	return New(name, array.MakeArrayOfNull(mem, dt, n))
}
