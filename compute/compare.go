package compute

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// Ordered is the set of dtypes that support ordering comparisons.
type Ordered interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

// Eq returns a boolean Series a == b. Nulls propagate.
func Eq(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Eq", opEq)
}

// Ne returns a boolean Series a != b. Nulls propagate.
func Ne(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Ne", opNe)
}

// Lt returns a boolean Series a < b. Nulls propagate.
func Lt(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Lt", opLt)
}

// Le returns a boolean Series a <= b. Nulls propagate.
func Le(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Le", opLe)
}

// Gt returns a boolean Series a > b. Nulls propagate.
func Gt(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Gt", opGt)
}

// Ge returns a boolean Series a >= b. Nulls propagate.
func Ge(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runCompare(ctx, a, b, opts, "Ge", opGe)
}

// GtLit returns a boolean Series a > lit. Scalar literal comparison avoids
// broadcasting the literal into a full-length Series: 2× faster and half
// the memory. Polars' `col > 123` expression compiles to a similar kernel.
// Supported dtypes: int64, float64. Other dtypes fall back to the broadcast
// form internally for correctness.
func GtLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opGt)
}

// LtLit is the <-with-literal variant. See GtLit.
func LtLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opLt)
}

// EqLit is the ==-with-literal variant.
func EqLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opEq)
}

// NeLit is the !=-with-literal variant.
func NeLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opNe)
}

// LeLit is the <=-with-literal variant.
func LeLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opLe)
}

// GeLit is the >=-with-literal variant.
func GeLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runCompareLit(ctx, a, lit, opts, opGe)
}

// runCompareLit dispatches to fast scalar-literal paths for the common
// int64 and float64 cases; other dtypes fall back to the broadcast form.
func runCompareLit(ctx context.Context, a *series.Series, lit any, opts []Option, op compareOp) (*series.Series, error) {
	if s, ok := lit.(string); ok && a.DType().IsDictionary() {
		return catCompareLit(a, s, opts, op)
	}
	if out, ok, err := extraCompareLit(ctx, a, lit, opts, op); ok {
		return out, err
	}
	cfg := resolve(opts)
	if s, ok := lit.(string); ok {
		if out, ok, err := compareStringScalar(ctx, a, s, cfg, op); ok {
			return out, err
		}
	}
	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	name := cfg.outName(a.Name())
	par := inferParallelism(cfg, a.Len())
	noNulls := aArr.NullN() == 0

	if noNulls {
		switch a.DType().ID() {
		case arrow.INT64:
			var v int64
			switch x := lit.(type) {
			case int64:
				v = x
			case int:
				v = int64(x)
			case int32:
				v = int64(x)
			default:
				goto fallback
			}
			return fastCompareInt64Lit(ctx, name, int64Values(aArr), v, op, cfg.alloc, par)
		case arrow.FLOAT64:
			var v float64
			switch x := lit.(type) {
			case float64:
				v = x
			case float32:
				v = float64(x)
			case int:
				v = float64(x)
			case int64:
				v = float64(x)
			default:
				goto fallback
			}
			if v != v {
				// NaN literal: rare, take the NaN-aware generic path.
				goto fallback
			}
			if op == opGt || op == opGe {
				// polars orders NaN above every number, so x > lit is
				// !(x <= lit): IEEE gives false for NaN on both sides,
				// and inverting the complement fixes NaN rows for free.
				return invertCompare(fastCompareFloat64Lit(ctx, name, float64Values(aArr), v, complementOp(op), cfg.alloc, par))
			}
			return fastCompareFloat64Lit(ctx, name, float64Values(aArr), v, op, cfg.alloc, par)
		}
	}

fallback:
	// An integer column against a float literal compares in f64, which
	// is the polars supertype. Truncating the literal would make
	// `a == 2.5` true for a == 2.
	if a.DType().ID() == arrow.INT64 || a.DType().ID() == arrow.UINT64 {
		switch lit.(type) {
		case float64, float32:
			fa, err := Cast(ctx, a, dtype.Float64(), WithAllocator(cfg.alloc))
			if err != nil {
				return nil, err
			}
			defer fa.Release()
			return runCompareLit(ctx, fa, lit, opts, op)
		}
	}
	// Broadcast fallback: materialise the literal and use the standard path.
	bs, err := literalSeries(a, lit)
	if err != nil {
		return nil, err
	}
	defer bs.Release()
	return runCompare(ctx, a, bs, opts, "CompareLit", op)
}

type compareOp int

const (
	opEq compareOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
)

func runCompare(ctx context.Context, a, b *series.Series, opts []Option, kernel string, op compareOp) (*series.Series, error) {
	if a.DType().IsDictionary() || b.DType().IsDictionary() {
		return catCompare(ctx, a, b, opts, kernel, op)
	}
	if out, ok, err := extraCompare(ctx, a, b, opts, op); ok {
		return out, err
	}
	if err := checkBinary(a, b); err != nil {
		return nil, err
	}
	cfg := resolve(opts)

	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	bArr, err := extractChunk(b, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bArr.Release()

	name := cfg.outName(a.Name())
	par := inferParallelism(cfg, a.Len())

	// Fast path: direct-bitmap write for no-null int64/float64 compares.
	// The result is packed into the Boolean array's bit buffer in one pass
	// with no []bool intermediate. Measured ~10x faster than the generic
	// compareOrdered path at 256K.
	noNulls := aArr.NullN() == 0 && bArr.NullN() == 0
	if noNulls {
		switch a.DType().ID() {
		case arrow.INT64:
			return fastCompareInt64(ctx, name, int64Values(aArr), int64Values(bArr), op, cfg.alloc, par)
		case arrow.FLOAT64:
			// The SIMD kernel uses IEEE semantics; NaN input goes to the
			// generic path, which orders NaN like polars.
			if !hasNaNFloat64(float64Values(aArr)) && !hasNaNFloat64(float64Values(bArr)) {
				return fastCompareFloat64(ctx, name, float64Values(aArr), float64Values(bArr), op, cfg.alloc, par)
			}
		}
	}

	switch a.DType().ID() {
	case arrow.BOOL:
		return compareBool(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.INT32:
		return compareOrdered(ctx, name, aArr, bArr, int32Values(aArr), int32Values(bArr), op, cfg.alloc, par)
	case arrow.INT64:
		return compareOrdered(ctx, name, aArr, bArr, int64Values(aArr), int64Values(bArr), op, cfg.alloc, par)
	case arrow.UINT32:
		return compareOrdered(ctx, name, aArr, bArr, uint32Values(aArr), uint32Values(bArr), op, cfg.alloc, par)
	case arrow.UINT64:
		return compareOrdered(ctx, name, aArr, bArr, uint64Values(aArr), uint64Values(bArr), op, cfg.alloc, par)
	case arrow.FLOAT32:
		return compareOrdered(ctx, name, aArr, bArr, float32Values(aArr), float32Values(bArr), op, cfg.alloc, par)
	case arrow.FLOAT64:
		return compareOrdered(ctx, name, aArr, bArr, float64Values(aArr), float64Values(bArr), op, cfg.alloc, par)
	case arrow.STRING:
		return compareStrings(ctx, name, aArr, bArr, op, cfg.alloc, par)
	}
	return nil, isUnsupported(kernel, a.DType())
}

// fastCompareInt64 writes the bitmap result of a binary int64 compare
// directly, one byte (8 rows) at a time. For n >= 128K it parallelizes by
// splitting the output byte range; each worker writes to a disjoint range
// of bytes in the packed bitmap so there's no contention.
func fastCompareInt64(ctx context.Context, name string, av, bv []int64, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	n := len(av)
	// Per-byte fill: 8 rows per byte. Disjoint byte ranges run in parallel.
	writeByteRange := func(bits []byte, start, end int) {
		// SIMD path: compare 8 (AVX-512) or two 4-lane (AVX2) chunks per
		// iteration and write the bitmap byte directly. Handles everything
		// through the last byte-aligned row; the tail loop finishes any
		// remaining partial byte.
		done := start
		if simdAvailable && hasSIMDInt64() && end-start >= 8 {
			simdEnd := start + ((end-start)/8)*8
			done = start + simdCompareInt64(av[start:simdEnd], bv[start:simdEnd], bits[start/8:], op)
		}
		// start, end are row indices aligned to 8 (except the very last).
		for row := done; row < end; row += 8 {
			chunk := min(row+8, end)
			var b byte
			switch op {
			case opEq:
				for j := row; j < chunk; j++ {
					if av[j] == bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opNe:
				for j := row; j < chunk; j++ {
					if av[j] != bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opLt:
				for j := row; j < chunk; j++ {
					if av[j] < bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opLe:
				for j := row; j < chunk; j++ {
					if av[j] <= bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opGt:
				for j := row; j < chunk; j++ {
					if av[j] > bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opGe:
				for j := row; j < chunk; j++ {
					if av[j] >= bv[j] {
						b |= 1 << (j - row)
					}
				}
			}
			bits[row/8] = b
		}
	}

	return series.BuildBoolDirect(name, n, poolingMem(mem), func(bits []byte) {
		if n < 128*1024 {
			writeByteRange(bits, 0, n)
			return
		}
		// Chunk on byte boundaries so workers never write the same byte.
		nBytes := (n + 7) / 8
		_ = pool.ParallelFor(ctx, nBytes, par, func(_ context.Context, bStart, bEnd int) error {
			rowStart := bStart * 8
			rowEnd := min(bEnd*8, n)
			writeByteRange(bits, rowStart, rowEnd)
			return nil
		})
	})
}

// fastCompareInt64Lit compares each element against a scalar literal.
// Reads half the memory of the broadcast version because we don't have a
// second n-sized array to load. Parallelises on disjoint bitmap byte ranges.
func fastCompareInt64Lit(ctx context.Context, name string, av []int64, lit int64, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	n := len(av)
	writeByteRange := func(bits []byte, start, end int) {
		done := start
		if simdAvailable && hasSIMDInt64() && end-start >= 8 {
			simdEnd := start + ((end-start)/8)*8
			done = start + simdCompareInt64Lit(av[start:simdEnd], lit, bits[start/8:], op)
		}
		for row := done; row < end; row += 8 {
			chunk := min(row+8, end)
			var b byte
			switch op {
			case opEq:
				for j := row; j < chunk; j++ {
					if av[j] == lit {
						b |= 1 << (j - row)
					}
				}
			case opNe:
				for j := row; j < chunk; j++ {
					if av[j] != lit {
						b |= 1 << (j - row)
					}
				}
			case opLt:
				for j := row; j < chunk; j++ {
					if av[j] < lit {
						b |= 1 << (j - row)
					}
				}
			case opLe:
				for j := row; j < chunk; j++ {
					if av[j] <= lit {
						b |= 1 << (j - row)
					}
				}
			case opGt:
				for j := row; j < chunk; j++ {
					if av[j] > lit {
						b |= 1 << (j - row)
					}
				}
			case opGe:
				for j := row; j < chunk; j++ {
					if av[j] >= lit {
						b |= 1 << (j - row)
					}
				}
			}
			bits[row/8] = b
		}
	}

	return series.BuildBoolDirect(name, n, poolingMem(mem), func(bits []byte) {
		if n < 128*1024 {
			writeByteRange(bits, 0, n)
			return
		}
		nBytes := (n + 7) / 8
		_ = pool.ParallelFor(ctx, nBytes, par, func(_ context.Context, bStart, bEnd int) error {
			rowStart := bStart * 8
			rowEnd := min(bEnd*8, n)
			writeByteRange(bits, rowStart, rowEnd)
			return nil
		})
	})
}

// fastCompareFloat64Lit is the float64 counterpart of fastCompareInt64Lit.
func fastCompareFloat64Lit(ctx context.Context, name string, av []float64, lit float64, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	n := len(av)
	writeByteRange := func(bits []byte, start, end int) {
		done := start
		if simdAvailable && hasSIMDInt64() && end-start >= 8 {
			simdEnd := start + ((end-start)/8)*8
			done = start + simdCompareFloat64Lit(av[start:simdEnd], lit, bits[start/8:], op)
		}
		for row := done; row < end; row += 8 {
			chunk := min(row+8, end)
			var b byte
			switch op {
			case opEq:
				for j := row; j < chunk; j++ {
					if av[j] == lit {
						b |= 1 << (j - row)
					}
				}
			case opNe:
				for j := row; j < chunk; j++ {
					if av[j] != lit {
						b |= 1 << (j - row)
					}
				}
			case opLt:
				for j := row; j < chunk; j++ {
					if av[j] < lit {
						b |= 1 << (j - row)
					}
				}
			case opLe:
				for j := row; j < chunk; j++ {
					if av[j] <= lit {
						b |= 1 << (j - row)
					}
				}
			case opGt:
				for j := row; j < chunk; j++ {
					if av[j] > lit {
						b |= 1 << (j - row)
					}
				}
			case opGe:
				for j := row; j < chunk; j++ {
					if av[j] >= lit {
						b |= 1 << (j - row)
					}
				}
			}
			bits[row/8] = b
		}
	}

	return series.BuildBoolDirect(name, n, poolingMem(mem), func(bits []byte) {
		if n < 128*1024 {
			writeByteRange(bits, 0, n)
			return
		}
		nBytes := (n + 7) / 8
		_ = pool.ParallelFor(ctx, nBytes, par, func(_ context.Context, bStart, bEnd int) error {
			rowStart := bStart * 8
			rowEnd := min(bEnd*8, n)
			writeByteRange(bits, rowStart, rowEnd)
			return nil
		})
	})
}

// literalSeries produces a broadcast Series of a's dtype and length filled
// with lit. Used as a fallback for dtypes outside the fast int64/float64
// scalar paths.
func literalSeries(a *series.Series, lit any) (*series.Series, error) {
	n := a.Len()
	switch a.DType().ID() {
	case arrow.INT64:
		v, err := coerceToInt64(lit)
		if err != nil {
			return nil, err
		}
		out := make([]int64, n)
		for i := range out {
			out[i] = v
		}
		return series.FromInt64("_lit", out, nil)
	case arrow.FLOAT64:
		v, err := coerceToFloat64(lit)
		if err != nil {
			return nil, err
		}
		out := make([]float64, n)
		for i := range out {
			out[i] = v
		}
		return series.FromFloat64("_lit", out, nil)
	case arrow.FLOAT32:
		v, err := coerceToFloat64(lit)
		if err != nil {
			return nil, err
		}
		out := make([]float32, n)
		for i := range out {
			out[i] = float32(v)
		}
		return series.FromFloat32("_lit", out, nil)
	case arrow.STRING:
		v, ok := lit.(string)
		if !ok {
			return nil, fmt.Errorf("compute: cannot compare str with %T", lit)
		}
		out := make([]string, n)
		for i := range out {
			out[i] = v
		}
		return series.FromString("_lit", out, nil)
	case arrow.BOOL:
		v, ok := lit.(bool)
		if !ok {
			return nil, fmt.Errorf("compute: cannot compare bool with %T", lit)
		}
		out := make([]bool, n)
		for i := range out {
			out[i] = v
		}
		return series.FromBool("_lit", out, nil)
	}
	return nil, fmt.Errorf("compute: literal comparison not supported for %s", a.DType())
}

func coerceToInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	}
	return 0, fmt.Errorf("compute: cannot coerce %T to int64", v)
}

func coerceToFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int32:
		return float64(x), nil
	}
	return 0, fmt.Errorf("compute: cannot coerce %T to float64", v)
}

// fastCompareFloat64 is the float64 counterpart.
func fastCompareFloat64(ctx context.Context, name string, av, bv []float64, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	n := len(av)
	writeByteRange := func(bits []byte, start, end int) {
		done := start
		if simdAvailable && hasSIMDInt64() && end-start >= 8 {
			simdEnd := start + ((end-start)/8)*8
			done = start + simdCompareFloat64(av[start:simdEnd], bv[start:simdEnd], bits[start/8:], op)
		}
		for row := done; row < end; row += 8 {
			chunk := min(row+8, end)
			var b byte
			switch op {
			case opEq:
				for j := row; j < chunk; j++ {
					if av[j] == bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opNe:
				for j := row; j < chunk; j++ {
					if av[j] != bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opLt:
				for j := row; j < chunk; j++ {
					if av[j] < bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opLe:
				for j := row; j < chunk; j++ {
					if av[j] <= bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opGt:
				for j := row; j < chunk; j++ {
					if av[j] > bv[j] {
						b |= 1 << (j - row)
					}
				}
			case opGe:
				for j := row; j < chunk; j++ {
					if av[j] >= bv[j] {
						b |= 1 << (j - row)
					}
				}
			}
			bits[row/8] = b
		}
	}
	return series.BuildBoolDirect(name, n, poolingMem(mem), func(bits []byte) {
		if n < 128*1024 {
			writeByteRange(bits, 0, n)
			return
		}
		nBytes := (n + 7) / 8
		_ = pool.ParallelFor(ctx, nBytes, par, func(_ context.Context, bStart, bEnd int) error {
			rowStart := bStart * 8
			rowEnd := min(bEnd*8, n)
			writeByteRange(bits, rowStart, rowEnd)
			return nil
		})
	})
}

func compareOrdered[T Ordered](
	ctx context.Context,
	name string,
	aArr, bArr arrow.Array,
	aVals, bVals []T,
	op compareOp,
	mem memory.Allocator,
	par int,
) (*series.Series, error) {
	fn := orderedPredicate[T](op)
	if fn == nil {
		return nil, ErrUnsupportedDType
	}
	n := aArr.Len()
	// Build the output validity directly from the two input bitmaps;
	// skip the []bool intermediate that fromBoolResult -> FromBool
	// would pack back into a bitmap.
	nullBuf, nulls := series.AndValidityBitmap(aArr, bArr, mem)
	return series.BuildBoolDirectWithValidity(name, n, mem, func(bits []byte) {
		// Parallel writes are safe only when each goroutine owns a
		// disjoint byte range - otherwise two goroutines sharing a
		// bitmap byte would race. Round the chunk size up to a
		// multiple of 8 rows so every worker starts on a byte
		// boundary.
		if par <= 1 || n < 64*1024 {
			compareFillBits(bits, aVals, bVals, fn, 0, n)
			return
		}
		chunk := (n + par - 1) / par
		if chunk&7 != 0 {
			chunk = (chunk + 7) & ^7
		}
		_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, _, _ int) error {
			// ParallelFor partitions n by its own scheme which isn't
			// byte-aligned. We ignore its s/e and re-partition below
			// using chunk boundaries that are multiples of 8.
			return nil
		})
		// Actual work: each goroutine handles [gStart, gEnd).
		var wg sync.WaitGroup
		for gStart := 0; gStart < n; gStart += chunk {
			gEnd := min(gStart+chunk, n)
			wg.Add(1)
			go func(s, e int) {
				defer wg.Done()
				compareFillBits(bits, aVals, bVals, fn, s, e)
			}(gStart, gEnd)
		}
		wg.Wait()
	}, nullBuf, nulls)
}

// compareFillBits writes the comparison result for rows [s, e) into
// bits. Caller guarantees s is a multiple of 8 (or the range is the
// whole array) so bit-ORs don't conflict across goroutines.
func compareFillBits[T Ordered](bits []byte, aVals, bVals []T, fn func(T, T) bool, s, e int) {
	// Process full bytes first for an 8-row unrolled inner loop.
	i := s
	for ; i+8 <= e; i += 8 {
		var b byte
		if fn(aVals[i+0], bVals[i+0]) {
			b |= 1 << 0
		}
		if fn(aVals[i+1], bVals[i+1]) {
			b |= 1 << 1
		}
		if fn(aVals[i+2], bVals[i+2]) {
			b |= 1 << 2
		}
		if fn(aVals[i+3], bVals[i+3]) {
			b |= 1 << 3
		}
		if fn(aVals[i+4], bVals[i+4]) {
			b |= 1 << 4
		}
		if fn(aVals[i+5], bVals[i+5]) {
			b |= 1 << 5
		}
		if fn(aVals[i+6], bVals[i+6]) {
			b |= 1 << 6
		}
		if fn(aVals[i+7], bVals[i+7]) {
			b |= 1 << 7
		}
		// Whole-byte write; safe to assign directly because (s%8==0) and i
		// steps by 8, so no other goroutine touches this byte.
		bits[i>>3] = b
	}
	for ; i < e; i++ {
		if fn(aVals[i], bVals[i]) {
			bits[i>>3] |= 1 << uint(i&7)
		}
	}
}

func orderedPredicate[T Ordered](op compareOp) func(T, T) bool {
	var zero T
	switch any(zero).(type) {
	case float32, float64:
		return floatTotalPredicate[T](op)
	}
	switch op {
	case opEq:
		return func(x, y T) bool { return x == y }
	case opNe:
		return func(x, y T) bool { return x != y }
	case opLt:
		return func(x, y T) bool { return x < y }
	case opLe:
		return func(x, y T) bool { return x <= y }
	case opGt:
		return func(x, y T) bool { return x > y }
	case opGe:
		return func(x, y T) bool { return x >= y }
	}
	return nil
}

// compareStrings uses the typed string accessor. Strings don't fit the
// Ordered numeric generic because the accessor is a method call per row, not
// a backing []string slice.
func compareStrings(ctx context.Context, name string, aArr, bArr arrow.Array, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	aStr := aArr.(*array.String)
	bStr := bArr.(*array.String)

	n := aArr.Len()
	out := make([]bool, n)
	valid := buildValidityAnd(aArr, bArr)
	pred := stringPredicate(op)
	if pred == nil {
		return nil, ErrUnsupportedDType
	}
	err := pool.ParallelFor(ctx, n, par, func(ctx context.Context, s, e int) error {
		if valid == nil {
			for i := s; i < e; i++ {
				out[i] = pred(aStr.Value(i), bStr.Value(i))
			}
			return nil
		}
		for i := s; i < e; i++ {
			if valid[i] {
				out[i] = pred(aStr.Value(i), bStr.Value(i))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(name, out, valid, mem)
}

func stringPredicate(op compareOp) func(string, string) bool {
	switch op {
	case opEq:
		return func(x, y string) bool { return x == y }
	case opNe:
		return func(x, y string) bool { return x != y }
	case opLt:
		return func(x, y string) bool { return x < y }
	case opLe:
		return func(x, y string) bool { return x <= y }
	case opGt:
		return func(x, y string) bool { return x > y }
	case opGe:
		return func(x, y string) bool { return x >= y }
	}
	return nil
}

func compareBool(ctx context.Context, name string, aArr, bArr arrow.Array, op compareOp, mem memory.Allocator, par int) (*series.Series, error) {
	if op != opEq && op != opNe {
		return nil, ErrUnsupportedDType
	}
	aBool := aArr.(*array.Boolean)
	bBool := bArr.(*array.Boolean)

	n := aArr.Len()
	out := make([]bool, n)
	valid := buildValidityAnd(aArr, bArr)
	eq := op == opEq
	err := pool.ParallelFor(ctx, n, par, func(ctx context.Context, s, e int) error {
		if valid == nil {
			for i := s; i < e; i++ {
				result := aBool.Value(i) == bBool.Value(i)
				if !eq {
					result = !result
				}
				out[i] = result
			}
			return nil
		}
		for i := s; i < e; i++ {
			if valid[i] {
				result := aBool.Value(i) == bBool.Value(i)
				if !eq {
					result = !result
				}
				out[i] = result
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fromBoolResult(name, out, valid, mem)
}

// floatTotalPredicate compares floats with the polars total order: NaN
// equals NaN and is greater than every number. x != x is the NaN test.
func floatTotalPredicate[T Ordered](op compareOp) func(T, T) bool {
	switch op {
	case opEq:
		return func(x, y T) bool { return x == y || (x != x && y != y) }
	case opNe:
		return func(x, y T) bool { return x != y && (x == x || y == y) }
	case opLt:
		return func(x, y T) bool { return x < y || (y != y && x == x) }
	case opLe:
		return func(x, y T) bool { return x <= y || y != y }
	case opGt:
		return func(x, y T) bool { return x > y || (x != x && y == y) }
	case opGe:
		return func(x, y T) bool { return x >= y || x != x }
	}
	return nil
}

// hasNaNFloat64 reports whether vals holds a NaN. It reuses the SIMD
// max kernel's NaN flag where available.
func hasNaNFloat64(vals []float64) bool {
	if len(vals) == 0 {
		return false
	}
	if simdAvailable && hasSIMDInt64() {
		_, nan := simdMaxFloat64(vals)
		return nan
	}
	for _, v := range vals {
		if v != v {
			return true
		}
	}
	return false
}

func complementOp(op compareOp) compareOp {
	switch op {
	case opGt:
		return opLe
	case opGe:
		return opLt
	case opLt:
		return opGe
	case opLe:
		return opGt
	case opEq:
		return opNe
	}
	return opEq
}

// invertCompare flips every value bit of a freshly built, null-free
// boolean result in place.
func invertCompare(s *series.Series, err error) (*series.Series, error) {
	if err != nil {
		return nil, err
	}
	arr := s.Chunk(0)
	buf := arr.Data().Buffers()[1]
	if buf == nil {
		return s, nil
	}
	n := arr.Len()
	bits := buf.Bytes()
	off := arr.Data().Offset()
	end := off + n
	i := off
	for ; i < end && i%8 != 0; i++ {
		bits[i/8] ^= 1 << (i % 8)
	}
	if wholeEnd := end / 8 * 8; i < wholeEnd {
		whole := bits[i/8 : wholeEnd/8]
		w := 0
		for ; w+8 <= len(whole); w += 8 {
			binary.LittleEndian.PutUint64(whole[w:], ^binary.LittleEndian.Uint64(whole[w:]))
		}
		for ; w < len(whole); w++ {
			whole[w] = ^whole[w]
		}
		i = wholeEnd
	}
	for ; i < end; i++ {
		bits[i/8] ^= 1 << (i % 8)
	}
	return s, nil
}
