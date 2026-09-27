package compute

import (
	"context"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// reductionWorkers caps the worker count used by parallel reduction
// kernels. On hyperthreaded x86 the second SMT sibling thrashes the
// same L1/L2 without adding throughput on a memory-bound VPADDQ/VADDPD
// loop - measured at ~12% faster to use physical cores only. On Apple
// Silicon (M-series p-cores) hyperthreading doesn't exist so this
// just acts as a generic upper bound.
const reductionMaxWorkers = 8

func cappedReductionWorkers(par int) int {
	if par <= 0 {
		par = pool.DefaultParallelism()
	}
	if par > reductionMaxWorkers {
		par = reductionMaxWorkers
	}
	if par < 1 {
		par = 1
	}
	return par
}

// Count returns the number of non-null values in s.
func Count(s *series.Series) int { return s.Len() - s.NullCount() }

// NullCount returns the number of null values in s.
func NullCount(s *series.Series) int { return s.NullCount() }

// SumInt64 returns the sum of non-null values. Overflows wrap per Go int64
// semantics, matching polars-core behavior for integer sums. Only supports
// integer dtypes; use SumFloat64 for floats.
func SumInt64(ctx context.Context, s *series.Series, opts ...Option) (int64, error) {
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return 0, err
	}
	defer arr.Release()

	par := inferParallelism(cfg, s.Len())
	n := arr.Len()
	// SIMD fast path for no-null int64. Below sumSerialCutoff one core
	// runs the SIMD kernel; above it the sum is memory-bandwidth-bound
	// and parallel partial sums saturate more of the memory system.
	if simdAvailable && s.DType().ID() == arrow.INT64 && arr.NullN() == 0 && hasSIMDInt64() {
		vals := int64Values(arr)
		if n < sumSerialCutoff {
			return simdSumInt64(vals), nil
		}
		// Parallel SIMD: each worker SIMD-reduces a partition.
		return parallelSIMDSumInt64(ctx, vals, par)
	}
	switch s.DType().ID() {
	case arrow.INT32:
		v, err := sumIntegerLike(ctx, arr, int32Values(arr), par, n)
		return int64(v), err
	case arrow.INT64:
		return sumIntegerLike(ctx, arr, int64Values(arr), par, n)
	case arrow.UINT32:
		v, err := sumIntegerLike(ctx, arr, uint32Values(arr), par, n)
		return int64(v), err
	case arrow.UINT64:
		v, err := sumIntegerLike(ctx, arr, uint64Values(arr), par, n)
		return int64(v), err
	}
	return 0, isUnsupported("SumInt64", s.DType())
}

// SumFloat64 returns the sum of non-null values for float dtypes.
func SumFloat64(ctx context.Context, s *series.Series, opts ...Option) (float64, error) {
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return 0, err
	}
	defer arr.Release()

	par := inferParallelism(cfg, s.Len())
	n := arr.Len()
	if simdAvailable && s.DType().ID() == arrow.FLOAT64 && arr.NullN() == 0 && hasSIMDInt64() {
		vals := float64Values(arr)
		if n < sumSerialCutoff {
			return simdSumFloat64(vals), nil
		}
		// Parallel SIMD: each worker runs simdSumFloat64 on its partition.
		// Measured >1.5× faster than the scalar parallel path at 1M.
		return parallelSIMDSumFloat64(ctx, vals, par)
	}
	switch s.DType().ID() {
	case arrow.FLOAT32:
		v, err := sumFloatLike(ctx, arr, float32Values(arr), par, n)
		return float64(v), err
	case arrow.FLOAT64:
		return sumFloatLike(ctx, arr, float64Values(arr), par, n)
	case arrow.INT32, arrow.INT64, arrow.UINT32, arrow.UINT64:
		v, err := SumInt64(ctx, s, opts...)
		return float64(v), err
	}
	return 0, isUnsupported("SumFloat64", s.DType())
}

// parallelSIMDSumInt64 reduces vals using k workers each running the SIMD
// SumInt64 kernel on its partition.
//
// Hot path picks the hand-rolled AVX2 asm kernel with software prefetch
// when available. Measured 2x faster than the archsimd path at 1M on
// L3-resident data because the explicit PREFETCHT0 hides the 30+ cycle
// L3 hit latency that archsimd's loads suffer serially.
func parallelSIMDSumInt64(ctx context.Context, vals []int64, par int) (int64, error) {
	n := len(vals)
	par = max(min(cappedReductionWorkers(par), n), 1)
	kernel := simdSumInt64
	if hasAVX2Prefetch {
		kernel = simdSumInt64AVX2Prefetch
	}
	parts := make([]int64, par)
	chunk := (n + par - 1) / par
	err := pool.ParallelFor(ctx, par, par, func(_ context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := min(s+chunk, n)
			if s >= e {
				continue
			}
			parts[w] = kernel(vals[s:e])
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, p := range parts {
		total += p
	}
	return total, nil
}

// parallelSIMDSumFloat64 reduces vals using k workers each running the
// SIMD SumFloat64 kernel on its partition. Partials are summed at the end.
func parallelSIMDSumFloat64(ctx context.Context, vals []float64, par int) (float64, error) {
	n := len(vals)
	par = max(min(cappedReductionWorkers(par), n), 1)
	kernel := simdSumFloat64
	if hasAVX2Prefetch {
		kernel = simdSumFloat64AVX2Prefetch
	}
	parts := make([]float64, par)
	chunk := (n + par - 1) / par
	err := pool.ParallelFor(ctx, par, par, func(_ context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := min(s+chunk, n)
			if s >= e {
				continue
			}
			parts[w] = kernel(vals[s:e])
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	var total float64
	for _, p := range parts {
		total += p
	}
	return total, nil
}

func sumIntegerLike[T int32 | int64 | uint32 | uint64](
	ctx context.Context,
	arr arrow.Array,
	vals []T,
	par, n int,
) (T, error) {
	var total T
	if arr.NullN() == 0 {
		parts, err := partialSumsNoNull(ctx, vals, par, n)
		if err != nil {
			return 0, err
		}
		for _, p := range parts {
			total += p
		}
		return total, nil
	}
	parts, err := partialSumsWithNulls(ctx, arr, vals, par, n)
	if err != nil {
		return 0, err
	}
	for _, p := range parts {
		total += p
	}
	return total, nil
}

func sumFloatLike[T float32 | float64](
	ctx context.Context,
	arr arrow.Array,
	vals []T,
	par, n int,
) (T, error) {
	var total T
	if arr.NullN() == 0 {
		parts, err := partialSumsNoNull(ctx, vals, par, n)
		if err != nil {
			return 0, err
		}
		for _, p := range parts {
			total += p
		}
		return total, nil
	}
	parts, err := partialSumsWithNulls(ctx, arr, vals, par, n)
	if err != nil {
		return 0, err
	}
	for _, p := range parts {
		total += p
	}
	return total, nil
}

func partialSumsNoNull[T int32 | int64 | uint32 | uint64 | float32 | float64](
	ctx context.Context, vals []T, par, n int,
) ([]T, error) {
	par = max(min(cappedReductionWorkers(par), n), 1)
	if n < minParallelRows {
		par = 1
	}

	parts := make([]T, par)
	chunk := (n + par - 1) / par
	err := pool.ParallelFor(ctx, par, par, func(ctx context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := s + chunk
			e = min(e, n)
			var local T
			for i := s; i < e; i++ {
				local += vals[i]
			}
			parts[w] = local
		}
		return nil
	})
	return parts, err
}

func partialSumsWithNulls[T int32 | int64 | uint32 | uint64 | float32 | float64](
	ctx context.Context, arr arrow.Array, vals []T, par, n int,
) ([]T, error) {
	par = max(min(cappedReductionWorkers(par), n), 1)
	if n < minParallelRows {
		par = 1
	}

	parts := make([]T, par)
	chunk := (n + par - 1) / par
	bits := arr.NullBitmapBytes()
	off := arr.Data().Offset()
	err := pool.ParallelFor(ctx, par, par, func(ctx context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := min(s+chunk, n)
			if s < e {
				parts[w] = maskedSum(vals, bits, off, s, e)
			}
		}
		return nil
	})
	return parts, err
}

// maskedSum adds vals[i] for every i in [s, e) whose validity bit
// (bit i+off of bits) is set, in index order. It reads the bitmap a
// byte at a time so all-valid and all-null bytes skip the per-row
// test, instead of calling arrow's IsValid through an interface for
// every row.
func maskedSum[T int32 | int64 | uint32 | uint64 | float32 | float64](vals []T, bits []byte, off, s, e int) T {
	var total T
	i := s
	for ; i < e && (i+off)&7 != 0; i++ {
		if bits[(i+off)>>3]&(1<<((i+off)&7)) != 0 {
			total += vals[i]
		}
	}
	for ; i+8 <= e; i += 8 {
		v := vals[i : i+8 : i+8]
		switch b := bits[(i+off)>>3]; b {
		case 0xFF:
			total += v[0]
			total += v[1]
			total += v[2]
			total += v[3]
			total += v[4]
			total += v[5]
			total += v[6]
			total += v[7]
		case 0:
		default:
			for j := range 8 {
				if b&(1<<j) != 0 {
					total += v[j]
				}
			}
		}
	}
	for ; i < e; i++ {
		if bits[(i+off)>>3]&(1<<((i+off)&7)) != 0 {
			total += vals[i]
		}
	}
	return total
}

// MeanFloat64 returns the arithmetic mean of non-null values. Returns
// (NaN, false, nil) when the Series is empty or fully null.
func MeanFloat64(ctx context.Context, s *series.Series, opts ...Option) (float64, bool, error) {
	c := Count(s)
	if c == 0 {
		return math.NaN(), false, nil
	}
	if dt := s.DType(); dt.IsInteger() || dt.IsBool() {
		// polars averages integers in f64: summing in i64 first would
		// wrap for large values.
		sum, err := sumIntsAsFloat(s)
		if err != nil {
			return 0, false, err
		}
		return sum / float64(c), true, nil
	}
	sum, err := SumFloat64(ctx, s, opts...)
	if err != nil {
		return 0, false, err
	}
	return sum / float64(c), true, nil
}

func sumIntsAsFloat(s *series.Series) (float64, error) {
	var total float64
	for _, c := range s.Chunks() {
		switch a := c.(type) {
		case *array.Int8:
			total += sumValsFloat(a, a.Int8Values())
		case *array.Int16:
			total += sumValsFloat(a, a.Int16Values())
		case *array.Int32:
			total += sumValsFloat(a, a.Int32Values())
		case *array.Int64:
			total += sumValsFloat(a, a.Int64Values())
		case *array.Uint8:
			total += sumValsFloat(a, a.Uint8Values())
		case *array.Uint16:
			total += sumValsFloat(a, a.Uint16Values())
		case *array.Uint32:
			total += sumValsFloat(a, a.Uint32Values())
		case *array.Uint64:
			total += sumValsFloat(a, a.Uint64Values())
		case *array.Boolean:
			for i := range a.Len() {
				if a.IsValid(i) && a.Value(i) {
					total++
				}
			}
		default:
			return 0, isUnsupported("MeanFloat64", s.DType())
		}
	}
	return total, nil
}

func sumValsFloat[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](a arrow.Array, vals []T) float64 {
	var total float64
	if a.NullN() == 0 {
		for _, v := range vals {
			total += float64(v)
		}
		return total
	}
	for i, v := range vals {
		if a.IsValid(i) {
			total += float64(v)
		}
	}
	return total
}

// MinInt64 returns the minimum non-null integer value as int64. The bool
// return is false when the Series is empty or fully null.
func MinInt64(ctx context.Context, s *series.Series, opts ...Option) (int64, bool, error) {
	return minMaxInt(ctx, s, opts, false)
}

// MaxInt64 returns the maximum non-null integer value as int64.
func MaxInt64(ctx context.Context, s *series.Series, opts ...Option) (int64, bool, error) {
	return minMaxInt(ctx, s, opts, true)
}

func minMaxInt(ctx context.Context, s *series.Series, opts []Option, isMax bool) (int64, bool, error) {
	if Count(s) == 0 {
		return 0, false, nil
	}
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return 0, false, err
	}
	defer arr.Release()

	par := inferParallelism(cfg, s.Len())
	n := arr.Len()
	switch s.DType().ID() {
	case arrow.INT32:
		v, ok, err := reduceIntChunks(ctx, arr, int32Values(arr), par, n, isMax)
		return int64(v), ok, err
	case arrow.INT64:
		return reduceIntChunks(ctx, arr, int64Values(arr), par, n, isMax)
	}
	return 0, false, isUnsupported("MinInt64/MaxInt64", s.DType())
}

// MinFloat64, MaxFloat64 are the float analogues. Like polars, NaN is
// ignored unless every non-null value is NaN, in which case the result
// is NaN.
func MinFloat64(ctx context.Context, s *series.Series, opts ...Option) (float64, bool, error) {
	return minMaxFloat(ctx, s, opts, false)
}

func MaxFloat64(ctx context.Context, s *series.Series, opts ...Option) (float64, bool, error) {
	return minMaxFloat(ctx, s, opts, true)
}

func minMaxFloat(ctx context.Context, s *series.Series, opts []Option, isMax bool) (float64, bool, error) {
	if Count(s) == 0 {
		return 0, false, nil
	}
	cfg := resolve(opts)
	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return 0, false, err
	}
	defer arr.Release()

	par := inferParallelism(cfg, s.Len())
	n := arr.Len()
	switch s.DType().ID() {
	case arrow.FLOAT32:
		v, ok, err := reduceFloatChunks(ctx, arr, float32Values(arr), par, n, isMax)
		return float64(v), ok, err
	case arrow.FLOAT64:
		// SIMD path: no-null inputs take the AVX2/AVX-512 MINPD/MAXPD
		// reduction per worker. Worker-local NaN scan runs in a prepass
		// so the hot path is pure SIMD.
		if simdAvailable && hasSIMDInt64() && arr.NullN() == 0 {
			if n < minMaxSerialCutoff {
				par = 1
			}
			return reduceFloat64SIMD(ctx, float64Values(arr), par, n, isMax)
		}
		return reduceFloatChunks(ctx, arr, float64Values(arr), par, n, isMax)
	case arrow.INT32, arrow.INT64:
		v, ok, err := minMaxInt(ctx, s, opts, isMax)
		return float64(v), ok, err
	}
	return 0, false, isUnsupported("MinFloat64/MaxFloat64", s.DType())
}

func reduceIntChunks[T int32 | int64](
	ctx context.Context, arr arrow.Array, vals []T, par, n int, isMax bool,
) (T, bool, error) {
	par = max(min(cappedReductionWorkers(par), n), 1)
	if n < minMaxSerialCutoff {
		par = 1
	}

	type result struct {
		val T
		ok  bool
	}
	parts := make([]result, par)
	chunk := (n + par - 1) / par
	noNulls := arr.NullN() == 0
	i64, isInt64 := any(vals).([]int64)

	err := pool.ParallelFor(ctx, par, par, func(ctx context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := s + chunk
			e = min(e, n)
			if s >= e {
				continue
			}
			var r result
			if noNulls && isInt64 {
				r.val = T(minMaxInt64Kernel(i64[s:e], isMax))
				r.ok = true
				parts[w] = r
				continue
			}
			if noNulls {
				r.val = vals[s]
				r.ok = true
				if isMax {
					for i := s + 1; i < e; i++ {
						if vals[i] > r.val {
							r.val = vals[i]
						}
					}
				} else {
					for i := s + 1; i < e; i++ {
						if vals[i] < r.val {
							r.val = vals[i]
						}
					}
				}
				parts[w] = r
				continue
			}
			for i := s; i < e; i++ {
				if !arr.IsValid(i) {
					continue
				}
				v := vals[i]
				if !r.ok {
					r.val = v
					r.ok = true
					continue
				}
				if isMax {
					if v > r.val {
						r.val = v
					}
				} else {
					if v < r.val {
						r.val = v
					}
				}
			}
			parts[w] = r
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	var final result
	for _, p := range parts {
		if !p.ok {
			continue
		}
		if !final.ok {
			final = p
			continue
		}
		if isMax {
			if p.val > final.val {
				final.val = p.val
			}
		} else {
			if p.val < final.val {
				final.val = p.val
			}
		}
	}
	return final.val, final.ok, nil
}

// reduceFloat64SIMD is the no-null float64 min/max kernel. Each worker
// reduces with MINPD/MAXPD and falls back to a scalar pass only when its
// partition holds a NaN. NaN semantics match polars: NaN is ignored
// unless every value is NaN.
func reduceFloat64SIMD(ctx context.Context, vals []float64, par, n int, isMax bool) (float64, bool, error) {
	par = max(min(cappedReductionWorkers(par), n), 1)

	type result struct {
		val    float64
		ok     bool
		anyNaN bool
	}
	parts := make([]result, par)
	chunk := (n + par - 1) / par
	err := pool.ParallelFor(ctx, par, par, func(_ context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := s + chunk
			e = min(e, n)
			if s >= e {
				continue
			}
			slice := vals[s:e]
			// SIMD reduction fused with NaN detection. If NaN appears we
			// must re-scan this partition for correct semantics, since
			// MINPD/MAXPD don't propagate NaN consistently.
			var best float64
			var anyNaN bool
			if isMax {
				best, anyNaN = simdMaxFloat64(slice)
			} else {
				best, anyNaN = simdMinFloat64(slice)
			}
			if !anyNaN {
				parts[w] = result{val: best, ok: true}
				continue
			}
			// Fall back to scalar NaN-aware loop for this partition.
			best = slice[0]
			bestIsNaN := best != best
			anyNaN = bestIsNaN
			if isMax {
				for _, v := range slice[1:] {
					if v > best {
						best = v
					} else if v != v {
						anyNaN = true
					} else if bestIsNaN {
						best = v
						bestIsNaN = false
					}
				}
			} else {
				for _, v := range slice[1:] {
					if v < best {
						best = v
					} else if v != v {
						anyNaN = true
					} else if bestIsNaN {
						best = v
						bestIsNaN = false
					}
				}
			}
			if !bestIsNaN {
				parts[w] = result{val: best, ok: true, anyNaN: anyNaN}
			} else {
				parts[w] = result{val: math.NaN(), ok: true, anyNaN: true}
			}
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	var final result
	for _, p := range parts {
		if !p.ok {
			continue
		}
		if !final.ok {
			final = p
			continue
		}
		if isMax {
			if greater(p.val, final.val) {
				final.val = p.val
			}
		} else {
			if less(p.val, final.val) {
				final.val = p.val
			}
		}
	}
	return final.val, final.ok, nil
}

func reduceFloatChunks[T float32 | float64](
	ctx context.Context, arr arrow.Array, vals []T, par, n int, isMax bool,
) (T, bool, error) {
	if par <= 0 {
		par = pool.DefaultParallelism()
	}
	if par > n {
		par = n
	}
	if par < 1 {
		par = 1
	}
	if n < minParallelRows {
		par = 1
	}

	type result struct {
		val T
		ok  bool
	}
	parts := make([]result, par)
	chunk := (n + par - 1) / par
	noNulls := arr.NullN() == 0

	// NaN is ignored unless every value is NaN (polars min/max).
	err := pool.ParallelFor(ctx, par, par, func(ctx context.Context, start, end int) error {
		for w := start; w < end; w++ {
			s := w * chunk
			e := s + chunk
			e = min(e, n)
			if s >= e {
				continue
			}
			var r result
			if noNulls {
				// Single-pass tight loop. `v < best` is false for NaN, so NaN
				// is skipped once best is a number; a leading NaN is replaced
				// by the first number. The result is NaN only when every
				// value is NaN, matching polars.
				chunk := vals[s:e]
				best := chunk[0]
				bestIsNaN := best != best
				if isMax {
					for _, v := range chunk[1:] {
						if v > best {
							best = v
						} else if bestIsNaN && v == v {
							best = v
							bestIsNaN = false
						}
					}
				} else {
					for _, v := range chunk[1:] {
						if v < best {
							best = v
						} else if bestIsNaN && v == v {
							best = v
							bestIsNaN = false
						}
					}
				}
				r.val = best
				r.ok = true
				parts[w] = r
				continue
			}
			for i := s; i < e; i++ {
				if !arr.IsValid(i) {
					continue
				}
				v := vals[i]
				if !r.ok {
					r.val = v
					r.ok = true
					continue
				}
				if isMax {
					if greater(v, r.val) {
						r.val = v
					}
				} else {
					if less(v, r.val) {
						r.val = v
					}
				}
			}
			parts[w] = r
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	var final result
	for _, p := range parts {
		if !p.ok {
			continue
		}
		if !final.ok {
			final = p
			continue
		}
		if isMax {
			if greater(p.val, final.val) {
				final.val = p.val
			}
		} else {
			if less(p.val, final.val) {
				final.val = p.val
			}
		}
	}
	return final.val, final.ok, nil
}

// greater and less order floats for min/max with NaN ignored: NaN never
// beats a number, and any number beats NaN.
func greater[T float32 | float64](a, b T) bool {
	if isNaN(a) {
		return false
	}
	if isNaN(b) {
		return true
	}
	return a > b
}

func less[T float32 | float64](a, b T) bool {
	if isNaN(a) {
		return false
	}
	if isNaN(b) {
		return true
	}
	return a < b
}

func isNaN[T float32 | float64](v T) bool { return v != v }
