package dataframe

import (
	"context"
	"fmt"

	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// NullStrategy controls how horizontal aggregates treat nulls across
// the participating columns. Ignore skips nulls (polars default);
// Propagate returns null whenever any input at that row is null.
type NullStrategy int

const (
	// IgnoreNulls skips null values during the reduction.
	IgnoreNulls NullStrategy = iota
	// PropagateNulls emits a null row when any input is null.
	PropagateNulls
)

// SumHorizontal returns a Series whose i-th value is the sum of row i
// across the specified columns (all numeric columns when cols is
// empty). Null handling follows strategy.
//
// Fast path: when every input is the same numeric dtype with no
// nulls, the reduction uses compute.Add pairwise (SIMD where
// available) and the result preserves the input dtype, matching
// polars' behaviour. Mixed dtypes or nulls fall back to a float64
// scalar loop.
func (df *DataFrame) SumHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	if fast, ok, err := df.sumHorizontalFast(ctx, strategy, cols); ok || err != nil {
		return fast, err
	}
	return df.reduceHorizontal(ctx, "sum", strategy, cols, horizSum)
}

// sumHorizontalFast handles the hot case: same-dtype, no-null inputs,
// IgnoreNulls strategy. Returns (result, ok=true, err) when taken.
// When ok=false the caller uses the generic path.
func (df *DataFrame) sumHorizontalFast(ctx context.Context, strategy NullStrategy, cols []string) (*series.Series, bool, error) {
	if strategy != IgnoreNulls {
		return nil, false, nil
	}
	selected, err := df.selectHorizontal(cols, dtype.DType.IsNumeric)
	if err != nil {
		return nil, true, err
	}
	if len(selected) == 0 {
		releaseAll(selected)
		return nil, false, nil
	}
	firstID := selected[0].DType().ID()
	allSame := true
	noNulls := true
	for _, c := range selected {
		if c.DType().ID() != firstID {
			allSame = false
			break
		}
		if c.NullCount() > 0 {
			noNulls = false
			break
		}
	}
	if !allSame || !noNulls {
		releaseAll(selected)
		return nil, false, nil
	}
	// Fuse into one pass over K streams: seed with the first column,
	// accumulate the rest. Pairwise compute.Add would cost K-1 full
	// passes plus K-1 temp buffers and validity recomputes.
	n := df.height
	switch firstID {
	case arrow.INT64:
		slices := make([][]int64, len(selected))
		for i, c := range selected {
			slices[i] = c.Chunk(0).(*array.Int64).Int64Values()
		}
		out, err := series.BuildInt64Direct("sum", n, memory.DefaultAllocator, func(buf []int64) {
			rowSumInt64(buf, slices)
		})
		releaseAll(selected)
		return out, true, err
	case arrow.FLOAT64:
		slices := make([][]float64, len(selected))
		for i, c := range selected {
			slices[i] = c.Chunk(0).(*array.Float64).Float64Values()
		}
		out, err := series.BuildFloat64Direct("sum", n, memory.DefaultAllocator, func(buf []float64) {
			rowSumFloat64(buf, slices)
		})
		releaseAll(selected)
		return out, true, err
	}
	releaseAll(selected)
	return nil, false, nil
}

// rowSumInt64 writes buf[i] = sum(slices[*][i]). Single fused pass:
// K+1 streams vs 2(K-1) for pairwise adds, no temps, no validity
// work (callers guarantee no nulls). Parallel caps mirror
// rowReduceInt64.
func rowSumInt64(buf []int64, slices [][]int64) {
	n := len(buf)
	const parallelCutoff = 128 * 1024
	if n < parallelCutoff {
		rowSumInt64Serial(buf, slices, 0, n)
		return
	}
	maxWorkers := 8
	if n <= 256*1024 {
		maxWorkers = 4
	}
	workers := min(runtime.GOMAXPROCS(0), maxWorkers)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		start := w * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rowSumInt64Serial(buf, slices, s, e)
		}(start, end)
	}
	wg.Wait()
}

func rowSumInt64Serial(buf []int64, slices [][]int64, start, end int) {
	copy(buf[start:end], slices[0][start:end])
	for _, s := range slices[1:] {
		for i := start; i < end; i++ {
			buf[i] += s[i]
		}
	}
}

// rowSumFloat64 is the float64 counterpart.
func rowSumFloat64(buf []float64, slices [][]float64) {
	n := len(buf)
	const parallelCutoff = 128 * 1024
	if n < parallelCutoff {
		rowSumFloat64Serial(buf, slices, 0, n)
		return
	}
	maxWorkers := 8
	if n <= 256*1024 {
		maxWorkers = 4
	}
	workers := min(runtime.GOMAXPROCS(0), maxWorkers)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		start := w * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rowSumFloat64Serial(buf, slices, s, e)
		}(start, end)
	}
	wg.Wait()
}

func rowSumFloat64Serial(buf []float64, slices [][]float64, start, end int) {
	copy(buf[start:end], slices[0][start:end])
	for _, s := range slices[1:] {
		for i := start; i < end; i++ {
			buf[i] += s[i]
		}
	}
}

// MeanHorizontal returns a Float64 Series with row-wise mean. Denominator
// per row is the number of non-null participants when strategy is
// IgnoreNulls; with PropagateNulls any null yields a null output.
func (df *DataFrame) MeanHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	return df.reduceHorizontal(ctx, "mean", strategy, cols, horizMean)
}

// MinHorizontal returns a Float64 Series with row-wise min. The output
// is null at row i when every participant at row i is null (IgnoreNulls)
// or when any participant is null (PropagateNulls).
func (df *DataFrame) MinHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	if fast, ok, err := df.minMaxHorizontalFast(ctx, strategy, cols, "min", false); ok || err != nil {
		return fast, err
	}
	return df.reduceHorizontal(ctx, "min", strategy, cols, horizMin)
}

// MaxHorizontal is the row-wise Max analogue of MinHorizontal.
func (df *DataFrame) MaxHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	if fast, ok, err := df.minMaxHorizontalFast(ctx, strategy, cols, "max", true); ok || err != nil {
		return fast, err
	}
	return df.reduceHorizontal(ctx, "max", strategy, cols, horizMax)
}

// minMaxHorizontalFast handles the hot same-dtype no-null case for
// row-wise min/max. Instead of routing through reduceHorizontal (which
// casts every column to float64 and allocates per-column [][]bool
// validity), it keeps the native dtype and writes the result directly
// into a pooled arrow buffer via BuildInt64Direct/BuildFloat64Direct.
// Polars returns the native dtype too, so this preserves semantics.
func (df *DataFrame) minMaxHorizontalFast(ctx context.Context, strategy NullStrategy, cols []string, outName string, isMax bool) (*series.Series, bool, error) {
	_ = ctx
	if strategy != IgnoreNulls {
		return nil, false, nil
	}
	selected, err := df.selectHorizontal(cols, dtype.DType.IsNumeric)
	if err != nil {
		return nil, true, err
	}
	if len(selected) == 0 {
		releaseAll(selected)
		return nil, false, nil
	}
	defer releaseAll(selected)
	firstID := selected[0].DType().ID()
	for _, c := range selected {
		if c.DType().ID() != firstID {
			return nil, false, nil
		}
		if c.NullCount() > 0 {
			return nil, false, nil
		}
	}
	n := df.height
	switch firstID {
	case arrow.INT64:
		slices := make([][]int64, len(selected))
		for i, c := range selected {
			slices[i] = c.Chunk(0).(*array.Int64).Int64Values()
		}
		out, err := series.BuildInt64Direct(outName, n, memory.DefaultAllocator, func(buf []int64) {
			rowReduceInt64(buf, slices, isMax)
		})
		return out, true, err
	case arrow.FLOAT64:
		slices := make([][]float64, len(selected))
		for i, c := range selected {
			slices[i] = c.Chunk(0).(*array.Float64).Float64Values()
		}
		out, err := series.BuildFloat64Direct(outName, n, memory.DefaultAllocator, func(buf []float64) {
			rowReduceFloat64(buf, slices, isMax)
		})
		return out, true, err
	}
	return nil, false, nil
}

// rowReduceInt64 writes buf[i] = reduce(slices[*][i]) across columns
// using either max or min. Parallelised across row partitions when the
// aggregate working set exceeds the single-core L2. The reduce is a
// simple comparison so it's memory-bound; serial throughput (~1 GB/s
// per core) × 8 cores reaches ~5-8 GB/s on L3-resident inputs.
func rowReduceInt64(buf []int64, slices [][]int64, isMax bool) {
	n := len(buf)
	const parallelCutoff = 128 * 1024
	if n < parallelCutoff {
		rowReduceInt64Serial(buf, slices, 0, n, isMax)
		return
	}
	// Worker count caps: 4 up to ~256K rows (fits in L2 comfortably),
	// 8 above that. On i7-10700 profiling showed 8 workers at 128K
	// each were L3-bound on output writes with 20% contention; 4
	// workers at 256K each use NT stores to bypass cache entirely.
	maxWorkers := 8
	if n <= 256*1024 {
		maxWorkers = 4
	}
	workers := min(runtime.GOMAXPROCS(0), maxWorkers)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		start := w * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rowReduceInt64Serial(buf, slices, s, e, isMax)
		}(start, end)
	}
	wg.Wait()
}

func rowReduceInt64Serial(buf []int64, slices [][]int64, start, end int, isMax bool) {
	copy(buf[start:end], slices[0][start:end])
	// SIMD fast path: compute.Max/MinInt64PairFold run an AVX2 kernel
	// (VPCMPGTQ + VPBLENDVB) on amd64 hosts with AVX2. For large
	// chunks (≥ 256K int64s = 2 MB) the NT-store variant skips the
	// read-for-ownership and halves output write bandwidth, worth
	// ~15-20% wall-time at 1M+ rows. Below that size the cached path
	// is better because the output likely lives in L2 already.
	const ntThreshold = 256 * 1024
	chunkLen := end - start
	useNT := chunkLen >= ntThreshold
	if isMax {
		for s := 1; s < len(slices); s++ {
			col := slices[s]
			bufView := buf[start:end]
			colView := col[start:end]
			var i int
			switch {
			case useNT && compute.MaxInt64PairFoldNT != nil:
				i = compute.MaxInt64PairFoldNT(bufView, colView)
			case compute.MaxInt64PairFold != nil:
				i = compute.MaxInt64PairFold(bufView, colView)
			}
			for ; i < len(bufView); i++ {
				bufView[i] = max(bufView[i], colView[i])
			}
		}
		return
	}
	for s := 1; s < len(slices); s++ {
		col := slices[s]
		bufView := buf[start:end]
		colView := col[start:end]
		var i int
		switch {
		case useNT && compute.MinInt64PairFoldNT != nil:
			i = compute.MinInt64PairFoldNT(bufView, colView)
		case compute.MinInt64PairFold != nil:
			i = compute.MinInt64PairFold(bufView, colView)
		}
		for ; i < len(bufView); i++ {
			bufView[i] = min(bufView[i], colView[i])
		}
	}
}

func rowReduceFloat64(buf []float64, slices [][]float64, isMax bool) {
	n := len(buf)
	const parallelCutoff = 128 * 1024
	if n < parallelCutoff {
		rowReduceFloat64Serial(buf, slices, 0, n, isMax)
		return
	}
	workers := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		start := w * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			rowReduceFloat64Serial(buf, slices, s, e, isMax)
		}(start, end)
	}
	wg.Wait()
}

func rowReduceFloat64Serial(buf []float64, slices [][]float64, start, end int, isMax bool) {
	copy(buf[start:end], slices[0][start:end])
	// Float max/min in Go 1.21+ intrinsify to MAXSD/MINSD (amd64) and
	// FMAX/FMIN (arm64). Note these follow IEEE 754-2019 semantics: if
	// either input is NaN the result is NaN, which matches polars'
	// propagation behaviour for no-null inputs.
	if isMax {
		for s := 1; s < len(slices); s++ {
			col := slices[s]
			for i := start; i < end; i++ {
				buf[i] = max(buf[i], col[i])
			}
		}
		return
	}
	for s := 1; s < len(slices); s++ {
		col := slices[s]
		for i := start; i < end; i++ {
			buf[i] = min(buf[i], col[i])
		}
	}
}

// AllHorizontal returns a Boolean Series that is true iff every
// participating boolean column is true at row i. Non-null-strategy
// semantics: IgnoreNulls treats null as "not present" (row is true if
// every non-null input is true); PropagateNulls yields null whenever
// any input is null.
func (df *DataFrame) AllHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	return df.reduceHorizontalBool(ctx, "all", strategy, cols, true)
}

// AnyHorizontal is the boolean OR analogue of AllHorizontal.
func (df *DataFrame) AnyHorizontal(ctx context.Context, strategy NullStrategy, cols ...string) (*series.Series, error) {
	return df.reduceHorizontalBool(ctx, "any", strategy, cols, false)
}

// reduceHorizontal is the shared driver for float-valued row reductions.
// It resolves the column set, casts every numeric input to float64 once,
// then calls kernel with the extracted slices and per-row per-column
// validity. Non-numeric columns produce an error.
func (df *DataFrame) reduceHorizontal(
	ctx context.Context,
	outName string,
	strategy NullStrategy,
	cols []string,
	kernel func(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy) (*series.Series, error),
) (*series.Series, error) {
	selected, err := df.selectHorizontal(cols, dtype.DType.IsNumeric)
	if err != nil {
		return nil, err
	}
	defer releaseAll(selected)

	n := df.height
	values := make([][]float64, len(selected))
	valid := make([][]bool, len(selected))
	for i, c := range selected {
		vs, v, err := floatColumnValues(ctx, c, n)
		if err != nil {
			return nil, fmt.Errorf("dataframe.%sHorizontal: column %q: %w", outName, c.Name(), err)
		}
		values[i] = vs
		valid[i] = v
	}
	return kernel(outName, n, values, valid, strategy)
}

func (df *DataFrame) reduceHorizontalBool(
	ctx context.Context,
	outName string,
	strategy NullStrategy,
	cols []string,
	identity bool,
) (*series.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selected, err := df.selectHorizontal(cols, func(dt dtype.DType) bool {
		return dt.ID() == arrow.BOOL
	})
	if err != nil {
		return nil, err
	}
	defer releaseAll(selected)

	n := df.height
	// Per-column unpacked bool + validity. For large frames we could
	// operate on packed bitmaps directly; the unpacked path matches
	// the rest of golars' simple kernels and keeps the code short.
	vals := make([][]bool, len(selected))
	valid := make([][]bool, len(selected))
	for i, c := range selected {
		vs, v, err := boolColumnValues(c, n)
		if err != nil {
			return nil, fmt.Errorf("dataframe.%sHorizontal: column %q: %w", outName, c.Name(), err)
		}
		vals[i] = vs
		valid[i] = v
	}

	out := make([]bool, n)
	outValid := make([]bool, n)
	anyInput := len(selected) > 0
	for r := range n {
		acc := identity
		sawValue := false
		sawNull := false
		for ci := range selected {
			if valid[ci] != nil && !valid[ci][r] {
				sawNull = true
				continue
			}
			sawValue = true
			if identity {
				acc = acc && vals[ci][r]
			} else {
				acc = acc || vals[ci][r]
			}
		}
		switch {
		case !anyInput:
			out[r] = identity
			outValid[r] = true
		case strategy == PropagateNulls && sawNull:
			outValid[r] = false
		case !sawValue:
			outValid[r] = false
		default:
			out[r] = acc
			outValid[r] = true
		}
	}
	return series.FromBool(outName, out, outValid)
}

func (df *DataFrame) selectHorizontal(cols []string, allow func(dtype.DType) bool) ([]*series.Series, error) {
	if len(cols) == 0 {
		picked := make([]*series.Series, 0, len(df.cols))
		for _, c := range df.cols {
			if allow(c.DType()) {
				picked = append(picked, c.Clone())
			}
		}
		return picked, nil
	}
	picked := make([]*series.Series, 0, len(cols))
	for _, name := range cols {
		c, err := df.Column(name)
		if err != nil {
			releaseAll(picked)
			return nil, err
		}
		if !allow(c.DType()) {
			releaseAll(picked)
			return nil, fmt.Errorf("dataframe: horizontal agg rejects dtype %s on column %q", c.DType(), name)
		}
		picked = append(picked, c.Clone())
	}
	return picked, nil
}

func releaseAll(ss []*series.Series) {
	for _, s := range ss {
		if s != nil {
			s.Release()
		}
	}
}

// floatColumnValues returns a []float64 with column c's values, and a
// validity slice (nil when no nulls). Integer and boolean columns are
// cast up to float64 transparently.
func floatColumnValues(ctx context.Context, s *series.Series, n int) ([]float64, []bool, error) {
	if s.DType().ID() == arrow.FLOAT64 {
		return extractFloat64(s.Chunk(0), n)
	}
	cast, err := compute.Cast(ctx, s, dtype.Float64())
	if err != nil {
		return nil, nil, err
	}
	defer cast.Release()
	return extractFloat64(cast.Chunk(0), n)
}

func extractFloat64(arr arrow.Array, n int) ([]float64, []bool, error) {
	a, ok := arr.(*array.Float64)
	if !ok {
		return nil, nil, fmt.Errorf("dataframe: expected float64 array, got %T", arr)
	}
	vs := make([]float64, n)
	copy(vs, a.Float64Values())
	var valid []bool
	if a.NullN() > 0 {
		valid = make([]bool, n)
		for i := range n {
			valid[i] = a.IsValid(i)
		}
	}
	return vs, valid, nil
}

func boolColumnValues(s *series.Series, n int) ([]bool, []bool, error) {
	arr, ok := s.Chunk(0).(*array.Boolean)
	if !ok {
		return nil, nil, fmt.Errorf("dataframe: expected boolean array, got %T", s.Chunk(0))
	}
	vs := make([]bool, n)
	var valid []bool
	if arr.NullN() > 0 {
		valid = make([]bool, n)
	}
	for i := range n {
		vs[i] = arr.Value(i)
		if valid != nil {
			valid[i] = arr.IsValid(i)
		}
	}
	return vs, valid, nil
}

// Kernels. Each builds a Float64 output. IgnoreNulls: output is null
// only when every participant at that row is null. PropagateNulls:
// output is null as soon as any participant is null.

func horizSum(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy) (*series.Series, error) {
	out := make([]float64, n)
	outValid := make([]bool, n)
	for r := range n {
		var sum float64
		saw := 0
		propagated := false
		for ci := range values {
			if valid[ci] != nil && !valid[ci][r] {
				if strategy == PropagateNulls {
					propagated = true
					break
				}
				continue
			}
			sum += values[ci][r]
			saw++
		}
		switch {
		case propagated:
			outValid[r] = false
		case saw == 0 && len(values) > 0:
			outValid[r] = false
		default:
			out[r] = sum
			outValid[r] = true
		}
	}
	return series.FromFloat64(outName, out, outValid)
}

func horizMean(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy) (*series.Series, error) {
	out := make([]float64, n)
	outValid := make([]bool, n)
	for r := range n {
		var sum float64
		saw := 0
		propagated := false
		for ci := range values {
			if valid[ci] != nil && !valid[ci][r] {
				if strategy == PropagateNulls {
					propagated = true
					break
				}
				continue
			}
			sum += values[ci][r]
			saw++
		}
		switch {
		case propagated:
			outValid[r] = false
		case saw == 0:
			outValid[r] = false
		default:
			out[r] = sum / float64(saw)
			outValid[r] = true
		}
	}
	return series.FromFloat64(outName, out, outValid)
}

func horizMin(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy) (*series.Series, error) {
	return horizMinMax(outName, n, values, valid, strategy, true)
}

func horizMax(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy) (*series.Series, error) {
	return horizMinMax(outName, n, values, valid, strategy, false)
}

func horizMinMax(outName string, n int, values [][]float64, valid [][]bool, strategy NullStrategy, isMin bool) (*series.Series, error) {
	out := make([]float64, n)
	outValid := make([]bool, n)
	for r := range n {
		var acc float64
		saw := 0
		propagated := false
		for ci := range values {
			if valid[ci] != nil && !valid[ci][r] {
				if strategy == PropagateNulls {
					propagated = true
					break
				}
				continue
			}
			v := values[ci][r]
			if saw == 0 {
				acc = v
			} else if isMin {
				if v < acc {
					acc = v
				}
			} else if v > acc {
				acc = v
			}
			saw++
		}
		switch {
		case propagated:
			outValid[r] = false
		case saw == 0:
			outValid[r] = false
		default:
			out[r] = acc
			outValid[r] = true
		}
	}
	return series.FromFloat64(outName, out, outValid)
}
