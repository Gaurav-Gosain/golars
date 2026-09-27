package dataframe

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// fusedOps enumerates the agg ops that share the same single-pass
// scan: Sum, Mean, Min, Max, Count, NullCount. First/Last are
// position-sensitive so they stay on the per-spec path.
func fusedOp(op expr.AggOp) bool {
	switch op {
	case expr.AggSum, expr.AggMean, expr.AggMin, expr.AggMax,
		expr.AggCount, expr.AggNullCount:
		return true
	}
	return false
}

// allFusable reports whether every spec in the bucket can share one
// scan. All specs must target a supported dtype (int64, float64) and
// a fusable op.
func allFusable(specs []aggSpec, dtypeSupported bool) bool {
	if !dtypeSupported || len(specs) < 2 {
		return false
	}
	for _, sp := range specs {
		if !fusedOp(sp.op) {
			return false
		}
	}
	return true
}

// fusedAccInt64 holds the per-group running state for every op a
// single Int64 pass might need to output. Any op we don't emit costs
// nothing since the accumulators are side-effects on a single struct
// pinned in L1 while the row iterates.
type fusedAccInt64 struct {
	sum       int64
	count     int64 // non-null count
	nullCount int64
	minVal    int64
	maxVal    int64
}

// fusedScanInt64 does one pass over vals+groupIDs computing
// {sum, count, nullCount, min, max} per group. The caller extracts
// whichever of these the spec list requested. Parallel fan-out
// (per-worker accumulators + merge) is handled in the int64 int-sum
// paths already; this kernel is currently serial: multi-agg's
// win is the shared scan, not parallelism. Callers at the 1M+ tier
// might want to parallelize later.
func fusedScanInt64(a *array.Int64, vals []int64, groupIDs []int, numGroups int, hasNulls bool) []fusedAccInt64 {
	acc := make([]fusedAccInt64, numGroups)
	for g := range acc {
		acc[g].minVal = math.MaxInt64
		acc[g].maxVal = math.MinInt64
	}
	if hasNulls {
		for i, gid := range groupIDs {
			if !a.IsValid(i) {
				acc[gid].nullCount++
				continue
			}
			p := &acc[gid]
			v := vals[i]
			p.sum += v
			p.count++
			if v < p.minVal {
				p.minVal = v
			}
			if v > p.maxVal {
				p.maxVal = v
			}
		}
		return acc
	}
	for i, gid := range groupIDs {
		p := &acc[gid]
		v := vals[i]
		p.sum += v
		p.count++
		if v < p.minVal {
			p.minVal = v
		}
		if v > p.maxVal {
			p.maxVal = v
		}
	}
	return acc
}

// fusedAccFloat64 mirrors fusedAccInt64 for f64.
type fusedAccFloat64 struct {
	sum       float64
	count     int64
	nullCount int64
	minVal    float64
	maxVal    float64
}

func fusedScanFloat64(a *array.Float64, vals []float64, groupIDs []int, numGroups int, hasNulls bool) []fusedAccFloat64 {
	acc := make([]fusedAccFloat64, numGroups)
	for g := range acc {
		acc[g].minVal = math.Inf(1)
		acc[g].maxVal = math.Inf(-1)
	}
	if hasNulls {
		for i, gid := range groupIDs {
			if !a.IsValid(i) {
				acc[gid].nullCount++
				continue
			}
			p := &acc[gid]
			v := vals[i]
			p.sum += v
			p.count++
			if v < p.minVal {
				p.minVal = v
			}
			if v > p.maxVal {
				p.maxVal = v
			}
		}
		return acc
	}
	for i, gid := range groupIDs {
		p := &acc[gid]
		v := vals[i]
		p.sum += v
		p.count++
		if v < p.minVal {
			p.minVal = v
		}
		if v > p.maxVal {
			p.maxVal = v
		}
	}
	return acc
}

// hashAggFusedInt64 runs one shared scan over an Int64 value column
// and extracts the per-spec output series. Returns ok=false if any
// spec is non-fusable or the input dtype changes.
func hashAggFusedInt64(arr *array.Int64, groupIDs []int, numGroups int, specs []aggSpec, mem memory.Allocator) ([]*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	vals := arr.Int64Values()
	acc := fusedScanInt64(arr, vals, groupIDs, numGroups, hasNulls)

	out := make([]*series.Series, len(specs))
	for i, sp := range specs {
		s, err := int64SpecFromAcc(sp, acc, numGroups, mem)
		if err != nil {
			for _, prev := range out[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, true, err
		}
		out[i] = s
	}
	return out, true, nil
}

func int64SpecFromAcc(sp aggSpec, acc []fusedAccInt64, numGroups int, mem memory.Allocator) (*series.Series, error) {
	switch sp.op {
	case expr.AggSum:
		sums := make([]int64, numGroups)
		for g := range numGroups {
			sums[g] = acc[g].sum
		}
		return series.FromInt64(sp.outputName, sums, nil, series.WithAllocator(mem))
	case expr.AggMean:
		out := make([]float64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = float64(acc[g].sum) / float64(acc[g].count)
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggMin:
		out := make([]int64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = acc[g].minVal
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromInt64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggMax:
		out := make([]int64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = acc[g].maxVal
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromInt64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggCount:
		counts := make([]int64, numGroups)
		for g := range numGroups {
			counts[g] = acc[g].count
		}
		return series.FromInt64(sp.outputName, counts, nil, series.WithAllocator(mem))
	case expr.AggNullCount:
		nc := make([]int64, numGroups)
		for g := range numGroups {
			nc[g] = acc[g].nullCount
		}
		return series.FromInt64(sp.outputName, nc, nil, series.WithAllocator(mem))
	}
	return nil, nil
}

// hashAggFusedFloat64 is the f64 analogue.
func hashAggFusedFloat64(arr *array.Float64, groupIDs []int, numGroups int, specs []aggSpec, mem memory.Allocator) ([]*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	vals := arr.Float64Values()
	acc := fusedScanFloat64(arr, vals, groupIDs, numGroups, hasNulls)

	out := make([]*series.Series, len(specs))
	for i, sp := range specs {
		s, err := float64SpecFromAcc(sp, acc, numGroups, mem)
		if err != nil {
			for _, prev := range out[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, true, err
		}
		out[i] = s
	}
	return out, true, nil
}

func float64SpecFromAcc(sp aggSpec, acc []fusedAccFloat64, numGroups int, mem memory.Allocator) (*series.Series, error) {
	switch sp.op {
	case expr.AggSum:
		sums := make([]float64, numGroups)
		for g := range numGroups {
			sums[g] = acc[g].sum
		}
		return series.FromFloat64(sp.outputName, sums, nil, series.WithAllocator(mem))
	case expr.AggMean:
		out := make([]float64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = acc[g].sum / float64(acc[g].count)
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggMin:
		out := make([]float64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = acc[g].minVal
			if acc[g].minVal > acc[g].maxVal {
				// Only NaN values: the seeds never moved (polars: NaN).
				out[g] = math.NaN()
			}
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggMax:
		out := make([]float64, numGroups)
		var valid []bool
		anyNull := false
		for g := range numGroups {
			if acc[g].count == 0 {
				anyNull = true
				continue
			}
			out[g] = acc[g].maxVal
			if acc[g].minVal > acc[g].maxVal {
				// Only NaN values: the seeds never moved (polars: NaN).
				out[g] = math.NaN()
			}
		}
		if anyNull {
			valid = make([]bool, numGroups)
			for g := range numGroups {
				valid[g] = acc[g].count > 0
			}
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(mem))
	case expr.AggCount:
		counts := make([]int64, numGroups)
		for g := range numGroups {
			counts[g] = acc[g].count
		}
		return series.FromInt64(sp.outputName, counts, nil, series.WithAllocator(mem))
	case expr.AggNullCount:
		nc := make([]int64, numGroups)
		for g := range numGroups {
			nc[g] = acc[g].nullCount
		}
		return series.FromInt64(sp.outputName, nc, nil, series.WithAllocator(mem))
	}
	return nil, nil
}
