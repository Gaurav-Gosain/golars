package dataframe

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/intmap"
	"github.com/Gaurav-Gosain/golars/series"
)

// hashAggSingleKey runs a single-pass hash groupby. It scans the key column
// once to assign a group id to every row, then scans each value column once
// per aggregation, accumulating into a per-group slot.
//
// This replaces the sort-based path for single-key groupby on primitive or
// string keys. It is O(n) instead of O(n log n) and avoids the sort's
// per-comparison closure overhead, which profiling identified as the
// dominant cost of the old implementation.
//
// Returns (result, true, err) when the fast path handled the groupby.
// Returns (nil, false, nil) when the dtype combination is not yet supported
// so the caller can fall back to the sort-based implementation.
func hashAggSingleKey(ctx context.Context, df *DataFrame, keyName string, specs []aggSpec, mem memory.Allocator) (*DataFrame, bool, error) {
	keyCol, err := df.Column(keyName)
	if err != nil {
		return nil, true, err
	}
	if keyCol.NumChunks() != 1 {
		return nil, false, nil
	}
	keyArr := keyCol.Chunk(0)

	// Assign group ids based on key dtype.
	var (
		groupIDs  []int
		numGroups int
		keyOut    *series.Series
	)
	switch kArr := keyArr.(type) {
	case *array.Int64:
		groupIDs, numGroups, keyOut, err = assignGroupsInt64(kArr, keyName, mem)
	case *array.Int32:
		groupIDs, numGroups, keyOut, err = assignGroupsInt32(kArr, keyName, mem)
	case *array.String:
		groupIDs, numGroups, keyOut, err = assignGroupsString(kArr, keyName, mem)
	case *array.Boolean:
		groupIDs, numGroups, keyOut, err = assignGroupsBool(kArr, keyName, mem)
	default:
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	return hashAggWithGroups(ctx, df, groupIDs, numGroups, []*series.Series{keyOut}, specs, mem)
}

// hashAggWithGroups runs the per-column aggregation loop shared by the
// single-key and multi-key hash paths. keyOuts holds one unique-key
// column per group key, already in final output order.
func hashAggWithGroups(ctx context.Context, df *DataFrame, groupIDs []int, numGroups int, keyOuts []*series.Series, specs []aggSpec, mem memory.Allocator) (*DataFrame, bool, error) {
	releaseKeys := func() {
		for _, k := range keyOuts {
			if k != nil {
				k.Release()
			}
		}
	}

	outSpecs := make([]*series.Series, len(specs))
	cleanupSpecs := func() {
		for _, c := range outSpecs {
			if c != nil {
				c.Release()
			}
		}
	}

	// Bucket specs by column so that multi-agg on the same column fuses
	// into a single value-scan. Original spec ordering is preserved via
	// idx tags; the final DataFrame places outputs at outSpecs[idx].
	type specRef struct {
		sp  aggSpec
		idx int
	}
	byCol := make(map[string][]specRef)
	order := []string{}
	for i, sp := range specs {
		if _, ok := byCol[sp.colName]; !ok {
			order = append(order, sp.colName)
		}
		byCol[sp.colName] = append(byCol[sp.colName], specRef{sp, i})
	}

	for _, col := range order {
		colSpecs := byCol[col]
		valCol, err := df.Column(col)
		if err != nil {
			releaseKeys()
			cleanupSpecs()
			return nil, true, err
		}
		if len(colSpecs) > 1 && valCol.NumChunks() == 1 {
			var plain []aggSpec
			for _, sr := range colSpecs {
				plain = append(plain, sr.sp)
			}
			arr := valCol.Chunk(0)
			var outs []*series.Series
			var okFused bool
			switch a := arr.(type) {
			case *array.Int64:
				if allFusable(plain, true) {
					outs, okFused, err = hashAggFusedInt64(a, groupIDs, numGroups, plain, mem)
				}
			case *array.Float64:
				if allFusable(plain, true) {
					outs, okFused, err = hashAggFusedFloat64(a, groupIDs, numGroups, plain, mem)
				}
			}
			if err != nil {
				releaseKeys()
				cleanupSpecs()
				return nil, true, err
			}
			if okFused {
				for i, s := range outs {
					outSpecs[colSpecs[i].idx] = s
				}
				continue
			}
		}
		// Per-spec path (fusion inapplicable or unsupported dtype).
		for _, sr := range colSpecs {
			out, ok, err := hashAggCompute(valCol, groupIDs, numGroups, sr.sp, mem)
			if err != nil {
				releaseKeys()
				cleanupSpecs()
				return nil, true, err
			}
			if !ok {
				releaseKeys()
				cleanupSpecs()
				return nil, false, nil
			}
			outSpecs[sr.idx] = out
		}
	}

	outCols := make([]*series.Series, 0, len(keyOuts)+len(specs))
	outCols = append(outCols, keyOuts...)
	outCols = append(outCols, outSpecs...)

	result, err := New(outCols...)
	if err != nil {
		for _, c := range outCols {
			if c != nil {
				c.Release()
			}
		}
		return nil, true, err
	}
	_ = ctx
	return result, true, nil
}

// assignGroupsInt64 returns the per-row group id slice (length n; -1 for
// rows whose key is null), the number of distinct groups, and a Series
// containing the unique group keys in first-seen order.
func assignGroupsInt64(arr *array.Int64, name string, mem memory.Allocator) ([]int, int, *series.Series, error) {
	n := arr.Len()
	vals := arr.Int64Values()
	hasNulls := arr.NullN() > 0

	// Parallel path: for large inputs without nulls, split discovery into
	// per-thread partial tables (phase 1), merge their unique-key lists
	// serially (phase 2), then parallel ID assignment (phase 3). This wins
	// over the serial loop once InsertOrGet dominates total time.
	if n >= hashAggParThreshold && !hasNulls {
		ids, uniqueKeys := parallelAssignInt64(vals, n)
		keyOut, err := series.FromInt64(name, uniqueKeys, nil, series.WithAllocator(mem))
		return ids, len(uniqueKeys), keyOut, err
	}

	groupIDs := make([]int, n)
	table := intmap.New(64)
	defer table.Release()
	var uniqueKeys []int64
	nullGroupID := -1

	if hasNulls {
		for i := range n {
			if arr.IsNull(i) {
				if nullGroupID < 0 {
					nullGroupID = len(uniqueKeys)
					uniqueKeys = append(uniqueKeys, 0)
				}
				groupIDs[i] = nullGroupID
				continue
			}
			groupIDs[i] = assignInt64Key(table, &uniqueKeys, vals[i])
		}
	} else {
		for i := range n {
			groupIDs[i] = assignInt64Key(table, &uniqueKeys, vals[i])
		}
	}

	var valid []bool
	if nullGroupID >= 0 {
		valid = make([]bool, len(uniqueKeys))
		for i := range valid {
			valid[i] = true
		}
		valid[nullGroupID] = false
	}
	keyOut, err := series.FromInt64(name, uniqueKeys, valid, series.WithAllocator(mem))
	return groupIDs, len(uniqueKeys), keyOut, err
}

// assignInt64Key maps one key to its group id, recording first-seen keys.
func assignInt64Key(table *intmap.Int64, uniqueKeys *[]int64, k int64) int {
	id, inserted := table.InsertOrGet(k, int32(len(*uniqueKeys)))
	if inserted {
		*uniqueKeys = append(*uniqueKeys, k)
	}
	return int(id)
}

// parallelAssignInt64 implements a two-phase group-id assignment with the
// inner probe loops partitioned across goroutines. First-seen order is
// serialAssignInt64 is the single-threaded equivalent of parallelAssignInt64.
// One intmap, one pass - produces (groupIDs, uniqueKeys) in first-seen order.
// Preferred at medium sizes where goroutine startup dominates.
func serialAssignInt64(vals []int64, n int) ([]int, []int64) {
	// Pre-size to n/4. This function is only called from Unique (see
	// uniqueFastSingle) - GroupBy goes through parallelAssignInt64
	// which has its own hint. For a typical "how many distinct?"
	// workload, cardinality averages ~25-30% of n, and pre-sizing
	// here avoids 2-3 full grow passes that profiling showed
	// accounted for 20% of Unique's total runtime at 262K rows. The
	// extra memory is also reused - intmap.New draws from a pool, so
	// the oversized slot array gets recycled across calls.
	hint := max(n/4, 16)
	m := intmap.New(hint)
	defer m.Release()
	uniques := make([]int64, 0, hint)
	groupIDs := make([]int, n)
	for i, v := range vals {
		nextID := int32(len(uniques))
		id, inserted := m.InsertOrGet(v, nextID)
		if inserted {
			uniques = append(uniques, v)
		}
		groupIDs[i] = int(id)
	}
	return groupIDs, uniques
}

// preserved: thread 0's uniques come first (in its scan order), then each
// subsequent thread contributes only keys it introduces for the first time.
//
// Note: callers where the downstream aggregation benefits from a
// parallel group-ID layout (e.g. hashAggSingleKey) stay on this path.
// Unique-style callers that only need the distinct-key set should
// prefer SerialAssignInt64 at medium sizes where the 3-phase parallel
// pipeline's goroutine-startup + merge overhead eats the win.
func parallelAssignInt64(vals []int64, n int) ([]int, []int64) {
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k

	// Pre-size per-thread hash tables generously. Undersizing forces
	// repeat grows (profiled at 23% of Unique total time at 25%-distinct
	// inputs) and each grow is a fresh mallocgc that the sync.Pool can't
	// recycle. Sizing to half the partition covers the 25%-distinct
	// bench input in one allocation; low-cardinality workloads
	// (Unique(groups=8)) waste some L2 but run <1% of their time in the
	// hash table anyway.
	localHint := max(chunk/2, 16)

	// Phase 1: each worker collects its unique keys in first-seen order.
	localUniques := make([][]int64, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			t := intmap.New(localHint)
			u := make([]int64, 0, localHint)
			for i := start; i < end; i++ {
				v := vals[i]
				nextID := int32(len(u))
				_, inserted := t.InsertOrGet(v, nextID)
				if inserted {
					u = append(u, v)
				}
			}
			t.Release()
			localUniques[p] = u
		}(p)
	}
	wg.Wait()

	// Phase 2: merge per-thread uniques into a global map + key list in
	// first-seen order (threads contribute serially to preserve ordering).
	// Pre-size to the sum of per-thread uniques; grow may still fire if the
	// same key appears in multiple threads, but only by ~log2(duplicate
	// ratio) rounds of growth.
	totalUniques := 0
	for _, u := range localUniques {
		totalUniques += len(u)
	}
	global := intmap.New(totalUniques)
	defer global.Release()
	uniqueKeys := make([]int64, 0, totalUniques)
	for p := range k {
		for _, v := range localUniques[p] {
			nextID := int32(len(uniqueKeys))
			_, inserted := global.InsertOrGet(v, nextID)
			if inserted {
				uniqueKeys = append(uniqueKeys, v)
			}
		}
	}

	// Phase 3: parallel ID lookup into the finalized global map. This is
	// read-only so probes run with no contention.
	groupIDs := make([]int, n)
	wg = sync.WaitGroup{}
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			for i := start; i < end; i++ {
				id, _ := global.Get(vals[i])
				groupIDs[i] = int(id)
			}
		}(p)
	}
	wg.Wait()
	return groupIDs, uniqueKeys
}

func assignGroupsInt32(arr *array.Int32, name string, mem memory.Allocator) ([]int, int, *series.Series, error) {
	n := arr.Len()
	groupIDs := make([]int, n)
	table := make(map[int32]int, 64)
	var uniqueKeys []int32
	nullGroupID := -1
	vals := arr.Int32Values()
	hasNulls := arr.NullN() > 0

	if hasNulls {
		for i := range n {
			if arr.IsNull(i) {
				if nullGroupID < 0 {
					nullGroupID = len(uniqueKeys)
					uniqueKeys = append(uniqueKeys, 0)
				}
				groupIDs[i] = nullGroupID
				continue
			}
			k := vals[i]
			id, ok := table[k]
			if !ok {
				id = len(uniqueKeys)
				table[k] = id
				uniqueKeys = append(uniqueKeys, k)
			}
			groupIDs[i] = id
		}
	} else {
		for i := range n {
			k := vals[i]
			id, ok := table[k]
			if !ok {
				id = len(uniqueKeys)
				table[k] = id
				uniqueKeys = append(uniqueKeys, k)
			}
			groupIDs[i] = id
		}
	}

	var valid []bool
	if nullGroupID >= 0 {
		valid = make([]bool, len(uniqueKeys))
		for i := range valid {
			valid[i] = true
		}
		valid[nullGroupID] = false
	}
	keyOut, err := series.FromInt32(name, uniqueKeys, valid, series.WithAllocator(mem))
	return groupIDs, len(uniqueKeys), keyOut, err
}

func assignGroupsString(arr *array.String, name string, mem memory.Allocator) ([]int, int, *series.Series, error) {
	n := arr.Len()
	groupIDs := make([]int, n)
	table := make(map[string]int, 64)
	var uniqueKeys []string
	nullGroupID := -1
	hasNulls := arr.NullN() > 0

	if hasNulls {
		for i := range n {
			if arr.IsNull(i) {
				if nullGroupID < 0 {
					nullGroupID = len(uniqueKeys)
					uniqueKeys = append(uniqueKeys, "")
				}
				groupIDs[i] = nullGroupID
				continue
			}
			k := arr.Value(i)
			id, ok := table[k]
			if !ok {
				id = len(uniqueKeys)
				table[k] = id
				uniqueKeys = append(uniqueKeys, k)
			}
			groupIDs[i] = id
		}
	} else {
		for i := range n {
			k := arr.Value(i)
			id, ok := table[k]
			if !ok {
				id = len(uniqueKeys)
				table[k] = id
				uniqueKeys = append(uniqueKeys, k)
			}
			groupIDs[i] = id
		}
	}

	var valid []bool
	if nullGroupID >= 0 {
		valid = make([]bool, len(uniqueKeys))
		for i := range valid {
			valid[i] = true
		}
		valid[nullGroupID] = false
	}
	keyOut, err := series.FromString(name, uniqueKeys, valid, series.WithAllocator(mem))
	return groupIDs, len(uniqueKeys), keyOut, err
}

func assignGroupsBool(arr *array.Boolean, name string, mem memory.Allocator) ([]int, int, *series.Series, error) {
	n := arr.Len()
	groupIDs := make([]int, n)
	// Boolean has at most 3 groups: false, true, null.
	var falseID, trueID, nullGroupID int = -1, -1, -1
	var uniqueKeys []bool
	var nullPos int // index of null in uniqueKeys if any
	hasNulls := arr.NullN() > 0

	assignBool := func(i int) {
		if arr.Value(i) {
			if trueID < 0 {
				trueID = len(uniqueKeys)
				uniqueKeys = append(uniqueKeys, true)
			}
			groupIDs[i] = trueID
		} else {
			if falseID < 0 {
				falseID = len(uniqueKeys)
				uniqueKeys = append(uniqueKeys, false)
			}
			groupIDs[i] = falseID
		}
	}
	if hasNulls {
		for i := range n {
			if arr.IsNull(i) {
				if nullGroupID < 0 {
					nullGroupID = len(uniqueKeys)
					nullPos = nullGroupID
					uniqueKeys = append(uniqueKeys, false)
				}
				groupIDs[i] = nullGroupID
				continue
			}
			assignBool(i)
		}
	} else {
		for i := range n {
			assignBool(i)
		}
	}

	var valid []bool
	if nullGroupID >= 0 {
		valid = make([]bool, len(uniqueKeys))
		for i := range valid {
			valid[i] = true
		}
		valid[nullPos] = false
	}
	keyOut, err := series.FromBool(name, uniqueKeys, valid, series.WithAllocator(mem))
	return groupIDs, len(uniqueKeys), keyOut, err
}

// hashAggCompute applies sp's aggregation to the value column using the
// pre-computed group id per row. Returns ok=false when the value dtype plus
// op combination is not yet supported.
func hashAggCompute(valCol *series.Series, groupIDs []int, numGroups int, sp aggSpec, mem memory.Allocator) (*series.Series, bool, error) {
	if valCol.NumChunks() != 1 {
		return nil, false, nil
	}
	arr := valCol.Chunk(0)

	switch sp.op {
	case expr.AggSum:
		return hashSum(arr, groupIDs, numGroups, sp.outputName, mem)
	case expr.AggMean:
		return hashMean(arr, groupIDs, numGroups, sp.outputName, mem)
	case expr.AggMin, expr.AggMax:
		return hashMinMax(arr, groupIDs, numGroups, sp, mem)
	case expr.AggCount:
		return hashCount(arr, groupIDs, numGroups, sp.outputName, mem)
	case expr.AggNullCount:
		return hashNullCount(arr, groupIDs, numGroups, sp.outputName, mem)
	case expr.AggFirst:
		return hashFirstLast(arr, groupIDs, numGroups, sp, mem, true)
	case expr.AggLast:
		return hashFirstLast(arr, groupIDs, numGroups, sp, mem, false)
	}
	return nil, false, nil
}

// hashAggParThreshold picks the minimum row count above which per-worker
// partial-sum accumulators pay for themselves. Below that the overhead of
// spawning goroutines and reducing K partials is worse than a serial scan.
const hashAggParThreshold = 128 * 1024

func hashSum(arr arrow.Array, groupIDs []int, numGroups int, name string, mem memory.Allocator) (*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	n := arr.Len()
	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		sums := partialSumInt64(a, vals, groupIDs, numGroups, hasNulls, n)
		s, err := series.FromInt64(name, sums, nil, series.WithAllocator(mem))
		return s, true, err
	case *array.Int32:
		vals := a.Int32Values()
		sums := make([]int64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += int64(vals[i])
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += int64(vals[i])
			}
		}
		s, err := series.FromInt64(name, sums, nil, series.WithAllocator(mem))
		return s, true, err
	case *array.Float64:
		vals := a.Float64Values()
		sums := partialSumFloat64(a, vals, groupIDs, numGroups, hasNulls, n)
		s, err := series.FromFloat64(name, sums, nil, series.WithAllocator(mem))
		return s, true, err
	case *array.Float32:
		vals := a.Float32Values()
		sums := make([]float64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += float64(vals[i])
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += float64(vals[i])
			}
		}
		s, err := series.FromFloat64(name, sums, nil, series.WithAllocator(mem))
		return s, true, err
	}
	return nil, false, nil
}

// partialSumInt64 computes grouped sums in parallel for large inputs using
// per-worker scratch arrays reduced at the end. Serial path matches the
// simple loop for small inputs or wide group cardinality.
func partialSumInt64(a arrow.Array, vals []int64, groupIDs []int, numGroups int, hasNulls bool, n int) []int64 {
	if n < hashAggParThreshold || numGroups*8 > n/4 {
		sums := make([]int64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += vals[i]
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += vals[i]
			}
		}
		return sums
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	partials := make([][]int64, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			local := make([]int64, numGroups)
			if hasNulls {
				for i := start; i < end; i++ {
					if a.IsValid(i) {
						local[groupIDs[i]] += vals[i]
					}
				}
			} else {
				for i := start; i < end; i++ {
					local[groupIDs[i]] += vals[i]
				}
			}
			partials[p] = local
		}(p)
	}
	wg.Wait()
	sums := partials[0]
	for p := 1; p < k; p++ {
		for g := range sums {
			sums[g] += partials[p][g]
		}
	}
	return sums
}

func partialSumCountFloat64(a arrow.Array, vals []float64, groupIDs []int, numGroups int, hasNulls bool, n int) ([]float64, []int64) {
	if n < hashAggParThreshold || numGroups*8 > n/4 {
		sums := make([]float64, numGroups)
		counts := make([]int64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += vals[i]
					counts[gid]++
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += vals[i]
				counts[gid]++
			}
		}
		return sums, counts
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	ps := make([][]float64, k)
	pc := make([][]int64, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			s := make([]float64, numGroups)
			c := make([]int64, numGroups)
			if hasNulls {
				for i := start; i < end; i++ {
					if a.IsValid(i) {
						s[groupIDs[i]] += vals[i]
						c[groupIDs[i]]++
					}
				}
			} else {
				for i := start; i < end; i++ {
					s[groupIDs[i]] += vals[i]
					c[groupIDs[i]]++
				}
			}
			ps[p] = s
			pc[p] = c
		}(p)
	}
	wg.Wait()
	sums := ps[0]
	counts := pc[0]
	for p := 1; p < k; p++ {
		for g := range sums {
			sums[g] += ps[p][g]
			counts[g] += pc[p][g]
		}
	}
	return sums, counts
}

func partialSumCountInt64AsFloat(a arrow.Array, vals []int64, groupIDs []int, numGroups int, hasNulls bool, n int) ([]float64, []int64) {
	if n < hashAggParThreshold || numGroups*8 > n/4 {
		sums := make([]float64, numGroups)
		counts := make([]int64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += float64(vals[i])
					counts[gid]++
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += float64(vals[i])
				counts[gid]++
			}
		}
		return sums, counts
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	ps := make([][]float64, k)
	pc := make([][]int64, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			s := make([]float64, numGroups)
			c := make([]int64, numGroups)
			if hasNulls {
				for i := start; i < end; i++ {
					if a.IsValid(i) {
						s[groupIDs[i]] += float64(vals[i])
						c[groupIDs[i]]++
					}
				}
			} else {
				for i := start; i < end; i++ {
					s[groupIDs[i]] += float64(vals[i])
					c[groupIDs[i]]++
				}
			}
			ps[p] = s
			pc[p] = c
		}(p)
	}
	wg.Wait()
	sums := ps[0]
	counts := pc[0]
	for p := 1; p < k; p++ {
		for g := range sums {
			sums[g] += ps[p][g]
			counts[g] += pc[p][g]
		}
	}
	return sums, counts
}

func partialSumFloat64(a arrow.Array, vals []float64, groupIDs []int, numGroups int, hasNulls bool, n int) []float64 {
	if n < hashAggParThreshold || numGroups*8 > n/4 {
		sums := make([]float64, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if a.IsValid(i) {
					sums[gid] += vals[i]
				}
			}
		} else {
			for i, gid := range groupIDs {
				sums[gid] += vals[i]
			}
		}
		return sums
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	partials := make([][]float64, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			local := make([]float64, numGroups)
			if hasNulls {
				for i := start; i < end; i++ {
					if a.IsValid(i) {
						local[groupIDs[i]] += vals[i]
					}
				}
			} else {
				for i := start; i < end; i++ {
					local[groupIDs[i]] += vals[i]
				}
			}
			partials[p] = local
		}(p)
	}
	wg.Wait()
	sums := partials[0]
	for p := 1; p < k; p++ {
		for g := range sums {
			sums[g] += partials[p][g]
		}
	}
	return sums
}

func hashMean(arr arrow.Array, groupIDs []int, numGroups int, name string, mem memory.Allocator) (*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	n := arr.Len()
	var (
		sums   []float64
		counts []int64
	)

	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		sums, counts = partialSumCountInt64AsFloat(a, vals, groupIDs, numGroups, hasNulls, n)
	case *array.Float64:
		vals := a.Float64Values()
		sums, counts = partialSumCountFloat64(a, vals, groupIDs, numGroups, hasNulls, n)
	default:
		return nil, false, nil
	}

	out := make([]float64, numGroups)
	var valid []bool
	anyNull := false
	for g := range numGroups {
		if counts[g] == 0 {
			anyNull = true
			continue
		}
		out[g] = sums[g] / float64(counts[g])
	}
	if anyNull {
		valid = make([]bool, numGroups)
		for g := range numGroups {
			valid[g] = counts[g] > 0
		}
	}
	s, err := series.FromFloat64(name, out, valid, series.WithAllocator(mem))
	return s, true, err
}

func hashMinMax(arr arrow.Array, groupIDs []int, numGroups int, sp aggSpec, mem memory.Allocator) (*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	isMax := sp.op == expr.AggMax

	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		out := make([]int64, numGroups)
		init := make([]bool, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if !a.IsValid(i) {
					continue
				}
				v := vals[i]
				if !init[gid] {
					out[gid] = v
					init[gid] = true
					continue
				}
				if isMax {
					if v > out[gid] {
						out[gid] = v
					}
				} else {
					if v < out[gid] {
						out[gid] = v
					}
				}
			}
		} else {
			for i, gid := range groupIDs {
				v := vals[i]
				if !init[gid] {
					out[gid] = v
					init[gid] = true
					continue
				}
				if isMax {
					if v > out[gid] {
						out[gid] = v
					}
				} else {
					if v < out[gid] {
						out[gid] = v
					}
				}
			}
		}
		valid := compactValid(init)
		s, err := series.FromInt64(sp.outputName, out, valid, series.WithAllocator(mem))
		return s, true, err
	case *array.Float64:
		vals := a.Float64Values()
		out := make([]float64, numGroups)
		init := make([]bool, numGroups)
		if hasNulls {
			for i, gid := range groupIDs {
				if !a.IsValid(i) {
					continue
				}
				v := vals[i]
				if !init[gid] {
					out[gid] = v
					init[gid] = true
					continue
				}
				if isMax {
					if floatGt(v, out[gid]) {
						out[gid] = v
					}
				} else {
					if floatLt(v, out[gid]) {
						out[gid] = v
					}
				}
			}
		} else {
			for i, gid := range groupIDs {
				v := vals[i]
				if !init[gid] {
					out[gid] = v
					init[gid] = true
					continue
				}
				if isMax {
					if floatGt(v, out[gid]) {
						out[gid] = v
					}
				} else {
					if floatLt(v, out[gid]) {
						out[gid] = v
					}
				}
			}
		}
		valid := compactValid(init)
		s, err := series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(mem))
		return s, true, err
	}
	return nil, false, nil
}

func hashCount(arr arrow.Array, groupIDs []int, numGroups int, name string, mem memory.Allocator) (*series.Series, bool, error) {
	counts := make([]int64, numGroups)
	if arr.NullN() == 0 {
		for _, gid := range groupIDs {
			counts[gid]++
		}
	} else {
		for i, gid := range groupIDs {
			if arr.IsValid(i) {
				counts[gid]++
			}
		}
	}
	s, err := series.FromInt64(name, counts, nil, series.WithAllocator(mem))
	return s, true, err
}

func hashNullCount(arr arrow.Array, groupIDs []int, numGroups int, name string, mem memory.Allocator) (*series.Series, bool, error) {
	counts := make([]int64, numGroups)
	if arr.NullN() == 0 {
		s, err := series.FromInt64(name, counts, nil, series.WithAllocator(mem))
		return s, true, err
	}
	for i, gid := range groupIDs {
		if arr.IsNull(i) {
			counts[gid]++
		}
	}
	s, err := series.FromInt64(name, counts, nil, series.WithAllocator(mem))
	return s, true, err
}

func hashFirstLast(arr arrow.Array, groupIDs []int, numGroups int, sp aggSpec, mem memory.Allocator, first bool) (*series.Series, bool, error) {
	// Track the row index to take for each group.
	indices := make([]int, numGroups)
	set := make([]bool, numGroups)
	if first {
		for i, gid := range groupIDs {
			if !set[gid] {
				indices[gid] = i
				set[gid] = true
			}
		}
	} else {
		for i, gid := range groupIDs {
			indices[gid] = i
			set[gid] = true
		}
	}
	// Some groups may have no rows (shouldn't happen since every group was
	// seen at least once when assigned). Defensive check.
	for _, ok := range set {
		if !ok {
			return nil, false, fmt.Errorf("hashFirstLast: empty group")
		}
	}
	// Take by indices.
	return takeForFirstLast(arr, indices, sp.outputName, mem)
}

func takeForFirstLast(arr arrow.Array, indices []int, name string, mem memory.Allocator) (*series.Series, bool, error) {
	hasNulls := arr.NullN() > 0
	switch a := arr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		out := make([]int64, len(indices))
		var valid []bool
		if hasNulls {
			valid = make([]bool, len(indices))
		}
		for g, i := range indices {
			out[g] = vals[i]
			if valid != nil {
				valid[g] = a.IsValid(i)
			}
		}
		s, err := series.FromInt64(name, out, valid, series.WithAllocator(mem))
		return s, true, err
	case *array.Float64:
		vals := a.Float64Values()
		out := make([]float64, len(indices))
		var valid []bool
		if hasNulls {
			valid = make([]bool, len(indices))
		}
		for g, i := range indices {
			out[g] = vals[i]
			if valid != nil {
				valid[g] = a.IsValid(i)
			}
		}
		s, err := series.FromFloat64(name, out, valid, series.WithAllocator(mem))
		return s, true, err
	case *array.String:
		out := make([]string, len(indices))
		var valid []bool
		if hasNulls {
			valid = make([]bool, len(indices))
		}
		for g, i := range indices {
			out[g] = a.Value(i)
			if valid != nil {
				valid[g] = a.IsValid(i)
			}
		}
		s, err := series.FromString(name, out, valid, series.WithAllocator(mem))
		return s, true, err
	}
	return nil, false, nil
}

// compactValid returns nil when every entry is true, so the output Series
// is built without a validity bitmap.
func compactValid(v []bool) []bool {
	for _, ok := range v {
		if !ok {
			return v
		}
	}
	return nil
}

// floatGt and floatLt handle NaN per polars: NaN sorts above all non-NaN for
// max, below for min.
func floatGt(a, b float64) bool {
	aNaN := a != a
	bNaN := b != b
	if aNaN && bNaN {
		return false
	}
	if aNaN {
		return true
	}
	if bNaN {
		return false
	}
	return a > b
}

func floatLt(a, b float64) bool {
	aNaN := a != a
	bNaN := b != b
	if aNaN && bNaN {
		return false
	}
	if aNaN {
		return false
	}
	if bNaN {
		return true
	}
	return a < b
}
