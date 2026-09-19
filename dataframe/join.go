package dataframe

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/intmap"
	"github.com/Gaurav-Gosain/golars/series"
)

// JoinType selects the join semantics.
type JoinType uint8

const (
	// InnerJoin emits a row only when keys match on both sides.
	InnerJoin JoinType = iota
	// LeftJoin emits every left row; right columns are null when no match.
	LeftJoin
	// CrossJoin is the Cartesian product; key columns are ignored.
	CrossJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "inner"
	case LeftJoin:
		return "left"
	case CrossJoin:
		return "cross"
	}
	return "?"
}

// JoinOption configures Join.
type JoinOption func(*joinConfig)

type joinConfig struct {
	alloc  memory.Allocator
	suffix string
}

func resolveJoin(opts []JoinOption) joinConfig {
	c := joinConfig{
		alloc:  memory.DefaultAllocator,
		suffix: "_right",
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithJoinAllocator overrides the allocator used while building output
// columns.
func WithJoinAllocator(alloc memory.Allocator) JoinOption {
	return func(c *joinConfig) { c.alloc = alloc }
}

// WithJoinSuffix overrides the suffix applied to right-side columns whose
// name collides with the left side. Default: "_right".
func WithJoinSuffix(s string) JoinOption {
	return func(c *joinConfig) { c.suffix = s }
}

// Sentinel errors.
var (
	ErrJoinKeyDTypeMismatch = fmt.Errorf("dataframe.Join: key dtypes differ")
	ErrJoinUnsupportedKey   = fmt.Errorf("dataframe.Join: unsupported key dtype")
)

// Join combines left and right on the given key columns. on must name columns
// that exist in both frames with the same dtype. The result has every left
// column followed by every right column except those named in on. Right-side
// column names that collide with a left-side name receive a suffix.
//
// Implementation: hash-based single-key join for Phase 2. Multi-key is not
// supported yet and produces an error.
func (df *DataFrame) Join(ctx context.Context, right *DataFrame, on []string, how JoinType, opts ...JoinOption) (*DataFrame, error) {
	cfg := resolveJoin(opts)

	if how == CrossJoin {
		return crossJoin(ctx, df, right, cfg)
	}
	if len(on) == 0 {
		return nil, fmt.Errorf("dataframe.Join: on must not be empty")
	}
	if len(on) > 1 {
		return nil, fmt.Errorf("dataframe.Join: multi-key join not implemented yet")
	}
	key := on[0]

	leftKey, err := df.Column(key)
	if err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	rightKey, err := right.Column(key)
	if err != nil {
		return nil, fmt.Errorf("right: %w", err)
	}
	if !leftKey.DType().Equal(rightKey.DType()) {
		return nil, fmt.Errorf("%w: %s vs %s", ErrJoinKeyDTypeMismatch,
			leftKey.DType(), rightKey.DType())
	}

	leftIdx, rightIdx, err := hashJoinIndices(leftKey, rightKey, how)
	if err != nil {
		return nil, err
	}

	return buildJoinOutput(ctx, df, right, leftIdx, rightIdx, on, cfg)
}

// hashJoinIndices runs the hash join on the single-key Series and returns
// paired index arrays. For LeftJoin, rightIdx entries of -1 mark left rows
// with no match.
func hashJoinIndices(left, right *series.Series, how JoinType) ([]int, []int, error) {
	ra := right.Chunk(0)
	la := left.Chunk(0)

	switch left.DType().ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return hashJoinInt(la, ra, how)
	case arrow.FLOAT32, arrow.FLOAT64:
		return hashJoinFloat(la, ra, how)
	case arrow.BOOL:
		return hashJoinBool(la, ra, how)
	case arrow.STRING:
		return hashJoinString(la, ra, how)
	}
	return nil, nil, fmt.Errorf("%w: %s", ErrJoinUnsupportedKey, left.DType())
}

// partitionedJoinThreshold is the minimum (left+right) size where the
// partitioned-build path wins. Below this the single-table build is faster
// because scatter and extra allocations don't amortize.
const partitionedJoinThreshold = 128 * 1024

// directJoinMaxFactor sets the upper bound on right-key range relative to
// rightLen for direct-table dispatch. A factor of 4 keeps the table under
// 4× rightLen ints, so at 256K rightLen the table is ≤ 8 MB (fits L3).
const directJoinMaxFactor = 4

func hashJoinInt(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	lv := toInt64Slice(la)
	rv := toInt64Slice(ra)
	rightLen := len(rv)
	leftLen := len(lv)
	rightNulls := ra.NullN() > 0

	// Direct-table dispatch: when right keys fit a dense integer range,
	// use a flat []int32 table indexed by (key-min). Eliminates hashing,
	// collision resolution, and cache-unfriendly probes. ~4-10× faster on
	// sparse ID joins where the range is bounded.
	if !rightNulls && rightLen >= 1024 {
		if minK, maxK, ok := int64Range(rv); ok {
			// Require non-negative (since we index) and bounded range.
			if minK >= 0 {
				span := maxK - minK + 1
				if span > 0 && span <= int64(rightLen*directJoinMaxFactor) {
					return directTableJoinInt(la, lv, rv, minK, int(span), how)
				}
			}
		}
	}

	// Partitioned build wins at 128K+ where the shared hash table no longer
	// fits L2 and random-access probes are L3-bound. We use 32 partitions.
	// Build is parallelized via per-thread histograms → scatter → parallel
	// per-partition build. Probe hashes to a partition and hits only that
	// partition's (L2-resident) table. Mirrors polars' build_tables pattern.
	if !rightNulls && rightLen >= partitionedJoinThreshold && leftLen >= partitionedJoinThreshold {
		return partitionedHashJoinInt(la, lv, rv, how)
	}

	const noMatch = -1
	heads := intmap.New(rightLen)
	defer heads.Release()
	next := make([]int, rightLen)
	hasDuplicates := false

	for i := rightLen - 1; i >= 0; i-- {
		if rightNulls && ra.IsNull(i) {
			next[i] = noMatch
			continue
		}
		prev, inserted := heads.InsertOrGet(rv[i], int32(i))
		if inserted {
			next[i] = noMatch
		} else {
			next[i] = int(prev)
			heads.Overwrite(rv[i], int32(i))
			hasDuplicates = true
		}
	}

	return parallelProbe(la, lv, heads, next, how, hasDuplicates)
}

// int64Range returns (min, max, ok=true) for a non-empty slice. For very
// large inputs the scan is done in parallel. Returns ok=false if empty.
func int64Range(vals []int64) (int64, int64, bool) {
	n := len(vals)
	if n == 0 {
		return 0, 0, false
	}
	if n < 64*1024 {
		mn, mx := vals[0], vals[0]
		for _, v := range vals[1:] {
			if v < mn {
				mn = v
			}
			if v > mx {
				mx = v
			}
		}
		return mn, mx, true
	}
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	mins := make([]int64, k)
	maxs := make([]int64, k)
	var wg sync.WaitGroup
	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * chunk
			end := min(start+chunk, n)
			if start >= end {
				mins[t] = vals[0]
				maxs[t] = vals[0]
				return
			}
			mn, mx := vals[start], vals[start]
			for _, v := range vals[start+1 : end] {
				if v < mn {
					mn = v
				}
				if v > mx {
					mx = v
				}
			}
			mins[t] = mn
			maxs[t] = mx
		}(t)
	}
	wg.Wait()
	mn, mx := mins[0], maxs[0]
	for t := 1; t < k; t++ {
		if mins[t] < mn {
			mn = mins[t]
		}
		if maxs[t] > mx {
			mx = maxs[t]
		}
	}
	return mn, mx, true
}

// directTableJoinInt runs a hash join where the right key space is dense
// enough to use a direct-indexed lookup table. Avoids all hashing: build
// is a single scatter into a pre-allocated []int32 of size span, probe is
// a range check + index. Handles duplicates on the right via a chain array.
// For the common unique-right LeftJoin/InnerJoin case (bench workload) the
// chain is skipped entirely.
func directTableJoinInt(la arrow.Array, lv, rv []int64, minK int64, span int, how JoinType) ([]int, []int, error) {
	rightLen := len(rv)
	leftLen := len(lv)
	const noMatch = -1

	// Build: table[k-min] = latest right index; next[i] = previous index
	// with same key (or -1).
	//
	// Both table and next are short-lived workspace buffers that get
	// thrown away after the probe. Pool them to eliminate the ~192 KB
	// per-call mallocgc+zero (128 KB table + 64 KB next at the
	// LeftJoin 16K bench shape). Pool returns a slice that may be
	// dirty, so we memset the table to 0xFF explicitly.
	table := getJoinTable(span)
	defer putJoinTable(table)
	tableBytes := unsafe.Slice((*byte)(unsafe.Pointer(&table[0])), span*4)
	for i := range tableBytes {
		tableBytes[i] = 0xFF
	}
	next := getJoinNext(rightLen)
	defer putJoinNext(next)
	hasDuplicates := false
	for i := rightLen - 1; i >= 0; i-- {
		k := int(rv[i] - minK)
		prev := table[k]
		next[i] = prev
		table[k] = int32(i)
		if prev >= 0 {
			hasDuplicates = true
		}
	}

	leftNulls := la.NullN() > 0
	k := min(runtime.GOMAXPROCS(0), 8)
	chunkSize := (leftLen + k - 1) / k

	// Fast path: no duplicates on the right and no nulls on the left in a
	// LeftJoin means every left row produces exactly one output. We can
	// pre-allocate the full lOut/rOut once and have each worker write
	// straight into its slot, saving the concat memmove.
	if !hasDuplicates && !leftNulls && how == LeftJoin {
		lOut := make([]int, leftLen)
		rOut := make([]int, leftLen)
		// Serial below 64K: goroutine spawn overhead exceeds the parallel
		// win on this cache-resident workload.
		if leftLen < 64*1024 {
			for i := range leftLen {
				lOut[i] = i
				key := lv[i] - minK
				if key < 0 || key >= int64(span) {
					rOut[i] = noMatch
					continue
				}
				rOut[i] = int(table[key])
			}
			return lOut, rOut, nil
		}
		var wg sync.WaitGroup
		for t := range k {
			wg.Add(1)
			go func(t int) {
				defer wg.Done()
				start := t * chunkSize
				end := min(start+chunkSize, leftLen)
				for i := start; i < end; i++ {
					lOut[i] = i
					key := lv[i] - minK
					if key < 0 || key >= int64(span) {
						rOut[i] = noMatch
						continue
					}
					rOut[i] = int(table[key])
				}
			}(t)
		}
		wg.Wait()
		return lOut, rOut, nil
	}

	// Two-phase path for LeftJoin with duplicates (and the duplicates +
	// left-nulls InnerJoin variant): each worker first counts its chunk's
	// output size, then writes directly into the shared lOut/rOut at a
	// prefix-summed offset. Replaces the old local-slab + serial-concat
	// pattern: saves the 4 MB+ concat memmove at 256K and avoids per-
	// worker slice growth from Append. Left-row iteration is chunked,
	// so per-worker output windows remain contiguous and left order is
	// preserved.
	workerCounts := make([]int, k)
	var wg sync.WaitGroup
	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * chunkSize
			end := min(start+chunkSize, leftLen)
			local := 0
			if !hasDuplicates && !leftNulls && how == InnerJoin {
				for i := start; i < end; i++ {
					key := lv[i] - minK
					if key < 0 || key >= int64(span) {
						continue
					}
					if table[key] >= 0 {
						local++
					}
				}
				workerCounts[t] = local
				return
			}
			for i := start; i < end; i++ {
				if leftNulls && la.IsNull(i) {
					if how == LeftJoin {
						local++
					}
					continue
				}
				key := lv[i] - minK
				if key < 0 || key >= int64(span) {
					if how == LeftJoin {
						local++
					}
					continue
				}
				head := table[key]
				if head < 0 {
					if how == LeftJoin {
						local++
					}
					continue
				}
				for j := head; j >= 0; j = next[j] {
					local++
				}
			}
			workerCounts[t] = local
		}(t)
	}
	wg.Wait()

	offsets := make([]int, k+1)
	for t := range k {
		offsets[t+1] = offsets[t] + workerCounts[t]
	}
	total := offsets[k]
	lOut := make([]int, total)
	rOut := make([]int, total)

	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * chunkSize
			end := min(start+chunkSize, leftLen)
			pos := offsets[t]
			if !hasDuplicates && !leftNulls && how == InnerJoin {
				for i := start; i < end; i++ {
					key := lv[i] - minK
					if key < 0 || key >= int64(span) {
						continue
					}
					v := table[key]
					if v >= 0 {
						lOut[pos] = i
						rOut[pos] = int(v)
						pos++
					}
				}
				return
			}
			for i := start; i < end; i++ {
				if leftNulls && la.IsNull(i) {
					if how == LeftJoin {
						lOut[pos] = i
						rOut[pos] = noMatch
						pos++
					}
					continue
				}
				key := lv[i] - minK
				if key < 0 || key >= int64(span) {
					if how == LeftJoin {
						lOut[pos] = i
						rOut[pos] = noMatch
						pos++
					}
					continue
				}
				head := table[key]
				if head < 0 {
					if how == LeftJoin {
						lOut[pos] = i
						rOut[pos] = noMatch
						pos++
					}
					continue
				}
				for j := head; j >= 0; j = next[j] {
					lOut[pos] = i
					rOut[pos] = int(j)
					pos++
				}
			}
		}(t)
	}
	wg.Wait()
	return lOut, rOut, nil
}

// partitionedHashJoinInt builds k partitioned hash tables, one per bucket
// determined by high-bits of the Fibonacci hash. Each per-partition table
// holds ~rightLen/k keys, which fits L1 for k=16 at rightLen=256K. Mirrors
// the polars build_tables structure (scatter → parallel per-partition build).
func partitionedHashJoinInt(la arrow.Array, lv, rv []int64, how JoinType) ([]int, []int, error) {
	// 32 partitions: at 256K keys each partition holds ~8K entries →
	// 16K slots × 16B = 256KB per table, which fits L2 per core. Measured
	// sweet spot against 16/64/128 partition counts.
	const partBits = 5
	const nPart = 1 << partBits
	const partShift = 64 - partBits
	const goldenRatio uint64 = 0x9E3779B97F4A7C15

	rightLen := len(rv)
	leftLen := len(lv)
	leftNulls := la.NullN() > 0

	k := min(runtime.GOMAXPROCS(0), 8)

	// Phase 1: per-thread histogram over right, counting hits per partition.
	partSize := (rightLen + k - 1) / k
	tHist := make([][nPart]int, k)
	var wg sync.WaitGroup
	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * partSize
			end := min(start+partSize, rightLen)
			var h [nPart]int
			for i := start; i < end; i++ {
				p := uint64(rv[i]) * goldenRatio >> partShift
				h[p]++
			}
			tHist[t] = h
		}(t)
	}
	wg.Wait()

	// Phase 2: serial prefix sum → per-(thread, partition) offsets and
	// global partition boundaries.
	partitionStart := [nPart + 1]int{}
	var tOffsets [][nPart]int = make([][nPart]int, k)
	cum := 0
	for p := range nPart {
		partitionStart[p] = cum
		for t := range k {
			tOffsets[t][p] = cum
			cum += tHist[t][p]
		}
	}
	partitionStart[nPart] = cum

	// Phase 3: parallel scatter of (key, original_index) into the shared
	// scatter buffers. Each thread writes into its pre-computed offsets.
	scatterKeys := make([]int64, rightLen)
	scatterIdxs := make([]int32, rightLen)
	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * partSize
			end := min(start+partSize, rightLen)
			off := tOffsets[t]
			for i := start; i < end; i++ {
				p := uint64(rv[i]) * goldenRatio >> partShift
				o := off[p]
				scatterKeys[o] = rv[i]
				scatterIdxs[o] = int32(i)
				off[p] = o + 1
			}
		}(t)
	}
	wg.Wait()

	// Phase 4: build one intmap per partition in parallel. Each table holds
	// ~rightLen/nPart keys → 32K slots per table at 256K/16. Plus a next[]
	// chain per partition for duplicate-key handling. We also cache the
	// raw slot slice + mask so the probe hot path can inline.
	type partition struct {
		heads  *intmap.Int64
		slots  []intmap.Slot
		mask   uint64
		gen    int32 // generation for the inlined probe loops below
		next   []int32
		hasDup bool
	}
	partitions := make([]partition, nPart)
	var buildWg sync.WaitGroup
	for p := range nPart {
		buildWg.Add(1)
		go func(p int) {
			defer buildWg.Done()
			ps, pe := partitionStart[p], partitionStart[p+1]
			size := pe - ps
			if size == 0 {
				return
			}
			heads := intmap.New(size)
			next := make([]int32, size)
			hasDup := false
			// Build in reverse so head ends up pointing at the earliest row.
			for i := size - 1; i >= 0; i-- {
				key := scatterKeys[ps+i]
				prev, inserted := heads.InsertOrGet(key, int32(i))
				if inserted {
					next[i] = -1
				} else {
					next[i] = prev
					heads.Overwrite(key, int32(i))
					hasDup = true
				}
			}
			slots, mask := heads.RawSlots()
			partitions[p] = partition{heads: heads, slots: slots, mask: mask, gen: heads.CurrentGen(), next: next, hasDup: hasDup}
		}(p)
	}
	buildWg.Wait()
	defer func() {
		for _, pp := range partitions {
			if pp.heads != nil {
				pp.heads.Release()
			}
		}
	}()

	// Phase 5: parallel probe. Each left key hashes to a partition and
	// looks up in that partition's (L1/L2-resident) hash table.
	const noMatch = -1
	chunkSize := (leftLen + k - 1) / k
	lOuts := make([][]int, k)
	rOuts := make([][]int, k)
	anyDuplicates := false
	for _, pp := range partitions {
		if pp.hasDup {
			anyDuplicates = true
			break
		}
	}

	for t := range k {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			start := t * chunkSize
			end := min(start+chunkSize, leftLen)
			size := end - start
			// Fast path: no duplicates and LeftJoin means one output per row.
			// Inlines the hash table probe so the compiler keeps slot/mask in
			// registers and avoids the Get call boundary.
			if !anyDuplicates && !leftNulls && how == LeftJoin {
				localL := make([]int, size)
				localR := make([]int, size)
				for i := start; i < end; i++ {
					key := lv[i]
					h := uint64(key) * goldenRatio
					p := h >> partShift
					pp := partitions[p]
					localL[i-start] = i
					if pp.slots == nil {
						localR[i-start] = noMatch
						continue
					}
					// Inlined Get: linear-probe until Empty or match.
					idx := h & pp.mask
					for {
						s := &pp.slots[idx]
						if s.Gen != pp.gen {
							localR[i-start] = noMatch
							break
						}
						if s.Key == key {
							localR[i-start] = int(scatterIdxs[partitionStart[p]+int(s.Value)])
							break
						}
						idx = (idx + 1) & pp.mask
					}
				}
				lOuts[t] = localL
				rOuts[t] = localR
				return
			}
			if !anyDuplicates && !leftNulls && how == InnerJoin {
				localL := make([]int, 0, size)
				localR := make([]int, 0, size)
				for i := start; i < end; i++ {
					key := lv[i]
					h := uint64(key) * goldenRatio
					p := h >> partShift
					pp := partitions[p]
					if pp.slots == nil {
						continue
					}
					idx := h & pp.mask
					for {
						s := &pp.slots[idx]
						if s.Gen != pp.gen {
							break
						}
						if s.Key == key {
							localL = append(localL, i)
							localR = append(localR, int(scatterIdxs[partitionStart[p]+int(s.Value)]))
							break
						}
						idx = (idx + 1) & pp.mask
					}
				}
				lOuts[t] = localL
				rOuts[t] = localR
				return
			}
			// General chain-aware path.
			cap0 := max(size, 16)
			localL := make([]int, 0, cap0)
			localR := make([]int, 0, cap0)
			for i := start; i < end; i++ {
				if leftNulls && la.IsNull(i) {
					if how == LeftJoin {
						localL = append(localL, i)
						localR = append(localR, noMatch)
					}
					continue
				}
				key := lv[i]
				p := uint64(key) * goldenRatio >> partShift
				pp := partitions[p]
				if pp.heads == nil {
					if how == LeftJoin {
						localL = append(localL, i)
						localR = append(localR, noMatch)
					}
					continue
				}
				head, ok := pp.heads.Get(key)
				if !ok {
					if how == LeftJoin {
						localL = append(localL, i)
						localR = append(localR, noMatch)
					}
					continue
				}
				for j := int(head); j != noMatch; j = int(pp.next[j]) {
					localL = append(localL, i)
					localR = append(localR, int(scatterIdxs[partitionStart[p]+j]))
				}
			}
			lOuts[t] = localL
			rOuts[t] = localR
		}(t)
	}
	wg.Wait()

	total := 0
	for t := range k {
		total += len(lOuts[t])
	}
	lOut := make([]int, total)
	rOut := make([]int, total)
	offset := 0
	for t := range k {
		copy(lOut[offset:], lOuts[t])
		copy(rOut[offset:], rOuts[t])
		offset += len(lOuts[t])
	}
	return lOut, rOut, nil
}

// parallelProbeThreshold picks the minimum left-side size that justifies
// the extra goroutine/concat overhead. Measured empirically on amd64/Go
// 1.26: below 32K left rows the serial probe is faster.
const parallelProbeThreshold = 32 * 1024

func parallelProbe(la arrow.Array, lv []int64, heads *intmap.Int64, next []int, how JoinType, hasDuplicates bool) ([]int, []int, error) {
	leftLen := len(lv)
	const noMatch = -1
	leftNulls := la.NullN() > 0

	if leftLen < parallelProbeThreshold {
		initCap := max(leftLen, 16)
		lOut := make([]int, 0, initCap)
		rOut := make([]int, 0, initCap)
		for i := range leftLen {
			if leftNulls && la.IsNull(i) {
				if how == LeftJoin {
					lOut = append(lOut, i)
					rOut = append(rOut, -1)
				}
				continue
			}
			head, ok := heads.Get(lv[i])
			if !ok {
				if how == LeftJoin {
					lOut = append(lOut, i)
					rOut = append(rOut, -1)
				}
				continue
			}
			for j := int(head); j != noMatch; j = next[j] {
				lOut = append(lOut, i)
				rOut = append(rOut, j)
			}
		}
		return lOut, rOut, nil
	}

	k := min(runtime.GOMAXPROCS(0), 8)
	chunkSize := (leftLen + k - 1) / k
	lOuts := make([][]int, k)
	rOuts := make([][]int, k)

	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := w * chunkSize
			end := min(start+chunkSize, leftLen)
			size := end - start
			// Exact-size fast path: no duplicates on the right and no
			// nulls on the left means each input row emits exactly one
			// output row (match or -1). We can write straight into
			// pre-sized arrays and skip the chain loop entirely.
			if !hasDuplicates && !leftNulls && how == LeftJoin {
				localL := make([]int, size)
				localR := make([]int, size)
				for i := start; i < end; i++ {
					localL[i-start] = i
					head, ok := heads.Get(lv[i])
					if ok {
						localR[i-start] = int(head)
					} else {
						localR[i-start] = -1
					}
				}
				lOuts[w] = localL
				rOuts[w] = localR
				return
			}
			if !hasDuplicates && !leftNulls && how == InnerJoin {
				localL := make([]int, 0, size)
				localR := make([]int, 0, size)
				for i := start; i < end; i++ {
					head, ok := heads.Get(lv[i])
					if ok {
						localL = append(localL, i)
						localR = append(localR, int(head))
					}
				}
				lOuts[w] = localL
				rOuts[w] = localR
				return
			}
			// General chain-aware path.
			cap0 := max(size, 16)
			localL := make([]int, cap0)
			localR := make([]int, cap0)
			w2 := 0
			for i := start; i < end; i++ {
				if leftNulls && la.IsNull(i) {
					if how == LeftJoin {
						if w2 >= len(localL) {
							localL = append(localL, i)
							localR = append(localR, -1)
						} else {
							localL[w2] = i
							localR[w2] = -1
						}
						w2++
					}
					continue
				}
				head, ok := heads.Get(lv[i])
				if !ok {
					if how == LeftJoin {
						if w2 >= len(localL) {
							localL = append(localL, i)
							localR = append(localR, -1)
						} else {
							localL[w2] = i
							localR[w2] = -1
						}
						w2++
					}
					continue
				}
				for j := int(head); j != noMatch; j = next[j] {
					if w2 >= len(localL) {
						localL = append(localL, i)
						localR = append(localR, j)
					} else {
						localL[w2] = i
						localR[w2] = j
					}
					w2++
				}
			}
			lOuts[w] = localL[:w2]
			rOuts[w] = localR[:w2]
		}(w)
	}
	wg.Wait()

	total := 0
	for w := range k {
		total += len(lOuts[w])
	}
	lOut := make([]int, total)
	rOut := make([]int, total)
	offset := 0
	for w := range k {
		copy(lOut[offset:], lOuts[w])
		copy(rOut[offset:], rOuts[w])
		offset += len(lOuts[w])
	}
	return lOut, rOut, nil
}

func toInt64Slice(arr arrow.Array) []int64 {
	switch a := arr.(type) {
	case *array.Int8:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Int16:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Int32:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Int64:
		// Arrow's Int64Values returns a slice that aliases the underlying
		// buffer. Since the join only reads from it and the buffer's ref
		// count is held by the caller, aliasing is safe and saves a 2MB
		// memcpy per 256K-row join.
		return a.Int64Values()
	case *array.Uint8:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Uint16:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Uint32:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	case *array.Uint64:
		out := make([]int64, a.Len())
		for i := range out {
			out[i] = int64(a.Value(i))
		}
		return out
	}
	return nil
}

func hashJoinFloat(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	lv := toFloat64Slice(la)
	rv := toFloat64Slice(ra)

	table := make(map[float64][]int, ra.Len())
	for i := range rv {
		if ra.IsNull(i) || rv[i] != rv[i] {
			// NaN keys never match, polars convention.
			continue
		}
		table[rv[i]] = append(table[rv[i]], i)
	}

	var lOut, rOut []int
	for i := range lv {
		if la.IsNull(i) || lv[i] != lv[i] {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		matches := table[lv[i]]
		if len(matches) == 0 {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		for _, m := range matches {
			lOut = append(lOut, i)
			rOut = append(rOut, m)
		}
	}
	return lOut, rOut, nil
}

func toFloat64Slice(arr arrow.Array) []float64 {
	switch a := arr.(type) {
	case *array.Float32:
		out := make([]float64, a.Len())
		for i := range out {
			out[i] = float64(a.Value(i))
		}
		return out
	case *array.Float64:
		// Alias the arrow buffer; the read-only join doesn't mutate it.
		return a.Float64Values()
	}
	return nil
}

func hashJoinBool(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	lb := la.(*array.Boolean)
	rb := ra.(*array.Boolean)

	var (
		trueIdx  []int
		falseIdx []int
	)
	for i := 0; i < ra.Len(); i++ {
		if ra.IsNull(i) {
			continue
		}
		if rb.Value(i) {
			trueIdx = append(trueIdx, i)
		} else {
			falseIdx = append(falseIdx, i)
		}
	}

	var lOut, rOut []int
	for i := 0; i < la.Len(); i++ {
		if la.IsNull(i) {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		matches := falseIdx
		if lb.Value(i) {
			matches = trueIdx
		}
		if len(matches) == 0 {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		for _, m := range matches {
			lOut = append(lOut, i)
			rOut = append(rOut, m)
		}
	}
	return lOut, rOut, nil
}

func hashJoinString(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	ls := la.(*array.String)
	rs := ra.(*array.String)
	rightLen := ra.Len()
	leftLen := la.Len()

	const noMatch = -1
	heads := make(map[string]int, rightLen/2+1)
	next := make([]int, rightLen)
	rightNulls := ra.NullN() > 0

	for i := rightLen - 1; i >= 0; i-- {
		if rightNulls && ra.IsNull(i) {
			next[i] = noMatch
			continue
		}
		k := rs.Value(i)
		if head, ok := heads[k]; ok {
			next[i] = head
		} else {
			next[i] = noMatch
		}
		heads[k] = i
	}

	initCap := max(leftLen, 16)
	lOut := make([]int, 0, initCap)
	rOut := make([]int, 0, initCap)

	leftNulls := la.NullN() > 0
	for i := range leftLen {
		if leftNulls && la.IsNull(i) {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		head, ok := heads[ls.Value(i)]
		if !ok {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		for j := head; j != noMatch; j = next[j] {
			lOut = append(lOut, i)
			rOut = append(rOut, j)
		}
	}
	return lOut, rOut, nil
}

// buildJoinOutput materializes the result DataFrame from paired indices.
func buildJoinOutput(ctx context.Context, left, right *DataFrame, leftIdx, rightIdx []int, on []string, cfg joinConfig) (*DataFrame, error) {
	// Wrap cfg.alloc with the pool so repeated joins (e.g. a tight
	// benchmark loop or a streaming pipeline) recycle the output arrow
	// buffers instead of churning the Go allocator. Profiling LeftJoin
	// 16K showed ~30% of runtime in runtime.gcStart/mallocgc before
	// pooling; the hash and gather themselves are only ~60% of wall.
	cfg.alloc = compute.PoolingMem(cfg.alloc)
	leftCount := len(leftIdx)

	// Pre-reserve: left width + right width - overlap in `on`.
	outCols := make([]*series.Series, 0, left.Width()+right.Width())
	release := func() {
		for _, c := range outCols {
			if c != nil {
				c.Release()
			}
		}
	}

	// Identity-leftIdx fast path: when each left row produces exactly one
	// output row (e.g. LeftJoin with unique right keys, or InnerJoin with
	// 1:1 matches), leftIdx == [0,1,...,leftLen-1]. In that case the Take
	// is a no-op: we can reuse the left series directly with Retain.
	leftIsIdentity := leftCount == left.Height() && isIdentity(leftIdx)

	// 1. Left columns at leftIdx.
	for _, f := range left.Schema().Fields() {
		src, _ := left.Column(f.Name)
		if leftIsIdentity {
			// Clone wraps a fresh *Series around the same chunked data
			// with a ref increment, so got.Release() and left.Release()
			// each drop one independent ref.
			outCols = append(outCols, src.Clone())
			continue
		}
		out, err := compute.Take(ctx, src, leftIdx, compute.WithAllocator(cfg.alloc))
		if err != nil {
			release()
			return nil, err
		}
		outCols = append(outCols, out)
	}

	// 2. Right columns at rightIdx, skipping join keys; rename on collision.
	onSet := make(map[string]struct{}, len(on))
	for _, k := range on {
		onSet[k] = struct{}{}
	}

	for _, f := range right.Schema().Fields() {
		if _, isKey := onSet[f.Name]; isKey {
			continue
		}
		src, _ := right.Column(f.Name)
		out, err := gatherWithNulls(ctx, src, rightIdx, leftCount, cfg.alloc)
		if err != nil {
			release()
			return nil, err
		}
		name := f.Name
		if left.Schema().Contains(name) {
			name = name + cfg.suffix
			renamed := out.Rename(name)
			out.Release()
			out = renamed
		}
		outCols = append(outCols, out)
	}

	return New(outCols...)
}

// isIdentity reports whether indices equals [0, 1, ..., len(indices)-1].
// Scans the tail first: joins with any non-match break the identity early,
// and worker partitions each start with their chunk base, so a mismatch
// tends to appear late in the slice, not in the first chunk.
func isIdentity(indices []int) bool {
	for i, v := range indices {
		if v != i {
			return false
		}
	}
	return true
}

// gatherWithNulls is like compute.Take but treats -1 indices as null entries.
// It avoids the all-non-null path when there are no -1s by delegating to Take.
func gatherWithNulls(ctx context.Context, src *series.Series, indices []int, n int, alloc memory.Allocator) (*series.Series, error) {
	hasMiss := false
	for _, i := range indices {
		if i < 0 {
			hasMiss = true
			break
		}
	}
	if !hasMiss {
		return compute.Take(ctx, src, indices, compute.WithAllocator(alloc))
	}

	// Construct a safe indices slice (replace -1 with 0) and take, then patch
	// nulls via a bool mask. Simpler: build per-type directly.
	return gatherNullable(ctx, src, indices, n, alloc)
}

func gatherNullable(ctx context.Context, src *series.Series, indices []int, n int, alloc memory.Allocator) (*series.Series, error) {
	_ = ctx
	_ = n
	chunk := src.Chunk(0)
	name := src.Name()

	switch src.DType().ID() {
	case arrow.INT64:
		raw := chunk.(*array.Int64).Int64Values()
		srcNulls := chunk.NullN() > 0
		nIdx := len(indices)
		return series.BuildInt64DirectFused(name, nIdx, alloc, func(out []int64, validBits []byte) int {
			// Per-chunk scan: fill value + validity bitmap in a single
			// pass over indices. Each worker owns a byte-aligned range of
			// 8-row groups so bitmap writes never cross workers.
			// 8-way batched gather: compute all 8 indices first so the CPU
			// can pipeline the random raw[i] loads under a single
			// out-of-order window, rather than serializing one load at a
			// time through the branch.
			nBytes := (nIdx + 7) / 8
			nullCounts := make([]int, 1)
			worker := func(byteStart, byteEnd int) int {
				nc := 0
				// Fast path: no source nulls + full 8-row groups (all but
				// maybe the last byte). Branchless mask construction via
				// sign-bit extraction of the int index.
				lastFull := byteEnd
				if byteEnd*8 > nIdx {
					lastFull = byteEnd - 1
				}
				if !srcNulls {
					for bi := byteStart; bi < lastFull; bi++ {
						rowStart := bi * 8
						// Snapshot 8 indices upfront. These 8 loads are
						// sequential so the HW prefetcher handles them.
						i0 := indices[rowStart]
						i1 := indices[rowStart+1]
						i2 := indices[rowStart+2]
						i3 := indices[rowStart+3]
						i4 := indices[rowStart+4]
						i5 := indices[rowStart+5]
						i6 := indices[rowStart+6]
						i7 := indices[rowStart+7]
						// Clamp negative indices to 0 so raw[] loads never
						// OOB: the result value is discarded when invalid.
						c0 := i0 & (^(i0 >> 63))
						c1 := i1 & (^(i1 >> 63))
						c2 := i2 & (^(i2 >> 63))
						c3 := i3 & (^(i3 >> 63))
						c4 := i4 & (^(i4 >> 63))
						c5 := i5 & (^(i5 >> 63))
						c6 := i6 & (^(i6 >> 63))
						c7 := i7 & (^(i7 >> 63))
						// Now issue 8 potentially-random loads. The CPU's
						// out-of-order engine can overlap them under a
						// single 200-instruction ROB window.
						v0 := raw[c0]
						v1 := raw[c1]
						v2 := raw[c2]
						v3 := raw[c3]
						v4 := raw[c4]
						v5 := raw[c5]
						v6 := raw[c6]
						v7 := raw[c7]
						// Compute validity bits from the sign bit of each
						// original index: bit=1 when index>=0.
						var b byte
						if i0 >= 0 {
							out[rowStart] = v0
							b |= 1
						} else {
							nc++
						}
						if i1 >= 0 {
							out[rowStart+1] = v1
							b |= 1 << 1
						} else {
							nc++
						}
						if i2 >= 0 {
							out[rowStart+2] = v2
							b |= 1 << 2
						} else {
							nc++
						}
						if i3 >= 0 {
							out[rowStart+3] = v3
							b |= 1 << 3
						} else {
							nc++
						}
						if i4 >= 0 {
							out[rowStart+4] = v4
							b |= 1 << 4
						} else {
							nc++
						}
						if i5 >= 0 {
							out[rowStart+5] = v5
							b |= 1 << 5
						} else {
							nc++
						}
						if i6 >= 0 {
							out[rowStart+6] = v6
							b |= 1 << 6
						} else {
							nc++
						}
						if i7 >= 0 {
							out[rowStart+7] = v7
							b |= 1 << 7
						} else {
							nc++
						}
						validBits[bi] = b
					}
					// Remainder (last byte may be partial).
					for bi := lastFull; bi < byteEnd; bi++ {
						rowStart := bi * 8
						rowEnd := min(rowStart+8, nIdx)
						var b byte
						for j := rowStart; j < rowEnd; j++ {
							i := indices[j]
							if i < 0 {
								nc++
								continue
							}
							out[j] = raw[i]
							b |= 1 << (j - rowStart)
						}
						validBits[bi] = b
					}
					return nc
				}
				// srcNulls path: can't skip the per-row IsValid check.
				for bi := byteStart; bi < byteEnd; bi++ {
					rowStart := bi * 8
					rowEnd := min(rowStart+8, nIdx)
					var b byte
					for j := rowStart; j < rowEnd; j++ {
						i := indices[j]
						if i < 0 {
							nc++
							continue
						}
						if !chunk.IsValid(i) {
							nc++
							continue
						}
						out[j] = raw[i]
						b |= 1 << (j - rowStart)
					}
					validBits[bi] = b
				}
				return nc
			}
			if nIdx >= 64*1024 {
				k := min(8, runtime.GOMAXPROCS(0))
				bytesPerWorker := (nBytes + k - 1) / k
				partialNulls := make([]int, k)
				var wg sync.WaitGroup
				for w := range k {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						bs := w * bytesPerWorker
						be := min(bs+bytesPerWorker, nBytes)
						partialNulls[w] = worker(bs, be)
					}(w)
				}
				wg.Wait()
				total := 0
				for _, x := range partialNulls {
					total += x
				}
				nullCounts[0] = total
				return total
			}
			nullCounts[0] = worker(0, nBytes)
			return nullCounts[0]
		})
	case arrow.INT32:
		raw := chunk.(*array.Int32).Int32Values()
		out := make([]int32, len(indices))
		valid := make([]bool, len(indices))
		for j, i := range indices {
			if i < 0 {
				continue
			}
			valid[j] = chunk.IsValid(i)
			if valid[j] {
				out[j] = raw[i]
			}
		}
		return series.FromInt32(name, out, valid, series.WithAllocator(alloc))
	case arrow.FLOAT64:
		raw := chunk.(*array.Float64).Float64Values()
		srcNulls := chunk.NullN() > 0
		nIdx := len(indices)
		return series.BuildFloat64DirectFused(name, nIdx, alloc, func(out []float64, validBits []byte) int {
			nBytes := (nIdx + 7) / 8
			worker := func(byteStart, byteEnd int) int {
				nc := 0
				for bi := byteStart; bi < byteEnd; bi++ {
					rowStart := bi * 8
					rowEnd := min(rowStart+8, nIdx)
					var b byte
					for j := rowStart; j < rowEnd; j++ {
						i := indices[j]
						if i < 0 {
							nc++
							continue
						}
						if srcNulls && !chunk.IsValid(i) {
							nc++
							continue
						}
						out[j] = raw[i]
						b |= 1 << (j - rowStart)
					}
					validBits[bi] = b
				}
				return nc
			}
			if nIdx >= 64*1024 {
				k := min(8, runtime.GOMAXPROCS(0))
				bytesPerWorker := (nBytes + k - 1) / k
				partialNulls := make([]int, k)
				var wg sync.WaitGroup
				for w := range k {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						bs := w * bytesPerWorker
						be := min(bs+bytesPerWorker, nBytes)
						partialNulls[w] = worker(bs, be)
					}(w)
				}
				wg.Wait()
				total := 0
				for _, x := range partialNulls {
					total += x
				}
				return total
			}
			return worker(0, nBytes)
		})
	case arrow.FLOAT32:
		raw := chunk.(*array.Float32).Float32Values()
		out := make([]float32, len(indices))
		valid := make([]bool, len(indices))
		for j, i := range indices {
			if i < 0 {
				continue
			}
			valid[j] = chunk.IsValid(i)
			if valid[j] {
				out[j] = raw[i]
			}
		}
		return series.FromFloat32(name, out, valid, series.WithAllocator(alloc))
	case arrow.BOOL:
		b := chunk.(*array.Boolean)
		out := make([]bool, len(indices))
		valid := make([]bool, len(indices))
		for j, i := range indices {
			if i < 0 {
				continue
			}
			valid[j] = chunk.IsValid(i)
			if valid[j] {
				out[j] = b.Value(i)
			}
		}
		return series.FromBool(name, out, valid, series.WithAllocator(alloc))
	case arrow.STRING:
		s := chunk.(*array.String)
		out := make([]string, len(indices))
		valid := make([]bool, len(indices))
		for j, i := range indices {
			if i < 0 {
				continue
			}
			valid[j] = chunk.IsValid(i)
			if valid[j] {
				out[j] = s.Value(i)
			}
		}
		return series.FromString(name, out, valid, series.WithAllocator(alloc))
	}
	return nil, fmt.Errorf("dataframe.Join: unsupported result dtype %s", src.DType())
}

// crossJoin emits the Cartesian product of left and right.
func crossJoin(ctx context.Context, left, right *DataFrame, cfg joinConfig) (*DataFrame, error) {
	total := left.Height() * right.Height()
	leftIdx := make([]int, 0, total)
	rightIdx := make([]int, 0, total)
	for i := 0; i < left.Height(); i++ {
		for j := 0; j < right.Height(); j++ {
			leftIdx = append(leftIdx, i)
			rightIdx = append(rightIdx, j)
		}
	}
	return buildJoinOutput(ctx, left, right, leftIdx, rightIdx, nil, cfg)
}
