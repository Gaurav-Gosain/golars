package dataframe

import (
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/strhash"
)

// String-key hash join.
//
// Both sides are hashed once (in parallel when large). Rows are then
// bucketed into partitions by the top hash bits, keeping ascending row
// order inside each partition. Each right partition gets its own
// open-addressing table, built in parallel; a slot stores the full
// hash, the key inline when it is at most 8 bytes, and the smallest
// right row index of its key, with further rows of the same key on a
// next[] chain in ascending order. Each left partition is probed
// against only its own table, so at large sizes the probes hit a table
// that fits in L2 instead of missing cache on one big table. The probe
// records one match head per left row; the output is then emitted in
// left row order and, per left row, ascending right row order. That is
// the order the previous map[string]int implementation produced. Null
// keys never match (polars join_nulls=False); for a left join a null
// left key emits a row with no right match.

// strJoinParallelMin is the side length where hashing, partitioning
// and probing fan out. Below it a single goroutine does all the work.
const strJoinParallelMin = 32 * 1024

// strSlot is one hash table entry. Keys of at most 8 bytes are also
// stored inline (packed in key, length in klen) so the equality check
// does not touch the right side's string data, which would be a random
// access per probe.
type strSlot struct {
	hash uint64
	key  uint64
	head int32 // right row index + 1; 0 means empty
	klen int32
}

type strTableJoin struct {
	slots []strSlot
	mask  uint64
}

// packShortKey packs a key of at most 8 bytes into a uint64. For a
// fixed length the packing is injective, which is all the equality
// check needs (it also compares lengths). Longer keys return ok=false
// and are compared through the string data.
func packShortKey(s string) (uint64, bool) {
	n := len(s)
	switch {
	case n > 8:
		return 0, false
	case n == 8:
		return binary.LittleEndian.Uint64(unsafe.Slice(unsafe.StringData(s), 8)), true
	case n >= 4:
		// Two overlapping 4-byte loads cover every byte.
		p := unsafe.Slice(unsafe.StringData(s), n)
		return uint64(binary.LittleEndian.Uint32(p)) | uint64(binary.LittleEndian.Uint32(p[n-4:]))<<32, true
	}
	var v uint64
	for i := n - 1; i >= 0; i-- {
		v = v<<8 | uint64(s[i])
	}
	return v, true
}

func strJoinWorkers(n int) int {
	if n < strJoinParallelMin {
		return 1
	}
	return min(runtime.GOMAXPROCS(0), 8, max(1, n/strJoinParallelMin))
}

// parallelRanges runs fn over [0,n) split into w contiguous ranges.
func parallelRanges(n, w int, fn func(part, lo, hi int)) {
	if w <= 1 {
		fn(0, 0, n)
		return
	}
	step := (n + w - 1) / w
	var wg sync.WaitGroup
	for p := range w {
		lo := p * step
		hi := min(lo+step, n)
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(p, lo, hi)
		}()
	}
	wg.Wait()
}

// forEachPartition runs fn(p) for every partition on a bounded pool.
func forEachPartition(nPart int, fn func(p int)) {
	if nPart == 1 {
		fn(0)
		return
	}
	workers := min(runtime.GOMAXPROCS(0), 8, nPart)
	var wg sync.WaitGroup
	var cur atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				p := int(cur.Add(1) - 1)
				if p >= nPart {
					return
				}
				fn(p)
			}
		}()
	}
	wg.Wait()
}

func hashStrings(s *array.String, workers int) []uint64 {
	n := s.Len()
	out := make([]uint64, n)
	parallelRanges(n, workers, func(_, lo, hi int) {
		for i := lo; i < hi; i++ {
			out[i] = strhash.String(s.Value(i))
		}
	})
	return out
}

// strRow is one key in partition order: everything the build and probe
// loops need without going back to the (randomly ordered) source rows.
// Keys longer than 8 bytes still read the string data for equality.
type strRow struct {
	hash uint64
	key  uint64 // packed key when klen <= 8
	row  int32
	klen int32
}

// partitionRows buckets the rows of s by the top partBits of their hash
// h. It returns the rows as strRow records in partition order, with
// partition p at bounds[p]:bounds[p+1] and ascending row order inside a
// partition. Null rows are left out. Large inputs use per-worker
// histograms and a parallel scatter; each worker owns a contiguous row
// range and its own write window per partition, so ascending order is
// preserved and the reads of h and s stay sequential.
func partitionRows(s *array.String, h []uint64, partBits int, workers int) ([]strRow, []int) {
	nPart := 1 << partBits
	shift := uint(64 - partBits)
	partOf := func(x uint64) int {
		if partBits == 0 {
			return 0
		}
		return int(x >> shift)
	}
	hasNulls := s.NullN() > 0
	n := len(h)
	workers = max(workers, 1)
	hist := make([][]int, workers)
	parallelRanges(n, workers, func(w, lo, hi int) {
		c := make([]int, nPart)
		for i := lo; i < hi; i++ {
			if hasNulls && s.IsNull(i) {
				continue
			}
			c[partOf(h[i])]++
		}
		hist[w] = c
	})
	bounds := make([]int, nPart+1)
	offs := make([][]int, workers)
	for w := range offs {
		offs[w] = make([]int, nPart)
	}
	cum := 0
	for p := range nPart {
		bounds[p] = cum
		for w := range workers {
			offs[w][p] = cum
			if hist[w] != nil {
				cum += hist[w][p]
			}
		}
	}
	bounds[nPart] = cum
	rows := make([]strRow, cum)
	parallelRanges(n, workers, func(w, lo, hi int) {
		off := offs[w]
		for i := lo; i < hi; i++ {
			if hasNulls && s.IsNull(i) {
				continue
			}
			k := s.Value(i)
			packed, _ := packShortKey(k)
			p := partOf(h[i])
			rows[off[p]] = strRow{hash: h[i], key: packed, row: int32(i), klen: int32(len(k))}
			off[p]++
		}
	})
	return rows, bounds
}

// matchesRow reports whether the slot holds the key of r. src is the
// array r was taken from; it is only read for keys over 8 bytes.
func (sl *strSlot) matchesRow(r *strRow, src, rs *array.String) bool {
	if sl.hash != r.hash || sl.klen != r.klen {
		return false
	}
	if r.klen <= 8 {
		return sl.key == r.key
	}
	return rs.Value(int(sl.head-1)) == src.Value(int(r.row))
}

func hashJoinString(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	ls := la.(*array.String)
	rs := ra.(*array.String)
	rightLen := rs.Len()
	leftLen := ls.Len()
	rWorkers := strJoinWorkers(rightLen)
	lWorkers := strJoinWorkers(leftLen)

	rh := hashStrings(rs, rWorkers)
	lh := hashStrings(ls, lWorkers)

	// Partition count: one table per ~16K right rows keeps each table
	// (24 bytes a slot at load <= 2/3) around 512 KB, inside L2.
	partBits := 0
	if rightLen >= strJoinParallelMin {
		partBits = min(8, max(1, bits.Len(uint(rightLen/(16*1024)))))
	}
	nPart := 1 << partBits

	rRows, rBounds := partitionRows(rs, rh, partBits, rWorkers)

	next := make([]int32, rightLen) // right row index + 1; 0 ends a chain
	tables := make([]strTableJoin, nPart)
	hasDup := make([]bool, nPart)
	forEachPartition(nPart, func(p int) {
		part := rRows[rBounds[p]:rBounds[p+1]]
		// Load factor at most 2/3 with linear probing.
		size := uint64(1) << bits.Len(uint(max(len(part)+len(part)/2, 8)-1))
		t := strTableJoin{slots: make([]strSlot, size), mask: size - 1}
		// Insert in descending row order so each chain ends up ascending.
		for k := len(part) - 1; k >= 0; k-- {
			r := &part[k]
			for pos := r.hash & t.mask; ; pos = (pos + 1) & t.mask {
				sl := &t.slots[pos]
				if sl.head == 0 {
					*sl = strSlot{hash: r.hash, key: r.key, head: r.row + 1, klen: r.klen}
					break
				}
				if sl.matchesRow(r, rs, rs) {
					next[r.row] = sl.head
					sl.head = r.row + 1
					hasDup[p] = true
					break
				}
			}
		}
		tables[p] = t
	})
	anyDup := false
	for _, d := range hasDup {
		anyDup = anyDup || d
	}

	// Probe: match[i] is the chain head (right row + 1) for left row i,
	// 0 for no match or a null key.
	match := make([]int32, leftLen)
	lRows, lBounds := partitionRows(ls, lh, partBits, lWorkers)
	probe := func(t *strTableJoin, part []strRow) {
		for k := range part {
			r := &part[k]
			for pos := r.hash & t.mask; ; pos = (pos + 1) & t.mask {
				sl := &t.slots[pos]
				if sl.head == 0 {
					break
				}
				if sl.matchesRow(r, ls, rs) {
					match[r.row] = sl.head
					break
				}
			}
		}
	}
	if nPart == 1 {
		// One table: split the probe by left row range instead.
		parallelRanges(len(lRows), lWorkers, func(_, lo, hi int) {
			probe(&tables[0], lRows[lo:hi])
		})
	} else {
		forEachPartition(nPart, func(p int) {
			probe(&tables[p], lRows[lBounds[p]:lBounds[p+1]])
		})
	}

	return emitStrJoin(match, next, anyDup, how, lWorkers)
}

// emitStrJoin turns per-left-row chain heads into the paired index
// arrays. Each worker owns a contiguous left range: a counting pass
// sizes its output window, then a second pass writes it in place.
func emitStrJoin(match, next []int32, anyDup bool, how JoinType, workers int) ([]int, []int, error) {
	n := len(match)
	isLeft := how == LeftJoin
	if !anyDup && isLeft {
		// Exactly one output row per left row.
		lOut := make([]int, n)
		rOut := make([]int, n)
		parallelRanges(n, workers, func(_, lo, hi int) {
			for i := lo; i < hi; i++ {
				lOut[i] = i
				rOut[i] = int(match[i]) - 1
			}
		})
		return lOut, rOut, nil
	}
	rowOut := func(i int) int {
		head := match[i]
		if head == 0 {
			if isLeft {
				return 1
			}
			return 0
		}
		if !anyDup {
			return 1
		}
		c := 0
		for j := head; j != 0; j = next[j-1] {
			c++
		}
		return c
	}
	workers = max(workers, 1)
	counts := make([]int, workers+1)
	parallelRanges(n, workers, func(w, lo, hi int) {
		c := 0
		for i := lo; i < hi; i++ {
			c += rowOut(i)
		}
		counts[w+1] = c
	})
	for w := range workers {
		counts[w+1] += counts[w]
	}
	total := counts[workers]
	lOut := make([]int, total)
	rOut := make([]int, total)
	parallelRanges(n, workers, func(w, lo, hi int) {
		pos := counts[w]
		for i := lo; i < hi; i++ {
			head := match[i]
			if head == 0 {
				if isLeft {
					lOut[pos] = i
					rOut[pos] = -1
					pos++
				}
				continue
			}
			for j := head; j != 0; j = next[j-1] {
				lOut[pos] = i
				rOut[pos] = int(j - 1)
				pos++
			}
		}
	})
	return lOut, rOut, nil
}
