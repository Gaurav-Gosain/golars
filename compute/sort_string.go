package compute

import (
	"encoding/binary"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// String sort without a comparator.
//
// The generic path sorts []int indices with a closure that calls
// IsValid and Value on every comparison, which costs about 20 string
// compares per row plus two indirect calls each. This path instead
// sorts (key, row) entries where key is the next 8 bytes of the string
// read big-endian and zero padded. Entries are ordered with a stable
// LSD radix sort, then every run of equal keys is refined on the next
// 8 bytes (MSD recursion on the prefix). A run whose strings all end
// inside the current window holds strings that differ only in trailing
// zero padding, so the shorter string is the smaller one; those runs
// are finished with a stable sort on length.
//
// Every step is stable, so equal strings keep their input order, which
// matches the comparator path (and polars with maintain_order=True).
// Bytes compare as unsigned, which is Go's string order and polars'
// binary UTF-8 order.

type strSortEntry struct {
	key uint64
	row int
}

// strSortSmall is the run length below which insertion sort beats the
// radix passes.
const strSortSmall = 48

// sortIndicesString returns the stable sort permutation for a string
// array, honoring direction and null placement.
func sortIndicesString(a *array.String, so SortOptions) []int {
	n := a.Len()
	out := make([]int, 0, n)
	var nulls []int
	entries := make([]strSortEntry, 0, n)
	if a.NullN() == 0 {
		for i := range n {
			entries = append(entries, strSortEntry{row: i})
		}
	} else {
		nulls = make([]int, 0, a.NullN())
		for i := range n {
			if a.IsValid(i) {
				entries = append(entries, strSortEntry{row: i})
			} else {
				nulls = append(nulls, i)
			}
		}
	}

	offsets := a.ValueOffsets()
	values := a.ValueBytes()
	base := int32(0)
	if len(offsets) > 0 {
		base = offsets[0]
	}
	ss := strSorter{
		offsets: offsets,
		values:  values,
		base:    base,
		desc:    so.Descending,
	}
	ss.sortTop(entries)

	if so.Nulls == NullsFirst {
		out = append(out, nulls...)
	}
	for _, e := range entries {
		out = append(out, e.row)
	}
	if so.Nulls != NullsFirst {
		out = append(out, nulls...)
	}
	return out
}

type strSorter struct {
	offsets []int32
	values  []byte
	base    int32
	desc    bool
}

// strSortParallelMin is the entry count above which the top level
// computes keys and refines runs on several goroutines.
const strSortParallelMin = 1 << 16

// sortTop sorts the full entry set. Large inputs compute first-level
// keys in parallel, radix sort once, then refine the independent runs
// of equal keys on a worker pool. Runs occupy disjoint ranges of es and
// of the scratch buffer, so workers share nothing.
func (ss *strSorter) sortTop(es []strSortEntry) {
	tmp := make([]strSortEntry, len(es))
	workers := runtime.GOMAXPROCS(0)
	if len(es) < strSortParallelMin || workers < 2 {
		ss.sortRun(es, tmp, 0)
		return
	}
	maxLens := make([]int, workers)
	chunk := (len(es) + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		lo := min(w*chunk, len(es))
		hi := min(lo+chunk, len(es))
		wg.Go(func() {
			m := 0
			for i := lo; i < hi; i++ {
				es[i].key = ss.keyAt(es[i].row, 0)
				s, e := ss.bounds(es[i].row)
				m = max(m, e-s)
			}
			maxLens[w] = m
		})
	}
	wg.Wait()
	maxLen := slices.Max(maxLens)

	type span struct{ lo, hi int }
	runPool := func(runs []span, fn func(r span)) {
		var next atomic.Int64
		for range workers {
			wg.Go(func() {
				for {
					i := int(next.Add(1)) - 1
					if i >= len(runs) {
						return
					}
					fn(runs[i])
				}
			})
		}
		wg.Wait()
	}

	// When the leading byte spreads the rows well, partition on it
	// (one counting pass and one scatter) and sort each partition on
	// its own worker. The per-partition radix skips the leading-byte
	// pass since it is constant there.
	var top [256]int
	for _, e := range es {
		top[e.key>>56]++
	}
	if slices.Max(top[:]) <= len(es)/2 {
		var parts []span
		sum := 0
		for b, c := range top {
			top[b] = sum
			if c > 1 {
				parts = append(parts, span{sum, sum + c})
			}
			sum += c
		}
		for _, e := range es {
			b := e.key >> 56
			tmp[top[b]] = e
			top[b]++
		}
		copy(es, tmp)
		runPool(parts, func(r span) {
			part, scratch := es[r.lo:r.hi], tmp[r.lo:r.hi]
			if len(part) < strSortSmall {
				insertionSortStrEntries(part)
			} else {
				radixSortStrEntries(part, scratch)
			}
			for lo := 0; lo < len(part); {
				hi := lo + 1
				for hi < len(part) && part[hi].key == part[lo].key {
					hi++
				}
				if hi-lo > 1 {
					ss.refine(part[lo:hi], scratch[lo:hi], 8, maxLen)
				}
				lo = hi
			}
		})
		return
	}

	radixSortStrEntries(es, tmp)
	var runs []span
	for lo := 0; lo < len(es); {
		hi := lo + 1
		for hi < len(es) && es[hi].key == es[lo].key {
			hi++
		}
		if hi-lo > 1 {
			runs = append(runs, span{lo, hi})
		}
		lo = hi
	}
	runPool(runs, func(r span) {
		ss.refine(es[r.lo:r.hi], tmp[r.lo:r.hi], 8, maxLen)
	})
}

func (ss *strSorter) bounds(row int) (int, int) {
	return int(ss.offsets[row] - ss.base), int(ss.offsets[row+1] - ss.base)
}

// keyAt reads bytes [s+depth, s+depth+8) of a row big-endian, zero
// padded past the end of the string. Descending sorts invert the key
// so the same ascending radix produces the reversed order.
func (ss *strSorter) keyAt(row, depth int) uint64 {
	s, e := ss.bounds(row)
	p := s + depth
	var k uint64
	if p+8 <= e {
		k = binary.BigEndian.Uint64(ss.values[p : p+8])
	} else if p < e {
		for j := 0; j < 8; j++ {
			k <<= 8
			if p+j < e {
				k |= uint64(ss.values[p+j])
			}
		}
	}
	if ss.desc {
		k = ^k
	}
	return k
}

// sortRun sorts es by the string bytes starting at depth. All entries
// in es are known to share their first depth bytes. tmp is scratch of
// the same length.
func (ss *strSorter) sortRun(es, tmp []strSortEntry, depth int) {
	if len(es) < 2 {
		return
	}
	maxLen := 0
	for i := range es {
		es[i].key = ss.keyAt(es[i].row, depth)
		s, e := ss.bounds(es[i].row)
		maxLen = max(maxLen, e-s)
	}
	if len(es) < strSortSmall {
		insertionSortStrEntries(es)
	} else {
		radixSortStrEntries(es, tmp)
	}
	// Refine every run of equal keys.
	next := depth + 8
	for lo := 0; lo < len(es); {
		hi := lo + 1
		for hi < len(es) && es[hi].key == es[lo].key {
			hi++
		}
		if hi-lo > 1 {
			ss.refine(es[lo:hi], tmp[lo:hi], next, maxLen)
		}
		lo = hi
	}
}

// refine orders a run whose members agree on every byte before depth.
func (ss *strSorter) refine(run, tmp []strSortEntry, depth, maxLen int) {
	if maxLen > depth && ss.anyLongerThan(run, depth) {
		ss.sortRun(run, tmp, depth)
	} else {
		ss.sortByLen(run)
	}
}

func (ss *strSorter) anyLongerThan(es []strSortEntry, n int) bool {
	for _, e := range es {
		s, t := ss.bounds(e.row)
		if t-s > n {
			return true
		}
	}
	return false
}

// sortByLen finishes a run whose members agree on every byte up to
// their own length and are zero beyond it. Shorter is smaller.
func (ss *strSorter) sortByLen(es []strSortEntry) {
	first := -1
	same := true
	for _, e := range es {
		s, t := ss.bounds(e.row)
		if first < 0 {
			first = t - s
		} else if t-s != first {
			same = false
			break
		}
	}
	if same {
		return
	}
	slices.SortStableFunc(es, func(x, y strSortEntry) int {
		xs, xe := ss.bounds(x.row)
		ys, ye := ss.bounds(y.row)
		d := (xe - xs) - (ye - ys)
		if ss.desc {
			d = -d
		}
		return d
	})
}

func insertionSortStrEntries(es []strSortEntry) {
	for i := 1; i < len(es); i++ {
		v := es[i]
		j := i - 1
		for j >= 0 && es[j].key > v.key {
			es[j+1] = es[j]
			j--
		}
		es[j+1] = v
	}
}

// radixSortStrEntries is a stable LSD radix sort on the 64-bit key with
// 8-bit digits. Digits where every entry lands in one bucket are
// skipped, which is common for ASCII prefixes.
func radixSortStrEntries(es, tmp []strSortEntry) {
	var counts [8][256]int
	for _, e := range es {
		k := e.key
		counts[0][k&0xff]++
		counts[1][(k>>8)&0xff]++
		counts[2][(k>>16)&0xff]++
		counts[3][(k>>24)&0xff]++
		counts[4][(k>>32)&0xff]++
		counts[5][(k>>40)&0xff]++
		counts[6][(k>>48)&0xff]++
		counts[7][(k>>56)&0xff]++
	}
	n := len(es)
	src, dst := es, tmp
	for pass := range 8 {
		c := &counts[pass]
		if slices.Contains(c[:], n) {
			continue
		}
		sum := 0
		for i := range c {
			v := c[i]
			c[i] = sum
			sum += v
		}
		shift := uint(pass * 8)
		for _, e := range src {
			d := (e.key >> shift) & 0xff
			dst[c[d]] = e
			c[d]++
		}
		src, dst = dst, src
	}
	if &src[0] != &es[0] {
		copy(es, src)
	}
}
