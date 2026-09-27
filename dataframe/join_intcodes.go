package dataframe

import (
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

// Join key codes only need to be dense and equal for equal keys; unlike
// group ids they need no first-seen numbering. That allows two cheaper
// encoders for integer keys than the group-by ones in keycodes.go:
//
//   - a direct offset (code = value - min) whenever the value range is
//     at most a few times the row count, which covers id-like keys;
//   - otherwise a partitioned hash encoder: rows are split by a hash of
//     the key into partitions, each worker numbers the keys of its own
//     partitions with a private table, and partition bases turn the
//     local numbers into global ones. Every key lives in exactly one
//     partition, so no merge is needed.

// joinDenseFactor bounds the direct table: the value range may be at
// most this many times the row count.
const joinDenseFactor = 4

// joinDenseMaxSpan caps the direct table size in slots.
const joinDenseMaxSpan = 1 << 27

// joinHashParMin is the row count from which integer keys are hashed in
// parallel partitions.
const joinHashParMin = 128 * 1024

const joinHashPartBits = 6

// encodeJoinIntCodes numbers an int32 or int64 key array. Null rows get
// the last code. codes comes from int32Scratch.
func encodeJoinIntCodes(a arrow.Array) (codes []int32, card int, ok bool) {
	switch t := a.(type) {
	case *array.Int64:
		codes, card = encodeJoinInts(t.Int64Values(), t)
		return codes, card, true
	case *array.Int32:
		codes, card = encodeJoinInts(t.Int32Values(), t)
		return codes, card, true
	}
	return nil, 0, false
}

// encodeJoinInts numbers vals; a supplies the validity and may be nil
// when there are no nulls.
func encodeJoinInts[T int32 | int64](vals []T, a arrow.Array) ([]int32, int) {
	n := len(vals)
	codes := int32Scratch.get(n)
	hasNulls := a != nil && a.NullN() > 0
	lo, hi, found := validIntRange(vals, a, hasNulls)
	if !found {
		// Empty or all null: one code for the nulls.
		clear(codes)
		return codes, 1
	}
	span := uint64(int64(hi)) - uint64(int64(lo))
	if span < uint64(max(joinDenseFactor*n, 1024)) && span < joinDenseMaxSpan {
		nullCode := int32(span + 1)
		fill := func(_, start, end int) {
			for i := start; i < end; i++ {
				codes[i] = int32(int64(vals[i]) - int64(lo))
			}
			if hasNulls {
				for i := start; i < end; i++ {
					if a.IsNull(i) {
						codes[i] = nullCode
					}
				}
			}
		}
		if k := denseWorkers(n); n >= denseParThreshold && k > 1 {
			runChunks(n, k, fill)
		} else {
			fill(0, 0, n)
		}
		return codes, int(span) + 2
	}
	if n < joinHashParMin {
		m := intmap.New(64)
		defer m.Release()
		next := int32(0)
		nullCode := int32(-1)
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				if nullCode < 0 {
					nullCode = next
					next++
				}
				codes[i] = nullCode
				continue
			}
			id, inserted := m.InsertOrGet(int64(v), next)
			if inserted {
				next++
			}
			codes[i] = id
		}
		return codes, int(next)
	}
	return codes, encodeJoinIntsPartitioned(vals, a, hasNulls, codes)
}

func validIntRange[T int32 | int64](vals []T, a arrow.Array, hasNulls bool) (lo, hi T, ok bool) {
	if len(vals) == 0 {
		return 0, 0, false
	}
	if a, isI64 := any(vals).([]int64); isI64 && !hasNulls {
		l, h, ok := int64Range(a)
		return T(l), T(h), ok
	}
	found := false
	for i, v := range vals {
		if hasNulls && a.IsNull(i) {
			continue
		}
		if !found {
			lo, hi, found = v, v, true
			continue
		}
		lo = min(lo, v)
		hi = max(hi, v)
	}
	return lo, hi, found
}

// combineJoinCodes folds per-column codes into one code per row
// (mixed radix). A small combined space is used directly; a larger one
// is renumbered with the integer encoder. ok=false means the combined
// space overflows 62 bits.
func combineJoinCodes(cols [][]int32, cards []int, n int, out []int32) (int, bool) {
	product := uint64(1)
	for _, c := range cards {
		hi, lo := bits.Mul64(product, uint64(max(c, 1)))
		if hi != 0 || lo >= 1<<62 {
			return 0, false
		}
		product = lo
	}
	k := denseWorkers(n)
	par := n >= denseParThreshold && k > 1
	run := func(fn func(_, start, end int)) {
		if par {
			runChunks(n, k, fn)
		} else {
			fn(0, 0, n)
		}
	}
	if product < uint64(max(joinDenseFactor*n, 1024)) && product < joinDenseMaxSpan {
		run(func(_, start, end int) {
			seg := out[start:end]
			copy(seg, cols[0][start:end])
			for c, codes := range cols[1:] {
				card := int32(max(cards[c+1], 1))
				for i, v := range codes[start:end] {
					seg[i] = seg[i]*card + v
				}
			}
		})
		return int(product), true
	}
	keys := make([]int64, n)
	run(func(_, start, end int) {
		seg := keys[start:end]
		for i, v := range cols[0][start:end] {
			seg[i] = int64(v)
		}
		for c, codes := range cols[1:] {
			card := int64(max(cards[c+1], 1))
			for i, v := range codes[start:end] {
				seg[i] = seg[i]*card + int64(v)
			}
		}
	})
	codes, card := encodeJoinInts(keys, nil)
	copy(out, codes)
	int32Scratch.put(codes)
	return card, true
}

func joinKeyPart(v int64) int {
	return int((uint64(v) * 0x9E3779B97F4A7C15) >> (64 - joinHashPartBits))
}

// encodeJoinIntsPartitioned is the parallel hash encoder. It returns the
// number of codes; null rows share the last one.
func encodeJoinIntsPartitioned[T int32 | int64](vals []T, a arrow.Array, hasNulls bool, codes []int32) int {
	const nParts = 1 << joinHashPartBits
	n := len(vals)
	k := denseWorkers(n)
	// counts[w][p] rows of chunk w in partition p.
	counts := make([][nParts]int32, k)
	runChunks(n, k, func(w, start, end int) {
		c := &counts[w]
		for i := start; i < end; i++ {
			if hasNulls && a.IsNull(i) {
				continue
			}
			c[joinKeyPart(int64(vals[i]))]++
		}
	})
	// Partition p holds rows[pstart[p]:pstart[p+1]]; each chunk writes
	// its rows of p at a private offset, keeping row order.
	var pstart [nParts + 1]int32
	offs := make([][nParts]int32, k)
	at := int32(0)
	for p := range nParts {
		pstart[p] = at
		for w := range k {
			offs[w][p] = at
			at += counts[w][p]
		}
	}
	pstart[nParts] = at
	rows := int32Scratch.get(int(at))
	defer int32Scratch.put(rows)
	runChunks(n, k, func(w, start, end int) {
		o := &offs[w]
		for i := start; i < end; i++ {
			if hasNulls && a.IsNull(i) {
				continue
			}
			p := joinKeyPart(int64(vals[i]))
			rows[o[p]] = int32(i)
			o[p]++
		}
	})
	// Number keys per partition with private tables.
	var local [nParts]int32
	runChunks(nParts, k, func(_, pLo, pHi int) {
		m := intmap.New(64)
		defer m.Release()
		for p := pLo; p < pHi; p++ {
			m.Reset()
			next := int32(0)
			for _, r := range rows[pstart[p]:pstart[p+1]] {
				id, inserted := m.InsertOrGet(int64(vals[r]), next)
				if inserted {
					next++
				}
				codes[r] = id
			}
			local[p] = next
		}
	})
	var base [nParts]int32
	total := int32(0)
	for p := range nParts {
		base[p] = total
		total += local[p]
	}
	runChunks(nParts, k, func(_, pLo, pHi int) {
		for p := pLo; p < pHi; p++ {
			if b := base[p]; b != 0 {
				for _, r := range rows[pstart[p]:pstart[p+1]] {
					codes[r] += b
				}
			}
		}
	})
	if hasNulls {
		for i := range n {
			if a.IsNull(i) {
				codes[i] = total
			}
		}
		total++
	}
	return int(total)
}
