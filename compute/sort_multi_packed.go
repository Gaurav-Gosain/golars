package compute

import (
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/series"
)

// packedMultiKeySort runs packedMultiKeyIndices on the columns' single
// chunks. ok is false when the columns do not qualify.
func packedMultiKeySort(cols []*series.Series, opts []SortOptions, n int, cfg config) ([]int, bool, error) {
	for _, c := range cols {
		switch c.DType().ID() {
		case arrow.INT64, arrow.INT32:
		default:
			return nil, false, nil
		}
		if c.NullCount() != 0 {
			return nil, false, nil
		}
	}
	arrs := make([]arrow.Array, len(cols))
	defer func() {
		for _, a := range arrs {
			if a != nil {
				a.Release()
			}
		}
	}()
	for i, c := range cols {
		a, err := extractChunk(c, cfg.alloc)
		if err != nil {
			return nil, false, err
		}
		arrs[i] = a
	}
	idx, ok := packedMultiKeyIndices(arrs, opts, n)
	return idx, ok, nil
}

// packedMultiKeyIndices sorts rows by several integer key columns by
// packing every row's keys and its row index into one uint64 and radix
// sorting those words directly.
//
// Each key is rebased to its column minimum (or taken from the column
// maximum when descending), so it needs only as many bits as the
// column's value range. When the key widths plus the width of a row
// index fit in 64 bits, the word (key0, key1, ..., row) orders exactly
// like the lexicographic key order with ties broken by row, which is
// the stable order. Compared with one indirect arg-radix sort per key,
// this reads each key column once, sorts contiguous words instead of
// gathering keys through the permutation, and only runs radix passes
// over bits that vary.
//
// It handles int64 and int32 keys without nulls; ok is false for
// anything else or when the keys are too wide to pack.
func packedMultiKeyIndices(arrs []arrow.Array, opts []SortOptions, n int) ([]int, bool) {
	if n < 2 {
		return nil, false
	}
	type keyCol struct {
		i64  []int64
		i32  []int32
		base uint64
		bits uint
		desc bool
	}
	keys := make([]keyCol, len(arrs))
	idxBits := uint(bits.Len(uint(n - 1)))
	total := idxBits
	for c, a := range arrs {
		if a.NullN() != 0 {
			return nil, false
		}
		kc := keyCol{desc: opts[c].Descending}
		var lo, hi int64
		switch a.DataType().ID() {
		case arrow.INT64:
			kc.i64 = int64Values(a)
			lo, hi = minMaxInt64(kc.i64)
		case arrow.INT32:
			kc.i32 = int32Values(a)
			l, h := minMaxInt32(kc.i32)
			lo, hi = int64(l), int64(h)
		default:
			return nil, false
		}
		kc.bits = uint(bits.Len64(uint64(hi) - uint64(lo)))
		if kc.desc {
			kc.base = uint64(hi)
		} else {
			kc.base = uint64(lo)
		}
		total += kc.bits
		if total > 64 {
			return nil, false
		}
		keys[c] = kc
	}

	words := make([]uint64, n)
	for _, kc := range keys {
		sh := kc.bits
		switch {
		case kc.i64 != nil && !kc.desc:
			for i, v := range kc.i64[:n] {
				words[i] = words[i]<<sh | (uint64(v) - kc.base)
			}
		case kc.i64 != nil:
			for i, v := range kc.i64[:n] {
				words[i] = words[i]<<sh | (kc.base - uint64(v))
			}
		case !kc.desc:
			for i, v := range kc.i32[:n] {
				words[i] = words[i]<<sh | (uint64(int64(v)) - kc.base)
			}
		default:
			for i, v := range kc.i32[:n] {
				words[i] = words[i]<<sh | (kc.base - uint64(int64(v)))
			}
		}
	}
	for i := range words {
		words[i] = words[i]<<idxBits | uint64(i)
	}

	if n >= parallelRadixCutoff {
		radixSortUint64(words)
	} else {
		// The row bits are already in ascending order and LSD passes
		// are stable, so only the key bits need sorting.
		radixSortUint64Bits(words, idxBits, total)
	}

	out := make([]int, n)
	mask := uint64(1)<<idxBits - 1
	for i, w := range words {
		out[i] = int(w & mask)
	}
	return out, true
}

// radixSortUint64Bits sorts vals by bits [lo, hi) with a serial LSD
// radix sort, leaving the relative order of equal keys unchanged. All
// digit histograms come from one read pass; digits that are the same
// in every value are skipped.
func radixSortUint64Bits(vals []uint64, lo, hi uint) {
	width := hi - lo
	if width == 0 {
		return
	}
	n := len(vals)
	// Small inputs keep 8-bit digits (256 buckets per pass stay in L1
	// and give long write runs); larger ones use 11-bit digits to cut
	// the number of passes.
	digit := uint(8)
	if n > 64*1024 {
		digit = 11
	}
	passes := int((width + digit - 1) / digit)
	nb := 1 << digit
	counts := make([]int, passes*nb)
	for _, v := range vals {
		x := v >> lo
		for p := range passes {
			counts[p*nb+int(x&uint64(nb-1))]++
			x >>= digit
		}
	}
	aux := make([]uint64, n)
	src, dst := vals, aux
	for p := range passes {
		shift := lo + uint(p)*digit
		c := counts[p*nb : (p+1)*nb]
		if c[int(src[0]>>shift&uint64(nb-1))] == n {
			continue
		}
		off := 0
		for d, k := range c {
			c[d] = off
			off += k
		}
		mask := uint64(nb - 1)
		for _, v := range src {
			d := int(v >> shift & mask)
			dst[c[d]] = v
			c[d]++
		}
		src, dst = dst, src
	}
	if &src[0] != &vals[0] {
		copy(vals, src)
	}
}

func minMaxInt64(v []int64) (int64, int64) {
	lo, hi := v[0], v[0]
	for _, x := range v[1:] {
		lo = min(lo, x)
		hi = max(hi, x)
	}
	return lo, hi
}

func minMaxInt32(v []int32) (int32, int32) {
	lo, hi := v[0], v[0]
	for _, x := range v[1:] {
		lo = min(lo, x)
		hi = max(hi, x)
	}
	return lo, hi
}
