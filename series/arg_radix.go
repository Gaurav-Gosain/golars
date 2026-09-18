package series

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// This file mirrors the arg-radix pattern from compute/sort_arg_radix.go
// for Series.ArgSort (compute cannot be imported here: it depends on
// series). Only ascending order is needed; descending callers reverse
// stably on top. Nulls sort last, NaN after all non-NaN, ties keep
// input order - exactly matching the comparison sort it replaces.

// argSortInt64Asc returns stable ascending indices over vals, which
// must already be the logical (offset-adjusted) values.
func argSortInt64Asc(a *array.Int64, vals []int64, n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	if a.NullN() == 0 {
		argRadixSortInt64Local(vals, idx)
		return idx
	}
	// Compact valid positions (in order), radix those, append null
	// positions (in order). Matches nulls-last stable comparison.
	valid := idx[:0]
	var nulls []int
	for _, i := range idx {
		if a.IsValid(i) {
			valid = append(valid, i)
		} else {
			nulls = append(nulls, i)
		}
	}
	argRadixSortInt64Local(vals, valid)
	return append(valid, nulls...)
}

// floatRank maps v to ascending uint64 order: -0 ties +0, NaN ranks
// largest. Plain function so the transform loops stay lean.
func floatRank(v float64) uint64 {
	if v == 0 {
		return 1 << 63
	}
	bits := math.Float64bits(v)
	switch {
	case v != v:
		return 0xFFF8000000000000
	case bits>>63 != 0:
		return ^bits
	default:
		return bits | (1 << 63)
	}
}

// argSortFloat64Asc mirrors argSortInt64Asc with NaN ranking largest.
func argSortFloat64Asc(a *array.Float64, vals []float64, n int) []int {
	ranks := make([]uint64, n)
	if a.NullN() == 0 {
		for i, v := range vals {
			ranks[i] = floatRank(v)
		}
	} else {
		for i, v := range vals {
			if !a.IsValid(i) {
				continue
			}
			ranks[i] = floatRank(v)
		}
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	if a.NullN() == 0 {
		argRadixSortUint64Local(ranks, idx)
		return idx
	}
	valid := idx[:0]
	var nulls []int
	for _, i := range idx {
		if a.IsValid(i) {
			valid = append(valid, i)
		} else {
			nulls = append(nulls, i)
		}
	}
	argRadixSortUint64Local(ranks, valid)
	return append(valid, nulls...)
}

// argRadixSortInt64Local reorders indices[] so keys[indices[i]] is
// ascending. Stable. 11-bit LSD digits with skip-pass.
func argRadixSortInt64Local(keys []int64, indices []int) {
	n := len(indices)
	if n < 64 {
		insertionSortArgInt64Local(keys, indices)
		return
	}
	aux := make([]int, n)
	const signBit uint64 = 1 << 63
	counts := make([]int, 2048)
	src, dst := indices, aux
	swaps := 0
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		for i := range counts {
			counts[i] = 0
		}
		for _, idx := range src {
			d := int((uint64(keys[idx]) ^ signBit) >> shift & mask)
			counts[d]++
		}
		if counts[int((uint64(keys[src[0]])^signBit)>>shift&mask)] == n {
			continue
		}
		var offset int
		for i := range counts {
			c := counts[i]
			counts[i] = offset
			offset += c
		}
		i := 0
		for ; i+1 < len(src); i += 2 {
			idx0 := src[i]
			idx1 := src[i+1]
			d0 := int((uint64(keys[idx0]) ^ signBit) >> shift & mask)
			d1 := int((uint64(keys[idx1]) ^ signBit) >> shift & mask)
			p0 := counts[d0]
			counts[d0] = p0 + 1
			p1 := counts[d1]
			counts[d1] = p1 + 1
			dst[p0] = idx0
			dst[p1] = idx1
		}
		for ; i < len(src); i++ {
			idx := src[i]
			d := int((uint64(keys[idx]) ^ signBit) >> shift & mask)
			dst[counts[d]] = idx
			counts[d]++
		}
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(indices, aux)
	}
}

// argRadixSortUint64Local is the unsigned variant (pre-ordered keys,
// e.g. canonical float ranks): identical structure, no sign flip.
func argRadixSortUint64Local(keys []uint64, indices []int) {
	n := len(indices)
	if n < 64 {
		insertionSortArgUint64Local(keys, indices)
		return
	}
	aux := make([]int, n)
	counts := make([]int, 2048)
	src, dst := indices, aux
	swaps := 0
	for pass := range 6 {
		shift := uint(pass * 11)
		mask := uint64(2047)
		if pass == 5 {
			mask = 0x1ff
		}
		for i := range counts {
			counts[i] = 0
		}
		for _, idx := range src {
			d := int(keys[idx] >> shift & mask)
			counts[d]++
		}
		if counts[int(keys[src[0]]>>shift&mask)] == n {
			continue
		}
		var offset int
		for i := range counts {
			c := counts[i]
			counts[i] = offset
			offset += c
		}
		i := 0
		for ; i+1 < len(src); i += 2 {
			idx0 := src[i]
			idx1 := src[i+1]
			d0 := int(keys[idx0] >> shift & mask)
			d1 := int(keys[idx1] >> shift & mask)
			p0 := counts[d0]
			counts[d0] = p0 + 1
			p1 := counts[d1]
			counts[d1] = p1 + 1
			dst[p0] = idx0
			dst[p1] = idx1
		}
		for ; i < len(src); i++ {
			idx := src[i]
			d := int(keys[idx] >> shift & mask)
			dst[counts[d]] = idx
			counts[d]++
		}
		src, dst = dst, src
		swaps++
	}
	if swaps%2 != 0 {
		copy(indices, aux)
	}
}

func insertionSortArgInt64Local(keys []int64, indices []int) {
	for i := 1; i < len(indices); i++ {
		idxI := indices[i]
		keyI := keys[idxI]
		j := i - 1
		for j >= 0 && keys[indices[j]] > keyI {
			indices[j+1] = indices[j]
			j--
		}
		indices[j+1] = idxI
	}
}

func insertionSortArgUint64Local(keys []uint64, indices []int) {
	for i := 1; i < len(indices); i++ {
		idxI := indices[i]
		keyI := keys[idxI]
		j := i - 1
		for j >= 0 && keys[indices[j]] > keyI {
			indices[j+1] = indices[j]
			j--
		}
		indices[j+1] = idxI
	}
}
