package dataframe

import (
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/strhash"
)

// Sampling decides between the two parallel string encoders. Low
// cardinality columns encode best as independent row chunks whose tiny
// dictionaries merge almost for free. High cardinality columns would
// make that merge redo most of the work serially with a cache miss per
// key, so they partition rows by key hash instead.
const (
	strSampleSize     = 4096
	strSampleHighCard = 256
)

// sampleDistinctStrings counts distinct values among up to size rows
// spread evenly over arr (nulls count as one value).
func sampleDistinctStrings(arr *array.String, size int) int {
	n := arr.Len()
	step := max(n/size, 1)
	seen := make(map[string]struct{}, 512)
	null := 0
	for i := 0; i < n; i += step {
		if arr.IsNull(i) {
			null = 1
			continue
		}
		seen[arr.Value(i)] = struct{}{}
		if len(seen) > strSampleHighCard {
			break
		}
	}
	return len(seen) + null
}

// strPartBits sets the number of hash partitions (1 << strPartBits).
const strPartBits = 3

// strBucketOf maps a key hash to its partition using bits the tables
// themselves do not index by (the packed table uses the top bits of a
// multiplicative hash, the byte table the low bits).
func strBucketOf(h uint64) int {
	h ^= h >> 31
	return int((h * 0xBF58476D1CE4E5B9) >> (64 - strPartBits))
}

// strRowHash returns the partitioning hash of row i, matching what the
// encoders use: the packed word times a multiplier for short strings,
// strhash for long ones, 0 for null.
func (e *strEncoder) strRowHash(i int) uint64 {
	if e.hasNulls && e.arr.IsNull(i) {
		return 0
	}
	s, t := e.offs[i], e.offs[i+1]
	if t-s <= 7 {
		return packShort(e.data, s, t) * 0x9E3779B97F4A7C15
	}
	if t-s <= 15 {
		return pairHash(packMid(e.data, s, t))
	}
	return strhash.String(bytesToString(e.data[s:t]))
}

// encodeBucketRows encodes, in row order, every row whose hash falls in
// partition b, writing partition-local codes into out[i]. Long strings
// reuse the precomputed hash.
func (e *strEncoder) encodeBucketRows(hashes []uint64, bucket []uint8, b int, out []int32) {
	offs := e.offs
	data := e.data
	for i, bi := range bucket {
		if int(bi) != b {
			continue
		}
		h := hashes[i]
		if e.hasNulls && e.arr.IsNull(i) {
			if e.nullCode < 0 {
				e.nullCode = int32(len(e.firstRows))
				e.firstRows = append(e.firstRows, int32(i))
			}
			out[i] = e.nullCode
			continue
		}
		s, t := offs[i], offs[i+1]
		if t-s <= 7 {
			key := packShort(data, s, t)
			code, ok := e.short.get(key)
			if !ok {
				code = int32(len(e.firstRows))
				e.firstRows = append(e.firstRows, int32(i))
				e.short.insert(key, code)
			}
			out[i] = code
			continue
		}
		if t-s <= 15 {
			out[i] = e.midCode(i, s, t)
			continue
		}
		code, inserted := e.long.insertOrGet(data, s, t, h, int32(len(e.firstRows)))
		if inserted {
			e.firstRows = append(e.firstRows, int32(i))
		}
		out[i] = code
	}
}

// encodeStringCodesPartitioned is the high-cardinality parallel encoder:
//
//  1. Hash every row (parallel over row chunks).
//  2. One worker per hash partition encodes that partition's rows in row
//     order. Each worker owns a disjoint key set, so no merge of keys is
//     needed, and its table is 1/8 of the dictionary, which keeps probes
//     in cache. Its codes are first-seen within the partition and its
//     first rows ascend.
//  3. Global first-seen order interleaves partitions by first row; a
//     marker array over rows yields it with a parallel count, prefix and
//     fill, then every row's local code is translated.
func encodeStringCodesPartitioned(arr *array.String) keyCodes {
	n := arr.Len()
	k := denseWorkers(n)
	hashes := uint64Scratch.get(n)
	// The bucket byte per row lets each partition worker stream 1 byte
	// per row instead of re-reading and re-mixing every 8-byte hash.
	bucket := uint8Scratch.get(n)
	proto := newStrEncoder(arr, 16)
	runChunks(n, k, func(_, s, e int) {
		for i := s; i < e; i++ {
			h := proto.strRowHash(i)
			hashes[i] = h
			bucket[i] = uint8(strBucketOf(h))
		}
	})

	const parts = 1 << strPartBits
	codes := int32Scratch.get(n)
	encs := make([]*strEncoder, parts)
	// The sampler already saw high cardinality, so presize each
	// partition's tables to skip most of the growth steps.
	hint := max(min(n/(parts*4), 1<<16), 1024)
	runChunks(parts, parts, func(b, _, _ int) {
		e := newStrEncoder(arr, hint)
		e.encodeBucketRows(hashes, bucket, b, codes)
		encs[b] = e
	})

	base := make([]int32, parts+1)
	for b, e := range encs {
		base[b+1] = base[b] + int32(len(e.firstRows))
	}
	g := int(base[parts])
	mark := int32Scratch.get(n)
	runChunks(n, k, func(_, s, e int) { clear(mark[s:e]) })
	runChunks(parts, parts, func(b, _, _ int) {
		for i, r := range encs[b].firstRows {
			mark[r] = base[b] + int32(i) + 1
		}
	})
	counts := make([]int32, k+1)
	runChunks(n, k, func(p, s, e int) {
		c := int32(0)
		for _, m := range mark[s:e] {
			if m != 0 {
				c++
			}
		}
		counts[p+1] = c
	})
	for p := range k {
		counts[p+1] += counts[p]
	}
	codeOfHandle := make([]int32, g)
	firstRows := make([]int32, g)
	runChunks(n, k, func(p, s, e int) {
		next := counts[p]
		for i, m := range mark[s:e] {
			if m != 0 {
				codeOfHandle[m-1] = next
				firstRows[next] = int32(s + i)
				next++
			}
		}
	})
	int32Scratch.put(mark)
	runChunks(n, k, func(_, s, e int) {
		for i := s; i < e; i++ {
			codes[i] = codeOfHandle[base[bucket[i]]+codes[i]]
		}
	})
	uint64Scratch.put(hashes)
	uint8Scratch.put(bucket)
	return keyCodes{codes: codes, firstRows: firstRows}
}
