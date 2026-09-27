package dataframe

import (
	mbits "math/bits"
	"slices"

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
// partition b, writing partition-local codes into out[i] when out is
// not nil. Mid and long strings reuse the precomputed hash.
func (e *strEncoder) encodeBucketRows(hashes []uint64, bucket []uint8, b int, out []int32) {
	offs := e.offs
	data := e.data
	for i, bi := range bucket {
		if int(bi) != b {
			continue
		}
		var code int32
		if e.hasNulls && e.arr.IsNull(i) {
			if e.nullCode < 0 {
				e.nullCode = int32(len(e.firstRows))
				e.firstRows = append(e.firstRows, int32(i))
			}
			code = e.nullCode
		} else {
			s, t := offs[i], offs[i+1]
			var inserted bool
			switch {
			case t-s <= 7:
				key := packShort(data, s, t)
				var ok bool
				code, ok = e.short.get(key)
				if !ok {
					code = int32(len(e.firstRows))
					e.short.insert(key, code)
					inserted = true
				}
			case t-s <= 15:
				lo, hi := packMid(data, s, t)
				code, inserted = e.mid.insertOrGet(lo, hi, hashes[i], int32(len(e.firstRows)))
			default:
				code, inserted = e.long.insertOrGet(data, s, t, hashes[i], int32(len(e.firstRows)))
			}
			if inserted {
				e.firstRows = append(e.firstRows, int32(i))
			}
		}
		if out != nil {
			out[i] = code
		}
	}
}

// strPartitioned holds the result of the partition phase shared by the
// partitioned encoders: one encoder per hash partition (tables already
// released, first rows ascending), the partition of every row, and the
// number of row chunks used for the parallel passes.
type strPartitioned struct {
	encs   []*strEncoder
	bucket []uint8
	k      int
}

// partitionStrings runs the first two phases of the partitioned
// encoder: hash every row, then encode each hash partition on its own
// worker. codes, when not nil, receives partition-local codes. The
// caller returns bucket to uint8Scratch.
func partitionStrings(arr *array.String, codes []int32) strPartitioned {
	n := arr.Len()
	k := denseWorkers(n)
	hashes := uint64Scratch.get(n)
	// The bucket byte per row lets each partition worker stream 1 byte
	// per row instead of re-reading and re-mixing every 8-byte hash.
	bucket := uint8Scratch.get(n)
	proto := newStrEncoder(arr, 16)
	proto.release()
	// classRows[p] counts worker p's rows of at most 7, 8 to 15 and more
	// bytes, which decides how to split the table presize below.
	classRows := make([][3]int, k)
	runChunks(n, k, func(p, s, e int) {
		var c [3]int
		offs := proto.offs
		for i := s; i < e; i++ {
			h := proto.strRowHash(i)
			hashes[i] = h
			bucket[i] = uint8(strBucketOf(h))
			switch l := offs[i+1] - offs[i]; {
			case l <= 7:
				c[0]++
			case l <= 15:
				c[1]++
			default:
				c[2]++
			}
		}
		classRows[p] = c
	})
	var class [3]int
	for _, c := range classRows {
		class[0] += c[0]
		class[1] += c[1]
		class[2] += c[2]
	}

	const parts = 1 << strPartBits
	encs := make([]*strEncoder, parts)
	// The sampler already saw high cardinality, so presize each
	// partition's tables to skip most of the growth steps, splitting the
	// estimate over the length classes in proportion to their rows.
	hint := max(min(n/(parts*4), 1<<16), 1024)
	classHint := func(c int) int { return max(int(int64(hint)*int64(class[c])/int64(n)), 16) }
	runChunks(parts, parts, func(b, _, _ int) {
		e := newStrEncoderSized(arr, classHint(0), classHint(1), classHint(2))
		e.encodeBucketRows(hashes, bucket, b, codes)
		e.release()
		encs[b] = e
	})
	uint64Scratch.put(hashes)
	return strPartitioned{encs: encs, bucket: bucket, k: k}
}

// encodeStringCodesPartitioned is the high-cardinality parallel encoder:
//
//  1. Hash every row (parallel over row chunks).
//  2. One worker per hash partition encodes that partition's rows in row
//     order. Each worker owns a disjoint key set, so no merge of keys is
//     needed, and its table is 1/8 of the dictionary, which keeps probes
//     in cache. Its codes are first-seen within the partition and its
//     first rows ascend.
//  3. Global first-seen order interleaves partitions by first row. A
//     bitmap over rows marks every first row; the global code of a key
//     is the rank of its first row's bit, and every row's local code is
//     translated through that rank.
func encodeStringCodesPartitioned(arr *array.String) keyCodes {
	n := arr.Len()
	codes := int32Scratch.get(n)
	sp := partitionStrings(arr, codes)
	encs, bucket, k := sp.encs, sp.bucket, sp.k
	parts := len(encs)

	ro := orderFirstRows(encs, n, k)
	base := make([]int32, parts+1)
	for b, e := range encs {
		base[b+1] = base[b] + int32(len(e.firstRows))
	}
	codeOfHandle := make([]int32, base[parts])
	runChunks(parts, parts, func(b, _, _ int) {
		dst := codeOfHandle[base[b]:base[b+1]]
		for i, r := range encs[b].firstRows {
			dst[i] = ro.rank(r)
		}
	})
	ro.release()
	runChunks(n, k, func(_, s, e int) {
		for i := s; i < e; i++ {
			codes[i] = codeOfHandle[base[bucket[i]]+codes[i]]
		}
	})
	uint8Scratch.put(bucket)
	return keyCodes{codes: codes, firstRows: ro.firstRows}
}

// rowOrder is the ascending list of the first rows of every partition,
// with a bitmap over rows and per-word prefix counts that give the
// position of any listed row in that list.
type rowOrder struct {
	firstRows []int32
	bits      []uint64
	prefix    []int32 // number of set bits before each word
}

// rank returns the position of first row r in firstRows.
func (ro *rowOrder) rank(r int32) int32 {
	w := r >> 6
	below := ro.bits[w] & (uint64(1)<<(uint(r)&63) - 1)
	return ro.prefix[w] + int32(mbits.OnesCount64(below))
}

func (ro *rowOrder) release() {
	uint64Scratch.put(ro.bits)
	int32Scratch.put(ro.prefix)
	ro.bits, ro.prefix = nil, nil
}

// orderFirstRows merges the ascending first rows of every encoder into
// one ascending list. Rows split into k ranges of whole 64-row words;
// worker p sets the bits of the first rows in its range (a binary
// search finds each encoder's sub-slice, and no two workers share a
// word), then lists them in order after a prefix sum over the ranges.
func orderFirstRows(encs []*strEncoder, n, k int) rowOrder {
	nw := (n + 63) / 64
	bits := uint64Scratch.get(nw)
	prefix := int32Scratch.get(nw)
	wper := (nw + k - 1) / k
	counts := make([]int, k+1)
	runChunks(k, k, func(p, _, _ int) {
		ws, we := p*wper, min((p+1)*wper, nw)
		if ws >= we {
			return
		}
		clear(bits[ws:we])
		lo, hi := int32(ws*64), int32(we*64)
		c := 0
		for _, e := range encs {
			rows := e.firstRows
			a, _ := slices.BinarySearch(rows, lo)
			for _, r := range rows[a:] {
				if r >= hi {
					break
				}
				bits[r>>6] |= 1 << (uint(r) & 63)
				c++
			}
		}
		counts[p+1] = c
	})
	for p := range k {
		counts[p+1] += counts[p]
	}
	firstRows := make([]int32, counts[k])
	runChunks(k, k, func(p, _, _ int) {
		ws, we := p*wper, min((p+1)*wper, nw)
		j := counts[p]
		for w := ws; w < we; w++ {
			prefix[w] = int32(j)
			for x := bits[w]; x != 0; x &= x - 1 {
				firstRows[j] = int32(w*64 + mbits.TrailingZeros64(x))
				j++
			}
		}
	})
	return rowOrder{firstRows: firstRows, bits: bits, prefix: prefix}
}

// distinctStringRows returns the first row of every distinct value of
// arr (nulls count as one value), in ascending row order. It is the
// part of dictionary encoding that Unique needs: high-cardinality
// columns skip the per-row codes and their translation.
func distinctStringRows(arr *array.String) []int32 {
	n := arr.Len()
	if n < strPartThreshold || denseWorkers(n) < 2 || sampleDistinctStrings(arr, strSampleSize) <= strSampleHighCard {
		kc := encodeStringCodes(arr)
		int32Scratch.put(kc.codes)
		return kc.firstRows
	}
	sp := partitionStrings(arr, nil)
	uint8Scratch.put(sp.bucket)
	ro := orderFirstRows(sp.encs, n, sp.k)
	ro.release()
	return ro.firstRows
}
