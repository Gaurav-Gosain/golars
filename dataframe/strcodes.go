package dataframe

import (
	"encoding/binary"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/strhash"
)

// strPartThreshold is the row count above which string encoding splits
// into per-worker partitions. Below it goroutine startup dominates.
const strPartThreshold = 64 * 1024

// strEncoder assigns dense first-seen codes to the values of one string
// array. Strings of at most 7 bytes (most categorical keys) are packed
// with their length into a single uint64 and looked up in an integer
// table, which avoids hashing and comparing bytes. Longer strings go
// through a byte-hashing table holding views into the array's data
// buffer, so no key is ever copied. Both tables share one code space.
type strEncoder struct {
	arr      *array.String
	data     []byte  // the whole data buffer, for in-bounds word loads
	offs     []int32 // absolute offsets into data, one per row plus one
	hasNulls bool

	short     packedTable
	mid       pairTable
	long      strTable
	nullCode  int32
	firstRows []int32

	// hashes, when trackHashes is set, records a hash per code (0 for
	// the null code) so a parallel merge can route each distinct key to
	// a hash partition without re-reading its bytes.
	trackHashes bool
	hashes      []uint64
}

func newStrEncoder(arr *array.String, hint int) *strEncoder {
	e := &strEncoder{arr: arr, hasNulls: arr.NullN() > 0, nullCode: -1}
	if bufs := arr.Data().Buffers(); len(bufs) > 2 && bufs[2] != nil {
		e.data = bufs[2].Bytes()
	}
	e.offs = arr.ValueOffsets()
	e.short.init(hint)
	e.mid.init(16)
	e.long.init(16)
	e.firstRows = make([]int32, 0, hint)
	return e
}

// newStrEncoderSized is newStrEncoder with a separate presize hint for
// each length class, so a caller that knows the key shape does not pay
// for tables that stay empty or for repeated growth of the one in use.
func newStrEncoderSized(arr *array.String, short, mid, long int) *strEncoder {
	e := &strEncoder{arr: arr, hasNulls: arr.NullN() > 0, nullCode: -1}
	if bufs := arr.Data().Buffers(); len(bufs) > 2 && bufs[2] != nil {
		e.data = bufs[2].Bytes()
	}
	e.offs = arr.ValueOffsets()
	e.short.init(short)
	e.mid.init(mid)
	e.long.init(long)
	e.firstRows = make([]int32, 0, max(short+mid+long, 16))
	return e
}

// release hands the encoder's hash tables back to the scratch pools.
// firstRows and hashes stay valid; the encoder must not encode again.
func (e *strEncoder) release() {
	if e == nil {
		return
	}
	e.short.release()
	e.mid.release()
	e.long.release()
}

// packShort packs a string of at most 7 bytes and its length into one
// word. Distinct short strings map to distinct words because the length
// sits in the top byte and the bytes past the length are zeroed. The
// word load stays inside data: forward from start when 8 bytes remain,
// otherwise backward from end.
func packShort(data []byte, start, end int32) uint64 {
	n := uint(end - start)
	var w uint64
	switch {
	case int(start)+8 <= len(data):
		w = binary.LittleEndian.Uint64(data[start:])
		w &= (uint64(1) << (8 * n)) - 1
	case end >= 8:
		w = binary.LittleEndian.Uint64(data[end-8:])
		w >>= 8 * (8 - n)
	default:
		for j := start; j < end; j++ {
			w |= uint64(data[j]) << (8 * uint(j-start))
		}
	}
	// Bit 63 marks an occupied slot; lengths up to 7 fit in bits 56..58.
	return w | uint64(n)<<56 | 1<<63
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// encodeRange writes the code of rows [start, end) into out.
func (e *strEncoder) encodeRange(start, end int, out []int32) {
	offs := e.offs
	data := e.data
	for i := start; i < end; i++ {
		if e.hasNulls && e.arr.IsNull(i) {
			if e.nullCode < 0 {
				e.nullCode = int32(len(e.firstRows))
				e.firstRows = append(e.firstRows, int32(i))
				if e.trackHashes {
					e.hashes = append(e.hashes, 0)
				}
			}
			out[i-start] = e.nullCode
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
				if e.trackHashes {
					e.hashes = append(e.hashes, key*0x9E3779B97F4A7C15)
				}
			}
			out[i-start] = code
			continue
		}
		if t-s <= 15 {
			out[i-start] = e.midCode(i, s, t)
			continue
		}
		out[i-start] = e.longCode(i, s, t)
	}
}

// packMid splits a string of 8 to 15 bytes into two words: the first 8
// bytes, and the rest packed like a short string (length in the top
// byte), so distinct keys give distinct pairs.
func packMid(data []byte, s, t int32) (uint64, uint64) {
	return binary.LittleEndian.Uint64(data[s:]), packShort(data, s+8, t)
}

func pairHash(lo, hi uint64) uint64 {
	return (lo*0x9E3779B97F4A7C15 ^ hi) * 0xBF58476D1CE4E5B9
}

// midCode looks up (inserting when new) a key of 8 to 15 bytes.
func (e *strEncoder) midCode(row int, s, t int32) int32 {
	lo, hi := packMid(e.data, s, t)
	code, inserted := e.mid.insertOrGet(lo, hi, pairHash(lo, hi), int32(len(e.firstRows)))
	if inserted {
		e.firstRows = append(e.firstRows, int32(row))
		if e.trackHashes {
			e.hashes = append(e.hashes, pairHash(lo, hi))
		}
	}
	return code
}

// pairEntry is one key of a pairTable with its code.
type pairEntry struct {
	lo, hi uint64
	code   int32
}

// pairTable maps two-word keys to codes. The probe array holds one word
// per slot, a 32-bit hash tag next to the entry index, and the keys sit
// in a dense entry array in insertion order. That keeps the probed
// memory at 8 bytes per slot, a third of storing the keys in the slots,
// so large tables stay in cache far longer; a tag match costs one more
// load to confirm the key.
type pairTable struct {
	slots   []uint64 // tag << 32 | (entry index + 1); 0 = empty
	shift   uint
	entries []pairEntry
}

var pairEntryScratch scratchPool[pairEntry]

func (t *pairTable) init(hint int) {
	size, bits := 16, uint(4)
	for size < hint*2 {
		size <<= 1
		bits++
	}
	t.slots = uint64Scratch.get(size)
	clear(t.slots)
	t.shift = 64 - bits
	t.entries = pairEntryScratch.get(max(size/2, 16))[:0]
}

// release returns the table memory to the scratch pools.
func (t *pairTable) release() {
	uint64Scratch.put(t.slots)
	pairEntryScratch.put(t.entries)
	t.slots, t.entries = nil, nil
}

// pairTag derives the slot tag from hash bits that the slot position
// (the top bits) does not use.
func pairTag(h uint64) uint64 { return uint64(uint32(h>>8)) | 1<<31 }

// insertOrGet returns the code of (lo, hi), inserting it with newCode
// when absent.
func (t *pairTable) insertOrGet(lo, hi, h uint64, newCode int32) (int32, bool) {
	slots := t.slots
	mask := uint64(len(slots) - 1)
	tag := pairTag(h)
	pos := h >> t.shift
	for ; ; pos++ {
		sl := slots[pos&mask]
		if sl == 0 {
			break
		}
		if sl>>32 == tag {
			en := &t.entries[uint32(sl)-1]
			if en.lo == lo && en.hi == hi {
				return en.code, false
			}
		}
	}
	t.entries = append(t.entries, pairEntry{lo: lo, hi: hi, code: newCode})
	slots[pos&mask] = tag<<32 | uint64(len(t.entries))
	if len(t.entries)*2 > len(slots) {
		t.grow()
	}
	return newCode, true
}

func (t *pairTable) grow() {
	size := 2 * len(t.slots)
	uint64Scratch.put(t.slots)
	t.slots = uint64Scratch.get(size)
	clear(t.slots)
	t.shift--
	mask := uint64(size - 1)
	for i, en := range t.entries {
		h := pairHash(en.lo, en.hi)
		pos := h >> t.shift
		for t.slots[pos&mask] != 0 {
			pos++
		}
		t.slots[pos&mask] = pairTag(h)<<32 | uint64(i+1)
	}
}

func (e *strEncoder) longCode(row int, s, t int32) int32 {
	h := strhash.String(bytesToString(e.data[s:t]))
	code, inserted := e.long.insertOrGet(e.data, s, t, h, int32(len(e.firstRows)))
	if inserted {
		e.firstRows = append(e.firstRows, int32(row))
		if e.trackHashes {
			e.hashes = append(e.hashes, h)
		}
	}
	return code
}

// codeOfRow returns the code for row i, inserting it when new. Used by
// the partition merge, which touches one row per distinct key.
func (e *strEncoder) codeOfRow(i int) int32 {
	var out [1]int32
	e.encodeRange(i, i+1, out[:])
	return out[0]
}

// encodeStringCodes dictionary-encodes a string column. Large inputs are
// encoded per partition in parallel, then merged in partition order so
// codes remain first-seen over the whole column.
func encodeStringCodes(arr *array.String) keyCodes {
	n := arr.Len()
	k := min(runtime.GOMAXPROCS(0), 8, n/(strPartThreshold/4))
	if n < strPartThreshold || k < 2 {
		e := newStrEncoder(arr, 64)
		codes := int32Scratch.get(n)
		e.encodeRange(0, n, codes)
		e.release()
		return keyCodes{codes: codes, firstRows: e.firstRows}
	}
	if sampleDistinctStrings(arr, strSampleSize) > strSampleHighCard {
		return encodeStringCodesPartitioned(arr)
	}
	chunk := (n + k - 1) / k
	codes := int32Scratch.get(n)
	parts := make([]*strEncoder, k)
	var wg sync.WaitGroup
	for p := range k {
		start := p * chunk
		end := min(start+chunk, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := newStrEncoder(arr, 64)
			e.trackHashes = true
			e.encodeRange(start, end, codes[start:end])
			parts[p] = e
		}()
	}
	wg.Wait()

	totalLocal := 0
	for _, pe := range parts {
		if pe != nil {
			totalLocal += len(pe.firstRows)
		}
	}
	var remaps [][]int32
	var firstRows []int32
	if totalLocal >= strParallelMergeMin {
		remaps, firstRows = mergeStrPartsParallel(arr, parts, n)
	} else {
		remaps, firstRows = mergeStrPartsSerial(arr, parts)
	}
	for _, pe := range parts {
		pe.release()
	}
	for p := range k {
		if remaps[p] == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			remap := remaps[p]
			seg := codes[start:end]
			for i, c := range seg {
				seg[i] = remap[c]
			}
		}()
	}
	wg.Wait()
	return keyCodes{codes: codes, firstRows: firstRows}
}

// strParallelMergeMin is the total number of partition-local distinct
// keys above which the hash-partitioned parallel merge replaces the
// serial one.
const strParallelMergeMin = 16 * 1024

// mergeStrPartsSerial re-encodes each part's first rows into one global
// encoder in part order, which visits every partition-local distinct
// key once and keeps codes first-seen over the whole column. A nil
// remap means the part's local codes are already global.
func mergeStrPartsSerial(arr *array.String, parts []*strEncoder) ([][]int32, []int32) {
	global := newStrEncoder(arr, len(parts[0].firstRows)+16)
	remaps := make([][]int32, len(parts))
	for p, pe := range parts {
		if pe == nil {
			continue
		}
		remap := make([]int32, len(pe.firstRows))
		identity := true
		for lc, r := range pe.firstRows {
			gc := global.codeOfRow(int(r))
			remap[lc] = gc
			identity = identity && gc == int32(lc)
		}
		if !identity {
			remaps[p] = remap
		}
	}
	global.release()
	return remaps, global.firstRows
}

// mergeStrPartsParallel is the merge for high-cardinality columns,
// where the serial merge would redo most of the encoding work alone.
//
//  1. Each distinct key of each part is routed by hash to one of h
//     buckets; bucket workers run in parallel, each re-encoding its keys
//     in part order into a private encoder. Parts cover increasing row
//     ranges and list their keys in first-seen order, so each bucket's
//     codes are first-seen too, and its first rows ascend.
//  2. Global first-seen order interleaves the buckets by first row.
//     Because every first row is a distinct row index, a marker array
//     over rows gives that order with a parallel count, prefix and fill,
//     instead of a serial merge.
func mergeStrPartsParallel(arr *array.String, parts []*strEncoder, n int) ([][]int32, []int32) {
	const hBits = 3
	const h = 1 << hBits
	bucketOf := func(x uint64) int {
		x ^= x >> 31
		return int((x * 0xBF58476D1CE4E5B9) >> (64 - hBits))
	}
	// local[p][lc] = code of that key inside its bucket's encoder.
	local := make([][]int32, len(parts))
	for p, pe := range parts {
		if pe != nil {
			local[p] = make([]int32, len(pe.firstRows))
		}
	}
	buckets := make([]*strEncoder, h)
	var wg sync.WaitGroup
	for b := range h {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := newStrEncoder(arr, 1024)
			for p, pe := range parts {
				if pe == nil {
					continue
				}
				lp := local[p]
				for lc, hv := range pe.hashes {
					if bucketOf(hv) == b {
						lp[lc] = e.codeOfRow(int(pe.firstRows[lc]))
					}
				}
			}
			buckets[b] = e
		}()
	}
	wg.Wait()
	for _, e := range buckets {
		e.release()
	}

	// Mark each group's first row with its bucket handle + 1, then number
	// the marks in row order.
	base := make([]int32, h+1)
	for b, e := range buckets {
		base[b+1] = base[b] + int32(len(e.firstRows))
	}
	g := int(base[h])
	mark := int32Scratch.get(n)
	k := denseWorkers(n)
	runChunks(n, k, func(_, s, e int) { clear(mark[s:e]) })
	runChunks(h, h, func(b, _, _ int) {
		for i, r := range buckets[b].firstRows {
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

	// Translate every part's local codes to global codes.
	remaps := make([][]int32, len(parts))
	var wg2 sync.WaitGroup
	for p, pe := range parts {
		if pe == nil {
			continue
		}
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			lp := local[p]
			for lc, hv := range pe.hashes {
				lp[lc] = codeOfHandle[base[bucketOf(hv)]+lp[lc]]
			}
			remaps[p] = lp
		}()
	}
	wg2.Wait()
	return remaps, firstRows
}

// packedSlot is one packedTable entry; see pairSlot.
type packedSlot struct {
	key  uint64
	code int32
}

// packedTable maps packed short-string words to codes with linear
// probing. A zero key marks an empty slot; packed keys always have bit
// 63 set.
type packedTable struct {
	slots []packedSlot
	shift uint
	n     int
}

var packedSlotScratch scratchPool[packedSlot]

func (t *packedTable) init(hint int) {
	size, bits := 16, uint(4)
	for size < hint*2 {
		size <<= 1
		bits++
	}
	t.slots = packedSlotScratch.get(size)
	clear(t.slots)
	t.shift = 64 - bits
	t.n = 0
}

// release returns the slot array to the scratch pool.
func (t *packedTable) release() {
	packedSlotScratch.put(t.slots)
	t.slots = nil
}

func (t *packedTable) get(key uint64) (int32, bool) {
	slots := t.slots
	mask := uint64(len(slots) - 1)
	pos := (key * 0x9E3779B97F4A7C15) >> t.shift
	for {
		sl := &slots[pos&mask]
		if sl.key == key {
			return sl.code, true
		}
		if sl.key == 0 {
			return 0, false
		}
		pos++
	}
}

func (t *packedTable) insert(key uint64, code int32) {
	slots := t.slots
	mask := uint64(len(slots) - 1)
	pos := (key * 0x9E3779B97F4A7C15) >> t.shift
	for slots[pos&mask].key != 0 {
		pos++
	}
	slots[pos&mask] = packedSlot{key: key, code: code}
	t.n++
	if t.n*2 > len(slots) {
		old := slots
		t.slots = packedSlotScratch.get(2 * len(old))
		clear(t.slots)
		t.shift--
		t.n = 0
		for _, sl := range old {
			if sl.key != 0 {
				t.insert(sl.key, sl.code)
			}
		}
		packedSlotScratch.put(old)
	}
}
