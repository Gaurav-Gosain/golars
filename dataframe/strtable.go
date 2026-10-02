package dataframe

import "bytes"

// strTable maps byte strings that live in one arrow data buffer to
// codes with linear probing. It stores keys as (offset, length) into
// that buffer rather than as Go strings, so the table holds no
// pointers: appends need no write barriers and the garbage collector
// never scans it, which matters at hundreds of thousands of keys.
//
// Each slot carries the top 32 hash bits next to the entry index, so a
// probe rejects most non-matching slots without touching the entry
// arrays or the key bytes.
type strTable struct {
	slots  []uint64 // hash tag << 32 | (entry index + 1); 0 = empty
	mask   uint64
	offs   []uint32
	lens   []uint32
	hashes []uint64
	codes  []int32
}

func (t *strTable) init(hint int) {
	size := 16
	for size < hint*2 {
		size <<= 1
	}
	t.slots = uint64Scratch.get(size)
	clear(t.slots)
	t.mask = uint64(size - 1)
	t.offs = t.offs[:0]
	t.lens = t.lens[:0]
	t.hashes = t.hashes[:0]
	t.codes = t.codes[:0]
}

// insertOrGet returns the code of data[s:e], inserting it with newCode
// when absent. data must be the same buffer on every call.
func (t *strTable) insertOrGet(data []byte, s, e int32, h uint64, newCode int32) (int32, bool) {
	key := data[s:e]
	tag := h >> 32
	pos := h & t.mask
	for {
		sl := t.slots[pos]
		if sl == 0 {
			break
		}
		if sl>>32 == tag {
			ent := uint32(sl) - 1
			o, l := t.offs[ent], t.lens[ent]
			if int(l) == len(key) && bytes.Equal(data[o:o+l], key) {
				return t.codes[ent], false
			}
		}
		pos = (pos + 1) & t.mask
	}
	ent := uint64(len(t.offs))
	t.offs = append(t.offs, uint32(s))
	t.lens = append(t.lens, uint32(e-s))
	t.hashes = append(t.hashes, h)
	t.codes = append(t.codes, newCode)
	t.slots[pos] = tag<<32 | (ent + 1)
	if len(t.offs)*2 > len(t.slots) {
		t.grow()
	}
	return newCode, true
}

// release returns the slot array to the scratch pool.
func (t *strTable) release() {
	uint64Scratch.put(t.slots)
	t.slots = nil
}

func (t *strTable) grow() {
	size := len(t.slots) * 2
	uint64Scratch.put(t.slots)
	t.slots = uint64Scratch.get(size)
	clear(t.slots)
	t.mask = uint64(size - 1)
	for i, h := range t.hashes {
		pos := h & t.mask
		for t.slots[pos] != 0 {
			pos = (pos + 1) & t.mask
		}
		t.slots[pos] = (h>>32)<<32 | uint64(i+1)
	}
}
