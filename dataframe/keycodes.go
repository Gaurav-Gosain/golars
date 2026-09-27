package dataframe

import (
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

// keyCodes is the dictionary encoding of one key column: every row gets
// a dense code in [0, card) assigned in first-seen order, and firstRows
// records the row where each code first appears. A null key is one more
// code like any other (its first row is null in the source array).
type keyCodes struct {
	codes     []int32
	firstRows []int32
}

func (k keyCodes) card() int { return len(k.firstRows) }

// denseIntRange is the widest value range encoded with a direct lookup
// table instead of a hash table.
const denseIntRange = 1 << 16

// encodeInt64Codes dictionary-encodes an int64 key column. Narrow value
// ranges use a direct table; everything else goes through intmap.
func encodeInt64Codes(arr *array.Int64) keyCodes {
	n := arr.Len()
	vals := arr.Int64Values()
	codes := int32Scratch.get(n)
	var firstRows []int32
	hasNulls := arr.NullN() > 0
	nullCode := int32(-1)

	lo, hi, found := validInt64Range(vals, arr.IsNull, hasNulls)
	if !found {
		lo, hi = 0, -1 // all null or empty: only the null slot
	}
	if hi-lo+1 >= 0 && hi-lo < int64(min(denseIntRange, 4*n+64)) {
		// Shift values into [0, span) with nulls in the extra last
		// slot, then number the slots by first appearance.
		span := int(hi - lo + 1)
		nullSlot := int32(span)
		fill := func(_, start, end int) {
			if !hasNulls {
				for i := start; i < end; i++ {
					codes[i] = int32(vals[i] - lo)
				}
				return
			}
			for i := start; i < end; i++ {
				if arr.IsNull(i) {
					codes[i] = nullSlot
				} else {
					codes[i] = int32(vals[i] - lo)
				}
			}
		}
		if k := denseWorkers(n); n >= denseParThreshold && k > 1 {
			runChunks(n, k, fill)
		} else {
			fill(0, 0, n)
		}
		return keyCodes{codes: codes, firstRows: denseEncode(codes, span+1)}
	}

	m := intmap.New(64)
	defer m.Release()
	for i, v := range vals {
		if hasNulls && arr.IsNull(i) {
			if nullCode < 0 {
				nullCode = int32(len(firstRows))
				firstRows = append(firstRows, int32(i))
			}
			codes[i] = nullCode
			continue
		}
		id, inserted := m.InsertOrGet(v, int32(len(firstRows)))
		if inserted {
			firstRows = append(firstRows, int32(i))
		}
		codes[i] = id
	}
	return keyCodes{codes: codes, firstRows: firstRows}
}

// encodeInt32Codes widens to the int64 encoder's logic without copying
// the column: int32 ranges are small enough that the dense path usually
// applies.
func encodeInt32Codes(arr *array.Int32) keyCodes {
	n := arr.Len()
	vals := arr.Int32Values()
	codes := int32Scratch.get(n)
	var firstRows []int32
	hasNulls := arr.NullN() > 0
	nullCode := int32(-1)
	m := intmap.New(64)
	defer m.Release()
	for i, v := range vals {
		if hasNulls && arr.IsNull(i) {
			if nullCode < 0 {
				nullCode = int32(len(firstRows))
				firstRows = append(firstRows, int32(i))
			}
			codes[i] = nullCode
			continue
		}
		id, inserted := m.InsertOrGet(int64(v), int32(len(firstRows)))
		if inserted {
			firstRows = append(firstRows, int32(i))
		}
		codes[i] = id
	}
	return keyCodes{codes: codes, firstRows: firstRows}
}

// encodeBoolCodes encodes a boolean key column (at most three codes).
func encodeBoolCodes(arr *array.Boolean) keyCodes {
	n := arr.Len()
	codes := int32Scratch.get(n)
	var firstRows []int32
	slot := [3]int32{-1, -1, -1} // false, true, null
	hasNulls := arr.NullN() > 0
	for i := range n {
		k := 0
		if hasNulls && arr.IsNull(i) {
			k = 2
		} else if arr.Value(i) {
			k = 1
		}
		if slot[k] < 0 {
			slot[k] = int32(len(firstRows))
			firstRows = append(firstRows, int32(i))
		}
		codes[i] = slot[k]
	}
	return keyCodes{codes: codes, firstRows: firstRows}
}

// encodeKeyColumn dispatches on the key column's arrow type.
func encodeKeyColumn(c multiKeyCol) keyCodes {
	switch {
	case c.i64 != nil:
		return encodeInt64Codes(c.i64)
	case c.i32 != nil:
		return encodeInt32Codes(c.i32)
	case c.str != nil:
		return encodeStringCodes(c.str)
	default:
		return encodeBoolCodes(c.bl)
	}
}

// maxDenseCombined bounds the direct lookup table used when the product
// of per-column cardinalities is small.
const maxDenseCombined = 1 << 22

// assignGroupsFromCodes combines per-column codes into first-seen group
// ids. It returns ok=false when the combined key space overflows 63 bits
// so the caller can use the byte-tuple path.
func assignGroupsFromCodes(kcs []keyCodes, n int) (groupIDs []int, firstRows []int32, ok bool) {
	product := uint64(1)
	for _, kc := range kcs {
		c := uint64(max(kc.card(), 1))
		hi, lo := bits.Mul64(product, c)
		if hi != 0 || lo >= 1<<62 {
			return nil, nil, false
		}
		product = lo
	}
	// Fold the columns into one mixed-radix key per row, built in place
	// in the group id buffer: each slot is read once and then overwritten
	// with its id. Each pass is a branch-free loop over dense slices.
	groupIDs = intScratch.get(n)
	keys := groupIDs
	fold := func(_, start, end int) {
		seg := keys[start:end]
		for i, c := range kcs[0].codes[start:end] {
			seg[i] = int(c)
		}
		for _, kc := range kcs[1:] {
			card := max(kc.card(), 1)
			codes := kc.codes[start:end]
			for i := range seg {
				seg[i] = seg[i]*card + int(codes[i])
			}
		}
	}
	if k := denseWorkers(n); n >= denseParThreshold && k > 1 {
		runChunks(n, k, fold)
	} else {
		fold(0, 0, n)
	}
	if product <= maxDenseCombined && product <= uint64(4*n+1024) {
		return groupIDs, denseEncode(keys, int(product)), true
	}
	m := intmap.New(64)
	defer m.Release()
	for i, key := range keys {
		id, inserted := m.InsertOrGet(int64(key), int32(len(firstRows)))
		if inserted {
			firstRows = append(firstRows, int32(i))
		}
		groupIDs[i] = int(id)
	}
	return groupIDs, firstRows, true
}
