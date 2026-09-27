package dataframe

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/intmap"
)

// groupIndex maps every row of a frame to a group. Group ids follow the
// first appearance of each key tuple, and the CSR layout lists the rows
// of each group in their original order:
//
//	rows[offsets[g]:offsets[g+1]] are the rows of group g.
type groupIndex struct {
	ids     []int
	n       int
	offsets []int
	rows    []int
}

// size returns the number of rows in group g.
func (gi *groupIndex) size(g int) int { return gi.offsets[g+1] - gi.offsets[g] }

// groupRows returns the rows of group g.
func (gi *groupIndex) groupRows(g int) []int { return gi.rows[gi.offsets[g]:gi.offsets[g+1]] }

// firstRows returns the first row of every group.
func (gi *groupIndex) firstRows() []int {
	out := make([]int, gi.n)
	for g := range gi.n {
		out[g] = gi.rows[gi.offsets[g]]
	}
	return out
}

// lastRows returns the last row of every group.
func (gi *groupIndex) lastRows() []int {
	out := make([]int, gi.n)
	for g := range gi.n {
		out[g] = gi.rows[gi.offsets[g+1]-1]
	}
	return out
}

// buildGroupIndex assigns group ids over the key arrays (all of length
// nRows, single chunk) and builds the CSR layout.
func buildGroupIndex(keys []arrow.Array, nRows int, mem memory.Allocator) (*groupIndex, error) {
	ids, n, err := assignGroupIDs(keys, nRows, mem)
	if err != nil {
		return nil, err
	}
	gi := &groupIndex{ids: ids, n: n}
	gi.offsets = make([]int, n+1)
	for _, g := range ids {
		gi.offsets[g+1]++
	}
	for g := range n {
		gi.offsets[g+1] += gi.offsets[g]
	}
	gi.rows = make([]int, nRows)
	cursor := make([]int, n)
	copy(cursor, gi.offsets[:n])
	for r, g := range ids {
		gi.rows[cursor[g]] = r
		cursor[g]++
	}
	return gi, nil
}

// assignGroupIDs returns first-appearance group ids for every row.
func assignGroupIDs(keys []arrow.Array, nRows int, mem memory.Allocator) ([]int, int, error) {
	if nRows == 0 {
		return nil, 0, nil
	}
	if len(keys) == 1 {
		switch a := keys[0].(type) {
		case *array.Int64:
			ids, n, keyOut, err := assignGroupsInt64(a, "", mem)
			if keyOut != nil {
				keyOut.Release()
			}
			return ids, n, err
		case *array.String:
			ids, n, keyOut, err := assignGroupsString(a, "", mem)
			if keyOut != nil {
				keyOut.Release()
			}
			return ids, n, err
		case *array.Boolean:
			ids, n, keyOut, err := assignGroupsBool(a, "", mem)
			if keyOut != nil {
				keyOut.Release()
			}
			return ids, n, err
		}
		if canon, ok := canonicalInt64Keys(keys[0]); ok {
			ids, n := assignCanonical(keys[0], canon, nRows)
			return ids, n, nil
		}
	} else {
		cols := make([]multiKeyCol, len(keys))
		fast := true
		for i, k := range keys {
			switch a := k.(type) {
			case *array.Int64:
				cols[i].i64 = a
			case *array.Int32:
				cols[i].i32 = a
			case *array.String:
				cols[i].str = a
			case *array.Boolean:
				cols[i].bl = a
			default:
				fast = false
			}
		}
		if fast {
			ids, uniques := assignGroupsMultiKey(cols, nRows)
			return ids, groupLen(uniques[0]), nil
		}
	}
	for _, k := range keys {
		if !encodableKey(k.DataType()) {
			return nil, 0, fmt.Errorf("dataframe.GroupBy: unsupported key dtype %s", k.DataType())
		}
	}
	ids := make([]int, nRows)
	table := make(map[string]int, 64)
	var buf []byte
	for r := range nRows {
		buf = buf[:0]
		for _, k := range keys {
			buf = appendAnyKey(buf, k, r)
		}
		id, ok := table[string(buf)]
		if !ok {
			id = len(table)
			table[string(buf)] = id
		}
		ids[r] = id
	}
	return ids, len(table), nil
}

// assignCanonical groups a fixed-width key whose values were mapped to
// canonical int64s. Null rows share one group placed at its first
// appearance.
func assignCanonical(arr arrow.Array, canon []int64, nRows int) ([]int, int) {
	if arr.NullN() == 0 {
		if nRows >= hashAggParThreshold {
			ids, uniques := parallelAssignInt64(canon, nRows)
			return ids, len(uniques)
		}
		ids, uniques := serialAssignInt64(canon, nRows)
		return ids, len(uniques)
	}
	ids := make([]int, nRows)
	table := intmap.New(64)
	defer table.Release()
	next := 0
	nullID := -1
	for i, k := range canon {
		if arr.IsNull(i) {
			if nullID < 0 {
				nullID = next
				next++
			}
			ids[i] = nullID
			continue
		}
		id, inserted := table.InsertOrGet(k, int32(next))
		if inserted {
			next++
		}
		ids[i] = int(id)
	}
	return ids, next
}

// canonicalInt64Keys maps a fixed-width array to one int64 per row such
// that equal keys map to equal values. Floats fold -0 into +0 and every
// NaN into one payload. Null slots hold arbitrary values.
func canonicalInt64Keys(arr arrow.Array) ([]int64, bool) {
	n := arr.Len()
	switch a := arr.(type) {
	case *array.Int8:
		return widenKeys(a.Int8Values()), true
	case *array.Int16:
		return widenKeys(a.Int16Values()), true
	case *array.Int32:
		return widenKeys(a.Int32Values()), true
	case *array.Int64:
		return widenKeys(a.Int64Values()), true
	case *array.Uint8:
		return widenKeys(a.Uint8Values()), true
	case *array.Uint16:
		return widenKeys(a.Uint16Values()), true
	case *array.Uint32:
		return widenKeys(a.Uint32Values()), true
	case *array.Uint64:
		return widenKeys(a.Uint64Values()), true
	case *array.Date32:
		return widenKeys(a.Date32Values()), true
	case *array.Date64:
		return widenKeys(a.Date64Values()), true
	case *array.Timestamp:
		return widenKeys(a.TimestampValues()), true
	case *array.Duration:
		return widenKeys(a.DurationValues()), true
	case *array.Time32:
		return widenKeys(a.Time32Values()), true
	case *array.Time64:
		return widenKeys(a.Time64Values()), true
	case *array.Float64:
		out := make([]int64, n)
		for i, v := range a.Float64Values() {
			out[i] = int64(canonFloatBits(v))
		}
		return out, true
	case *array.Float32:
		out := make([]int64, n)
		for i, v := range a.Float32Values() {
			out[i] = int64(canonFloatBits(float64(v)))
		}
		return out, true
	}
	return nil, false
}

type keyInt interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func widenKeys[T keyInt](vals []T) []int64 {
	out := make([]int64, len(vals))
	for i, v := range vals {
		out[i] = int64(v)
	}
	return out
}

func canonFloatBits(v float64) uint64 {
	if v != v {
		return 0x7FF8000000000000
	}
	if v == 0 {
		return 0
	}
	return math.Float64bits(v)
}

// encodableKey reports whether appendAnyKey can encode dt.
func encodableKey(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.NULL, arrow.BOOL, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY,
		arrow.FLOAT32, arrow.FLOAT64:
		return true
	}
	fw, ok := dt.(arrow.FixedWidthDataType)
	return ok && fw.BitWidth()%8 == 0 && fw.BitWidth() > 0
}

// appendAnyKey appends a self-delimiting encoding of row i of arr. Every
// value starts with a null marker; variable-length values carry a length
// prefix so concatenated tuples cannot collide.
func appendAnyKey(buf []byte, arr arrow.Array, i int) []byte {
	// A Null-dtype array has no validity bitmap, so IsNull reports false.
	if arr.DataType().ID() == arrow.NULL || arr.IsNull(i) {
		return append(buf, 0x00)
	}
	buf = append(buf, 0x01)
	var tmp [8]byte
	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(i) {
			return append(buf, 0x01)
		}
		return append(buf, 0x00)
	case *array.Float64:
		binary.LittleEndian.PutUint64(tmp[:], canonFloatBits(a.Value(i)))
		return append(buf, tmp[:]...)
	case *array.Float32:
		binary.LittleEndian.PutUint64(tmp[:], canonFloatBits(float64(a.Value(i))))
		return append(buf, tmp[:]...)
	case *array.String:
		s := a.Value(i)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
		buf = append(buf, tmp[:4]...)
		return append(buf, s...)
	case *array.LargeString:
		s := a.Value(i)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
		buf = append(buf, tmp[:4]...)
		return append(buf, s...)
	case *array.Binary:
		s := a.Value(i)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
		buf = append(buf, tmp[:4]...)
		return append(buf, s...)
	case *array.LargeBinary:
		s := a.Value(i)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
		buf = append(buf, tmp[:4]...)
		return append(buf, s...)
	}
	fw := arr.DataType().(arrow.FixedWidthDataType)
	w := fw.BitWidth() / 8
	d := arr.Data()
	start := (d.Offset() + i) * w
	return append(buf, d.Buffers()[1].Bytes()[start:start+w]...)
}

// quantileSelect computes the same value as sorting vals with
// sortFloatsNaNLast and calling quantileSorted, but uses selection
// instead of a full sort. vals is reordered in place.
func quantileSelect(vals []float64, q float64, method QuantileMethod) (float64, bool) {
	n := len(vals)
	if n == 0 {
		return 0, false
	}
	// Move NaNs to the tail: they sort after every other value.
	m := n
	for i := 0; i < m; {
		if vals[i] != vals[i] {
			m--
			vals[i], vals[m] = vals[m], vals[i]
			continue
		}
		i++
	}
	if n == 1 {
		return vals[0], true
	}
	pos := q * float64(n-1)
	at := func(k int) float64 {
		if k >= m {
			return math.NaN()
		}
		return selectKth(vals[:m], k)
	}
	switch method.orDefault() {
	case QuantileNearest:
		return at(clampIdx(int(math.Round(pos)), n)), true
	case QuantileLower:
		return at(clampIdx(int(math.Floor(pos)), n)), true
	case QuantileHigher:
		return at(clampIdx(int(math.Ceil(pos)), n)), true
	case QuantileEquiprobable:
		return at(clampIdx(int(math.Ceil(q*float64(n)))-1, n)), true
	}
	lo := clampIdx(int(math.Floor(pos)), n)
	hi := clampIdx(int(math.Ceil(pos)), n)
	vlo := at(lo)
	if lo == hi {
		return vlo, true
	}
	// After selecting lo, every value right of it is >= vals[lo], so
	// the next order statistic is their minimum.
	vhi := math.NaN()
	if hi < m {
		vhi = vals[hi]
		for _, x := range vals[hi+1 : m] {
			if x < vhi {
				vhi = x
			}
		}
	}
	if method.orDefault() == QuantileMidpoint {
		return (vlo + vhi) / 2, true
	}
	frac := pos - float64(lo)
	return vlo + (vhi-vlo)*frac, true
}

// selectKth partially orders vals (no NaNs) so vals[k] holds the k-th
// smallest value, everything left of k is <= it and everything right
// is >= it, and returns vals[k].
func selectKth(vals []float64, k int) float64 {
	lo, hi := 0, len(vals)-1
	for hi-lo > 16 {
		mid := lo + (hi-lo)/2
		if vals[mid] < vals[lo] {
			vals[mid], vals[lo] = vals[lo], vals[mid]
		}
		if vals[hi] < vals[lo] {
			vals[hi], vals[lo] = vals[lo], vals[hi]
		}
		if vals[hi] < vals[mid] {
			vals[hi], vals[mid] = vals[mid], vals[hi]
		}
		pivot := vals[mid]
		i, j := lo, hi
		for i <= j {
			for vals[i] < pivot {
				i++
			}
			for vals[j] > pivot {
				j--
			}
			if i <= j {
				vals[i], vals[j] = vals[j], vals[i]
				i++
				j--
			}
		}
		switch {
		case k <= j:
			hi = j
		case k >= i:
			lo = i
		default:
			return vals[k]
		}
	}
	for i := lo + 1; i <= hi; i++ {
		x := vals[i]
		j := i - 1
		for j >= lo && vals[j] > x {
			vals[j+1] = vals[j]
			j--
		}
		vals[j+1] = x
	}
	return vals[k]
}
