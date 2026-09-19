package dataframe

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// AppendKeyTuple appends the binary encoding of row's key tuple to buf
// and reports whether every column has a supported key dtype (int64,
// int32, float32, float64, string, boolean). Variable-length fields
// carry a length prefix so concatenations cannot collide; every field
// carries a null marker byte. Float keys are canonicalized (-0 as +0,
// one NaN payload) to match the sort-based grouping, where -0 == +0
// and all NaNs compare equal.
//
// Callers hashing the result should look the map up with
// table[string(buf)] directly: the runtime skips the conversion
// allocation on hits, so only first-seen tuples allocate.
func AppendKeyTuple(buf []byte, cols []*series.Series, row int) ([]byte, bool) {
	for _, c := range cols {
		if c.NumChunks() != 1 {
			return buf, false
		}
		var ok bool
		buf, ok = encodeKeyValue(buf, c.Chunk(0), row)
		if !ok {
			return buf, false
		}
	}
	return buf, true
}

// SupportedKeyTuple reports whether every column can feed AppendKeyTuple.
func SupportedKeyTuple(cols []*series.Series) bool {
	for _, c := range cols {
		if c.NumChunks() != 1 {
			return false
		}
		switch c.Chunk(0).(type) {
		case *array.Int64, *array.Int32, *array.Float32, *array.Float64, *array.String, *array.Boolean:
		default:
			return false
		}
	}
	return true
}

// encodeKeyValue appends one key value; ok=false for unsupported dtype.
func encodeKeyValue(buf []byte, arr arrow.Array, i int) ([]byte, bool) {
	var tmp [8]byte
	switch a := arr.(type) {
	case *array.Int64:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		buf = append(buf, 0x01)
		binary.LittleEndian.PutUint64(tmp[:], uint64(a.Value(i)))
		return append(buf, tmp[:]...), true
	case *array.Int32:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		buf = append(buf, 0x01)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(a.Value(i)))
		return append(buf, tmp[:4]...), true
	case *array.Float64:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		v := a.Value(i)
		bits := math.Float64bits(v)
		if v != v {
			bits = 0x7FF8000000000000
		}
		if v == 0 {
			bits = 0
		}
		buf = append(buf, 0x01)
		binary.LittleEndian.PutUint64(tmp[:], bits)
		return append(buf, tmp[:]...), true
	case *array.Float32:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		v := a.Value(i)
		bits := uint64(math.Float32bits(v))
		if v != v {
			bits = 0x7FC00000
		}
		if v == 0 {
			bits = 0
		}
		buf = append(buf, 0x01)
		binary.LittleEndian.PutUint64(tmp[:], bits)
		return append(buf, tmp[:]...), true
	case *array.String:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		buf = append(buf, 0x01)
		s := a.Value(i)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
		buf = append(buf, tmp[:4]...)
		return append(buf, s...), true
	case *array.Boolean:
		if a.IsNull(i) {
			return append(buf, 0x00), true
		}
		buf = append(buf, 0x01)
		if a.Value(i) {
			return append(buf, 0x01), true
		}
		return append(buf, 0x00), true
	}
	return buf, false
}

// hashAggMultiKey runs the hash groupby for multi-key inputs. It hashes
// an encoded key tuple per row (O(n)), orders the distinct groups by
// key (O(g log g), g << n), then reuses the single-key per-column
// aggregation machinery. Output row order matches the sort-based path
// (ascending keys, nulls last) so callers observe no change.
//
// Returns (nil, false, nil) when a key dtype is unsupported so Agg can
// fall back to the sort-based implementation.
func hashAggMultiKey(ctx context.Context, df *DataFrame, keys []string, specs []aggSpec, mem memory.Allocator) (*DataFrame, bool, error) {
	cols := make([]multiKeyCol, len(keys))
	n := -1
	for i, name := range keys {
		col, err := df.Column(name)
		if err != nil {
			return nil, true, err
		}
		if col.NumChunks() != 1 {
			return nil, false, nil
		}
		arr := col.Chunk(0)
		if n < 0 {
			n = arr.Len()
		} else if arr.Len() != n {
			return nil, false, nil
		}
		c := multiKeyCol{name: name}
		switch a := arr.(type) {
		case *array.Int64:
			c.i64 = a
		case *array.Int32:
			c.i32 = a
		case *array.String:
			c.str = a
		case *array.Boolean:
			c.bl = a
		default:
			return nil, false, nil
		}
		cols[i] = c
	}

	groupIDs, uniques := assignGroupsMultiKey(cols, n)
	orderGroupsByKey(cols, uniques, groupIDs)

	keyOuts := make([]*series.Series, len(cols))
	for i, u := range uniques {
		s, err := u.series(cols[i].name, mem)
		if err != nil {
			for _, prev := range keyOuts[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, true, err
		}
		keyOuts[i] = s
	}
	return hashAggWithGroups(ctx, df, groupIDs, groupLen(uniques[0]), keyOuts, specs, mem)
}

// multiKeyCol is one group-by key column in its concrete arrow type.
type multiKeyCol struct {
	name string
	i64  *array.Int64
	i32  *array.Int32
	str  *array.String
	bl   *array.Boolean
}

// keyUniques accumulates the distinct key values of one column, one
// entry per group. valid is nil until the first null key arrives.
type keyUniques struct {
	i64     []int64
	i32     []int32
	str     []string
	boolean []bool
	valid   []bool
}

func (u *keyUniques) series(name string, mem memory.Allocator) (*series.Series, error) {
	switch {
	case u.i64 != nil:
		return series.FromInt64(name, u.i64, u.valid, series.WithAllocator(mem))
	case u.i32 != nil:
		return series.FromInt32(name, u.i32, u.valid, series.WithAllocator(mem))
	case u.str != nil:
		return series.FromString(name, u.str, u.valid, series.WithAllocator(mem))
	default:
		return series.FromBool(name, u.boolean, u.valid, series.WithAllocator(mem))
	}
}

// appendNull records a null key for column c.
func (u *keyUniques) appendNull(c multiKeyCol) {
	if u.valid == nil {
		u.valid = make([]bool, groupLen(u))
		for k := range u.valid {
			u.valid[k] = true
		}
	}
	switch {
	case c.i64 != nil:
		u.i64 = append(u.i64, 0)
	case c.i32 != nil:
		u.i32 = append(u.i32, 0)
	case c.str != nil:
		u.str = append(u.str, "")
	default:
		u.boolean = append(u.boolean, false)
	}
	u.valid = append(u.valid, false)
}

// appendValue records row i of column c. Only called for valid rows.
func (u *keyUniques) appendValue(c multiKeyCol, i int) {
	switch {
	case c.i64 != nil:
		u.i64 = append(u.i64, c.i64.Value(i))
	case c.i32 != nil:
		u.i32 = append(u.i32, c.i32.Value(i))
	case c.str != nil:
		u.str = append(u.str, c.str.Value(i))
	default:
		u.boolean = append(u.boolean, c.bl.Value(i))
	}
	if u.valid != nil {
		u.valid = append(u.valid, true)
	}
}

// rowKeyNull reports whether row i of column c holds a null key.
func rowKeyNull(c multiKeyCol, i int) bool {
	switch {
	case c.i64 != nil:
		return c.i64.IsNull(i)
	case c.i32 != nil:
		return c.i32.IsNull(i)
	case c.str != nil:
		return c.str.IsNull(i)
	default:
		return c.bl.IsNull(i)
	}
}

// assignGroupsMultiKey maps every row to a first-seen group id and
// collects the per-column unique keys. The string(buf) map lookup
// allocates nothing on hits: only first-seen tuples copy into the
// table. Large inputs split into per-thread partitions merged
// serially, mirroring parallelAssignInt64.
func assignGroupsMultiKey(cols []multiKeyCol, n int) ([]int, []*keyUniques) {
	if n >= hashAggParThreshold {
		if ids, uniques, ok := assignGroupsMultiKeyAdaptive(cols, n); ok {
			return ids, uniques
		}
	}
	return assignGroupsMultiKeyRange(cols, 0, n)
}

// assignGroupsMultiKeyAdaptive samples the first partition serially,
// then fans out only when groups are large enough for probe
// parallelism to beat duplication: every worker rediscovers every
// group, so high-cardinality inputs (small groups) continue serially
// reusing the sample's table, while low-cardinality inputs (big
// groups) go parallel. Either way the grouping is exact; only the
// speed differs.
func assignGroupsMultiKeyAdaptive(cols []multiKeyCol, n int) ([]int, []*keyUniques, bool) {
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	end := min(chunk, n)
	table := make(map[string]int)
	uniques := make([]*keyUniques, len(cols))
	for i := range uniques {
		uniques[i] = &keyUniques{}
	}
	groupIDs := make([]int, n)
	assignRangeInto(cols, 0, end, table, uniques, groupIDs[:end])
	// Heuristic: parallel pays when the sample's average group holds
	// at least 32 rows (probe-bound regime). Below that, table and
	// merge overhead dominate and serial wins.
	if ng := groupLen(uniques[0]); ng == 0 || end/ng < 32 {
		assignRangeInto(cols, end, n, table, uniques, groupIDs[end:])
		return groupIDs, uniques, true
	}
	// Sample discarded; parallel assignment wins big enough to dwarf
	// one partition of repeated work.
	return assignGroupsMultiKeyParallel(cols, n)
}

// multiKeyPart is one worker's local assignment over a row partition.
type multiKeyPart struct {
	ids     []int // local group id per row, relative to part start
	uniques []*keyUniques
}

// assignGroupsMultiKeyRange assigns rows [start, end) to first-seen
// local group ids.
func assignGroupsMultiKeyRange(cols []multiKeyCol, start, end int) ([]int, []*keyUniques) {
	uniques := make([]*keyUniques, len(cols))
	for i := range uniques {
		uniques[i] = &keyUniques{}
	}
	groupIDs := make([]int, end-start)
	assignRangeInto(cols, start, end, make(map[string]int), uniques, groupIDs)
	return groupIDs, uniques
}

// assignRangeInto assigns rows [start, end), writing ids into out
// (sized end-start) and accumulating uniques into shared state, so an
// adaptive sample can continue serially without repeated work. Lookups
// use string(buf) directly in the map index, which skips the conversion
// allocation on hits; only first-seen tuples copy.
//
// NOTE: the tuple encoding is inlined here rather than calling
// encodeKeyTuple because that helper exceeds the inliner budget and a
// call per row costs ~10% on this loop. The dtype branches predict
// perfectly (fixed columns), so the inline form keeps only the work.
func assignRangeInto(cols []multiKeyCol, start, end int, table map[string]int, uniques []*keyUniques, out []int) {
	var tmp [8]byte
	buf := make([]byte, 0, 32*len(cols))
	for i := start; i < end; i++ {
		buf = buf[:0]
		for _, c := range cols {
			switch {
			case c.i64 != nil:
				if c.i64.IsNull(i) {
					buf = append(buf, 0x00)
					continue
				}
				buf = append(buf, 0x01)
				binary.LittleEndian.PutUint64(tmp[:], uint64(c.i64.Value(i)))
				buf = append(buf, tmp[:]...)
			case c.i32 != nil:
				if c.i32.IsNull(i) {
					buf = append(buf, 0x00)
					continue
				}
				buf = append(buf, 0x01)
				binary.LittleEndian.PutUint32(tmp[:4], uint32(c.i32.Value(i)))
				buf = append(buf, tmp[:4]...)
			case c.str != nil:
				if c.str.IsNull(i) {
					buf = append(buf, 0x00)
					continue
				}
				buf = append(buf, 0x01)
				s := c.str.Value(i)
				binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
				buf = append(buf, tmp[:4]...)
				buf = append(buf, s...)
			default:
				if c.bl.IsNull(i) {
					buf = append(buf, 0x00)
					continue
				}
				buf = append(buf, 0x01)
				if c.bl.Value(i) {
					buf = append(buf, 0x01)
				} else {
					buf = append(buf, 0x00)
				}
			}
		}
		id, ok := table[string(buf)]
		if !ok {
			id = len(table)
			table[string(buf)] = id
			for ci, c := range cols {
				if rowKeyNull(c, i) {
					uniques[ci].appendNull(c)
				} else {
					uniques[ci].appendValue(c, i)
				}
			}
		}
		out[i-start] = id
	}
}

// assignGroupsMultiKeyParallel is the parallel assignment path: each
// worker assigns its row partition independently, a serial merge
// dedups cross-partition groups, then workers rewrite local ids to
// global ones. Output groups are first-seen per partition merged in
// partition order; orderGroupsByKey sorts them afterwards regardless.
func assignGroupsMultiKeyParallel(cols []multiKeyCol, n int) ([]int, []*keyUniques, bool) {
	k := min(runtime.GOMAXPROCS(0), 8)
	chunk := (n + k - 1) / k
	parts := make([]multiKeyPart, k)
	var wg sync.WaitGroup
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			end := min(start+chunk, n)
			if start >= end {
				parts[p].uniques = make([]*keyUniques, len(cols))
				for i := range parts[p].uniques {
					parts[p].uniques[i] = &keyUniques{}
				}
				return
			}
			ids, uniques := assignGroupsMultiKeyRange(cols, start, end)
			parts[p] = multiKeyPart{ids: ids, uniques: uniques}
		}(p)
	}
	wg.Wait()

	// Serial merge: re-encode each partition's uniques into the global
	// table (hundreds of rows, not n), recording local-to-global maps.
	global := make([]*keyUniques, len(cols))
	for i := range global {
		global[i] = &keyUniques{}
	}
	globalTable := make(map[string]int)
	remaps := make([][]int, k)
	var buf []byte
	for p := range k {
		part := &parts[p]
		remap := make([]int, groupLen(part.uniques[0]))
		for gi := range remap {
			buf = encodeUniqueTuple(buf[:0], cols, part.uniques, gi)
			if id, ok := globalTable[string(buf)]; ok {
				remap[gi] = id
				continue
			}
			id := len(globalTable)
			globalTable[string(buf)] = id
			remap[gi] = id
			for ci := range cols {
				mergeUnique(global[ci], part.uniques[ci], gi)
			}
		}
		remaps[p] = remap
	}

	// Parallel rewrite into the final id slice.
	groupIDs := make([]int, n)
	wg = sync.WaitGroup{}
	for p := range k {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start := p * chunk
			remap := remaps[p]
			for j, lid := range parts[p].ids {
				groupIDs[start+j] = remap[lid]
			}
		}(p)
	}
	wg.Wait()
	return groupIDs, global, true
}

// encodeUniqueTuple encodes the gi-th unique of every column. Used to
// re-key partition-local uniques into the global table during merge.
func encodeUniqueTuple(buf []byte, cols []multiKeyCol, uniques []*keyUniques, gi int) []byte {
	var tmp [8]byte
	for ci, c := range cols {
		u := uniques[ci]
		isNull := u.valid != nil && !u.valid[gi]
		switch {
		case c.i64 != nil:
			if isNull {
				buf = append(buf, 0x00)
				continue
			}
			buf = append(buf, 0x01)
			binary.LittleEndian.PutUint64(tmp[:], uint64(u.i64[gi]))
			buf = append(buf, tmp[:]...)
		case c.i32 != nil:
			if isNull {
				buf = append(buf, 0x00)
				continue
			}
			buf = append(buf, 0x01)
			binary.LittleEndian.PutUint32(tmp[:4], uint32(u.i32[gi]))
			buf = append(buf, tmp[:4]...)
		case c.str != nil:
			if isNull {
				buf = append(buf, 0x00)
				continue
			}
			buf = append(buf, 0x01)
			s := u.str[gi]
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len(s)))
			buf = append(buf, tmp[:4]...)
			buf = append(buf, s...)
		default:
			if isNull {
				buf = append(buf, 0x00)
				continue
			}
			buf = append(buf, 0x01)
			if u.boolean[gi] {
				buf = append(buf, 0x01)
			} else {
				buf = append(buf, 0x00)
			}
		}
	}
	return buf
}

// mergeUnique appends the gi-th partition unique into the global set,
// materializing validity bitmaps when either side has nulls.
func mergeUnique(dst, src *keyUniques, gi int) {
	switch {
	case src.i64 != nil:
		dst.i64 = append(dst.i64, src.i64[gi])
	case src.i32 != nil:
		dst.i32 = append(dst.i32, src.i32[gi])
	case src.str != nil:
		dst.str = append(dst.str, src.str[gi])
	default:
		dst.boolean = append(dst.boolean, src.boolean[gi])
	}
	if src.valid != nil {
		if dst.valid == nil {
			// Existing global entries predate any null: all valid.
			for range groupLen(dst) - 1 {
				dst.valid = append(dst.valid, true)
			}
		}
		dst.valid = append(dst.valid, src.valid[gi])
	} else if dst.valid != nil {
		dst.valid = append(dst.valid, true)
	}
}

// orderGroupsByKey sorts the groups into ascending-key order (nulls
// last, matching SortBy defaults and therefore the sort-based path),
// remaps groupIDs to the new order, and reorders the unique slices in
// place via the same permutation.
func orderGroupsByKey(cols []multiKeyCol, uniques []*keyUniques, groupIDs []int) {
	numGroups := groupLen(uniques[0])
	order := make([]int, numGroups)
	for g := range order {
		order[g] = g
	}
	slices.SortFunc(order, func(a, b int) int {
		return compareKeyTuple(cols, uniques, a, b)
	})
	remap := make([]int, numGroups)
	for newID, old := range order {
		remap[old] = newID
	}
	for i, gid := range groupIDs {
		groupIDs[i] = remap[gid]
	}
	for ci, c := range uniques {
		switch {
		case cols[ci].i64 != nil:
			applyPerm(c.i64, order)
		case cols[ci].i32 != nil:
			applyPerm(c.i32, order)
		case cols[ci].str != nil:
			applyPerm(c.str, order)
		default:
			applyPerm(c.boolean, order)
		}
		if c.valid != nil {
			applyPerm(c.valid, order)
		}
	}
}

func groupLen(u *keyUniques) int {
	switch {
	case u.i64 != nil:
		return len(u.i64)
	case u.i32 != nil:
		return len(u.i32)
	case u.str != nil:
		return len(u.str)
	default:
		return len(u.boolean)
	}
}

// applyPerm permutes s so that s[new] = old s[order[new]].
func applyPerm[T any](s []T, order []int) {
	tmp := make([]T, len(s))
	for newID, old := range order {
		tmp[newID] = s[old]
	}
	copy(s, tmp)
}

// compareKeyTuple orders two groups lexicographically by key:
// ascending values, nulls after non-nulls on every column.
func compareKeyTuple(cols []multiKeyCol, uniques []*keyUniques, a, b int) int {
	for ci, c := range cols {
		u := uniques[ci]
		aNull := u.valid != nil && !u.valid[a]
		bNull := u.valid != nil && !u.valid[b]
		if aNull != bNull {
			if aNull {
				return 1
			}
			return -1
		}
		if aNull {
			continue
		}
		var cmp int
		switch {
		case c.i64 != nil:
			cmp = compareOrdered(u.i64[a], u.i64[b])
		case c.i32 != nil:
			cmp = compareOrdered(u.i32[a], u.i32[b])
		case c.str != nil:
			cmp = compareOrdered(u.str[a], u.str[b])
		default:
			cmp = compareBool(u.boolean[a], u.boolean[b])
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareOrdered[T int64 | int32 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}
