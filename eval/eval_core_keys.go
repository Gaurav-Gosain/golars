package eval

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// singleKeyIDs assigns first-seen group ids for one integer or string
// key column with a typed map, avoiding the per-row key encoding of the
// general tuple path. Nulls form one group. ok is false for other
// dtypes.
func singleKeyIDs(arr arrow.Array, height int) ([]int, int, bool) {
	switch a := arr.(type) {
	case *array.Int64:
		ids, n := typedKeyIDs(a.Int64Values(), arr, height)
		return ids, n, true
	case *array.Int32:
		ids, n := typedKeyIDs(a.Int32Values(), arr, height)
		return ids, n, true
	case *array.String:
		ids := make([]int, height)
		table := make(map[string]int)
		next, nullID := 0, -1
		hasNulls := a.NullN() > 0
		for r := range height {
			if hasNulls && a.IsNull(r) {
				if nullID < 0 {
					nullID = next
					next++
				}
				ids[r] = nullID
				continue
			}
			k := a.Value(r)
			id, ok := table[k]
			if !ok {
				id = next
				next++
				table[k] = id
			}
			ids[r] = id
		}
		return ids, next, true
	}
	return nil, 0, false
}

func typedKeyIDs[T int64 | int32](vals []T, arr arrow.Array, height int) ([]int, int) {
	ids := make([]int, height)
	table := make(map[T]int)
	next, nullID := 0, -1
	hasNulls := arr.NullN() > 0
	for r := range height {
		if hasNulls && arr.IsNull(r) {
			if nullID < 0 {
				nullID = next
				next++
			}
			ids[r] = nullID
			continue
		}
		k := vals[r]
		id, ok := table[k]
		if !ok {
			id = next
			next++
			table[k] = id
		}
		ids[r] = id
	}
	return ids, next
}
