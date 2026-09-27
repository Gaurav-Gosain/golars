package dataframe

import (
	"context"
	"fmt"
	"iter"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// rowCounts groups the rows of df by every column (or subset) and
// returns the per-row group id and the size of each group.
func (df *DataFrame) rowCounts(subset []string) ([]int, []int, error) {
	cols, err := df.subsetColumns(subset)
	if err != nil {
		return nil, nil, err
	}
	ids, first := rowGroupIDs(cols, df.height)
	counts := make([]int, len(first))
	for _, id := range ids {
		counts[id]++
	}
	return ids, counts, nil
}

// subsetColumns resolves names to columns; nil or empty means all.
func (df *DataFrame) subsetColumns(subset []string) ([]*series.Series, error) {
	if len(subset) == 0 {
		return df.cols, nil
	}
	cols := make([]*series.Series, len(subset))
	for i, n := range subset {
		c, err := df.Column(n)
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	return cols, nil
}

// IsDuplicated returns a boolean Series (named "") that is true for every
// row whose values appear more than once in df. Nulls compare equal to
// nulls and NaN to NaN. Mirrors polars' DataFrame.is_duplicated.
func (df *DataFrame) IsDuplicated(ctx context.Context) (*series.Series, error) {
	return df.duplicateMask(ctx, true)
}

// IsUnique returns a boolean Series (named "") that is true for rows that
// occur exactly once. Mirrors polars' DataFrame.is_unique.
func (df *DataFrame) IsUnique(ctx context.Context) (*series.Series, error) {
	return df.duplicateMask(ctx, false)
}

func (df *DataFrame) duplicateMask(ctx context.Context, dup bool) (*series.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids, counts, err := df.rowCounts(nil)
	if err != nil {
		return nil, err
	}
	return series.BuildBoolDirect("", df.height, memory.DefaultAllocator, func(bits []byte) {
		for i, id := range ids {
			if (counts[id] > 1) == dup {
				bits[i>>3] |= 1 << (uint(i) & 7)
			}
		}
	})
}

// NUnique returns the number of distinct rows, considering only subset
// when given. Mirrors polars' DataFrame.n_unique.
func (df *DataFrame) NUnique(ctx context.Context, subset ...string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	_, counts, err := df.rowCounts(subset)
	if err != nil {
		return 0, err
	}
	return len(counts), nil
}

// ApproxNUnique returns a one-row frame with the number of distinct
// values of every column as u32 (null counts as a value). The count is
// exact; polars uses HyperLogLog but the exact count is always a valid
// answer. Mirrors polars' DataFrame.approx_n_unique.
func (df *DataFrame) ApproxNUnique(ctx context.Context) (*DataFrame, error) {
	out := make([]*series.Series, 0, len(df.cols))
	for _, c := range df.cols {
		if err := ctx.Err(); err != nil {
			releaseAll(out)
			return nil, err
		}
		_, first := rowGroupIDs([]*series.Series{c}, df.height)
		s, err := scalarSeries(c.Name(), arrow.PrimitiveTypes.Uint32, uint64(len(first)), true, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}

// HashRows returns a u64 Series (named "") holding a hash of every row.
// Equal rows hash equally within one process and across runs for the
// same seed. The values differ from polars' hash_rows, which uses its own
// internal hash function.
func (df *DataFrame) HashRows(ctx context.Context, seed uint64) (*series.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	enc := newRowKeyEncoder(df.cols)
	vals := make([]uint64, df.height)
	buf := make([]byte, 0, 64)
	for r := range df.height {
		buf = enc.append(buf[:0], r)
		vals[r] = hashBytes(seed, buf)
	}
	return series.FromUint64("", vals, nil)
}

// hashBytes is FNV-1a over b, seeded and finished with a 64-bit mixer
// so that short keys spread across the full range.
func hashBytes(seed uint64, b []byte) uint64 {
	h := uint64(14695981039346656037) ^ seed
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// Item returns the single value of a 1x1 frame. Mirrors polars'
// DataFrame.item() without arguments.
func (df *DataFrame) Item() (any, error) {
	if df.height != 1 || len(df.cols) != 1 {
		return nil, fmt.Errorf("dataframe.Item: can only call on a frame of shape (1, 1), got (%d, %d)", df.height, len(df.cols))
	}
	return cellValue(firstChunk(df.cols[0]), 0), nil
}

// ItemAt returns the value at row, column. A negative row counts from the
// end. Mirrors polars' DataFrame.item(row, column).
func (df *DataFrame) ItemAt(row int, column string) (any, error) {
	c, err := df.Column(column)
	if err != nil {
		return nil, err
	}
	if row < 0 {
		row += df.height
	}
	if row < 0 || row >= df.height {
		return nil, fmt.Errorf("dataframe.ItemAt: row %d out of bounds for height %d", row, df.height)
	}
	return cellValue(firstChunk(c), row), nil
}

// IterRows yields every row as a fresh []any in column order. Values
// follow cellValue conventions: nil for null, native Go widths for
// integers, time.Time for dates and datetimes. Mirrors polars'
// DataFrame.iter_rows().
func (df *DataFrame) IterRows() iter.Seq[[]any] {
	return func(yield func([]any) bool) {
		arrs := make([]arrow.Array, len(df.cols))
		for i, c := range df.cols {
			arrs[i] = firstChunk(c)
		}
		for r := range df.height {
			row := make([]any, len(arrs))
			for i, a := range arrs {
				row[i] = cellValue(a, r)
			}
			if !yield(row) {
				return
			}
		}
	}
}

// IterRowsNamed is IterRows yielding a column-name keyed map per row.
// Mirrors polars' DataFrame.iter_rows(named=True).
func (df *DataFrame) IterRowsNamed() iter.Seq[map[string]any] {
	return func(yield func(map[string]any) bool) {
		names := df.sch.Names()
		arrs := make([]arrow.Array, len(df.cols))
		for i, c := range df.cols {
			arrs[i] = firstChunk(c)
		}
		for r := range df.height {
			row := make(map[string]any, len(arrs))
			for i, a := range arrs {
				row[names[i]] = cellValue(a, r)
			}
			if !yield(row) {
				return
			}
		}
	}
}

// ToDicts returns every row as a map from column name to value. Mirrors
// polars' DataFrame.to_dicts().
func (df *DataFrame) ToDicts() []map[string]any {
	out := make([]map[string]any, 0, df.height)
	for row := range df.IterRowsNamed() {
		out = append(out, row)
	}
	return out
}

// RowsByKeyOptions mirrors the keyword arguments of polars'
// DataFrame.rows_by_key.
type RowsByKeyOptions struct {
	// Named makes each row a map[string]any instead of a []any.
	Named bool
	// IncludeKey keeps the key columns in the row values.
	IncludeKey bool
	// Unique keeps only the last row per key; each map value then holds
	// exactly one row.
	Unique bool
}

// RowsByKey groups the rows of df by the key columns. The map key is the
// key value for a single key column, or a [k]any array for k key
// columns (comparable, so it can be used as a map key directly; for
// example m[[2]any{int64(1), "x"}]). Each map value lists the rows with
// that key in input order; rows are []any, or map[string]any when
// opts.Named is set. Mirrors polars' DataFrame.rows_by_key.
func (df *DataFrame) RowsByKey(keys []string, opts RowsByKeyOptions) (map[any][]any, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("dataframe.RowsByKey: at least one key column required")
	}
	keyIdx := make([]int, len(keys))
	isKey := make(map[int]bool, len(keys))
	for i, k := range keys {
		idx, ok := df.sch.Index(k)
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, k)
		}
		keyIdx[i] = idx
		isKey[idx] = true
	}
	var valIdx []int
	for i := range df.cols {
		if opts.IncludeKey || !isKey[i] {
			valIdx = append(valIdx, i)
		}
	}
	names := df.sch.Names()
	arrs := make([]arrow.Array, len(df.cols))
	for i, c := range df.cols {
		arrs[i] = firstChunk(c)
	}
	var arrType reflect.Type
	if len(keys) > 1 {
		arrType = reflect.ArrayOf(len(keys), reflect.TypeFor[any]())
	}
	out := make(map[any][]any)
	for r := range df.height {
		var key any
		if len(keys) == 1 {
			key = cellValue(arrs[keyIdx[0]], r)
			if err := checkHashable(key); err != nil {
				return nil, err
			}
		} else {
			kv := reflect.New(arrType).Elem()
			for i, ki := range keyIdx {
				v := cellValue(arrs[ki], r)
				if err := checkHashable(v); err != nil {
					return nil, err
				}
				if v != nil {
					kv.Index(i).Set(reflect.ValueOf(v))
				}
			}
			key = kv.Interface()
		}
		var row any
		if opts.Named {
			m := make(map[string]any, len(valIdx))
			for _, i := range valIdx {
				m[names[i]] = cellValue(arrs[i], r)
			}
			row = m
		} else {
			t := make([]any, len(valIdx))
			for j, i := range valIdx {
				t[j] = cellValue(arrs[i], r)
			}
			row = t
		}
		if opts.Unique {
			out[key] = []any{row}
		} else {
			out[key] = append(out[key], row)
		}
	}
	return out, nil
}

func checkHashable(v any) error {
	if v != nil && !reflect.TypeOf(v).Comparable() {
		return fmt.Errorf("dataframe.RowsByKey: key value of type %T is not hashable", v)
	}
	return nil
}
