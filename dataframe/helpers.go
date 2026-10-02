package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// chunkValueAt extracts a typed scalar from the chunk at index i. The
// caller must have checked IsValid(i).
//
// Temporal types (Timestamp, Date*, Time*, Duration) stringify via the
// arrow ValueStr formatter since there's no single idiomatic Go type
// that covers all of them; callers who need the raw int64 can access
// the chunk directly by type assertion.
func chunkValueAt(chunk arrow.Array, i int) any {
	switch a := any(chunk).(type) {
	case *array.Int64:
		return a.Value(i)
	case *array.Int32:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return a.Value(i)
	case *array.Uint64:
		return a.Value(i)
	case *array.Uint32:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.String:
		return a.Value(i)
	case *array.Binary:
		return a.Value(i)
	case *array.Timestamp, *array.Date32, *array.Date64, *array.Time32, *array.Time64, *array.Duration:
		if v, ok := chunk.(interface{ ValueStr(int) string }); ok {
			return v.ValueStr(i)
		}
	}
	return cellValue(chunk, i)
}

// IsEmpty reports whether the DataFrame has zero rows (regardless of
// how many columns it has).
func (df *DataFrame) IsEmpty() bool { return df.height == 0 }

// ColumnNames returns a fresh slice of the column names in order.
func (df *DataFrame) ColumnNames() []string { return df.sch.Names() }

// DTypes returns a fresh slice of per-column dtypes in column order.
func (df *DataFrame) DTypes() []dtype.DType { return df.sch.DTypes() }

// Limit is an alias for Head: kept for polars-style symmetry so
// `df.Limit(10)` reads the same in golars scripts and in polars code.
func (df *DataFrame) Limit(n int) *DataFrame { return df.Head(n) }

// Clear returns a DataFrame with the same schema but zero rows.
func (df *DataFrame) Clear() *DataFrame {
	return Empty(df.sch)
}

// Equals reports whether two DataFrames have the same column names,
// dtypes, row count, and element-wise values. Uses Series.Equal for
// per-column comparison (NaN != NaN convention).
func (df *DataFrame) Equals(other *DataFrame) bool {
	if df == nil || other == nil {
		return df == other
	}
	if df.height != other.height {
		return false
	}
	if !df.sch.Equal(other.sch) {
		return false
	}
	for i, c := range df.cols {
		if !c.Equal(other.cols[i]) {
			return false
		}
	}
	return true
}

// WithColumns returns a DataFrame extended (or overwritten) with the
// given Series. Any Series whose name already exists replaces the
// existing column; new names are appended on the right. Mirrors
// polars' DataFrame.with_columns(*cols): the single-column variant
// is WithColumn.
func (df *DataFrame) WithColumns(cols ...*series.Series) (*DataFrame, error) {
	if len(cols) == 0 {
		return df.Clone(), nil
	}
	result := df
	for _, c := range cols {
		next, err := result.WithColumn(c)
		if err != nil {
			if result != df {
				result.Release()
			}
			return nil, err
		}
		if result != df {
			result.Release()
		}
		result = next
	}
	return result, nil
}

// NullCount returns a new DataFrame with one row: for every column,
// the count of nulls. The result schema preserves names; dtypes become
// u32. Mirrors polars' DataFrame.null_count.
func (df *DataFrame) NullCount() *DataFrame {
	out, _ := df.reduceFrame(context.Background(), reduceNullCount, reduceArgs{})
	return out
}

// EstimatedSize returns a best-effort byte count of the underlying
// arrow buffers. Matches polars' DataFrame.estimated_size (in bytes).
func (df *DataFrame) EstimatedSize() int {
	total := 0
	for _, c := range df.cols {
		chunk := c.Chunk(0)
		d := chunk.Data()
		for _, b := range d.Buffers() {
			if b != nil {
				total += b.Len()
			}
		}
	}
	return total
}

// Shuffle returns a DataFrame with the same rows in random order.
// Equivalent to Sample(ctx, Height(), false, seed).
func (df *DataFrame) Shuffle(ctx context.Context, seed uint64) (*DataFrame, error) {
	return df.Sample(ctx, df.height, false, seed)
}

// Row returns the i-th row as a []any slice, one element per column in
// schema order. Nulls come through as nil. Intended for debugging and
// REPL display; per-cell typed access is faster via Column().Chunk(0).
func (df *DataFrame) Row(i int) ([]any, error) {
	if i < 0 || i >= df.height {
		return nil, fmt.Errorf("dataframe: row %d out of bounds [0, %d)", i, df.height)
	}
	out := make([]any, len(df.cols))
	for j, c := range df.cols {
		chunk := c.Chunk(0)
		if !chunk.IsValid(i) {
			out[j] = nil
			continue
		}
		out[j] = chunkValueAt(chunk, i)
	}
	return out, nil
}

// Rows materialises the DataFrame as a slice of row slices. Large
// DataFrames should prefer streaming access.
func (df *DataFrame) Rows() ([][]any, error) {
	out := make([][]any, df.height)
	for i := 0; i < df.height; i++ {
		r, err := df.Row(i)
		if err != nil {
			return nil, err
		}
		out[i] = r
	}
	return out, nil
}

// ToMap returns a map[string][]any representation. Useful for JSON
// roundtripping and quick assertions in tests; costs a full
// materialisation.
func (df *DataFrame) ToMap() (map[string][]any, error) {
	m := make(map[string][]any, len(df.cols))
	for _, c := range df.cols {
		vals := make([]any, c.Len())
		chunk := c.Chunk(0)
		for i := 0; i < c.Len(); i++ {
			if !chunk.IsValid(i) {
				continue
			}
			vals[i] = chunkValueAt(chunk, i)
		}
		m[c.Name()] = vals
	}
	return m, nil
}

// Gather returns a DataFrame composed of the rows at the given
// indices. Negative indices are treated as count-from-end (polars-
// style). Out-of-range indices return an error.
func (df *DataFrame) Gather(ctx context.Context, indices []int) (*DataFrame, error) {
	if df.height == 0 {
		return df.Clone(), nil
	}
	normalised := make([]int, len(indices))
	for i, idx := range indices {
		if idx < 0 {
			idx = df.height + idx
		}
		if idx < 0 || idx >= df.height {
			return nil, fmt.Errorf("dataframe: gather index %d out of range", indices[i])
		}
		normalised[i] = idx
	}
	cols := make([]*series.Series, len(df.cols))
	for j, c := range df.cols {
		out, err := compute.Take(ctx, c, normalised)
		if err != nil {
			for _, r := range cols[:j] {
				if r != nil {
					r.Release()
				}
			}
			return nil, err
		}
		cols[j] = out
	}
	return New(cols...)
}

// Reverse returns a DataFrame with rows in reverse order.
func (df *DataFrame) Reverse(ctx context.Context) (*DataFrame, error) {
	if df.height == 0 {
		return df.Clone(), nil
	}
	idx := make([]int, df.height)
	for i := range idx {
		idx[i] = df.height - 1 - i
	}
	return df.Gather(ctx, idx)
}

// Glimpse returns a compact "peek" string: dtype header plus first few
// rows per column. Intended for interactive exploration.
func (df *DataFrame) Glimpse(nRows int) string {
	if nRows <= 0 {
		nRows = 5
	}
	head := df.Head(nRows)
	defer head.Release()
	return head.String()
}

// HasColumn is a symmetric predicate to Contains: polars' users may
// reach for either name.
func (df *DataFrame) HasColumn(name string) bool { return df.sch.Contains(name) }
