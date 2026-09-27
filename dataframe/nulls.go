package dataframe

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// DropNulls returns a new DataFrame with every row that contains a
// null in any of the given columns removed. Passing no column names
// checks every column.
//
// Mirrors polars' DataFrame.drop_nulls(subset=None). The returned
// frame is a fresh DataFrame; callers must Release it.
func (df *DataFrame) DropNulls(ctx context.Context, cols ...string) (*DataFrame, error) {
	if df.height == 0 {
		return df.Clone(), nil
	}
	if len(cols) == 0 {
		cols = df.sch.Names()
	}
	// Single-column fast path: the source's own validity bitmap IS the
	// keep mask (valid=keep, null=drop). Wrap the existing arrow bitmap
	// buffer as a zero-copy Boolean Array and hand it to Filter, which
	// already has an optimised byte-scan collectMaskIndices path. This
	// skips the []bool allocation, the unpack-then-repack, and the
	// per-row IsValid() loop that the generic path pays.
	if len(cols) == 1 {
		if out, ok, err := dropNullsFastSingle(ctx, df, cols[0]); ok {
			return out, err
		}
	}
	// Resolve the columns once and collect their null counts. Columns
	// with NullCount == 0 don't participate in the mask.
	type maskSrc struct {
		col *series.Series
		// pre-extracted first-chunk validity bytes. Nil when the chunk
		// has no nulls.
		bits []byte
	}
	srcs := make([]maskSrc, 0, len(cols))
	for _, name := range cols {
		col, err := df.Column(name)
		if err != nil {
			return nil, err
		}
		if col.NullCount() == 0 {
			continue
		}
		chunk := col.Chunk(0)
		data := chunk.Data()
		if data.Offset() != 0 || len(data.Buffers()) < 1 || data.Buffers()[0] == nil {
			// Rare: sliced chunk or no bitmap. Fall back to per-element IsValid.
			keep := make([]bool, df.height)
			for i := range keep {
				keep[i] = true
			}
			for _, name := range cols {
				c, err := df.Column(name)
				if err != nil {
					return nil, err
				}
				if c.NullCount() == 0 {
					continue
				}
				ch := c.Chunk(0)
				for i := range df.height {
					if !ch.IsValid(i) {
						keep[i] = false
					}
				}
			}
			mask, err := series.FromBool("__keep", keep, nil)
			if err != nil {
				return nil, err
			}
			defer mask.Release()
			return df.Filter(ctx, mask)
		}
		srcs = append(srcs, maskSrc{col: col, bits: data.Buffers()[0].Bytes()})
	}
	// All columns had zero nulls: clone.
	if len(srcs) == 0 {
		return df.Clone(), nil
	}
	// Fast path: AND validity bitmaps directly at the byte level, producing
	// a []bool mask the Filter pipeline already knows how to consume. Per-row
	// ch.IsValid() loop was the hot spot: at 262K rows it accounted for
	// ~30x slowdown vs the bitmap byte-scan approach.
	keep := make([]bool, df.height)
	nBytes := (df.height + 7) >> 3
	// Seed from the first source: set keep[i] = bit i of that source.
	first := srcs[0].bits
	for b := range nBytes {
		byt := first[b]
		base := b << 3
		end := min(base+8, df.height)
		for i := base; i < end; i++ {
			keep[i] = (byt>>(i-base))&1 == 1
		}
	}
	// AND subsequent sources.
	for s := 1; s < len(srcs); s++ {
		other := srcs[s].bits
		for b := range nBytes {
			byt := other[b]
			base := b << 3
			end := min(base+8, df.height)
			for i := base; i < end; i++ {
				if (byt>>(i-base))&1 == 0 {
					keep[i] = false
				}
			}
		}
	}
	mask, err := series.FromBool("__keep", keep, nil)
	if err != nil {
		return nil, err
	}
	defer mask.Release()
	return df.Filter(ctx, mask)
}

// dropNullsFastSingle is the single-column fast path used by
// DropNulls. The source's validity bitmap IS the keep mask (a valid
// row is kept, a null row is dropped), so we skip the generic
// []bool allocate → arrow pack → Filter pipeline (pprof showed 2 ms
// per call at 262K with ~50% of that in collectMaskIndices and arrow
// builder overhead). Instead we hand the validity bitmap straight to
// the fused scatter kernel that Filter already uses for its no-null
// ultra-fast path. Output always has no nulls (by definition of
// DropNulls).
func dropNullsFastSingle(ctx context.Context, df *DataFrame, name string) (*DataFrame, bool, error) {
	_ = ctx
	col, err := df.Column(name)
	if err != nil {
		return nil, true, err
	}
	if col.NullCount() == 0 {
		return df.Clone(), true, nil
	}
	// The fused kernels below filter only this column, from its first
	// chunk. That is the whole frame only for a single-column,
	// single-chunk frame; otherwise the other columns were dropped
	// from the result (found by internal/difftest).
	if df.Width() != 1 || col.NumChunks() != 1 {
		return nil, false, nil
	}
	chunk := col.Chunk(0)
	data := chunk.Data()
	if data.Offset() != 0 || len(data.Buffers()) < 1 || data.Buffers()[0] == nil {
		return nil, false, nil
	}
	validBytes := data.Buffers()[0].Bytes()
	n := df.height
	var filtered *series.Series
	switch col.DType().ID() {
	case arrow.INT64:
		src := chunk.(*array.Int64).Int64Values()
		filtered, err = compute.FusedFilterInt64ByBitmap(name, src, validBytes, n, nil)
	case arrow.FLOAT64:
		src := chunk.(*array.Float64).Float64Values()
		filtered, err = compute.FusedFilterFloat64ByBitmap(name, src, validBytes, n, nil)
	default:
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	out, err := New(filtered)
	if err != nil {
		filtered.Release()
		return nil, true, err
	}
	return out, true, nil
}

// FillNull returns a new DataFrame where nulls are replaced with
// value in every column whose dtype matches the value's kind, mirroring
// polars' fill_null(value, matches_supertype=True): a number fills every
// numeric column (a float value widens integer columns to f64), a bool
// fills boolean columns and a string fills string columns. Other columns
// are cloned unchanged.
func (df *DataFrame) FillNull(value any) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		filled, err := fillNullValue(c, value)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[i] = filled
	}
	return New(out...)
}

// Unique returns a DataFrame with duplicate rows removed. Row equality
// considers every column; the first occurrence of each distinct row
// wins, preserving input order.
//
// Implementation: single-column int64 and float64 inputs go through a
// direct hash-dedup (~3-4x faster than the generic groupby path since
// it skips the per-group Agg machinery). Everything else falls back to
// GroupBy-with-no-aggs, which is equivalent in semantics.
func (df *DataFrame) Unique(ctx context.Context) (*DataFrame, error) {
	if df.height == 0 || df.Width() == 0 {
		return df.Clone(), nil
	}
	if df.Width() == 1 {
		if out, ok, err := uniqueFastSingle(df); ok {
			return out, err
		}
	}
	names := df.ColumnNames()
	return df.GroupBy(names...).Agg(ctx, nil)
}

// uniqueFastSingle runs single-column Unique via a direct hash dedup.
// Returns (result, true, err) when handled; (nil, false, nil) when the
// dtype isn't covered and the caller should fall back.
func uniqueFastSingle(df *DataFrame) (*DataFrame, bool, error) {
	col := df.cols[0]
	if col.NullCount() > 0 {
		// Null-handling is subtle (null is its own group) - defer to the
		// generic GroupBy path which already handles it correctly.
		return nil, false, nil
	}
	chunk := col.Chunk(0)
	n := chunk.Len()
	switch a := chunk.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		// Unique always prefers the serial path: goroutine startup and
		// the 3-phase merge pipeline in parallelAssignInt64 eat ~4 ms
		// of overhead at 262K, much more than the parallel speedup
		// gives us back when the caller only wants the distinct-key
		// list (no aggregation follows). Benchmarked: parallel 6 ms,
		// serial 2 ms on this input shape.
		_, uniq := serialAssignInt64(vals, n)
		s, err := series.FromInt64(col.Name(), uniq, nil)
		if err != nil {
			return nil, true, err
		}
		out, err := New(s)
		if err != nil {
			s.Release()
			return nil, true, err
		}
		return out, true, nil
	case *array.String:
		// Dictionary-encode and keep each code's first row: the same
		// first-seen order the group-by path produces, without the
		// per-row group id array or the aggregation machinery.
		rows := distinctStringRows(a)
		s, err := compute.TakeInt32(context.Background(), col, rows)
		if err != nil {
			return nil, true, err
		}
		out, err := New(s)
		if err != nil {
			s.Release()
			return nil, true, err
		}
		return out, true, nil
	}
	return nil, false, nil
}

// WithRowIndex prepends a u32 column named `name` with row numbers
// starting at `offset`, matching polars' IdxSize dtype. Polars default is
// offset=0 and name="index". The offset must be non-negative and the last
// index must fit in u32. The returned frame shares every original column
// by reference.
func (df *DataFrame) WithRowIndex(name string, offset int64) (*DataFrame, error) {
	if df.Contains(name) {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, name)
	}
	if offset < 0 {
		return nil, fmt.Errorf("dataframe.WithRowIndex: offset %d must be non-negative", offset)
	}
	if offset+int64(df.height) > math.MaxUint32+1 {
		return nil, fmt.Errorf("dataframe.WithRowIndex: row index overflows u32 (offset %d, height %d)", offset, df.height)
	}
	vals := make([]uint32, df.height)
	for i := range vals {
		vals[i] = uint32(offset + int64(i))
	}
	idx, err := series.FromUint32(name, vals, nil)
	if err != nil {
		return nil, err
	}
	cols := make([]*series.Series, 0, len(df.cols)+1)
	cols = append(cols, idx)
	for _, c := range df.cols {
		cols = append(cols, c.Clone())
	}
	return New(cols...)
}

// HStack returns a DataFrame horizontally concatenating this frame and
// other. Row counts must match; duplicate column names are rejected.
// Mirrors polars' DataFrame.hstack.
func (df *DataFrame) HStack(other *DataFrame) (*DataFrame, error) {
	if df.height != other.height {
		return nil, fmt.Errorf("%w: left=%d right=%d", ErrHeightMismatch, df.height, other.height)
	}
	cols := make([]*series.Series, 0, len(df.cols)+len(other.cols))
	seen := make(map[string]struct{}, len(df.cols)+len(other.cols))
	for _, c := range df.cols {
		seen[c.Name()] = struct{}{}
		cols = append(cols, c.Clone())
	}
	for _, c := range other.cols {
		if _, dup := seen[c.Name()]; dup {
			for _, already := range cols {
				already.Release()
			}
			return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, c.Name())
		}
		seen[c.Name()] = struct{}{}
		cols = append(cols, c.Clone())
	}
	return New(cols...)
}

// Extend vertically appends other onto df. Schemas must match
// (polars-compatible: VStack is the stricter version used for a
// pre-validated schema; we collapse both into one method with schema
// equality enforcement). Use Concat for the variadic alternative.
func (df *DataFrame) Extend(other *DataFrame) (*DataFrame, error) {
	if !df.sch.Equal(other.sch) {
		return nil, fmt.Errorf("dataframe.Extend: schemas differ: %s vs %s", df.sch, other.sch)
	}
	return Concat(df, other)
}

// AnyNullMask returns a boolean Series whose i-th entry is true when
// ANY column at row i is null. Useful for custom row-level null
// handling. The returned Series itself never has nulls.
func (df *DataFrame) AnyNullMask(_ context.Context) (*series.Series, error) {
	out := make([]bool, df.height)
	for _, c := range df.cols {
		if c.NullCount() == 0 {
			continue
		}
		chunk := c.Chunk(0)
		for i := range df.height {
			if !chunk.IsValid(i) {
				out[i] = true
			}
		}
	}
	return series.FromBool("any_null", out, nil)
}

// Apply maps fn over every column of df, returning a new DataFrame
// built from the transformed columns. Column names and order are
// preserved. If fn returns an error for any column, Apply releases
// progress and propagates the error.
func (df *DataFrame) Apply(fn func(*series.Series) (*series.Series, error)) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		res, err := fn(c)
		if err != nil {
			for _, prev := range out[:i] {
				if prev != nil {
					prev.Release()
				}
			}
			return nil, err
		}
		out[i] = res
	}
	return New(out...)
}

// SampleFrac returns a DataFrame of exactly floor(fraction * height)
// rows drawn without replacement (replacement=true to allow
// repetition). fraction must be in [0, 1] without replacement, or any
// non-negative value with replacement. Mirrors polars' df.sample(fraction=).
func (df *DataFrame) SampleFrac(ctx context.Context, fraction float64, withReplacement bool, seed uint64) (*DataFrame, error) {
	if fraction < 0 {
		return nil, fmt.Errorf("dataframe.SampleFrac: fraction must be >= 0, got %v", fraction)
	}
	if !withReplacement && fraction > 1 {
		return nil, fmt.Errorf("dataframe.SampleFrac: fraction > 1 requires withReplacement")
	}
	n := int(float64(df.height) * fraction)
	return df.Sample(ctx, n, withReplacement, seed)
}

// ensure compute stays referenced even when helpers below are dropped.
var _ = compute.Filter
var _ = array.NewInt64Data
