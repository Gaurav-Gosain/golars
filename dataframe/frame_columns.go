package dataframe

import (
	"context"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// GetColumn returns the named column. It is the polars-named alias of
// Column; the returned Series is owned by the frame.
func (df *DataFrame) GetColumn(name string) (*series.Series, error) { return df.Column(name) }

// GetColumns returns every column in order. Alias of Columns.
func (df *DataFrame) GetColumns() []*series.Series { return df.Columns() }

// GetColumnIndex returns the position of the named column. Mirrors
// polars' DataFrame.get_column_index.
func (df *DataFrame) GetColumnIndex(name string) (int, error) {
	i, ok := df.sch.Index(name)
	if !ok {
		return -1, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
	}
	return i, nil
}

// CollectSchema returns the schema. Eager frames already know it; the
// method exists for parity with polars' DataFrame.collect_schema.
func (df *DataFrame) CollectSchema() *schema.Schema { return df.sch }

// InsertColumn returns a frame with s inserted at position index. A
// negative index counts from the end, so -1 inserts before the last
// column. The name must be new. On success the caller's reference to s is
// consumed. Mirrors polars' DataFrame.insert_column.
func (df *DataFrame) InsertColumn(index int, s *series.Series) (*DataFrame, error) {
	w := len(df.cols)
	if index < 0 {
		index += w
	}
	if index < 0 || index > w {
		return nil, fmt.Errorf("dataframe.InsertColumn: column index %d is out of range (frame has %d columns)", index, w)
	}
	if df.sch.Contains(s.Name()) {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, s.Name())
	}
	if w > 0 && s.Len() != df.height {
		return nil, fmt.Errorf("%w: %q has length %d, expected %d", ErrHeightMismatch, s.Name(), s.Len(), df.height)
	}
	cols := make([]*series.Series, 0, w+1)
	for i, c := range df.cols {
		if i == index {
			cols = append(cols, s)
		}
		cols = append(cols, c.Clone())
	}
	if index == w {
		cols = append(cols, s)
	}
	out, err := New(cols...)
	if err != nil {
		for _, c := range cols {
			if c != s {
				c.Release()
			}
		}
		return nil, err
	}
	return out, nil
}

// ReplaceColumn returns a frame where the column at index is replaced by
// s, which may carry a different name and dtype. A negative index counts
// from the end. On success the caller's reference to s is consumed.
// Mirrors polars' DataFrame.replace_column.
func (df *DataFrame) ReplaceColumn(index int, s *series.Series) (*DataFrame, error) {
	w := len(df.cols)
	if index < 0 {
		index += w
	}
	if index < 0 || index >= w {
		return nil, fmt.Errorf("dataframe.ReplaceColumn: column index %d is out of range (frame has %d columns)", index, w)
	}
	if s.Len() != df.height {
		return nil, fmt.Errorf("%w: %q has length %d, expected %d", ErrHeightMismatch, s.Name(), s.Len(), df.height)
	}
	cols := make([]*series.Series, w)
	for i, c := range df.cols {
		if i == index {
			cols[i] = s
			continue
		}
		cols[i] = c.Clone()
	}
	out, err := New(cols...)
	if err != nil {
		for i, c := range cols {
			if i != index {
				c.Release()
			}
		}
		return nil, err
	}
	return out, nil
}

// IterColumns yields every column in order. The yielded Series are owned
// by the frame. Mirrors polars' DataFrame.iter_columns.
func (df *DataFrame) IterColumns() iter.Seq[*series.Series] {
	return func(yield func(*series.Series) bool) {
		for _, c := range df.cols {
			if !yield(c) {
				return
			}
		}
	}
}

// IterSlices yields consecutive zero-copy row slices of at most n rows.
// Each yielded frame is released when the loop body returns; Clone it to
// keep it longer. Mirrors polars' DataFrame.iter_slices.
func (df *DataFrame) IterSlices(n int) iter.Seq[*DataFrame] {
	return func(yield func(*DataFrame) bool) {
		if n <= 0 {
			return
		}
		for off := 0; off < df.height; off += n {
			sl, err := df.Slice(off, min(n, df.height-off))
			if err != nil {
				return
			}
			cont := yield(sl)
			sl.Release()
			if !cont {
				return
			}
		}
	}
}

// ToSeries returns a new reference to the column at index (negative
// counts from the end). The caller owns the result. Mirrors polars'
// DataFrame.to_series.
func (df *DataFrame) ToSeries(index int) (*series.Series, error) {
	w := len(df.cols)
	if index < 0 {
		index += w
	}
	if index < 0 || index >= w {
		return nil, fmt.Errorf("dataframe.ToSeries: index %d out of range for %d columns", index, w)
	}
	return df.cols[index].Clone(), nil
}

// ToStruct packs every column into a single struct Series called name.
// Mirrors polars' DataFrame.to_struct.
func (df *DataFrame) ToStruct(name string) (*series.Series, error) {
	if len(df.cols) == 0 {
		return nil, fmt.Errorf("dataframe.ToStruct: frame has no columns")
	}
	arrs := make([]arrow.Array, len(df.cols))
	fields := make([]arrow.Field, len(df.cols))
	for i, c := range df.cols {
		a, err := c.Consolidated()
		if err != nil {
			for _, p := range arrs[:i] {
				p.Release()
			}
			return nil, err
		}
		arrs[i] = a
		fields[i] = arrow.Field{Name: c.Name(), Type: a.DataType(), Nullable: true}
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	st, err := array.NewStructArrayWithFields(arrs, fields)
	if err != nil {
		return nil, err
	}
	return seriesFromArray(name, st)
}

// Rechunk returns a frame whose columns each hold a single contiguous
// chunk. Mirrors polars' DataFrame.rechunk.
func (df *DataFrame) Rechunk() *DataFrame {
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		cols[i] = c.Rechunk()
	}
	return &DataFrame{sch: df.sch, cols: cols, height: df.height}
}

// NChunks returns the chunk count of the first column (0 for a frame
// without columns). Mirrors polars' DataFrame.n_chunks("first").
func (df *DataFrame) NChunks() int {
	if len(df.cols) == 0 {
		return 0
	}
	return df.cols[0].NumChunks()
}

// NChunksAll returns the chunk count of every column. Mirrors polars'
// DataFrame.n_chunks("all").
func (df *DataFrame) NChunksAll() []int {
	out := make([]int, len(df.cols))
	for i, c := range df.cols {
		out[i] = c.NumChunks()
	}
	return out
}

// DTypeCast maps every column of dtype From to dtype To.
type DTypeCast struct {
	From dtype.DType
	To   dtype.DType
}

// Cast casts every column to to. With strict set, a value that cannot be
// represented in the target dtype is an error; otherwise it becomes null.
// Mirrors polars' DataFrame.cast(dtype, strict=...).
func (df *DataFrame) Cast(ctx context.Context, to dtype.DType, strict bool) (*DataFrame, error) {
	return df.castWith(ctx, strict, func(_ string, _ dtype.DType) (dtype.DType, bool) { return to, true })
}

// CastColumns casts the named columns. Every name must exist. Mirrors
// polars' DataFrame.cast({name: dtype}).
func (df *DataFrame) CastColumns(ctx context.Context, casts map[string]dtype.DType, strict bool) (*DataFrame, error) {
	for name := range casts {
		if !df.sch.Contains(name) {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
		}
	}
	return df.castWith(ctx, strict, func(name string, _ dtype.DType) (dtype.DType, bool) {
		to, ok := casts[name]
		return to, ok
	})
}

// CastDTypes casts every column whose dtype equals one of the From
// dtypes. Mirrors polars' DataFrame.cast({dtype: dtype}).
func (df *DataFrame) CastDTypes(ctx context.Context, casts []DTypeCast, strict bool) (*DataFrame, error) {
	return df.castWith(ctx, strict, func(_ string, dt dtype.DType) (dtype.DType, bool) {
		for _, c := range casts {
			if c.From.Equal(dt) {
				return c.To, true
			}
		}
		return dtype.DType{}, false
	})
}

func (df *DataFrame) castWith(ctx context.Context, strict bool, target func(string, dtype.DType) (dtype.DType, bool)) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		to, ok := target(c.Name(), c.DType())
		if !ok || to.Equal(c.DType()) {
			out[i] = c.Clone()
			continue
		}
		casted, err := castSeries(ctx, c, to, strict)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[i] = casted
	}
	return New(out...)
}

// castSeries casts s to to. A strict cast fails when the cast introduced
// nulls that were not in the input.
func castSeries(ctx context.Context, s *series.Series, to dtype.DType, strict bool) (*series.Series, error) {
	if s.DType().IsNull() {
		arr := array.MakeArrayOfNull(memory.DefaultAllocator, to.Arrow(), s.Len())
		return seriesFromArray(s.Name(), arr)
	}
	casted, err := compute.Cast(ctx, s, to)
	if err != nil {
		return nil, err
	}
	if strict && casted.NullCount() > s.NullCount() {
		failed := casted.NullCount() - s.NullCount()
		casted.Release()
		return nil, fmt.Errorf("dataframe: conversion from `%s` to `%s` failed in column %q for %d out of %d values",
			s.DType(), to, s.Name(), failed, s.Len())
	}
	return casted, nil
}

// MatchToSchemaOptions mirrors the keyword arguments of polars'
// DataFrame.match_to_schema. Zero values mean polars' defaults.
type MatchToSchemaOptions struct {
	// InsertMissing inserts an all-null column for every schema column
	// the frame lacks (polars missing_columns="insert"). Default raises.
	InsertMissing bool
	// IgnoreExtra drops frame columns absent from the schema (polars
	// extra_columns="ignore"). Default raises.
	IgnoreExtra bool
	// UpcastIntegers allows lossless integer widening (polars
	// integer_cast="upcast"). Default forbids any integer dtype change.
	UpcastIntegers bool
	// UpcastFloats allows f32 to f64 (polars float_cast="upcast").
	UpcastFloats bool
}

// MatchToSchema returns a frame whose columns match sch exactly, in the
// schema's order. Mirrors polars' DataFrame.match_to_schema for the
// column-level options; struct field options are not supported.
func (df *DataFrame) MatchToSchema(ctx context.Context, sch *schema.Schema, opts MatchToSchemaOptions) (*DataFrame, error) {
	if !opts.IgnoreExtra {
		for _, name := range df.sch.Names() {
			if !sch.Contains(name) {
				return nil, fmt.Errorf("dataframe.MatchToSchema: extra column in frame: %q", name)
			}
		}
	}
	out := make([]*series.Series, 0, sch.Len())
	for _, f := range sch.Fields() {
		c, err := df.Column(f.Name)
		if err != nil {
			if !opts.InsertMissing {
				releaseAll(out)
				return nil, fmt.Errorf("dataframe.MatchToSchema: missing column in frame: %q", f.Name)
			}
			arr := array.MakeArrayOfNull(memory.DefaultAllocator, f.DType.Arrow(), df.height)
			s, err := seriesFromArray(f.Name, arr)
			if err != nil {
				releaseAll(out)
				return nil, err
			}
			out = append(out, s)
			continue
		}
		if c.DType().Equal(f.DType) {
			out = append(out, c.Clone())
			continue
		}
		if !canUpcast(c.DType(), f.DType, opts) {
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.MatchToSchema: data type mismatch for column %s: incoming: %s != target: %s",
				f.Name, c.DType(), f.DType)
		}
		casted, err := castSeries(ctx, c, f.DType, true)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, casted)
	}
	return New(out...)
}

// canUpcast reports whether from widens losslessly into to under the
// allowed cast options.
func canUpcast(from, to dtype.DType, opts MatchToSchemaOptions) bool {
	if from.IsInteger() && to.IsInteger() {
		if !opts.UpcastIntegers {
			return false
		}
		fb, fs := intInfo(from.ID())
		tb, ts := intInfo(to.ID())
		switch {
		case fs == ts:
			return tb > fb
		case !fs && ts:
			return tb > fb
		}
		return false
	}
	if from.IsFloating() && to.IsFloating() {
		return opts.UpcastFloats && from.ID() == arrow.FLOAT32 && to.ID() == arrow.FLOAT64
	}
	return false
}

func intInfo(id arrow.Type) (bits int, signed bool) {
	switch id {
	case arrow.INT8:
		return 8, true
	case arrow.INT16:
		return 16, true
	case arrow.INT32:
		return 32, true
	case arrow.INT64:
		return 64, true
	case arrow.UINT8:
		return 8, false
	case arrow.UINT16:
		return 16, false
	case arrow.UINT32:
		return 32, false
	}
	return 64, false
}

// Fold reduces the columns left to right with fn, starting from the
// first column: acc = fn(acc, next). fn receives borrowed Series and must
// return a new Series owned by the caller. Mirrors polars'
// DataFrame.fold.
func (df *DataFrame) Fold(fn func(acc, next *series.Series) (*series.Series, error)) (*series.Series, error) {
	if len(df.cols) == 0 {
		return nil, fmt.Errorf("dataframe.Fold: frame has no columns")
	}
	acc := df.cols[0].Clone()
	for _, c := range df.cols[1:] {
		next, err := fn(acc, c)
		acc.Release()
		if err != nil {
			return nil, err
		}
		acc = next
	}
	return acc, nil
}
