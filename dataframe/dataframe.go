// Package dataframe defines DataFrame, an ordered collection of equal-length
// Series.
//
// A DataFrame is immutable: every transformation returns a new DataFrame.
// Where possible, the returned DataFrame shares underlying arrow buffers with
// the source through reference counting.
//
// Ownership. A DataFrame owns one reference to each of its Series. Column
// accessors (Column, ColumnAt, Columns) return the owned references directly
// without cloning. Callers that want a Series to outlive the DataFrame must
// call Series.Clone themselves. Calling Release on the DataFrame releases its
// reference to every contained Series; references returned from accessors
// become invalid at that point unless the caller cloned them.
package dataframe

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// Sentinel errors returned by DataFrame operations.
var (
	ErrColumnNotFound   = errors.New("dataframe: column not found")
	ErrDuplicateColumn  = errors.New("dataframe: duplicate column name")
	ErrHeightMismatch   = errors.New("dataframe: columns have different heights")
	ErrSliceOutOfBounds = errors.New("dataframe: slice out of bounds")
)

// DataFrame is an ordered set of equal-length named columns.
type DataFrame struct {
	sch    *schema.Schema
	cols   []*series.Series
	height int
}

// New builds a DataFrame from the given columns. All columns must have the
// same length; duplicate names are rejected. On success New consumes the
// caller's references to the input Series; callers must not Release them
// afterward. On error the caller retains ownership.
func New(cols ...*series.Series) (*DataFrame, error) {
	if len(cols) == 0 {
		sch, _ := schema.New()
		return &DataFrame{sch: sch}, nil
	}

	height := cols[0].Len()
	fields := make([]schema.Field, len(cols))
	seen := make(map[string]struct{}, len(cols))

	for i, c := range cols {
		if c.Len() != height {
			return nil, fmt.Errorf("%w: %q has length %d, expected %d",
				ErrHeightMismatch, c.Name(), c.Len(), height)
		}
		if _, dup := seen[c.Name()]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, c.Name())
		}
		seen[c.Name()] = struct{}{}
		fields[i] = schema.Field{Name: c.Name(), DType: c.DType()}
	}

	sch, err := schema.New(fields...)
	if err != nil {
		return nil, err
	}

	owned := make([]*series.Series, len(cols))
	copy(owned, cols)
	return &DataFrame{sch: sch, cols: owned, height: height}, nil
}

// Empty returns an empty DataFrame with the given schema. Every column is
// zero-length.
func Empty(sch *schema.Schema) *DataFrame {
	cols := make([]*series.Series, sch.Len())
	for i, f := range sch.Fields() {
		cols[i] = series.Empty(f.Name, f.DType)
	}
	return &DataFrame{sch: sch, cols: cols, height: 0}
}

// FromRecord adapts an arrow RecordBatch. Every column is retained; the
// caller's reference to rec is unchanged.
func FromRecord(rec arrow.RecordBatch) (*DataFrame, error) {
	ar := rec.Schema()
	cols := make([]*series.Series, rec.NumCols())
	for i := range cols {
		col := rec.Column(i)
		col.Retain()
		chunked := arrow.NewChunked(col.DataType(), []arrow.Array{col})
		col.Release() // NewChunked took its own reference
		cols[i] = series.FromChunked(ar.Field(i).Name, chunked)
		chunked.Release() // FromChunked retained again
	}
	return New(cols...)
}

// Width returns the number of columns.
func (df *DataFrame) Width() int { return len(df.cols) }

// Height returns the number of rows.
func (df *DataFrame) Height() int { return df.height }

// Shape returns (height, width).
func (df *DataFrame) Shape() (int, int) { return df.height, len(df.cols) }

// Schema returns the DataFrame schema.
func (df *DataFrame) Schema() *schema.Schema { return df.sch }

// Column returns the Series with the given name. The returned Series is owned
// by the DataFrame; do not Release it. Clone if you need independent ownership.
func (df *DataFrame) Column(name string) (*series.Series, error) {
	i, ok := df.sch.Index(name)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
	}
	return df.cols[i], nil
}

// ColumnAt returns the Series at position i. See Column for ownership notes.
func (df *DataFrame) ColumnAt(i int) *series.Series { return df.cols[i] }

// Columns returns all columns in order. See Column for ownership notes.
func (df *DataFrame) Columns() []*series.Series {
	out := make([]*series.Series, len(df.cols))
	copy(out, df.cols)
	return out
}

// Contains reports whether a column of the given name exists.
func (df *DataFrame) Contains(name string) bool { return df.sch.Contains(name) }

// Release drops the DataFrame's reference to every contained Series.
func (df *DataFrame) Release() {
	for _, c := range df.cols {
		c.Release()
	}
	df.cols = nil
}

// Clone returns a DataFrame that shares buffers with the source. Both
// DataFrames must be Released independently.
func (df *DataFrame) Clone() *DataFrame {
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		cols[i] = c.Clone()
	}
	return &DataFrame{sch: df.sch, cols: cols, height: df.height}
}

// Select returns a DataFrame with only the named columns in the order
// requested. The result shares buffers with the source.
func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	newSch, err := df.sch.Select(names...)
	if err != nil {
		return nil, err
	}
	cols := make([]*series.Series, len(names))
	for i, n := range names {
		idx, _ := df.sch.Index(n)
		cols[i] = df.cols[idx].Clone()
	}
	return &DataFrame{sch: newSch, cols: cols, height: df.height}, nil
}

// Drop returns a DataFrame without the named columns. Missing names are
// ignored. The result shares buffers with the source.
func (df *DataFrame) Drop(names ...string) *DataFrame {
	if len(names) == 0 {
		return df.Clone()
	}
	drop := make(map[string]struct{}, len(names))
	for _, n := range names {
		drop[n] = struct{}{}
	}
	newSch := df.sch.Drop(names...)
	cols := make([]*series.Series, 0, len(df.cols))
	for i, c := range df.cols {
		if _, removed := drop[df.sch.Field(i).Name]; removed {
			continue
		}
		cols = append(cols, c.Clone())
	}
	return &DataFrame{sch: newSch, cols: cols, height: df.height}
}

// Rename returns a DataFrame where oldName is replaced by newName.
func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	if oldName == newName {
		if !df.Contains(oldName) {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, oldName)
		}
		return df.Clone(), nil
	}
	idx, ok := df.sch.Index(oldName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, oldName)
	}
	if df.sch.Contains(newName) {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, newName)
	}
	newSch, err := df.sch.Rename(oldName, newName)
	if err != nil {
		return nil, err
	}
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		if i == idx {
			cols[i] = c.Rename(newName)
		} else {
			cols[i] = c.Clone()
		}
	}
	return &DataFrame{sch: newSch, cols: cols, height: df.height}, nil
}

// WithColumn appends s if its name is new, or replaces the existing column
// with that name in place. The column must have the same length as the
// DataFrame unless the DataFrame is empty of columns; a length-1 column
// broadcasts. On success WithColumn consumes the caller's reference to s.
func (df *DataFrame) WithColumn(s *series.Series) (*DataFrame, error) {
	if len(df.cols) > 0 && s.Len() == 1 && df.height != 1 {
		// A length-1 column (a scalar such as col("a").sum()) broadcasts
		// to the frame height, as in polars with_columns.
		b, err := s.Broadcast(df.height)
		if err != nil {
			return nil, err
		}
		s.Release()
		s = b
	}
	if len(df.cols) > 0 && s.Len() != df.height {
		return nil, fmt.Errorf("%w: %q has length %d, expected %d",
			ErrHeightMismatch, s.Name(), s.Len(), df.height)
	}

	newHeight := df.height
	if len(df.cols) == 0 {
		newHeight = s.Len()
	}

	if idx, ok := df.sch.Index(s.Name()); ok {
		cols := make([]*series.Series, len(df.cols))
		for i, c := range df.cols {
			if i == idx {
				cols[i] = s
			} else {
				cols[i] = c.Clone()
			}
		}
		newSch := df.sch.WithField(schema.Field{Name: s.Name(), DType: s.DType()})
		return &DataFrame{sch: newSch, cols: cols, height: newHeight}, nil
	}

	cols := make([]*series.Series, 0, len(df.cols)+1)
	for _, c := range df.cols {
		cols = append(cols, c.Clone())
	}
	cols = append(cols, s)
	newSch := df.sch.WithField(schema.Field{Name: s.Name(), DType: s.DType()})
	return &DataFrame{sch: newSch, cols: cols, height: newHeight}, nil
}

// Slice returns a DataFrame of [offset, offset+length) rows. Buffers are shared.
func (df *DataFrame) Slice(offset, length int) (*DataFrame, error) {
	if offset < 0 || length < 0 || offset+length > df.height {
		return nil, fmt.Errorf("%w: offset=%d length=%d height=%d",
			ErrSliceOutOfBounds, offset, length, df.height)
	}
	cols := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		sl, err := c.Slice(offset, length)
		if err != nil {
			for _, r := range cols[:i] {
				if r != nil {
					r.Release()
				}
			}
			return nil, err
		}
		cols[i] = sl
	}
	return &DataFrame{sch: df.sch, cols: cols, height: length}, nil
}

// Head returns the first n rows, or the whole frame if n >= height.
func (df *DataFrame) Head(n int) *DataFrame {
	if n < 0 {
		n = 0
	}
	if n > df.height {
		n = df.height
	}
	out, _ := df.Slice(0, n)
	return out
}

// Tail returns the last n rows.
func (df *DataFrame) Tail(n int) *DataFrame {
	if n < 0 {
		n = 0
	}
	if n > df.height {
		n = df.height
	}
	out, _ := df.Slice(df.height-n, n)
	return out
}

// String returns a polars-style box-drawn table repr. Uses
// DefaultFormatOptions; call Format for custom bounds.
func (df *DataFrame) String() string {
	return df.Format(DefaultFormatOptions())
}

// Summary returns the one-line "dataframe [H x W] schema{...}" shape the
// previous String() produced. Kept for compact log output.
func (df *DataFrame) Summary() string {
	return fmt.Sprintf("dataframe [%d x %d] %s", df.height, len(df.cols), df.sch)
}
