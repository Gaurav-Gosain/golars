package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/series"
)

// Concat stacks frames vertically (rows appended). All frames must
// share identical schemas (column names and dtypes in the same order).
// Height of the result is the sum of heights; column order matches
// the first frame.
//
// Mirrors polars pl.concat(..., how="vertical"). Concat borrows its
// inputs: the result holds its own references to the input buffers,
// so the caller still owns every input frame and must release it.
func Concat(frames ...*DataFrame) (*DataFrame, error) {
	switch len(frames) {
	case 0:
		return New()
	case 1:
		return frames[0].Clone(), nil
	}
	first := frames[0]
	if err := checkSchemasMatch(first, frames[1:]); err != nil {
		return nil, err
	}
	ncols := first.Width()
	out := make([]*series.Series, ncols)
	for col := range ncols {
		chunks := make([]arrow.Array, 0, len(frames))
		for _, f := range frames {
			c := f.ColumnAt(col)
			for _, chunk := range c.Chunks() {
				chunk.Retain()
				chunks = append(chunks, chunk)
			}
		}
		if len(chunks) == 0 {
			// Every input column has zero chunks (all frames empty).
			out[col] = series.Empty(first.ColumnAt(col).Name(), first.ColumnAt(col).DType())
			continue
		}
		// series.New consumes the chunk references on success, so only
		// the error path releases them.
		merged, err := series.New(first.ColumnAt(col).Name(), chunks...)
		if err != nil {
			for _, c := range chunks {
				c.Release()
			}
			for _, s := range out[:col] {
				if s != nil {
					s.Release()
				}
			}
			return nil, err
		}
		out[col] = merged
	}
	return New(out...)
}

func checkSchemasMatch(first *DataFrame, rest []*DataFrame) error {
	for i, f := range rest {
		if f.Width() != first.Width() {
			return fmt.Errorf("dataframe: Concat: frame %d has %d columns, expected %d",
				i+1, f.Width(), first.Width())
		}
		for c := range first.Width() {
			a := first.ColumnAt(c)
			b := f.ColumnAt(c)
			if a.Name() != b.Name() {
				return fmt.Errorf("dataframe: Concat: frame %d col %d name %q != %q",
					i+1, c, b.Name(), a.Name())
			}
			if a.DType().ID() != b.DType().ID() {
				return fmt.Errorf("dataframe: Concat: frame %d col %q dtype %s != %s",
					i+1, a.Name(), b.DType(), a.DType())
			}
		}
	}
	return nil
}

// VStack is a two-argument alias for Concat(df, other).
func (df *DataFrame) VStack(other *DataFrame) (*DataFrame, error) {
	return Concat(df, other)
}
