package dataframe

import (
	"context"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// ExplodeColumns explodes several list columns together: row i of every
// named column must hold lists of the same length (null and empty lists
// both count as a single null row). Other columns repeat. With one
// column it is Explode. Mirrors polars' DataFrame.explode([...]).
func (df *DataFrame) ExplodeColumns(ctx context.Context, cols ...string) (*DataFrame, error) {
	switch len(cols) {
	case 0:
		return df.Clone(), nil
	case 1:
		return df.Explode(ctx, cols[0])
	}
	exploded := make(map[int]arrow.Array, len(cols))
	release := func() {
		for _, a := range exploded {
			a.Release()
		}
	}
	var plan []int
	for _, name := range cols {
		idx, ok := df.sch.Index(name)
		if !ok {
			release()
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
		}
		if _, dup := exploded[idx]; dup {
			continue
		}
		s := df.cols[idx]
		if !s.DType().IsList() {
			release()
			return nil, fmt.Errorf("%w: %q has dtype %s", ErrNotList, name, s.DType())
		}
		ch := s.ToArrowChunked()
		listArr, err := concatListChunks(ch)
		ch.Release()
		if err != nil {
			release()
			return nil, err
		}
		takeIdx, nullMask, total := explodePlan(listArr)
		if plan == nil {
			plan = takeIdx
		} else if !slices.Equal(plan, takeIdx) {
			listArr.Release()
			release()
			return nil, fmt.Errorf("dataframe.ExplodeColumns: exploded columns must have matching element counts")
		}
		exploded[idx] = explodeValues(listArr, nullMask, total)
		listArr.Release()
	}
	out := make([]*series.Series, 0, len(df.cols))
	for i, c := range df.cols {
		if vals, ok := exploded[i]; ok {
			delete(exploded, i)
			s, err := seriesFromArray(c.Name(), vals)
			if err != nil {
				releaseAll(out)
				release()
				return nil, err
			}
			out = append(out, s)
			continue
		}
		taken, err := takeOptional(ctx, c, plan, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			release()
			return nil, fmt.Errorf("dataframe.ExplodeColumns: take on %q: %w", c.Name(), err)
		}
		out = append(out, taken)
	}
	return New(out...)
}
