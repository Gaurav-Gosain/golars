package parquet

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
)

// Scan returns a LazyFrame that reads the Parquet file at path only
// when Collect runs. Options propagate to ReadFile. When the optimizer
// can prove only some columns are used, only those are decoded.
//
// The schema comes from the file footer when the file can be opened at
// plan time, so the optimizer can push filters and column pruning
// through joins and other nodes that need their input schema. If the
// footer cannot be read yet, the schema is left unknown and errors
// surface at Collect as before.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	load := func(ctx context.Context, cols []string) (*dataframe.DataFrame, error) {
		if cols == nil {
			return ReadFile(ctx, path, opts...)
		}
		return ReadFile(ctx, path, append(append([]Option(nil), opts...), WithColumns(cols...))...)
	}
	open := func(_ context.Context, bo lazy.BatchOptions) (lazy.BatchSource, error) {
		cols := bo.Columns
		if cols == nil {
			cols = resolve(opts).columns
		}
		r, err := OpenRowGroups(path, cols, opts...)
		if err != nil {
			return nil, err
		}
		return rowGroupBatches{r}, nil
	}
	return lazy.FromSourceBatched("parquet:"+path, scanSchema(path, opts), load, open)
}

// rowGroupBatches adapts RowGroupReader to lazy.BatchSource.
type rowGroupBatches struct{ *RowGroupReader }

func (b rowGroupBatches) NumBatches() int { return b.NumRowGroups() }
func (b rowGroupBatches) ReadBatch(ctx context.Context, i int) (*dataframe.DataFrame, error) {
	return b.Read(ctx, i)
}

func scanSchema(path string, opts []Option) *schema.Schema {
	sc, err := ReadSchema(path)
	if err != nil {
		return nil
	}
	if cols := resolve(opts).columns; len(cols) > 0 {
		if sc, err = sc.Select(cols...); err != nil {
			return nil
		}
	}
	return sc
}
