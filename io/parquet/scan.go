package parquet

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads the Parquet file at path only
// when Collect runs. Options propagate to ReadFile. When the optimizer
// can prove only some columns are used, only those are decoded.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSourceProjected("parquet:"+path, nil, func(ctx context.Context, cols []string) (*dataframe.DataFrame, error) {
		if cols == nil {
			return ReadFile(ctx, path, opts...)
		}
		return ReadFile(ctx, path, append(append([]Option(nil), opts...), WithColumns(cols...))...)
	})
}
