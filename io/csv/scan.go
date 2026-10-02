package csv

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Scan returns a LazyFrame that reads path only at Collect time.
// Options propagate to ReadFile. Mirrors polars' `pl.scan_csv(path)`:
// the file open is deferred, and when the optimizer can prove only some
// columns are used only those are parsed.
func Scan(path string, opts ...Option) lazy.LazyFrame {
	return lazy.FromSourceProjected("csv:"+path, nil, func(ctx context.Context, cols []string) (*dataframe.DataFrame, error) {
		if cols == nil {
			return ReadFile(ctx, path, opts...)
		}
		if resolve(opts).schema != nil {
			// An explicit schema describes every column in the file;
			// read it whole and project afterwards.
			df, err := ReadFile(ctx, path, opts...)
			if err != nil {
				return nil, err
			}
			defer df.Release()
			return df.Select(cols...)
		}
		return ReadFile(ctx, path, append(append([]Option(nil), opts...), WithColumns(cols...))...)
	})
}
