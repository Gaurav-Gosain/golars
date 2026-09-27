package parquet

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// RowGroupReader reads one row group at a time from an open parquet
// file. The lazy engine uses it to run scans as morsels: each row group
// is decoded, filtered and projected (or partially aggregated) on its
// own, so the full column set of the file is never materialised at
// once. Read is safe for concurrent use with different indices.
type RowGroupReader struct {
	f      *os.File
	fr     *pqarrow.FileReader
	leaves []int
	n      int
}

// OpenRowGroups opens path for row-group reads of the named columns
// (nil reads every column).
func OpenRowGroups(path string, columns []string, opts ...Option) (*RowGroupReader, error) {
	cfg := resolve(opts)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %q: %w", path, err)
	}
	pf, err := file.NewParquetReader(f, file.WithReadProps(parquet.NewReaderProperties(cfg.alloc)))
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	// Columns of one row group decode in parallel; row groups themselves
	// are spread over workers by the caller.
	props := pqarrow.ArrowReadProperties{Parallel: true, BatchSize: batchSizeFor(pf), PreAllocBinaryData: true}
	if len(cfg.dictionary) > 0 {
		sc := pf.MetaData().Schema
		for _, name := range cfg.dictionary {
			// Only flat top-level string columns; anything else reads as
			// usual.
			if idx := sc.ColumnIndexByName(name); idx >= 0 {
				props.SetReadDict(idx, true)
			}
		}
	}
	fr, err := pqarrow.NewFileReader(pf, props, cfg.alloc)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	leaves, err := projectLeaves(fr.Manifest, columns)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	return &RowGroupReader{f: f, fr: fr, leaves: leaves, n: pf.NumRowGroups()}, nil
}

// NumRowGroups returns the number of row groups in the file.
func (r *RowGroupReader) NumRowGroups() int { return r.n }

// Read decodes row group i into a DataFrame with one chunk per column.
func (r *RowGroupReader) Read(ctx context.Context, i int) (*dataframe.DataFrame, error) {
	tbl, err := r.fr.ReadRowGroups(ctx, r.leaves, []int{i})
	if err != nil {
		return nil, fmt.Errorf("parquet: read row group %d: %w", i, err)
	}
	defer tbl.Release()
	return tableToDataFrame(tbl)
}

// Close releases the file.
func (r *RowGroupReader) Close() error { return r.f.Close() }
