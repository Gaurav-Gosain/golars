package parquet

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
)

// RowGroupReader reads one row group at a time from a parquet file. The
// lazy engine uses it to run scans as morsels: each row group is
// decoded, filtered and projected (or partially aggregated) on its own,
// so the full column set of the file is never materialised at once.
// Flat columns go through the native reader, like ReadFile; pqarrow
// handles the rest. Read is safe for concurrent use with different
// indices.
type RowGroupReader struct {
	m   *mmapfile.File
	r   *sliceReader
	pf  *file.Reader
	fr  *pqarrow.FileReader
	cfg config
}

// OpenRowGroups opens path for row-group reads of the named columns
// (nil reads every column).
func OpenRowGroups(path string, columns []string, opts ...Option) (rg *RowGroupReader, err error) {
	cfg := resolve(opts)
	cfg.columns = columns
	m, err := mmapfile.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %q: %w", path, err)
	}
	defer func() {
		if err != nil {
			m.Close()
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer mmapfile.Recover(&err)
	r := newSliceReader(m.Data)
	pf, err := file.NewParquetReader(r, file.WithReadProps(parquet.NewReaderProperties(cfg.alloc)))
	if err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	// pqarrow reads the columns the native reader does not handle.
	props := pqarrow.ArrowReadProperties{Parallel: true, BatchSize: batchSizeFor(pf), PreAllocBinaryData: true}
	fr, err := pqarrow.NewFileReader(pf, props, cfg.alloc)
	if err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	if _, err := projectFields(fr.Manifest, columns); err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	return &RowGroupReader{m: m, r: r, pf: pf, fr: fr, cfg: cfg}, nil
}

// NumRowGroups returns the number of row groups in the file.
func (g *RowGroupReader) NumRowGroups() int { return g.pf.NumRowGroups() }

// Read decodes row group i into a DataFrame.
func (g *RowGroupReader) Read(ctx context.Context, i int) (df *dataframe.DataFrame, err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer mmapfile.Recover(&err)
	if !g.cfg.noNative {
		df, err = readNativeRowGroups(ctx, g.r, g.pf, g.fr, g.cfg, []int{i})
		if !errors.Is(err, errUnsupported) {
			if err != nil {
				return nil, fmt.Errorf("parquet: read row group %d: %w", i, err)
			}
			return df, nil
		}
	}
	leaves, err := projectLeaves(g.fr.Manifest, g.cfg.columns)
	if err != nil {
		return nil, err
	}
	tbl, err := g.fr.ReadRowGroups(ctx, leaves, []int{i})
	if err != nil {
		return nil, fmt.Errorf("parquet: read row group %d: %w", i, err)
	}
	defer tbl.Release()
	return tableToDataFrame(tbl)
}

// Close unmaps the file.
func (g *RowGroupReader) Close() error { return g.m.Close() }
