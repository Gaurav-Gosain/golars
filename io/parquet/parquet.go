// Package parquet reads and writes Parquet files using arrow-go's pqarrow
// bridge.
//
// Parquet is a columnar on-disk format with compression, statistics, and
// predicate pushdown support. golars exposes a small Read/Write surface
// here, plus column projection (WithColumns) and footer-only schema reads
// (ReadSchema) so lazy scans decode only what a query needs.
package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// Option configures a Read or Write call.
type Option func(*config)

type config struct {
	alloc       memory.Allocator
	compression compress.Compression
	chunkSize   int64
	httpClient  *http.Client
	columns     []string
	// noNative forces the pqarrow reader and writer (tests compare the
	// two paths).
	noNative bool
}

func resolve(opts []Option) config {
	c := config{
		alloc:       memory.DefaultAllocator,
		compression: compress.Codecs.Snappy,
		chunkSize:   1 << 16, // 64K rows per row group
		httpClient:  http.DefaultClient,
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the allocator used for intermediate buffers.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *config) { c.alloc = alloc }
}

// WithCompression sets the compression codec for written files. Supported
// values are compress.Codecs.Snappy (default), Gzip, Brotli, Zstd, Lz4, and
// Uncompressed.
func WithCompression(codec compress.Compression) Option {
	return func(c *config) { c.compression = codec }
}

// WithChunkSize controls the row-group size used when writing.
func WithChunkSize(rows int64) Option {
	return func(c *config) {
		if rows > 0 {
			c.chunkSize = rows
		}
	}
}

// WithColumns restricts Read to the named top-level columns. Only those
// columns are decoded and the result keeps the requested order. An
// unknown or repeated name is an error. Mirrors polars'
// pl.read_parquet(path, columns=[...]).
func WithColumns(names ...string) Option {
	return func(c *config) { c.columns = append([]string{}, names...) }
}

// Read reads a parquet file from r into a DataFrame. r must support random
// access because parquet reads the footer before the body.
func Read(ctx context.Context, r parquet.ReaderAtSeeker, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)

	pf, err := file.NewParquetReader(r, file.WithReadProps(parquet.NewReaderProperties(cfg.alloc)))
	if err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	props := pqarrow.ArrowReadProperties{Parallel: true, BatchSize: batchSizeFor(pf), PreAllocBinaryData: true}
	fr, err := pqarrow.NewFileReader(pf, props, cfg.alloc)
	if err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	if !cfg.noNative {
		df, err := readNativeTable(ctx, r, pf, fr, cfg)
		if err == nil {
			return df, nil
		}
		if !errors.Is(err, errUnsupported) {
			return nil, fmt.Errorf("parquet: read: %w", err)
		}
		pqarrowReads.Add(1)
	}
	table, err := readTable(ctx, fr, pf.NumRowGroups(), cfg)
	if err != nil {
		return nil, fmt.Errorf("parquet: read: %w", err)
	}
	defer table.Release()

	return tableToDataFrame(table)
}

// ReadSchema returns the schema of the parquet file at path. Only the
// footer is read; no column data is decoded.
func ReadSchema(path string) (*schema.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %q: %w", path, err)
	}
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("parquet: read schema: %w", err)
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("parquet: read schema: %w", err)
	}
	sc, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("parquet: read schema: %w", err)
	}
	return schema.FromArrow(sc), nil
}

// ReadBytes is a convenience wrapper over Read for in-memory data.
func ReadBytes(ctx context.Context, b []byte, opts ...Option) (*dataframe.DataFrame, error) {
	return Read(ctx, newSliceReader(b), opts...)
}

// ReadFile opens path and reads the parquet file into a DataFrame. The
// file is memory mapped where the platform supports it; no part of the
// result refers to the mapping.
func ReadFile(ctx context.Context, path string, opts ...Option) (*dataframe.DataFrame, error) {
	m, err := mmapfile.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %q: %w", path, err)
	}
	defer m.Close()
	return readGuarded(ctx, m.Data, opts)
}

// readGuarded reads a possibly memory mapped file. A memory fault on the
// calling goroutine (the file shrank while mapped) becomes an error;
// decode workers and the reader handed to pqarrow guard themselves.
func readGuarded(ctx context.Context, b []byte, opts []Option) (df *dataframe.DataFrame, err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		if errors.Is(err, mmapfile.ErrFault) && !strings.HasPrefix(err.Error(), "parquet:") {
			err = fmt.Errorf("parquet: read: %w", err)
		}
	}()
	defer mmapfile.Recover(&err)
	return ReadBytes(ctx, b, opts...)
}

// ReadURL fetches parquet from an http(s) URL and reads it into a
// DataFrame. The body is buffered in memory because parquet reads the
// footer before the body. For very large remote files prefer streaming
// to a temp file and using ReadFile. Mirrors polars' pl.read_parquet(url).
func ReadURL(ctx context.Context, url string, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cfg.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("parquet.ReadURL: status %s for %s", resp.Status, url)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parquet.ReadURL: %w", err)
	}
	return ReadBytes(ctx, data, opts...)
}

// WithHTTPClient overrides the HTTP client used by ReadURL.
func WithHTTPClient(c *http.Client) Option { return func(cf *config) { cf.httpClient = c } }

// Write writes df to w in parquet format.
func Write(ctx context.Context, w io.Writer, df *dataframe.DataFrame, opts ...Option) error {
	cfg := resolve(opts)
	if err := ctx.Err(); err != nil {
		return err
	}

	table, err := dataFrameToTable(df, cfg.alloc)
	if err != nil {
		return fmt.Errorf("parquet: build table: %w", err)
	}
	defer table.Release()

	if err := writeTable(ctx, table, w, cfg); err != nil {
		return fmt.Errorf("parquet: write: %w", err)
	}
	return nil
}

// WriteFile creates path and writes df as parquet.
func WriteFile(ctx context.Context, path string, df *dataframe.DataFrame, opts ...Option) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("parquet: create %q: %w", path, err)
	}
	// pqarrow closes the underlying writer itself on successful write. We
	// still defer a Close for the error path; a best-effort second close is
	// a no-op on an already-closed *os.File beyond reporting.
	defer f.Close()
	return Write(ctx, f, df, opts...)
}

func dataFrameToTable(df *dataframe.DataFrame, _ memory.Allocator) (arrow.Table, error) {
	sch := df.Schema().ToArrow()
	cols := make([]arrow.Column, df.Width())

	for i, s := range df.Columns() {
		// arrow.NewColumn retains the chunked array internally, and
		// array.NewTable below retains it again. We release the retain from
		// NewColumn ourselves so that table.Release balances our contribution
		// exactly.
		col := arrow.NewColumn(sch.Field(i), s.Chunked())
		cols[i] = *col
		col.Release()
	}

	return array.NewTable(sch, cols, int64(df.Height())), nil
}

func tableToDataFrame(t arrow.Table) (*dataframe.DataFrame, error) {
	numCols := int(t.NumCols())
	seriesCols := make([]*series.Series, numCols)

	for i := range numCols {
		col := t.Column(i)
		chunked := col.Data()
		s := series.FromChunked(col.Name(), chunked)
		seriesCols[i] = s
	}

	df, err := dataframe.New(seriesCols...)
	if err != nil {
		for _, s := range seriesCols {
			if s != nil {
				s.Release()
			}
		}
		return nil, fmt.Errorf("parquet: build dataframe: %w", err)
	}
	return df, nil
}
