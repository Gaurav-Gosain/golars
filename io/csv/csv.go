// Package csv reads and writes RFC 4180 CSV.
//
// Reading uses a native parallel parser: the input is split into
// record-aligned chunks that are parsed concurrently straight into arrow
// buffers. Type inference is used by default; pass WithSchema to read a
// known schema with stronger type control. Writing uses arrow-go's csv
// writer.
package csv

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/series"
)

// Option configures a Read or Write call.
type Option func(*config)

type config struct {
	alloc       memory.Allocator
	schema      *arrow.Schema
	hasHeader   bool
	delimiter   rune
	chunk       int
	nullValues  []string
	includeCols []string
	httpClient  *http.Client
	// tryParseDates is nil when unset (arrow's own inference applies).
	tryParseDates *bool
	columnTypes   map[string]arrow.DataType
}

func resolve(opts []Option) config {
	c := config{
		alloc:      memory.DefaultAllocator,
		hasHeader:  true,
		delimiter:  ',',
		chunk:      1 << 13, // 8K rows per batch
		httpClient: http.DefaultClient,
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the allocator used for buffers.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *config) { c.alloc = alloc }
}

// WithSchema specifies the arrow schema to read into. When unset the reader
// infers column types from the CSV contents.
func WithSchema(s *schema.Schema) Option {
	return func(c *config) { c.schema = s.ToArrow() }
}

// WithHasHeader controls whether the first row is treated as column names.
// Default: true.
func WithHasHeader(has bool) Option {
	return func(c *config) { c.hasHeader = has }
}

// WithDelimiter overrides the field separator. Default: ','.
func WithDelimiter(d rune) Option {
	return func(c *config) { c.delimiter = d }
}

// WithChunkSize controls the number of rows per record batch in the
// arrow-go fallback reader (used for multi-byte delimiters). The native
// reader sizes its parallel chunks by bytes and ignores it.
func WithChunkSize(rows int) Option {
	return func(c *config) {
		if rows > 0 {
			c.chunk = rows
		}
	}
}

// WithNullValues treats the given strings as null on read.
func WithNullValues(values ...string) Option {
	return func(c *config) { c.nullValues = append([]string(nil), values...) }
}

// WithColumns selects a subset of columns to read by name. Unknown names are
// an error.
func WithColumns(names ...string) Option {
	return func(c *config) { c.includeCols = append([]string(nil), names...) }
}

// Read reads CSV from r into a DataFrame. If no schema is supplied via
// WithSchema, column types are inferred from the first 100 data rows:
// each column gets the first of Int64, Boolean, Date32, Time32,
// Timestamp, Float64, String that parses every sampled value. A value
// past the sample that does not fit widens the column (for example
// Int64 to Float64) and the input is parsed again. With an explicit
// schema a value that does not parse is an error.
//
// Empty fields are null in non-string columns. String columns only
// produce nulls for values listed in WithNullValues.
//
// The input is read fully into memory and parsed in parallel.
func Read(ctx context.Context, r io.Reader, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	if cfg.tryParseDates != nil {
		return readTryParseDates(ctx, r, cfg, opts)
	}
	data, err := readAll(r)
	if err != nil {
		return nil, fmt.Errorf("csv: read: %w", err)
	}
	return readBytes(ctx, data, cfg)
}

func readBytes(ctx context.Context, data []byte, cfg config) (*dataframe.DataFrame, error) {
	df, err := readNative(ctx, data, cfg)
	if errors.Is(err, errFallback) {
		return readArrow(ctx, bytes.NewReader(data), cfg)
	}
	return df, err
}

// readAll reads r fully, presizing the buffer when the size is known.
func readAll(r io.Reader) ([]byte, error) {
	size := 0
	switch v := r.(type) {
	case interface{ Stat() (os.FileInfo, error) }:
		if st, err := v.Stat(); err == nil && st.Mode().IsRegular() {
			size = int(st.Size())
		}
	case interface{ Len() int }:
		size = v.Len()
	}
	if size <= 0 {
		return io.ReadAll(r)
	}
	// io.ReadFull into an exact buffer; bytes.Buffer.ReadFrom would
	// double the buffer when it runs out of slack at the end.
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	switch err {
	case nil:
	case io.ErrUnexpectedEOF, io.EOF:
		return buf[:n], nil
	default:
		return nil, err
	}
	rest, err := io.ReadAll(r) // the input grew after Stat
	if err != nil {
		return nil, err
	}
	return append(buf, rest...), nil
}

// readArrow is the arrow-go based reader. It handles inputs the native
// reader does not support (multi-byte delimiters, invalid UTF-8 in
// inferred string columns).
func readArrow(ctx context.Context, r io.Reader, cfg config) (*dataframe.DataFrame, error) {
	readerOpts := []arrowcsv.Option{
		arrowcsv.WithAllocator(cfg.alloc),
		arrowcsv.WithHeader(cfg.hasHeader),
		arrowcsv.WithComma(cfg.delimiter),
		arrowcsv.WithChunk(cfg.chunk),
	}
	// polars reads an empty field as null for every dtype
	// (missing_is_null); arrow's default treats it as a parse error for
	// numbers and as "" for strings.
	readerOpts = append(readerOpts, arrowcsv.WithNullReader(true, append([]string{""}, cfg.nullValues...)...))
	if len(cfg.includeCols) > 0 {
		readerOpts = append(readerOpts, arrowcsv.WithIncludeColumns(cfg.includeCols))
	}
	if len(cfg.columnTypes) > 0 {
		readerOpts = append(readerOpts, arrowcsv.WithColumnTypes(cfg.columnTypes))
	}

	br := bufio.NewReaderSize(r, headPeekSize)
	if head, ok := peekNoRows(br, cfg.hasHeader); ok {
		return readNoRows(head, cfg)
	}
	var reader *arrowcsv.Reader
	if cfg.schema != nil {
		reader = arrowcsv.NewReader(br, cfg.schema, readerOpts...)
	} else {
		reader = arrowcsv.NewInferringReader(br, readerOpts...)
	}
	defer reader.Release()

	var sch *arrow.Schema
	var chunks [][]arrow.Array
	var numCols int

	for reader.Next() {
		if err := ctx.Err(); err != nil {
			releaseChunks(chunks)
			return nil, err
		}
		rec := reader.RecordBatch()
		if sch == nil {
			sch = rec.Schema()
			numCols = int(rec.NumCols())
			chunks = make([][]arrow.Array, numCols)
		}
		for i := range numCols {
			col := rec.Column(i)
			col.Retain()
			chunks[i] = append(chunks[i], col)
		}
	}
	if err := reader.Err(); err != nil {
		releaseChunks(chunks)
		return nil, fmt.Errorf("csv: read: %w", err)
	}
	if sch == nil {
		// No data rows; fall back to the reader's schema (available after
		// NewInferringReader builds the header).
		sch = reader.Schema()
		numCols = sch.NumFields()
		chunks = make([][]arrow.Array, numCols)
	}

	cols := make([]*series.Series, numCols)
	for i := range numCols {
		name := sch.Field(i).Name
		if len(chunks[i]) == 0 {
			cols[i] = series.Empty(name, dtype.FromArrow(sch.Field(i).Type))
			continue
		}
		s, err := series.New(name, chunks[i]...)
		if err != nil {
			for _, pc := range cols[:i] {
				if pc != nil {
					pc.Release()
				}
			}
			releaseChunks(chunks[i:])
			return nil, fmt.Errorf("csv: build series %q: %w", name, err)
		}
		cols[i] = s
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
		return nil, fmt.Errorf("csv: build dataframe: %w", err)
	}
	return df, nil
}

// ReadFile opens path and reads CSV into a DataFrame. The file is
// memory mapped where the platform supports it and parsed in place; the
// result does not refer to the mapping.
func ReadFile(ctx context.Context, path string, opts ...Option) (*dataframe.DataFrame, error) {
	if cfg := resolve(opts); cfg.tryParseDates == nil {
		m, err := mmapfile.Open(path)
		if err != nil {
			return nil, fmt.Errorf("csv: open %q: %w", path, err)
		}
		defer m.Close()
		return readBytes(ctx, m.Data, cfg)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("csv: open %q: %w", path, err)
	}
	defer f.Close()
	return Read(ctx, f, opts...)
}

// ReadURL fetches CSV from an http(s) URL and reads it into a DataFrame.
// The context governs cancellation of the HTTP request. Non-2xx responses
// are returned as errors. A default http.Client is used unless overridden
// via WithHTTPClient. Mirrors polars' pl.read_csv(url) convenience.
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
		return nil, fmt.Errorf("csv.ReadURL: status %s for %s", resp.Status, url)
	}
	return Read(ctx, resp.Body, opts...)
}

// WithHTTPClient overrides the HTTP client used by ReadURL.
func WithHTTPClient(c *http.Client) Option { return func(cf *config) { cf.httpClient = c } }

// Write writes df as RFC 4180 CSV to w. The first row contains column names
// unless WithHasHeader(false) is supplied.
func Write(ctx context.Context, w io.Writer, df *dataframe.DataFrame, opts ...Option) error {
	cfg := resolve(opts)
	if err := ctx.Err(); err != nil {
		return err
	}

	sch := df.Schema().ToArrow()

	// Build a single RecordBatch for simplicity. Long term we stream via
	// df's own chunks.
	cols := make([]arrow.Array, df.Width())
	releaseAll := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	for i := range df.Width() {
		s := df.ColumnAt(i)
		arr, err := concatChunks(s, cfg.alloc)
		if err != nil {
			releaseAll()
			return fmt.Errorf("csv: column %q: %w", s.Name(), err)
		}
		cols[i] = arr
	}
	rec := array.NewRecordBatch(sch, cols, int64(df.Height()))
	defer rec.Release()
	releaseAll()

	// arrow-go csv's Writer does not accept WithAllocator; allocation for
	// writing is handled internally by encoding/csv and the writer's
	// per-row formatting buffers.
	writer := arrowcsv.NewWriter(w, sch,
		arrowcsv.WithHeader(cfg.hasHeader),
		arrowcsv.WithComma(cfg.delimiter),
		// polars writes a null as an empty field; arrow's default is NULL.
		arrowcsv.WithNullWriter(""),
	)
	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("csv: write: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("csv: flush: %w", err)
	}
	return writer.Error()
}

// WriteFile creates path and writes df as CSV.
func WriteFile(ctx context.Context, path string, df *dataframe.DataFrame, opts ...Option) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("csv: create %q: %w", path, err)
	}
	if err := Write(ctx, f, df, opts...); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func concatChunks(s *series.Series, mem memory.Allocator) (arrow.Array, error) {
	chunks := s.Chunks()
	switch len(chunks) {
	case 0:
		return array.MakeArrayOfNull(mem, s.DType().Arrow(), 0), nil
	case 1:
		chunks[0].Retain()
		return chunks[0], nil
	default:
		return array.Concatenate(chunks, mem)
	}
}

func releaseChunks(chunks [][]arrow.Array) {
	for _, cs := range chunks {
		for _, c := range cs {
			c.Release()
		}
	}
}
