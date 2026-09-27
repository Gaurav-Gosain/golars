// Package ipc reads and writes the Arrow IPC stream format.
//
// IPC is the native wire format for Apache Arrow and the fastest way to move
// DataFrames between processes. No type coercion is performed; columns
// round-trip exactly.
package ipc

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowipc "github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Option configures a Read or Write call.
type Option func(*config)

type config struct {
	alloc memory.Allocator
}

func resolve(opts []Option) config {
	c := config{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the allocator used when constructing buffers.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *config) { c.alloc = alloc }
}

// Read consumes an Arrow IPC stream from r and returns a DataFrame. Each
// incoming record batch becomes one chunk per column; columns may be chunked
// when the stream contains multiple batches.
func Read(ctx context.Context, r io.Reader, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	reader, err := arrowipc.NewReader(r, arrowipc.WithAllocator(cfg.alloc))
	if err != nil {
		return nil, fmt.Errorf("ipc: new reader: %w", err)
	}
	defer reader.Release()

	sch := reader.Schema()
	numCols := sch.NumFields()
	chunks := make([][]arrow.Array, numCols)

	releaseChunks := func() {
		for _, cs := range chunks {
			for _, c := range cs {
				c.Release()
			}
		}
	}

	for reader.Next() {
		if err := ctx.Err(); err != nil {
			releaseChunks()
			return nil, err
		}
		rec := reader.RecordBatch()
		for i := range numCols {
			col := rec.Column(i)
			col.Retain()
			chunks[i] = append(chunks[i], col)
		}
	}
	if err := reader.Err(); err != nil {
		releaseChunks()
		return nil, fmt.Errorf("ipc: read stream: %w", err)
	}

	return buildDataFrameFromChunks(sch, chunks)
}

// buildDataFrameFromChunks materialises a DataFrame from the stream
// reader's per-column chunk slices. Consumes chunk references: the
// resulting Series own them.
func buildDataFrameFromChunks(sch *arrow.Schema, chunks [][]arrow.Array) (*dataframe.DataFrame, error) {
	numCols := sch.NumFields()
	cols := make([]*series.Series, numCols)
	for i := range numCols {
		name := sch.Field(i).Name
		if len(chunks[i]) == 0 {
			cols[i] = series.Empty(name, dtype.FromArrow(sch.Field(i).Type))
			continue
		}
		s, err := series.New(name, chunks[i]...)
		if err != nil {
			// Columns before i own their chunks through the Series
			// built from them; series.New does not consume on error,
			// so column i onward still holds raw references.
			for _, pc := range cols[:i] {
				pc.Release()
			}
			for _, cs := range chunks[i:] {
				for _, c := range cs {
					c.Release()
				}
			}
			return nil, fmt.Errorf("ipc: build series %q: %w", name, err)
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
		return nil, fmt.Errorf("ipc: build dataframe: %w", err)
	}
	return df, nil
}

// Write emits df as a single record batch in Arrow IPC stream format.
// Multi-chunk columns are concatenated to a single chunk before writing.
func Write(ctx context.Context, w io.Writer, df *dataframe.DataFrame, opts ...Option) error {
	cfg := resolve(opts)
	if err := ctx.Err(); err != nil {
		return err
	}

	sch := df.Schema().ToArrow()
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
			return fmt.Errorf("ipc: column %q: %w", s.Name(), err)
		}
		cols[i] = arr
	}

	rec := array.NewRecordBatch(sch, cols, int64(df.Height()))
	defer rec.Release()
	releaseAll() // rec retained our references

	writer := arrowipc.NewWriter(w,
		arrowipc.WithSchema(sch),
		arrowipc.WithAllocator(cfg.alloc),
	)
	if err := writer.Write(rec); err != nil {
		_ = writer.Close()
		return fmt.Errorf("ipc: write: %w", err)
	}
	return writer.Close()
}

// ReadFile opens path for reading and calls Read.
func ReadFile(ctx context.Context, path string, opts ...Option) (*dataframe.DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ipc: open %q: %w", path, err)
	}
	defer f.Close()
	return Read(ctx, f, opts...)
}

// WriteFile creates path and writes df via Write.
func WriteFile(ctx context.Context, path string, df *dataframe.DataFrame, opts ...Option) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("ipc: create %q: %w", path, err)
	}
	if err := Write(ctx, f, df, opts...); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// concatChunks flattens a Series to a single arrow.Array. The caller owns one
// reference and must Release.
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
