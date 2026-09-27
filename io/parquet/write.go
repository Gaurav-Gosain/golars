package parquet

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// writeBufferSize batches the many small page writes into large syscalls.
const writeBufferSize = 1 << 20

// dictSampleSize is how many values the dictionary heuristic inspects.
const dictSampleSize = 1024

// writeTable writes tbl to w. Flat schemas take a parallel path that
// encodes and compresses the columns of each row group concurrently;
// nested schemas go through pqarrow.WriteTable.
func writeTable(ctx context.Context, tbl arrow.Table, w io.Writer, cfg config) error {
	sc := tbl.Schema()
	opts := []parquet.WriterProperty{
		parquet.WithCompression(cfg.compression),
		parquet.WithAllocator(cfg.alloc),
	}
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(cfg.alloc), pqarrow.WithStoreSchema())
	if !cfg.noNative && isFlat(sc) {
		err := writeNative(ctx, tbl, w, cfg, parquet.NewWriterProperties(opts...), arrProps)
		if err == nil {
			if c, ok := w.(io.Closer); ok {
				return c.Close()
			}
			return nil
		}
		if !errors.Is(err, errNotNative) {
			return err
		}
	}

	// Dictionary pages only pay off for low-cardinality columns. For
	// high-cardinality data they cost a hash insert per value on write,
	// a fallback re-encode once the dictionary page overflows, and a
	// lazily grown dictionary on read.
	for i, f := range sc.Fields() {
		opts = append(opts, parquet.WithDictionaryFor(f.Name, dictionaryWorthwhile(tbl.Column(i).Data())))
	}
	props := parquet.NewWriterProperties(opts...)

	aw := newAsyncWriter(w)
	bw := bufio.NewWriterSize(aw, writeBufferSize)
	var err error
	if isFlat(sc) {
		err = writeFlat(ctx, tbl, bw, cfg.chunkSize, props, arrProps)
	} else {
		err = pqarrow.WriteTable(tbl, bw, cfg.chunkSize, props, arrProps)
	}
	if err == nil {
		err = bw.Flush()
	}
	if cerr := aw.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	// pqarrow closes an io.Closer sink once the footer is written. The
	// bufio wrapper hides that, so keep the old contract explicitly.
	if c, ok := w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func isFlat(sc *arrow.Schema) bool {
	for _, f := range sc.Fields() {
		switch f.Type.(type) {
		case arrow.NestedType, *arrow.DictionaryType, arrow.ExtensionType:
			return false
		}
	}
	return true
}

func writeFlat(ctx context.Context, tbl arrow.Table, w io.Writer, chunkSize int64, props *parquet.WriterProperties, arrProps pqarrow.ArrowWriterProperties) error {
	sc := tbl.Schema()
	pqs, err := pqarrow.ToParquet(sc, props, arrProps)
	if err != nil {
		return err
	}
	fw, err := file.NewParquetWriterWithError(w, pqs.Root(), file.WithWriterProps(props),
		file.WithWriteMetadata(arrowSchemaMeta(sc, props.Allocator())))
	if err != nil {
		return err
	}
	chunkSize = min(chunkSize, props.MaxRowGroupLength())
	nrows := tbl.NumRows()
	ncols := int(tbl.NumCols())

	// Shared read-only definition levels for slices without nulls.
	ones := make([]int16, min(chunkSize, max(nrows, 1)))
	for i := range ones {
		ones[i] = 1
	}
	scratch := make([][]int16, ncols)
	errs := make([]error, ncols)
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))

	writeRowGroup := func(off, size int64) error {
		rgw, err := fw.AppendBufferedRowGroupChecked()
		if err != nil {
			return err
		}
		var wg sync.WaitGroup
		for c := range ncols {
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				errs[c] = writeColumnRange(ctx, rgw, c, tbl.Column(c).Data(), off, size,
					sc.Field(c).Nullable, ones, &scratch[c], &arrProps)
			})
		}
		wg.Wait()
		return errors.Join(errs...)
	}

	if nrows == 0 {
		err = writeRowGroup(0, 0)
	}
	for off := int64(0); off < nrows && err == nil; off += chunkSize {
		err = writeRowGroup(off, min(chunkSize, nrows-off))
		if err == nil {
			err = ctx.Err()
		}
	}
	if err != nil {
		_ = fw.Close()
		return err
	}
	return fw.Close()
}

// writeColumnRange writes rows [off, off+size) of one flat column into
// its buffered column chunk writer. It runs concurrently with the other
// columns of the same row group; every column owns its writer and page
// buffer, and gets its own arrow write context.
func writeColumnRange(ctx context.Context, rgw file.BufferedRowGroupWriter, col int, data *arrow.Chunked,
	off, size int64, nullable bool, ones []int16, scratch *[]int16, arrProps *pqarrow.ArrowWriterProperties) error {
	cw, err := rgw.Column(col)
	if err != nil {
		return err
	}
	wctx := pqarrow.NewArrowWriteContext(ctx, arrProps)
	wroteValues := false
	pos := int64(0)
	end := off + size
	for _, chunk := range data.Chunks() {
		clen := int64(chunk.Len())
		lo, hi := max(off, pos), min(end, pos+clen)
		pos += clen
		if lo >= hi {
			if pos >= end {
				break
			}
			continue
		}
		slice := array.NewSlice(chunk, lo-(pos-clen), hi-(pos-clen))
		var defs []int16
		if nullable {
			n := slice.Len()
			if slice.NullN() == 0 && n <= len(ones) {
				defs = ones[:n]
			} else {
				if cap(*scratch) < n {
					*scratch = make([]int16, n)
				}
				defs = (*scratch)[:n]
				for i := range n {
					if slice.IsValid(i) {
						defs[i] = 1
					} else {
						defs[i] = 0
					}
				}
			}
		}
		wroteValues = wroteValues || slice.NullN() < slice.Len()
		err := pqarrow.WriteArrowToColumn(wctx, cw, slice, defs, nil, nullable)
		slice.Release()
		if err != nil {
			return err
		}
	}
	// Build and compress the last page here, in parallel with the other
	// columns. Otherwise it happens in rowGroupWriter.Close, which runs
	// serially for every column. Pages of dictionary-encoded columns stay
	// buffered in the writer until Close emits the dictionary page first.
	// A column whose buffered tail holds only nulls is left for Close.
	// So is a range without any non-null value: the plain boolean
	// encoder has no bit writer yet and its size estimate panics.
	if fl, ok := cw.(interface{ FlushCurrentPage() error }); ok && wroteValues && size > 0 && hasBufferedValues(cw.CurrentEncoder()) {
		return fl.FlushCurrentPage()
	}
	return nil
}

// hasBufferedValues reports whether enc holds values not yet written to
// a page. A dictionary encoder's EstimatedDataEncodedSize is never zero
// (it counts the bit-width byte), so after the writer has just cut a
// page it would still look non-empty, and flushing that empty page
// panics inside arrow's level encoding. Its observed raw size is reset
// with every page and grows with every buffered value.
func hasBufferedValues(enc interface{ EstimatedDataEncodedSize() int64 }) bool {
	if d, ok := enc.(interface{ ObservedRawSize() int64 }); ok {
		return d.ObservedRawSize() > 0
	}
	return enc.EstimatedDataEncodedSize() > 0
}

// asyncWriter hands full buffers to a goroutine that writes them to w, so
// the file writes overlap with encoding the next row group.
type asyncWriter struct {
	w    io.Writer
	ch   chan []byte
	done chan struct{}
	err  error
	pool sync.Pool
}

func newAsyncWriter(w io.Writer) *asyncWriter {
	a := &asyncWriter{w: w, ch: make(chan []byte, 4), done: make(chan struct{})}
	go func() {
		defer close(a.done)
		for b := range a.ch {
			if a.err == nil {
				_, a.err = a.w.Write(b)
			}
			a.pool.Put(&b)
		}
	}()
	return a
}

func (a *asyncWriter) Write(p []byte) (int, error) {
	var b []byte
	if v, ok := a.pool.Get().(*[]byte); ok {
		b = (*v)[:0]
	}
	b = append(b, p...)
	a.ch <- b
	return len(p), nil
}

// Close waits for pending writes and returns the first write error.
func (a *asyncWriter) Close() error {
	close(a.ch)
	<-a.done
	return a.err
}

// dictionaryWorthwhile samples up to dictSampleSize evenly spaced values
// and reports whether at most a quarter of them are distinct.
func dictionaryWorthwhile(data *arrow.Chunked) bool {
	n := data.Len()
	if n == 0 {
		return true
	}
	step := max(n/dictSampleSize, 1)
	sampled, distinct := 0, 0
	seenStr := map[string]struct{}{}
	seenNum := map[uint64]struct{}{}
	pos := 0
	next := 0
	for _, chunk := range data.Chunks() {
		clen := chunk.Len()
		for next < pos+clen {
			i := next - pos
			next += step
			if chunk.IsNull(i) {
				continue
			}
			var fresh bool
			switch a := chunk.(type) {
			case *array.String:
				fresh = addStr(seenStr, a.Value(i))
			case *array.LargeString:
				fresh = addStr(seenStr, a.Value(i))
			case *array.Binary:
				fresh = addStr(seenStr, string(a.Value(i)))
			case *array.Int64:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Int32:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Int16:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Int8:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Uint64:
				fresh = addNum(seenNum, a.Value(i))
			case *array.Uint32:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Float64:
				fresh = addNum(seenNum, math.Float64bits(a.Value(i)))
			case *array.Float32:
				fresh = addNum(seenNum, uint64(math.Float32bits(a.Value(i))))
			case *array.Date32:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Date64:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			case *array.Timestamp:
				fresh = addNum(seenNum, uint64(a.Value(i)))
			default:
				// Unknown layout: keep the library default.
				return true
			}
			sampled++
			if fresh {
				distinct++
			}
		}
		pos += clen
	}
	return sampled == 0 || distinct*4 <= sampled
}

func addStr(m map[string]struct{}, s string) bool {
	if _, ok := m[s]; ok {
		return false
	}
	m[s] = struct{}{}
	return true
}

func addNum(m map[uint64]struct{}, v uint64) bool {
	if _, ok := m[v]; ok {
		return false
	}
	m[v] = struct{}{}
	return true
}
