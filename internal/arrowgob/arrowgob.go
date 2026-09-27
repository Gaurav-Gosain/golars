// Package arrowgob encodes named chunked columns as an Arrow IPC stream,
// for the GobEncode and GobDecode methods of DataFrame and Series.
//
// The stream carries the exact arrow types of the columns, so every dtype
// round-trips: dictionaries (categoricals), timestamps with their time
// zone, lists, structs and so on. Chunks are written as separate record
// batches, aligned across columns with zero-copy slices, so nothing is
// concatenated on the way out.
package arrowgob

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// rowsKey is schema metadata that records the row count. A stream of a
// frame without columns has no record batches that could carry it.
const rowsKey = "golars.rows"

// Encode writes the columns (all of length rows) as an Arrow IPC stream.
func Encode(names []string, cols []*arrow.Chunked, rows int) ([]byte, error) {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{Name: names[i], Type: c.DataType(), Nullable: true}
	}
	md := arrow.NewMetadata([]string{rowsKey}, []string{strconv.Itoa(rows)})
	sch := arrow.NewSchema(fields, &md)

	var buf bytes.Buffer
	size := 1 << 10
	for _, c := range cols {
		for _, a := range c.Chunks() {
			size += dataSize(a.Data())
		}
	}
	buf.Grow(size)
	w := ipc.NewWriter(&buf, ipc.WithSchema(sch), ipc.WithAllocator(memory.DefaultAllocator))
	if len(cols) > 0 && rows > 0 {
		if err := writeBatches(w, sch, fields, cols, rows); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("arrowgob: write: %w", err)
	}
	return buf.Bytes(), nil
}

// dataSize estimates the encoded size of an array's buffers (the whole
// buffers, even for a slice), to size the output once.
func dataSize(d arrow.ArrayData) int {
	n := 64
	for _, b := range d.Buffers() {
		if b != nil {
			n += b.Len() + 8
		}
	}
	for _, c := range d.Children() {
		n += dataSize(c)
	}
	if dict, ok := d.Dictionary().(*array.Data); ok && dict != nil {
		n += dataSize(dict)
	}
	return n
}

func writeBatches(w *ipc.Writer, sch *arrow.Schema, fields []arrow.Field, cols []*arrow.Chunked, rows int) error {
	arrCols := make([]arrow.Column, len(cols))
	for i, c := range cols {
		arrCols[i] = *arrow.NewColumn(fields[i], c)
	}
	tbl := array.NewTable(sch, arrCols, int64(rows))
	for i := range arrCols {
		arrCols[i].Release()
	}
	defer tbl.Release()
	// A chunk size of -1 yields one batch per run of aligned chunks.
	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()
	for tr.Next() {
		if err := w.Write(tr.RecordBatch()); err != nil {
			return fmt.Errorf("arrowgob: write: %w", err)
		}
	}
	if err := tr.Err(); err != nil {
		return fmt.Errorf("arrowgob: write: %w", err)
	}
	return nil
}

// Decode reads a stream written by Encode. The caller owns one reference
// to each returned column.
func Decode(data []byte) (names []string, cols []*arrow.Chunked, rows int, err error) {
	r, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("arrowgob: read: %w", err)
	}
	defer r.Release()
	sch := r.Schema()
	n := sch.NumFields()
	chunks := make([][]arrow.Array, n)
	release := func() {
		for _, cs := range chunks {
			for _, c := range cs {
				c.Release()
			}
		}
	}
	read := 0
	for r.Next() {
		rec := r.RecordBatch()
		read += int(rec.NumRows())
		for i := range n {
			col := rec.Column(i)
			col.Retain()
			chunks[i] = append(chunks[i], col)
		}
	}
	if err := r.Err(); err != nil {
		release()
		return nil, nil, 0, fmt.Errorf("arrowgob: read: %w", err)
	}
	rows = read
	md := sch.Metadata()
	if i := md.FindKey(rowsKey); i >= 0 {
		v, perr := strconv.Atoi(md.Values()[i])
		if perr != nil || v < 0 || (n > 0 && v != read) {
			release()
			return nil, nil, 0, errors.New("arrowgob: row count does not match the stream")
		}
		rows = v
	}
	names = make([]string, n)
	cols = make([]*arrow.Chunked, n)
	for i := range n {
		f := sch.Field(i)
		names[i] = f.Name
		cols[i] = arrow.NewChunked(f.Type, chunks[i]) // retains each chunk
		for _, c := range chunks[i] {
			c.Release()
		}
	}
	return names, cols, rows, nil
}
