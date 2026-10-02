package csv

import (
	"bufio"
	"bytes"
	stdcsv "encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// peekNoRows reports whether the CSV in br has no data rows, looking
// only at input that fits in br's buffer. arrow's reader panics on a
// CSV with a header and no data rows, so Read handles that case itself.
// It returns the peeked bytes when there are no rows.
func peekNoRows(br *bufio.Reader, hasHeader bool) ([]byte, bool) {
	head, err := br.Peek(headPeekSize)
	if !errors.Is(err, io.EOF) {
		return nil, false
	}
	rd := stdcsv.NewReader(bytes.NewReader(head))
	rd.FieldsPerRecord = -1
	rd.LazyQuotes = true
	limit := 0
	if hasHeader {
		limit = 1
	}
	records := 0
	for {
		if _, err := rd.Read(); err != nil {
			break
		}
		records++
		if records > limit {
			return nil, false
		}
	}
	return head, true
}

const headPeekSize = 1 << 16

// headerOnlySchema builds the schema of a CSV without data rows. Like
// polars, every column is a string unless a column type was given.
func headerOnlySchema(head []byte, cfg config) (*arrow.Schema, error) {
	if !cfg.hasHeader {
		return arrow.NewSchema(nil, nil), nil
	}
	rd := stdcsv.NewReader(bytes.NewReader(head))
	rd.Comma = cfg.delimiter
	rd.FieldsPerRecord = -1
	names, err := rd.Read()
	if errors.Is(err, io.EOF) {
		return arrow.NewSchema(nil, nil), nil
	}
	if err != nil {
		return nil, err
	}
	if len(cfg.includeCols) > 0 {
		names = cfg.includeCols
	}
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		dt := arrow.DataType(arrow.BinaryTypes.String)
		if t, ok := cfg.columnTypes[name]; ok {
			dt = t
		}
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

// readNoRows builds the empty frame for a CSV without data rows.
func readNoRows(head []byte, cfg config) (*dataframe.DataFrame, error) {
	sch := cfg.schema
	if sch == nil {
		var err error
		if sch, err = headerOnlySchema(head, cfg); err != nil {
			return nil, fmt.Errorf("csv: read header: %w", err)
		}
	}
	cols := make([]*series.Series, sch.NumFields())
	for i, f := range sch.Fields() {
		cols[i] = series.Empty(f.Name, dtype.FromArrow(f.Type))
	}
	return dataframe.New(cols...)
}
