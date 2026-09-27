package dataframe

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/internal/arrowgob"
	"github.com/Gaurav-Gosain/golars/series"
)

// GobEncode implements [encoding/gob.GobEncoder]. The frame is written as
// an Arrow IPC stream, so every dtype round-trips exactly (categoricals,
// time zones, lists, structs). Chunks are kept as record batches without
// being concatenated.
//
// A nil frame cannot be encoded; gob skips nil pointers in structs and
// slices on its own.
func (df *DataFrame) GobEncode() ([]byte, error) {
	if df == nil {
		return nil, fmt.Errorf("dataframe: GobEncode of a nil *DataFrame")
	}
	names := make([]string, len(df.cols))
	cols := make([]*arrow.Chunked, len(df.cols))
	for i, c := range df.cols {
		names[i] = c.Name()
		cols[i] = c.Chunked()
	}
	return arrowgob.Encode(names, cols, df.height)
}

// GobDecode implements [encoding/gob.GobDecoder]. It replaces the contents
// of df with the decoded frame. The previous columns are not released,
// since other frames may share them.
func (df *DataFrame) GobDecode(data []byte) error {
	names, chunked, rows, err := arrowgob.Decode(data)
	if err != nil {
		return fmt.Errorf("dataframe: GobDecode: %w", err)
	}
	cols := make([]*series.Series, len(chunked))
	for i, c := range chunked {
		cols[i] = series.FromChunked(names[i], c)
		c.Release() // FromChunked retained it
	}
	out, err := New(cols...)
	if err != nil {
		for _, c := range cols {
			c.Release()
		}
		return fmt.Errorf("dataframe: GobDecode: %w", err)
	}
	out.height = rows
	*df = *out
	return nil
}
