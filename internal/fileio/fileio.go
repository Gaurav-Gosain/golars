// Package fileio picks a golars reader, writer or lazy scan from a
// file extension. The CLI, the MCP server, the browse TUI and the
// script transpiler all dispatch on extensions the same way, so the
// mapping lives here once.
package fileio

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// Format names a file format golars can read and write.
type Format string

// Supported formats.
const (
	CSV     Format = "csv"
	TSV     Format = "tsv"
	Parquet Format = "parquet"
	IPC     Format = "ipc"
	JSON    Format = "json"
	NDJSON  Format = "ndjson"
)

// extensions maps a lower-case extension (with the dot) to a format.
var extensions = map[string]Format{
	".csv":     CSV,
	".tsv":     TSV,
	".parquet": Parquet,
	".pq":      Parquet,
	".arrow":   IPC,
	".ipc":     IPC,
	".json":    JSON,
	".ndjson":  NDJSON,
	".jsonl":   NDJSON,
}

// Extensions lists every supported extension without the leading dot,
// in a stable order. Used for shell completion hints and help text.
func Extensions() []string {
	return []string{"csv", "tsv", "parquet", "pq", "arrow", "ipc", "json", "ndjson", "jsonl"}
}

// FormatOf returns the format implied by path's extension.
func FormatOf(path string) (Format, bool) {
	f, ok := extensions[strings.ToLower(filepath.Ext(path))]
	return f, ok
}

// ParseFormat maps a user-facing format name ("csv", "arrow",
// "jsonl", ...) to a Format.
func ParseFormat(name string) (Format, bool) {
	return FormatOf("x." + strings.TrimPrefix(strings.ToLower(name), "."))
}

func unsupported(path string) error {
	ext := filepath.Ext(path)
	if ext == "" {
		return fmt.Errorf("cannot infer file format of %q: no extension (want one of .%s)",
			path, strings.Join(Extensions(), " ."))
	}
	return fmt.Errorf("unsupported file extension %q (want one of .%s)",
		ext, strings.Join(Extensions(), " ."))
}

// csvOptions returns the reader options for delimited text. Empty
// fields read as null, matching polars.read_csv.
func csvOptions(f Format) []iocsv.Option {
	opts := []iocsv.Option{iocsv.WithNullValues("")}
	if f == TSV {
		opts = append(opts, iocsv.WithDelimiter('\t'))
	}
	return opts
}

// Read loads the file at path eagerly, choosing the reader from the
// extension.
func Read(ctx context.Context, path string) (*dataframe.DataFrame, error) {
	f, ok := FormatOf(path)
	if !ok {
		return nil, unsupported(path)
	}
	switch f {
	case CSV, TSV:
		return iocsv.ReadFile(ctx, path, csvOptions(f)...)
	case Parquet:
		return ioparquet.ReadFile(ctx, path)
	case IPC:
		return ipc.ReadFile(ctx, path)
	case JSON:
		return iojson.ReadFile(ctx, path)
	case NDJSON:
		return iojson.ReadNDJSONFile(ctx, path)
	}
	return nil, unsupported(path)
}

// Scan returns a lazy scan of path, choosing the reader from the
// extension. The file is not opened until the plan is collected.
func Scan(path string) (lazy.LazyFrame, error) {
	f, ok := FormatOf(path)
	if !ok {
		return lazy.LazyFrame{}, unsupported(path)
	}
	return ScanAs(f, path), nil
}

// ScanAs returns a lazy scan of path using format f regardless of
// the extension.
func ScanAs(f Format, path string) lazy.LazyFrame {
	switch f {
	case Parquet:
		return ioparquet.Scan(path)
	case IPC:
		return ipc.Scan(path)
	case JSON:
		return iojson.Scan(path)
	case NDJSON:
		return iojson.ScanNDJSON(path)
	}
	return iocsv.Scan(path, csvOptions(f)...)
}

// Write writes df to path, choosing the writer from the extension.
func Write(ctx context.Context, path string, df *dataframe.DataFrame) error {
	f, ok := FormatOf(path)
	if !ok {
		return unsupported(path)
	}
	switch f {
	case CSV:
		return iocsv.WriteFile(ctx, path, df)
	case TSV:
		return iocsv.WriteFile(ctx, path, df, iocsv.WithDelimiter('\t'))
	case Parquet:
		return ioparquet.WriteFile(ctx, path, df)
	case IPC:
		return ipc.WriteFile(ctx, path, df)
	case JSON:
		return iojson.WriteFile(ctx, path, df)
	case NDJSON:
		return iojson.WriteNDJSONFile(ctx, path, df)
	}
	return unsupported(path)
}

// WriteTo serialises df to w in format f.
func WriteTo(ctx context.Context, w io.Writer, df *dataframe.DataFrame, f Format) error {
	switch f {
	case CSV:
		return iocsv.Write(ctx, w, df)
	case TSV:
		return iocsv.Write(ctx, w, df, iocsv.WithDelimiter('\t'))
	case Parquet:
		return ioparquet.Write(ctx, w, df)
	case IPC:
		return ipc.Write(ctx, w, df)
	case JSON:
		return iojson.Write(ctx, w, df)
	case NDJSON:
		return iojson.WriteNDJSON(ctx, w, df)
	}
	return fmt.Errorf("unsupported format %q", f)
}
