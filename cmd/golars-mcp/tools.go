package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/sql"
)

// Tool is the minimal metadata + executor pair for an MCP tool.
type Tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Run         func(args json.RawMessage) (any, error)
}

var tools []Tool

func init() {
	tools = []Tool{
		{
			Name:        "schema",
			Description: "Return column names and dtypes for a data file (CSV, Parquet, Arrow/IPC, JSON, NDJSON).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Absolute path to the data file."},
				},
				"required": []string{"path"},
			},
			Run: runSchema,
		},
		{
			Name:        "head",
			Description: "Return the first N rows of a data file as a table.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Absolute path to the data file."},
					"n":    map[string]any{"type": "integer", "minimum": 0, "default": 10, "description": "Number of rows to return (default 10)."},
				},
				"required": []string{"path"},
			},
			Run: runHead,
		},
		{
			Name:        "describe",
			Description: "Return describe()-style summary stats for every column of a data file.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Absolute path to the data file."},
				},
				"required": []string{"path"},
			},
			Run: runDescribe,
		},
		{
			Name:        "sql",
			Description: "Run a SQL query against one or more data files. Each file is registered as a table named after its filename stem.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "SQL query text."},
					"files": map[string]any{
						"type":        "array",
						"description": "File paths to register as tables (table name = filename without extension).",
						"items":       map[string]any{"type": "string"},
					},
				},
				"required": []string{"query", "files"},
			},
			Run: runSQL,
		},
		{
			Name:        "row_count",
			Description: "Return only the row count + column count for a data file (cheap to call repeatedly).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Absolute path to the data file."},
				},
				"required": []string{"path"},
			},
			Run: runRowCount,
		},
		{
			Name:        "null_counts",
			Description: "Return the null count per column of a data file.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string", "description": "Absolute path to the data file."},
				},
				"required": []string{"path"},
			},
			Run: runNullCounts,
		},
	}
}

func findTool(name string) *Tool {
	for i := range tools {
		if tools[i].Name == name {
			return &tools[i]
		}
	}
	return nil
}

// --- tool implementations ----------------------------------------

// loadByExt picks a reader based on the file extension. Mirrors
// cmd/golars/subcmd_inspect.go but kept self-contained so the MCP
// binary doesn't import the main-package.
func loadByExt(ctx context.Context, path string) (*dataframe.DataFrame, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("data path must be a regular file")
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv", ".tsv":
		opts := []iocsv.Option{}
		if ext == ".tsv" {
			opts = append(opts, iocsv.WithDelimiter('\t'))
		}
		return iocsv.ReadFile(ctx, path, opts...)
	case ".parquet", ".pq":
		return ioparquet.ReadFile(ctx, path)
	case ".arrow", ".ipc":
		return ipc.ReadFile(ctx, path)
	case ".json":
		return iojson.ReadFile(ctx, path)
	case ".ndjson", ".jsonl":
		return iojson.ReadNDJSONFile(ctx, path)
	}
	return nil, fmt.Errorf("unsupported file extension %q", ext)
}

func runSchema(args json.RawMessage) (any, error) {
	path, err := asString(args, "path")
	if err != nil {
		return nil, err
	}
	df, err := loadByExt(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	columns := make([]any, 0, df.Width())
	for _, f := range df.Schema().Fields() {
		columns = append(columns, map[string]any{
			"name":  f.Name,
			"dtype": f.DType.String(),
		})
	}
	structured := map[string]any{
		"path":    path,
		"rows":    df.Height(),
		"columns": columns,
	}
	// Pretty text fallback for hosts that only show text.
	var b strings.Builder
	fmt.Fprintf(&b, "%s - %d rows × %d cols\n", path, df.Height(), df.Width())
	for _, f := range df.Schema().Fields() {
		fmt.Fprintf(&b, "  %-24s  %s\n", f.Name, f.DType)
	}
	return structuredResult(structured, verbatim(b.String())), nil
}

func runRowCount(args json.RawMessage) (any, error) {
	path, err := asString(args, "path")
	if err != nil {
		return nil, err
	}
	df, err := loadByExt(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	structured := map[string]any{
		"rows":    df.Height(),
		"columns": df.Width(),
	}
	return structuredResult(structured, fmt.Sprintf("%d rows × %d cols", df.Height(), df.Width())), nil
}

func runHead(args json.RawMessage) (any, error) {
	path, err := asString(args, "path")
	if err != nil {
		return nil, err
	}
	n, err := asInt(args, "n", 10)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("argument %q must be non-negative", "n")
	}
	df, err := loadByExt(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	head := df.Head(n)
	defer head.Release()
	return frameAsResult(head)
}

func runDescribe(args json.RawMessage) (any, error) {
	path, err := asString(args, "path")
	if err != nil {
		return nil, err
	}
	df, err := loadByExt(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	desc, err := df.Describe(context.Background())
	if err != nil {
		return nil, err
	}
	defer desc.Release()
	return frameAsResult(desc)
}

func runNullCounts(args json.RawMessage) (any, error) {
	path, err := asString(args, "path")
	if err != nil {
		return nil, err
	}
	df, err := loadByExt(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	out := make([]any, 0, df.Width())
	for _, c := range df.Columns() {
		out = append(out, map[string]any{
			"column": c.Name(),
			"nulls":  c.NullCount(),
		})
	}
	structured := map[string]any{"counts": out, "total_rows": df.Height()}
	return structuredResult(structured, fmt.Sprintf("per-column nulls (of %d rows)", df.Height())), nil
}

func runSQL(args json.RawMessage) (any, error) {
	query, err := asString(args, "query")
	if err != nil {
		return nil, err
	}
	files, err := asStringSlice(args, "files")
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("argument %q must contain at least one file", "files")
	}
	names := make(map[string]bool, len(files))
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
		if names[name] {
			return nil, fmt.Errorf("duplicate table name %q", name)
		}
		names[name] = true
	}
	ctx := context.Background()
	session := sql.NewSession()
	defer session.Close()
	for _, f := range files {
		df, err := loadByExt(ctx, f)
		if err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
		if err := session.Register(name, df); err != nil {
			df.Release()
			return nil, err
		}
		df.Release()
	}
	out, err := session.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer out.Release()
	return frameAsResult(out)
}

// frameAsResult packages a DataFrame into an MCP tool result:
// a structured {columns, rows} payload plus a CSV text fallback.
// CSV is cheaper in tokens than a box-drawing table and every LLM
// parses it natively.
func frameAsResult(df *dataframe.DataFrame) (any, error) {
	names := df.ColumnNames()
	rows, err := df.Rows()
	if err != nil {
		return nil, err
	}
	rawRows := make([][]any, len(rows))
	copy(rawRows, rows)
	structured := map[string]any{
		"columns":  names,
		"rows":     rawRows,
		"rowCount": df.Height(),
	}
	var buf strings.Builder
	if err := iocsv.Write(context.Background(), &buf, df); err != nil {
		return nil, err
	}
	return structuredResult(structured, verbatim(buf.String())), nil
}
