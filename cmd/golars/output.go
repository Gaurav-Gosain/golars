package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
)

// outputFormat names the serialisation we use for CLI results. The
// default ("table") is the pretty rounded-corner renderer used by
// the REPL; every other format exists so downstream shell pipes
// (jq, awk, xsv, duckdb) can consume golars output directly.
type outputFormat string

const (
	fmtTable    outputFormat = "table"
	fmtCSV      outputFormat = "csv"
	fmtTSV      outputFormat = "tsv"
	fmtJSON     outputFormat = "json"
	fmtNDJSON   outputFormat = "ndjson"
	fmtMarkdown outputFormat = "markdown"
	fmtParquet  outputFormat = "parquet"
	fmtArrow    outputFormat = "arrow"
)

// supportedFormats lists every format the --format flag accepts.
// Exposed so help text stays in sync with what's actually implemented.
var supportedFormats = []outputFormat{
	fmtTable, fmtCSV, fmtTSV, fmtJSON, fmtNDJSON,
	fmtMarkdown, fmtParquet, fmtArrow,
}

// formatFlags binds the standard --format / -o flag plus the bool
// shorthands (--json, --csv, ...) onto a cobra.Command. Every data-
// returning subcommand uses one of these so the flag surface is
// uniform and cobra generates tab completion for the format enum.
type formatFlags struct {
	format   string
	json     bool
	ndjson   bool
	csv      bool
	tsv      bool
	markdown bool
	parquet  bool
	arrow    bool
}

// bindFormatFlags registers the flags on cmd and returns a handle the
// RunE reads back. Registers a completion func for -o/--format so
// `golars sql -o <TAB>` offers the valid enum values.
func bindFormatFlags(cmd *cobra.Command) *formatFlags {
	ff := &formatFlags{}
	cmd.Flags().StringVarP(&ff.format, "format", "o", "table", "output format: table, csv, tsv, json, ndjson, markdown, parquet, arrow")
	cmd.Flags().BoolVar(&ff.json, "json", false, "shorthand for --format json")
	cmd.Flags().BoolVar(&ff.ndjson, "ndjson", false, "shorthand for --format ndjson")
	cmd.Flags().BoolVar(&ff.csv, "csv", false, "shorthand for --format csv")
	cmd.Flags().BoolVar(&ff.tsv, "tsv", false, "shorthand for --format tsv")
	cmd.Flags().BoolVar(&ff.markdown, "markdown", false, "shorthand for --format markdown")
	cmd.Flags().BoolVar(&ff.parquet, "parquet", false, "shorthand for --format parquet")
	cmd.Flags().BoolVar(&ff.arrow, "arrow", false, "shorthand for --format arrow")
	_ = cmd.RegisterFlagCompletionFunc("format", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		names := make([]string, len(supportedFormats))
		for i, f := range supportedFormats {
			names[i] = string(f)
		}
		return names, cobra.ShellCompDirectiveNoFileComp
	})
	return ff
}

// resolve collapses the flag state into a single outputFormat.
// Bool shortcuts win over --format. Multiple shortcuts are unusual
// but we just pick the first one set in a fixed order.
func (ff *formatFlags) resolve() (outputFormat, error) {
	switch {
	case ff.json:
		return fmtJSON, nil
	case ff.ndjson:
		return fmtNDJSON, nil
	case ff.csv:
		return fmtCSV, nil
	case ff.tsv:
		return fmtTSV, nil
	case ff.markdown:
		return fmtMarkdown, nil
	case ff.parquet:
		return fmtParquet, nil
	case ff.arrow:
		return fmtArrow, nil
	}
	f := outputFormat(strings.ToLower(ff.format))
	if !isKnownFormat(f) {
		return "", fmt.Errorf("unknown output format %q", f)
	}
	return f, nil
}

// extractFormatFlag scans args for `-o FORMAT`, `--format FORMAT`,
// `--json`, `--csv`, `--tsv`, `--ndjson`, `--markdown`, `--parquet`,
// `--arrow`. Returns the parsed format plus the args with the flag
// removed. Unknown formats produce an error.
func extractFormatFlag(args []string) (outputFormat, []string, error) {
	out := make([]string, 0, len(args))
	format := fmtTable
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-o", "--format":
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("flag %s requires a value", args[i])
			}
			f := outputFormat(strings.ToLower(args[i+1]))
			if !isKnownFormat(f) {
				return "", nil, fmt.Errorf("unknown output format %q", f)
			}
			format = f
			i++
		case "--json":
			format = fmtJSON
		case "--ndjson":
			format = fmtNDJSON
		case "--csv":
			format = fmtCSV
		case "--tsv":
			format = fmtTSV
		case "--markdown":
			format = fmtMarkdown
		case "--parquet":
			format = fmtParquet
		case "--arrow":
			format = fmtArrow
		default:
			out = append(out, args[i])
		}
	}
	return format, out, nil
}

func isKnownFormat(f outputFormat) bool {
	return slices.Contains(supportedFormats, f)
}

// writeFrame serialises df to w in the requested format. When w
// points at a non-terminal and the format supports it (csv, tsv,
// json, ndjson) we skip the pretty-table path so the output stays
// machine-parseable when piped.
func writeFrame(ctx context.Context, w io.Writer, df *dataframe.DataFrame, format outputFormat) error {
	switch format {
	case "", fmtTable:
		fmt.Fprint(w, df.String())
		return nil
	case fmtCSV:
		return iocsv.Write(ctx, w, df)
	case fmtTSV:
		return iocsv.Write(ctx, w, df, iocsv.WithDelimiter('\t'))
	case fmtMarkdown:
		return writeMarkdown(w, df)
	case fmtJSON:
		return iojson.Write(ctx, w, df)
	case fmtNDJSON:
		return iojson.WriteNDJSON(ctx, w, df)
	case fmtParquet:
		// Parquet can only be written to files; emit to stdout by
		// writing a temp file and streaming its contents.
		return writeParquetStream(ctx, w, df)
	case fmtArrow:
		return ipc.Write(ctx, w, df)
	}
	return fmt.Errorf("unsupported format %q", format)
}

// writeMarkdown emits a GitHub-flavored markdown table. Null cells
// display as empty to match Jupyter/GitHub rendering. Pipe, backslash,
// and newline characters in header/cell values get escaped so strings
// like "a|b" don't shatter the table structure.
func writeMarkdown(w io.Writer, df *dataframe.DataFrame) error {
	names := df.ColumnNames()
	rows, err := df.Rows()
	if err != nil {
		return err
	}
	// Header.
	fmt.Fprint(w, "|")
	for _, n := range names {
		fmt.Fprintf(w, " %s |", escapeMarkdownCell(n))
	}
	fmt.Fprintln(w)
	fmt.Fprint(w, "|")
	for range names {
		fmt.Fprint(w, "---|")
	}
	fmt.Fprintln(w)
	// Body.
	for _, r := range rows {
		fmt.Fprint(w, "|")
		for _, v := range r {
			if v == nil {
				fmt.Fprint(w, "  |")
				continue
			}
			fmt.Fprintf(w, " %s |", escapeMarkdownCell(fmt.Sprintf("%v", v)))
		}
		fmt.Fprintln(w)
	}
	return nil
}

// escapeMarkdownCell makes s safe to embed between `|` delimiters.
// GitHub-Flavored Markdown treats `\|` as a literal pipe inside a
// table cell; newlines inside cells need `<br>` because raw `\n`
// would terminate the row. Backslash escapes apply first so we don't
// double-escape later additions.
func escapeMarkdownCell(s string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`|`, `\|`,
		"\r\n", "<br>",
		"\n", "<br>",
		"\r", "<br>",
	)
	return replacer.Replace(s)
}

// writeParquetStream writes df to a temp file then streams it to w.
// Parquet is a random-access format so we can't stream directly to
// stdout; using a tmp file is the standard workaround (pandas +
// polars-cli do the same).
func writeParquetStream(ctx context.Context, w io.Writer, df *dataframe.DataFrame) error {
	f, err := os.CreateTemp("", "golars-*.parquet")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	if err := ioparquet.WriteFile(ctx, name, df); err != nil {
		return err
	}
	in, err := os.Open(name)
	if err != nil {
		return err
	}
	defer in.Close()
	_, err = io.Copy(w, in)
	return err
}

// renderFrame picks between the pretty pre-rendered table and a
// format-aware writer based on the `format` argument. Every CLI
// subcommand that surfaces a DataFrame result should funnel through
// here.
func renderFrame(df *dataframe.DataFrame, format outputFormat) {
	if format == "" || format == fmtTable {
		printTable(df)
		return
	}
	if err := writeFrame(context.Background(), os.Stdout, df, format); err != nil {
		fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
	}
	// Always emit a trailing newline for line-oriented formats so
	// downstream tools see a clean end-of-stream.
	switch format {
	case fmtCSV, fmtTSV, fmtJSON, fmtNDJSON, fmtMarkdown:
		fmt.Println()
	}
}

// --- small helpers re-used by subcommands that don't go via renderFrame.

// parseIntDefault returns n parsed from s, or def when s is empty.
func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// Silence unused import when the package isn't rebuilt on older Go.
var _ = csv.NewWriter
var _ = json.NewEncoder
var _ = array.NewRecordBatch
