package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
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

func isKnownFormat(f outputFormat) bool {
	return slices.Contains(supportedFormats, f)
}

// writeFrame serialises df to w in the requested format.
func writeFrame(ctx context.Context, w io.Writer, df *dataframe.DataFrame, format outputFormat) error {
	switch format {
	case "", fmtTable:
		_, err := fmt.Fprint(w, df.String())
		return err
	case fmtMarkdown:
		return writeMarkdown(w, df)
	}
	f, known := fileio.ParseFormat(string(format))
	if !known {
		return fmt.Errorf("unsupported format %q", format)
	}
	return fileio.WriteTo(ctx, w, df, f)
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

// renderFrame prints df as the styled table or in a machine format.
// Every subcommand that prints a DataFrame goes through here.
func renderFrame(df *dataframe.DataFrame, format outputFormat) error {
	if format == "" || format == fmtTable {
		printTable(df)
		return nil
	}
	w := &lastByteWriter{w: os.Stdout}
	if err := writeFrame(context.Background(), w, df, format); err != nil {
		return err
	}
	// Text formats end with a newline so shells and line-oriented
	// tools see a complete last line. Binary formats are left as is.
	if format != fmtParquet && format != fmtArrow && w.last != '\n' && w.n > 0 {
		_, err := fmt.Fprintln(os.Stdout)
		return err
	}
	return nil
}

// lastByteWriter remembers the last byte written through it.
type lastByteWriter struct {
	w    io.Writer
	n    int
	last byte
}

func (l *lastByteWriter) Write(p []byte) (int, error) {
	n, err := l.w.Write(p)
	if n > 0 {
		l.n += n
		l.last = p[n-1]
	}
	return n, err
}
