package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

// loadByExt dispatches on the file extension. Used by every
// file-input subcommand so every format the REPL supports is also
// supported by the CLI wrappers.
func loadByExt(ctx context.Context, path string) (*dataframe.DataFrame, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv", ".tsv":
		opts := []iocsv.Option{iocsv.WithNullValues("")}
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
	return nil, fmt.Errorf("unsupported extension %q", ext)
}

// dataFileCompletion returns a cobra completion func that advertises
// the data-file extensions we accept. Makes `golars schema <TAB>`
// restrict to csv/parquet/... in shells that honour it.
func dataFileCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{"csv", "tsv", "parquet", "pq", "arrow", "ipc", "json", "ndjson", "jsonl"}, cobra.ShellCompDirectiveFilterFileExt
}

// newSchemaCmd prints the schema (column name + dtype) for a data file.
func newSchemaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schema FILE",
		Short:   "print column names and dtypes",
		Example: "golars schema data.csv",
		Args:    cobra.ExactArgs(1),
	}
	ff := bindFormatFlags(cmd)
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		df, err := loadByExt(context.Background(), args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		if format == "" || format == fmtTable {
			fmt.Printf("%s  %d rows × %d cols\n",
				headerStyle.Render(args[0]), df.Height(), df.Width())
			for _, f := range df.Schema().Fields() {
				fmt.Printf("  %s  %s\n",
					cmdStyle.Render(fmt.Sprintf("%-24s", f.Name)),
					dimStyle.Render(f.DType.String()))
			}
			return nil
		}
		out, err := schemaAsFrame(df)
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer out.Release()
		renderFrame(out, format)
		return nil
	}
	return cmd
}

// schemaAsFrame turns a DataFrame's schema into a 2-column frame so
// the non-table output paths can serialise it uniformly.
func schemaAsFrame(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	names := make([]string, 0, df.Width())
	dtypes := make([]string, 0, df.Width())
	for _, f := range df.Schema().Fields() {
		names = append(names, f.Name)
		dtypes = append(dtypes, f.DType.String())
	}
	nameCol, err := cliSeriesFromString("name", names)
	if err != nil {
		return nil, err
	}
	dtypeCol, err := cliSeriesFromString("dtype", dtypes)
	if err != nil {
		nameCol.Release()
		return nil, err
	}
	return dataframe.New(nameCol, dtypeCol)
}

// newStatsCmd runs df.Describe on the file's contents.
func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "stats FILE",
		Aliases: []string{"describe"},
		Short:   "print describe()-style summary statistics",
		Example: "golars stats data.csv",
		Args:    cobra.ExactArgs(1),
	}
	ff := bindFormatFlags(cmd)
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		ctx := context.Background()
		df, err := loadByExt(ctx, args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		desc, err := df.Describe(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer desc.Release()
		renderFrame(desc, format)
		return nil
	}
	return cmd
}

// newHeadCmd prints the first N rows (default 10).
func newHeadCmd() *cobra.Command { return headOrTailCmd("head", true) }

// newTailCmd prints the last N rows (default 10).
func newTailCmd() *cobra.Command { return headOrTailCmd("tail", false) }

func headOrTailCmd(name string, isHead bool) *cobra.Command {
	cmd := &cobra.Command{
		Use:     name + " FILE [N]",
		Short:   "print first N rows of FILE",
		Example: "golars " + name + " data.csv 20",
		Args:    cobra.RangeArgs(1, 2),
	}
	if !isHead {
		cmd.Short = "print last N rows of FILE"
	}
	ff := bindFormatFlags(cmd)
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		n := 10
		if len(args) >= 2 {
			if v, err := strconv.Atoi(args[1]); err == nil && v > 0 {
				n = v
			}
		}
		ctx := context.Background()
		df, err := loadByExt(ctx, args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		var out *dataframe.DataFrame
		if isHead {
			out = df.Head(n)
		} else {
			out = df.Tail(n)
		}
		defer out.Release()
		renderFrame(out, format)
		return nil
	}
	return cmd
}

// cliSeriesFromString builds a single string-typed series with the
// given name + values. Sugar so the CLI doesn't repeat imports.
func cliSeriesFromString(name string, values []string) (*series.Series, error) {
	return series.FromString(name, values, nil)
}
