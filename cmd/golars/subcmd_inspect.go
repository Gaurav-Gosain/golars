package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
	"github.com/Gaurav-Gosain/golars/series"
)

// dataFileCompletion returns a cobra completion func that advertises
// the data-file extensions we accept. Makes `golars schema <TAB>`
// restrict to csv/parquet/... in shells that honour it.
func dataFileCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return fileio.Extensions(), cobra.ShellCompDirectiveFilterFileExt
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
		df, err := fileio.Read(context.Background(), args[0])
		if err != nil {
			return err
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
			return err
		}
		defer out.Release()
		return renderFrame(out, format)
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
	nameCol, err := series.FromString("name", names, nil)
	if err != nil {
		return nil, err
	}
	dtypeCol, err := series.FromString("dtype", dtypes, nil)
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
		df, err := fileio.Read(ctx, args[0])
		if err != nil {
			return err
		}
		defer df.Release()
		desc, err := df.Describe(ctx)
		if err != nil {
			return err
		}
		defer desc.Release()
		return renderFrame(desc, format)
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
		n, err := optCount(args, 1, 10, "row count")
		if err != nil {
			return err
		}
		ctx := context.Background()
		df, err := fileio.Read(ctx, args[0])
		if err != nil {
			return err
		}
		defer df.Release()
		var out *dataframe.DataFrame
		if isHead {
			out = df.Head(n)
		} else {
			out = df.Tail(n)
		}
		defer out.Release()
		return renderFrame(out, format)
	}
	return cmd
}
