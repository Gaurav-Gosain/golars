package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/fileio"
)

// newDoctorCmd reports environment information useful for bug triage.
func newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "doctor",
		Short:   "environment diagnostic for bug reports",
		Example: "golars doctor",
		Args:    cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Println(headerStyle.Render("golars doctor"))
			kv := func(k, v string) {
				fmt.Printf("  %s  %s\n",
					cmdStyle.Render(fmt.Sprintf("%-16s", k)),
					v,
				)
			}
			kv("version", version)
			kv("go", runtime.Version())
			kv("os/arch", runtime.GOOS+"/"+runtime.GOARCH)
			kv("cpus", strconv.Itoa(runtime.NumCPU()))
			kv("max procs", strconv.Itoa(runtime.GOMAXPROCS(0)))
			kv("simd build", simdBuildStatus())
			home, _ := os.UserHomeDir()
			kv("home", home)
			wd, _ := os.Getwd()
			kv("cwd", wd)
			kv("NO_COLOR", envOr("NO_COLOR", "(unset)"))
			kv("COLORTERM", envOr("COLORTERM", "(unset)"))
			kv("TERM", envOr("TERM", "(unset)"))
			fmt.Println()
			fmt.Println(dimStyle.Render("Include this output when filing bug reports."))
			return nil
		},
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// newPeekCmd prints schema + first N rows + shape in one call.
func newPeekCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "peek FILE [N]",
		Short:   "schema + first N rows + shape in one call",
		Example: "golars peek data.csv 20",
		Args:    cobra.RangeArgs(1, 2),
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
		if format == "" || format == fmtTable {
			fmt.Printf("%s  %d rows × %d cols\n",
				headerStyle.Render(args[0]), df.Height(), df.Width())
			fmt.Println()
			for _, f := range df.Schema().Fields() {
				fmt.Printf("  %s  %s\n",
					cmdStyle.Render(fmt.Sprintf("%-24s", f.Name)),
					dimStyle.Render(f.DType.String()))
			}
			fmt.Println()
			head := df.Head(n)
			defer head.Release()
			printTable(head)
			return nil
		}
		head := df.Head(n)
		defer head.Release()
		return renderFrame(head, format)
	}
	return cmd
}

// newSampleCmd returns a uniform-random sample of N rows from a file.
// Uses Sample(replacement=false) so N is capped to the frame height.
func newSampleCmd() *cobra.Command {
	var n int
	var seed uint64
	cmd := &cobra.Command{
		Use:     "sample FILE",
		Short:   "uniform-random sample of rows from a data file",
		Example: "golars sample -n 100 --seed 42 data.csv",
		Args:    cobra.ExactArgs(1),
	}
	cmd.Flags().IntVarP(&n, "count", "n", 100, "number of rows to sample")
	cmd.Flags().Uint64Var(&seed, "seed", 0, "PRNG seed (0 = time-based)")
	ff := bindFormatFlags(cmd)
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		if n <= 0 {
			return fmt.Errorf("--count must be positive")
		}
		if seed == 0 {
			seed = uint64(time.Now().UnixNano())
		}
		ctx := context.Background()
		df, err := fileio.Read(ctx, args[0])
		if err != nil {
			return err
		}
		defer df.Release()
		k := min(n, df.Height())
		out, err := df.Sample(ctx, k, false, seed)
		if err != nil {
			return err
		}
		defer out.Release()
		return renderFrame(out, format)
	}
	return cmd
}

// newConvertCmd reads SRC and writes DST, inferring both formats from
// the extensions. With DST "-" the data is written to stdout in the
// source format, which is handy for piping remote-friendly formats.
func newConvertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "convert SRC DST",
		Short:   "transcode between csv/tsv/parquet/arrow/json/ndjson",
		Example: "golars convert in.csv out.parquet",
		Args:    cobra.ExactArgs(2),
	}
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		src, dst := args[0], args[1]
		ctx := context.Background()
		df, err := fileio.Read(ctx, src)
		if err != nil {
			return err
		}
		defer df.Release()
		if dst == "-" {
			// Stream to stdout in the source's own format.
			f, _ := fileio.FormatOf(src)
			return fileio.WriteTo(ctx, os.Stdout, df, f)
		}
		if err := fileio.Write(ctx, dst, df); err != nil {
			return err
		}
		ok("wrote %s (%s)", dst, shape(df))
		return nil
	}
	return cmd
}

// newCatCmd vertically concatenates multiple files and prints the
// result. Every file must share a schema: the DataFrame equivalent
// of `cat x.csv y.csv`.
func newCatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cat FILE [FILE...]",
		Short:   "vstack multiple files with matching schemas",
		Example: "golars cat a.csv b.csv c.csv",
		Args:    cobra.MinimumNArgs(1),
	}
	ff := bindFormatFlags(cmd)
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		ctx := context.Background()
		frames := make([]*dataframe.DataFrame, 0, len(args))
		defer func() {
			for _, f := range frames {
				f.Release()
			}
		}()
		for _, f := range args {
			df, err := fileio.Read(ctx, f)
			if err != nil {
				return err
			}
			frames = append(frames, df)
		}
		out, err := dataframe.Concat(frames...)
		if err != nil {
			return err
		}
		defer out.Release()
		return renderFrame(out, format)
	}
	return cmd
}
