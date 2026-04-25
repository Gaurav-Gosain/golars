package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
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
		renderFrame(head, format)
		return nil
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
		df, err := loadByExt(ctx, args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		k := min(n, df.Height())
		out, err := df.Sample(ctx, k, false, seed)
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

// newConvertCmd reads SRC, writes DST. Both formats inferred from
// extension. If DST is "-" the result is streamed to stdout in the
// target format.
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
		df, err := loadByExt(ctx, src)
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		if dst == "-" {
			format := outputFormat(strings.TrimPrefix(strings.ToLower(filepath.Ext(src)), "."))
			switch format {
			case "parquet", "pq":
				format = fmtParquet
			case "arrow", "ipc":
				format = fmtArrow
			case "jsonl", "ndjson":
				format = fmtNDJSON
			}
			if err := writeFrame(ctx, os.Stdout, df, format); err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			return nil
		}
		if err := writeByExt(ctx, dst, df); err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		fmt.Printf("%s wrote %s (%d × %d)\n",
			successStyle.Render("✓"), dst, df.Height(), df.Width())
		return nil
	}
	return cmd
}

// writeByExt picks a writer based on dst's extension.
func writeByExt(ctx context.Context, dst string, df *dataframe.DataFrame) error {
	ext := strings.ToLower(filepath.Ext(dst))
	switch ext {
	case ".csv":
		return iocsv.WriteFile(ctx, dst, df)
	case ".tsv":
		return iocsv.WriteFile(ctx, dst, df, iocsv.WithDelimiter('\t'))
	case ".parquet", ".pq":
		return ioparquet.WriteFile(ctx, dst, df)
	case ".arrow", ".ipc":
		return ipc.WriteFile(ctx, dst, df)
	case ".json":
		return iojson.WriteFile(ctx, dst, df)
	case ".ndjson", ".jsonl":
		return iojson.WriteNDJSONFile(ctx, dst, df)
	}
	return fmt.Errorf("unsupported destination extension %q", ext)
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
			df, err := loadByExt(ctx, f)
			if err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			frames = append(frames, df)
		}
		out, err := dataframe.Concat(frames...)
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

// cmdReservoirSample is a streaming uniform sampler kept in tree for
// future --stream support on very large files. Not wired into the
// router: the eager path in newSampleCmd handles typical in-memory
// frames fine.
func cmdReservoirSample(rows int, src string) (*dataframe.DataFrame, error) {
	df, err := loadByExt(context.Background(), src)
	if err != nil {
		return nil, err
	}
	defer df.Release()
	if rows >= df.Height() {
		return df.Clone(), nil
	}
	r := rand.New(rand.NewPCG(1, 2))
	picks := make([]int, rows)
	for i := range rows {
		picks[i] = i
	}
	for i := rows; i < df.Height(); i++ {
		j := int(r.Uint64N(uint64(i + 1)))
		if j < rows {
			picks[j] = i
		}
	}
	idx := make([]int64, rows)
	for i, p := range picks {
		idx[i] = int64(p)
	}
	_ = idx
	return df.Sample(context.Background(), rows, false, 42)
}

var _ = series.Empty
