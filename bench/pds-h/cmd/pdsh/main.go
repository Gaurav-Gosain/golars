// Command pdsh runs one or more PDS-H / TPC-H queries against a
// directory of parquet tables and emits per-run timings to
// bench/pds-h/output/timings.csv in the upstream polars-benchmark
// schema.
//
// Usage:
//
//	pdsh -q 1 -data path/to/tables [-sf 1.0] [-repeats 3]
//	pdsh -q 1,6 -data ...            # multiple queries
//	pdsh -q all -data ...            # every registered query
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/bench/pds-h/queries"
	"github.com/Gaurav-Gosain/golars/io/parquet"
)

func main() {
	var (
		qFlag       = flag.String("q", "", "query numbers (comma-separated, or 'all')")
		dataDir     = flag.String("data", "", "directory with <table>.parquet files")
		sfFlag      = flag.String("sf", "unset", "scale factor label for the csv (e.g. 1, 10, synthetic)")
		repeats     = flag.Int("repeats", 1, "run each query N times and report each timing")
		outPath     = flag.String("out", "bench/pds-h/output/timings.csv", "csv path (appended)")
		printOutput = flag.Bool("print", false, "print the result frame after each query (slow on large SFs)")
		cpuProfile  = flag.String("cpuprofile", "", "write a CPU profile covering all runs to this file")
		dumpDir     = flag.String("dump", "", "write each query result to DIR/q<N>.parquet (for answer checks)")
	)
	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fatalf("cpu profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fatalf("cpu profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}
	if *qFlag == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}

	ids, err := parseQueryList(*qFlag)
	if err != nil {
		fatalf("parse -q: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fatalf("mkdir output: %v", err)
	}
	csv, err := openAppendingCsv(*outPath)
	if err != nil {
		fatalf("open csv: %v", err)
	}
	defer csv.Close()

	ctx := context.Background()
	for _, id := range ids {
		fn, err := queries.Get(id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping q%d: %v\n", id, err)
			continue
		}
		for rep := 0; rep < *repeats; rep++ {
			if err := runOne(ctx, id, rep, fn, *dataDir, *sfFlag, *printOutput, *dumpDir, csv); err != nil {
				fmt.Fprintf(os.Stderr, "q%d rep %d: %v\n", id, rep, err)
			}
		}
	}
}

// runOne executes one query+repetition, prints its timing, and
// appends one row to the timings csv.
func runOne(ctx context.Context, id, rep int, fn queries.QueryFn, dataDir, sf string, printOut bool, dumpDir string, csv *csvAppender) error {
	lf, err := fn(dataDir)
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}
	start := time.Now()
	df, err := lf.Collect(ctx)
	elapsed := time.Since(start)
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}
	defer df.Release()

	fmt.Printf("q%-2d  rep=%d  %s  %d×%d\n", id, rep,
		elapsed.Truncate(time.Microsecond), df.Height(), df.Width())
	if printOut {
		fmt.Println(df.Summary())
	}
	if dumpDir != "" && rep == 0 {
		if err := parquet.WriteFile(ctx, filepath.Join(dumpDir, fmt.Sprintf("q%d.parquet", id)), df); err != nil {
			return fmt.Errorf("dump: %w", err)
		}
	}
	return csv.Append(id, elapsed.Seconds(), sf)
}

// parseQueryList accepts "1", "1,6", or "all".
func parseQueryList(spec string) ([]int, error) {
	if spec == "all" {
		return queries.Available(), nil
	}
	parts := strings.Split(spec, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("query id %q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

// csvAppender writes rows in the upstream polars-benchmark schema.
// We open in append mode so consecutive runs accumulate.
type csvAppender struct {
	f *os.File
}

func openAppendingCsv(path string) (*csvAppender, error) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := &csvAppender{f: f}
	if os.IsNotExist(statErr) {
		if _, err := f.WriteString("solution,version,query_number,duration[s],io_type,scale_factor\n"); err != nil {
			f.Close()
			return nil, err
		}
	}
	return w, nil
}

func (c *csvAppender) Append(query int, seconds float64, sf string) error {
	row := fmt.Sprintf("golars,%s,%d,%.6f,parquet,%s\n",
		golarsVersion(), query, seconds, sf)
	_, err := c.f.WriteString(row)
	return err
}

func (c *csvAppender) Close() error { return c.f.Close() }

// golarsVersion returns the version string we stamp into the csv.
// Hard-coded for now; wire to build-time ldflags when the cmd/golars
// CLI does the same.
func golarsVersion() string { return "0.1.0" }

func fatalf(fmtStr string, args ...any) {
	fmt.Fprintf(os.Stderr, fmtStr+"\n", args...)
	os.Exit(1)
}
