package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// cmdHorizontal implements .sum_horizontal / .mean_horizontal / ...
// Syntax: .sum_horizontal OUT [COL...]
func (s *state) cmdHorizontal(op string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .%s OUT [COL...]", op)
	}
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	out := args[0]
	cols := args[1:]
	lf := s.currentLazy()
	switch op {
	case "sum_horizontal":
		lf = lf.SumHorizontal(out, cols...)
	case "mean_horizontal":
		lf = lf.MeanHorizontal(out, cols...)
	case "min_horizontal":
		lf = lf.MinHorizontal(out, cols...)
	case "max_horizontal":
		lf = lf.MaxHorizontal(out, cols...)
	case "all_horizontal":
		lf = lf.AllHorizontal(out, cols...)
	case "any_horizontal":
		lf = lf.AnyHorizontal(out, cols...)
	default:
		return fmt.Errorf("unknown horizontal op %q", op)
	}
	s.lf = &lf
	fmt.Printf("%s added %s -> %q to pipeline\n",
		successStyle.Render("✓"), strings.ToUpper(op), out)
	return nil
}

// cmdFrameAgg implements .sum_all / .mean_all / ... which collapse to
// a single row of per-column aggregates.
func (s *state) cmdFrameAgg(op string) error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	var out *dataframe.DataFrame
	switch op {
	case "sum_all":
		out, err = df.SumAll(s.ctx)
	case "mean_all":
		out, err = df.MeanAll(s.ctx)
	case "min_all":
		out, err = df.MinAll(s.ctx)
	case "max_all":
		out, err = df.MaxAll(s.ctx)
	case "std_all":
		out, err = df.StdAll(s.ctx)
	case "var_all":
		out, err = df.VarAll(s.ctx)
	case "median_all":
		out, err = df.MedianAll(s.ctx)
	case "count_all":
		out, err = df.CountAll(s.ctx)
	case "null_count_all":
		out, err = df.NullCountAll(s.ctx)
	default:
		return fmt.Errorf("unknown frame agg %q", op)
	}
	if err != nil {
		return err
	}
	defer out.Release()
	printTable(out)
	return nil
}

// cmdScan registers a lazy scan over a file. Syntax:
//
//	.scan_csv PATH [as NAME]
//	.scan_parquet PATH [as NAME]
//	.scan_ipc PATH [as NAME]
//	.scan_json PATH [as NAME]
//	.scan_ndjson PATH [as NAME]
//
// Without `as NAME` the scan replaces the focused frame's lazy plan
// (the backing DataFrame is dropped: Collect re-opens the file).
func (s *state) cmdScan(format string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .scan_%s PATH [as NAME]", format)
	}
	path := args[0]
	var name string
	if len(args) >= 3 && strings.EqualFold(args[1], "as") {
		name = args[2]
	}
	lf, err := scanByFormat(format, path)
	if err != nil {
		return err
	}
	if name != "" {
		// Materialise the scan into the staged frames slot, matching
		// `.load ... as NAME` semantics. Scripts can still switch to
		// the lazy form by `.use NAME` then `.scan_...` on the focus.
		df, err := lf.Collect(s.ctx)
		if err != nil {
			return err
		}
		if existing, ok := s.frames[name]; ok && existing != nil && existing.df != nil {
			existing.df.Release()
		}
		s.frames[name] = &namedFrame{df: df, path: path}
		fmt.Printf("%s scanned %s as %s (%d x %d)\n",
			successStyle.Render("✓"), path, cmdStyle.Render(name),
			df.Height(), df.Width())
		return nil
	}
	// Replace the current focus' lazy plan with the scan. Keep the
	// backing df nil so Collect runs the scan path.
	if s.df != nil {
		s.df.Release()
		s.df = nil
	}
	s.lf = &lf
	s.path = path
	fmt.Printf("%s registered scan_%s %s\n",
		successStyle.Render("✓"), format, path)
	return nil
}

func scanByFormat(format, path string) (lazy.LazyFrame, error) {
	switch format {
	case "csv":
		return iocsv.Scan(path), nil
	case "parquet":
		return ioparquet.Scan(path), nil
	case "ipc", "arrow":
		return ipc.Scan(path), nil
	case "json":
		return iojson.Scan(path), nil
	case "ndjson", "jsonl":
		return iojson.ScanNDJSON(path), nil
	}
	return lazy.LazyFrame{}, fmt.Errorf("unknown scan format %q", format)
}

// cmdFillNan applies frame-level fill_nan. Syntax: .fill_nan VALUE
func (s *state) cmdFillNan(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: .fill_nan VALUE")
	}
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	v, err := parseFloat(args[0])
	if err != nil {
		return err
	}
	lf := s.currentLazy().FillNan(v)
	s.lf = &lf
	fmt.Printf("%s added FILL_NAN value=%v to pipeline\n", successStyle.Render("✓"), v)
	return nil
}

// cmdForwardFill / cmdBackwardFill apply frame-level directional fills.
// Syntax: .forward_fill [LIMIT]
func (s *state) cmdForwardFill(args []string) error {
	return s.cmdDirectionalFill("forward_fill", args, true)
}

func (s *state) cmdBackwardFill(args []string) error {
	return s.cmdDirectionalFill("backward_fill", args, false)
}

func (s *state) cmdDirectionalFill(name string, args []string, forward bool) error {
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	limit := 0
	if len(args) >= 1 {
		if v, ok := parseNat(args[0]); ok {
			limit = v
		} else {
			return fmt.Errorf(".%s: invalid limit %q", name, args[0])
		}
	}
	lf := s.currentLazy()
	if forward {
		lf = lf.ForwardFill(limit)
	} else {
		lf = lf.BackwardFill(limit)
	}
	s.lf = &lf
	fmt.Printf("%s added %s limit=%d to pipeline\n",
		successStyle.Render("✓"), strings.ToUpper(name), limit)
	return nil
}

// parseFloat coerces a CLI arg to float64. Rejects non-numeric.
func parseFloat(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", s)
	}
	return v, nil
}

// scanFormatFromExt is the fallback when scan_auto is used.
func scanFormatFromExt(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv", ".tsv":
		return "csv"
	case ".parquet", ".pq":
		return "parquet"
	case ".arrow", ".ipc":
		return "ipc"
	case ".json":
		return "json"
	case ".ndjson", ".jsonl":
		return "ndjson"
	}
	return ""
}
