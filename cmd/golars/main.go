// Command golars is an interactive REPL for exploring DataFrames with golars.
//
// It is deliberately small in surface: a handful of dot-commands load data,
// build a lazy pipeline, inspect the plan, and materialize the result.
// Filters are written in a small predicate grammar (col op value, combined
// with AND/OR) so no embedded language or parser is required.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/repl"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/series"
)

var _ = series.Empty // keep import available for future column-construction commands

const version = "0.1.0"

var (
	primaryColor = lipgloss.Color("#8B5CF6")
	accentColor  = lipgloss.Color("#10B981")
	errorColor   = lipgloss.Color("#EF4444")
	warningColor = lipgloss.Color("#F59E0B")
	infoColor    = lipgloss.Color("#3B82F6")
	dimColor     = lipgloss.Color("#6B7280")
	headerColor  = lipgloss.Color("#F472B6")

	logoStyle    = lipgloss.NewStyle().Foreground(primaryColor).Bold(true)
	promptStyle  = lipgloss.NewStyle().Foreground(primaryColor).Bold(true)
	errStyle     = lipgloss.NewStyle().Foreground(errorColor).Bold(true)
	errMsgStyle  = lipgloss.NewStyle().Foreground(errorColor)
	successStyle = lipgloss.NewStyle().Foreground(accentColor)
	infoStyle    = lipgloss.NewStyle().Foreground(infoColor)
	dimStyle     = lipgloss.NewStyle().Foreground(dimColor)
	cmdStyle     = lipgloss.NewStyle().Foreground(warningColor)
	titleStyle   = lipgloss.NewStyle().Foreground(primaryColor).Bold(true).Underline(true)
	headerStyle  = lipgloss.NewStyle().Foreground(headerColor).Bold(true)
)

type state struct {
	ctx        context.Context
	df         *dataframe.DataFrame // the focused frame's materialised source
	path       string               // origin path for the focused frame
	lf         *lazy.LazyFrame      // the focused frame's lazy pipeline
	focused    string               // name of the currently focused frame (empty = default)
	frames     map[string]*namedFrame
	showTiming bool
	startTime  time.Time
	evalCount  int
}

// namedFrame stores a multi-source script's additional frames. The
// currently-focused frame is always in state.df / state.lf; all other
// loaded frames are parked here until `.use NAME` promotes one. This
// lets scripts juggle N sources without forcing an immutable
// registry: a single-frame workflow ignores the map entirely.
type namedFrame struct {
	df   *dataframe.DataFrame
	path string
	lf   *lazy.LazyFrame
}

func main() { os.Exit(execCLI()) }

// newState constructs a fresh REPL/script state. Keeping this small
// so cli.go and any future entry points can build one without
// re-declaring the field set.
func newState(timing bool) *state {
	return &state{
		ctx:        context.Background(),
		startTime:  time.Now(),
		showTiming: timing,
		frames:     make(map[string]*namedFrame),
	}
}

// runScript executes a .glr file through the same per-line dispatcher
// the REPL uses. Trace prints each line as it runs so the output is a
// self-contained transcript.
func runScript(s *state, path string) error {
	runner := script.Runner{
		Exec: script.ExecutorFunc(s.handle),
		Trace: func(line string) {
			fmt.Println(promptStyle.Render("golars") + dimStyle.Render(" > ") + line)
		},
	}
	return runner.RunFile(path)
}

func (s *state) repl() error {
	p := newCLIPrompt(s)
	defer p.Close()

	banner()

	// Buffer for trailing-backslash continuation. Trailing `\` asks
	// the REPL to keep reading the next physical line into the same
	// logical statement, matching the script-file behaviour.
	var cont strings.Builder

	for {
		line, err := p.ReadLine()
		if err != nil {
			if errors.Is(err, repl.ErrCanceled) {
				cont.Reset()
				continue
			}
			if errors.Is(err, repl.ErrEOF) {
				fmt.Println(dimStyle.Render("bye"))
				return nil
			}
			return err
		}
		if body, ok := strings.CutSuffix(strings.TrimRight(line, " \t"), "\\"); ok {
			cont.WriteString(body)
			cont.WriteByte(' ')
			continue
		}
		if cont.Len() > 0 {
			cont.WriteString(line)
			line = cont.String()
			cont.Reset()
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			fmt.Println(dimStyle.Render("bye"))
			return nil
		}
		start := time.Now()
		if err := s.handle(line); err != nil {
			printErr(err)
		}
		if s.showTiming {
			printTiming(time.Since(start))
		}
	}
}

func prompt() string { return promptStyle.Render("golars") + dimStyle.Render(" » ") }

func banner() {
	fmt.Println()
	fmt.Println(logoStyle.Render("  ▓▓▓  golars v" + version))
	fmt.Println(dimStyle.Render("  pure-Go DataFrames with lazy plan and optimizer"))
	fmt.Println(dimStyle.Render("  type ") + cmdStyle.Render(".help") + dimStyle.Render(" to list commands"))
	fmt.Println()
}

// handle dispatches a single input line.
func (s *state) handle(line string) error {
	s.evalCount++
	// Accept both ".cmd args..." (REPL style) and "cmd args..."
	// (script style). Strip inline comments so pasted script lines
	// work in the REPL too.
	if i := strings.IndexByte(line, '#'); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	if line == "" {
		return nil
	}
	if !strings.HasPrefix(line, ".") {
		line = "." + line
	}
	parts := fields(line)
	cmd := strings.ToLower(parts[0])
	args := parts[1:]

	switch cmd {
	case ".help", ".h", ".?":
		return s.cmdHelp()
	case ".exit", ".quit", ".q":
		fmt.Println(dimStyle.Render("bye"))
		os.Exit(0)
	case ".clear":
		fmt.Print("\033[H\033[2J")
		return nil
	case ".load":
		if len(args) == 0 {
			return fmt.Errorf(".load requires a path (optionally `as NAME`)")
		}
		// Syntax: load PATH [as NAME]
		if len(args) >= 3 && strings.EqualFold(args[1], "as") {
			return s.loadAs(args[0], args[2])
		}
		return s.load(args[0])
	case ".use":
		if len(args) == 0 {
			return fmt.Errorf(".use NAME: switch focus to a loaded frame")
		}
		return s.cmdUse(args[0])
	case ".stash":
		if len(args) == 0 {
			return fmt.Errorf(".stash NAME: snapshot the current focus for later .use")
		}
		return s.cmdStash(args[0])
	case ".frames":
		return s.cmdFrames()
	case ".drop_frame":
		if len(args) == 0 {
			return fmt.Errorf(".drop_frame NAME: release a loaded frame")
		}
		return s.cmdDropFrame(args[0])
	case ".save":
		if len(args) == 0 {
			return fmt.Errorf(".save requires a path")
		}
		return s.save(args[0])
	case ".show":
		return s.cmdShow(args)
	case ".ishow", ".browse":
		return s.cmdInteractiveShow(args)
	case ".schema":
		return s.cmdSchema()
	case ".head":
		return s.cmdHead(args)
	case ".tail":
		return s.cmdTail(args)
	case ".select":
		return s.cmdSelect(args)
	case ".drop":
		return s.cmdDrop(args)
	case ".filter":
		return s.cmdFilter(strings.TrimSpace(strings.TrimPrefix(line, parts[0])))
	case ".sort":
		return s.cmdSort(args)
	case ".limit":
		return s.cmdLimit(args)
	case ".groupby":
		return s.cmdGroupBy(args)
	case ".join":
		return s.cmdJoin(args)
	case ".source":
		if len(args) == 0 {
			return fmt.Errorf(".source requires a script path")
		}
		return s.cmdSource(args[0])
	case ".describe":
		return s.cmdDescribe()
	case ".explain":
		return s.cmdExplain()
	case ".explain_tree", ".tree":
		return s.cmdExplainTree()
	case ".graph", ".show_graph":
		return s.cmdShowGraph()
	case ".mermaid":
		return s.cmdMermaid()
	case ".collect":
		return s.cmdCollect()
	case ".reset":
		return s.cmdReset()
	case ".timing":
		s.showTiming = !s.showTiming
		if s.showTiming {
			fmt.Println(successStyle.Render("timing on"))
		} else {
			fmt.Println(infoStyle.Render("timing off"))
		}
		return nil
	case ".info":
		return s.cmdInfo()
	case ".reverse":
		return s.cmdReverse()
	case ".sample":
		return s.cmdSample(args)
	case ".shuffle":
		return s.cmdShuffle(args)
	case ".null_count", ".null-count":
		return s.cmdNullCount()
	case ".glimpse":
		return s.cmdGlimpse(args)
	case ".size":
		return s.cmdSize()
	case ".unique":
		return s.cmdUnique()
	case ".with":
		return s.cmdWith(strings.TrimSpace(strings.TrimPrefix(line, parts[0])))
	case ".unnest":
		return s.cmdUnnest(args)
	case ".explode":
		return s.cmdExplode(args)
	case ".upsample":
		return s.cmdUpsample(args)
	case ".cast":
		return s.cmdCast(args)
	case ".fill_null", ".fillnull":
		return s.cmdFillNull(args)
	case ".drop_null", ".dropnull":
		return s.cmdDropNull(args)
	case ".rename":
		return s.cmdRename(args)
	case ".sum":
		return s.cmdScalarAgg("sum", args)
	case ".mean", ".avg":
		return s.cmdScalarAgg("mean", args)
	case ".min":
		return s.cmdScalarAgg("min", args)
	case ".max":
		return s.cmdScalarAgg("max", args)
	case ".median":
		return s.cmdScalarAgg("median", args)
	case ".std":
		return s.cmdScalarAgg("std", args)
	case ".write":
		if len(args) == 0 {
			return fmt.Errorf("usage: .write PATH")
		}
		return s.save(args[0])
	case ".with_row_index":
		return s.cmdWithRowIndex(args)
	case ".pwd":
		return s.cmdPwd()
	case ".ls":
		return s.cmdLs(args)
	case ".cd":
		return s.cmdCd(args)
	case ".sum_horizontal":
		return s.cmdHorizontal("sum_horizontal", args)
	case ".mean_horizontal":
		return s.cmdHorizontal("mean_horizontal", args)
	case ".min_horizontal":
		return s.cmdHorizontal("min_horizontal", args)
	case ".max_horizontal":
		return s.cmdHorizontal("max_horizontal", args)
	case ".all_horizontal":
		return s.cmdHorizontal("all_horizontal", args)
	case ".any_horizontal":
		return s.cmdHorizontal("any_horizontal", args)
	case ".sum_all":
		return s.cmdFrameAgg("sum_all")
	case ".mean_all":
		return s.cmdFrameAgg("mean_all")
	case ".min_all":
		return s.cmdFrameAgg("min_all")
	case ".max_all":
		return s.cmdFrameAgg("max_all")
	case ".std_all":
		return s.cmdFrameAgg("std_all")
	case ".var_all":
		return s.cmdFrameAgg("var_all")
	case ".median_all":
		return s.cmdFrameAgg("median_all")
	case ".count_all":
		return s.cmdFrameAgg("count_all")
	case ".null_count_all":
		return s.cmdFrameAgg("null_count_all")
	case ".scan_csv":
		return s.cmdScan("csv", args)
	case ".scan_parquet":
		return s.cmdScan("parquet", args)
	case ".scan_ipc", ".scan_arrow":
		return s.cmdScan("ipc", args)
	case ".scan_json":
		return s.cmdScan("json", args)
	case ".scan_ndjson", ".scan_jsonl":
		return s.cmdScan("ndjson", args)
	case ".fill_nan":
		return s.cmdFillNan(args)
	case ".forward_fill", ".ff":
		return s.cmdForwardFill(args)
	case ".backward_fill", ".bf":
		return s.cmdBackwardFill(args)
	case ".top_k":
		return s.cmdTopK("top_k", args)
	case ".bottom_k":
		return s.cmdTopK("bottom_k", args)
	case ".transpose":
		return s.cmdTranspose(args)
	case ".unpivot", ".melt":
		return s.cmdUnpivot(args)
	case ".partition_by":
		return s.cmdPartitionBy(args)
	case ".skew":
		return s.cmdScalarStat("skew", args)
	case ".kurtosis":
		return s.cmdScalarStat("kurtosis", args)
	case ".approx_n_unique", ".approx_nunique":
		return s.cmdScalarStat("approx_n_unique", args)
	case ".pivot":
		return s.cmdPivot(args)
	case ".corr":
		return s.cmdCorrCov("corr", args)
	case ".cov":
		return s.cmdCorrCov("cov", args)
	case ".scan_auto":
		if len(args) == 0 {
			return fmt.Errorf("usage: .scan_auto PATH [as NAME]")
		}
		format := scanFormatFromExt(args[0])
		if format == "" {
			return fmt.Errorf(".scan_auto: cannot infer format from %q", args[0])
		}
		return s.cmdScan(format, args)
	}
	// Typo? Nudge the user toward the closest known command.
	if suggest := script.SuggestCommand(cmd); suggest != "" {
		return fmt.Errorf("unknown command %s (did you mean .%s?)", cmd, suggest)
	}
	return fmt.Errorf("unknown command %s", cmd)
}

// Commands ------------------------------------------------------------------

func (s *state) cmdHelp() error {
	fmt.Println()
	fmt.Println(titleStyle.Render("commands"))
	fmt.Println()
	entries := []struct{ cmd, desc string }{
		{".load <path> [as NAME]", "load csv|parquet|arrow; `as NAME` stages additional source"},
		{".use NAME", "focus a clone of a named frame (NAME stays in the registry)"},
		{".stash NAME", "snapshot the current focus as NAME for later .use"},
		{".frames", "list loaded frames (focused marked with *)"},
		{".drop_frame NAME", "release a named frame"},
		{".save <path>", "save current pipeline result to csv|parquet|arrow"},
		{".show", "collect and print first 10 rows"},
		{".ishow", "open focused pipeline in the browse TUI (alt screen)"},
		{".head [n]", "collect and print first n rows (default 10)"},
		{".tail [n]", "collect and print last n rows"},
		{".schema", "show schema of source"},
		{".describe", "show summary stats (min, max, mean, nulls)"},
		{".select <col>...", "project columns (lazy op)"},
		{".drop <col>...", "drop columns (lazy op)"},
		{".filter <pred>", "filter rows, e.g. .filter age > 30"},
		{".sort <col> [asc|desc]", "sort (lazy op)"},
		{".limit <n>", "limit rows (lazy op)"},
		{".groupby <k,...> <col:op[:as]>...", "group by with aggregations"},
		{".join <path|NAME> on <k> [inner|left|cross]", "join focused pipeline with a file OR named frame"},
		{".explain", "print logical plan, optimizer trace, optimized plan"},
		{".explain_tree", "explain rendered as a box-drawn tree (alias: .tree)"},
		{".graph", "styled plan tree for the terminal (alias: .show_graph)"},
		{".mermaid", "emit the plan as a Mermaid flowchart (pipe into mmdc)"},
		{".collect", "materialize pipeline into a new source"},
		{".reset", "discard the current lazy pipeline"},
		{".timing", "toggle timing display"},
		{".source <path>", "run a golars script (.glr) inline"},
		{".reverse", "reverse row order of the current pipeline"},
		{".sample <N> [seed]", "sample N rows without replacement"},
		{".shuffle [seed]", "shuffle rows in place (materialises)"},
		{".unique", "drop duplicate rows over all columns"},
		{".with <name> = <expr>", "append a derived column (arithmetic + string + agg + rolling)"},
		{".unnest <col>", "project struct fields as top-level columns"},
		{".explode <col>", "fan out list elements into individual rows"},
		{".upsample <col> <every>", "interpolate sorted timestamps at a regular interval"},
		{".null_count", "per-column null count as a 1-row frame"},
		{".glimpse [N]", "compact peek (default 5 rows)"},
		{".size", "estimated arrow byte size of the pipeline result"},
		{".info", "runtime information"},
		{".clear", "clear the screen"},
		{".exit", "quit the repl"},
	}
	for _, e := range entries {
		fmt.Printf("  %s  %s\n",
			cmdStyle.Render(padRight(e.cmd, 26)),
			dimStyle.Render(e.desc))
	}
	fmt.Println()
	fmt.Println(titleStyle.Render("predicate grammar"))
	fmt.Println()
	fmt.Println("  " + dimStyle.Render("col op value [and|or col op value]...  no parens, left-to-right"))
	fmt.Println("  " + dimStyle.Render("ops: == != < <= > >=, is_null, is_not_null"))
	fmt.Println("  " + dimStyle.Render("values: integers, floats, \"double-quoted strings\", true, false"))
	fmt.Println()
	return nil
}

func (s *state) cmdInfo() error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Println()
	fmt.Println(titleStyle.Render("runtime"))
	fmt.Println()
	rows := [][2]string{
		{"version", version},
		{"go", runtime.Version()},
		{"arch", fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)},
		{"heap", fmt.Sprintf("%.2f MB", float64(ms.HeapAlloc)/1024/1024)},
		{"gc runs", fmt.Sprintf("%d", ms.NumGC)},
		{"uptime", time.Since(s.startTime).Round(time.Second).String()},
		{"commands", fmt.Sprintf("%d", s.evalCount)},
	}
	if s.df != nil {
		rows = append(rows,
			[2]string{"source", s.path},
			[2]string{"rows", fmt.Sprintf("%d", s.df.Height())},
			[2]string{"cols", fmt.Sprintf("%d", s.df.Width())},
		)
	}
	for _, r := range rows {
		fmt.Printf("  %s  %s\n", dimStyle.Render(padRight(r[0], 12)), r[1])
	}
	fmt.Println()
	return nil
}

func (s *state) load(path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	var (
		df  *dataframe.DataFrame
		err error
	)
	switch ext {
	case ".csv", ".tsv":
		opts := []iocsv.Option{}
		if ext == ".tsv" {
			opts = append(opts, iocsv.WithDelimiter('\t'))
		}
		df, err = iocsv.ReadFile(s.ctx, path, opts...)
	case ".parquet", ".pq":
		df, err = ioparquet.ReadFile(s.ctx, path)
	case ".arrow", ".ipc":
		df, err = ipc.ReadFile(s.ctx, path)
	default:
		return fmt.Errorf("unsupported extension %q", ext)
	}
	if err != nil {
		return err
	}
	if s.df != nil {
		s.df.Release()
	}
	s.df = df
	s.path = path
	s.lf = nil
	fmt.Printf("%s loaded %s (%d × %d)\n",
		successStyle.Render("ok"), path, df.Height(), df.Width())
	return nil
}

func (s *state) save(path string) error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv", ".tsv":
		opts := []iocsv.Option{}
		if ext == ".tsv" {
			opts = append(opts, iocsv.WithDelimiter('\t'))
		}
		if err := iocsv.WriteFile(s.ctx, path, df, opts...); err != nil {
			return err
		}
	case ".parquet", ".pq":
		if err := ioparquet.WriteFile(s.ctx, path, df); err != nil {
			return err
		}
	case ".arrow", ".ipc":
		if err := ipc.WriteFile(s.ctx, path, df); err != nil {
			return err
		}
	case ".json":
		if err := iojson.WriteFile(s.ctx, path, df); err != nil {
			return err
		}
	case ".ndjson", ".jsonl":
		if err := iojson.WriteNDJSONFile(s.ctx, path, df); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported extension %q (want .csv|.tsv|.parquet|.arrow|.ipc|.json|.ndjson)", ext)
	}
	fmt.Printf("%s wrote %s (%d x %d)\n",
		successStyle.Render("ok"), path, df.Height(), df.Width())
	return nil
}

func (s *state) cmdShow(args []string) error {
	n := 10
	if len(args) > 0 {
		if v, ok := parseNat(args[0]); ok {
			n = v
		}
	}
	return s.previewHead(n)
}

func (s *state) cmdHead(args []string) error {
	n := 10
	if len(args) > 0 {
		if v, ok := parseNat(args[0]); ok {
			n = v
		}
	}
	return s.previewHead(n)
}

func (s *state) cmdTail(args []string) error {
	n := 10
	if len(args) > 0 {
		if v, ok := parseNat(args[0]); ok {
			n = v
		}
	}
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()
	if n > df.Height() {
		n = df.Height()
	}
	tail := df.Tail(n)
	defer tail.Release()
	printTable(tail)
	return nil
}

func (s *state) previewHead(n int) error {
	if s.lf == nil && s.df == nil {
		return fmt.Errorf("no source loaded; use .load <path>")
	}
	// Use the lazy pipeline + head; run it.
	lf := s.currentLazy().Head(n)
	df, err := lf.Collect(s.ctx)
	if err != nil {
		return err
	}
	defer df.Release()
	printTable(df)
	fmt.Println(dimStyle.Render(fmt.Sprintf("  %d rows shown", df.Height())))
	return nil
}

func (s *state) cmdSchema() error {
	if s.df == nil {
		return fmt.Errorf("no source loaded")
	}
	sch := s.currentLazy().Plan().Children()
	_ = sch
	schema, err := s.currentLazy().Schema()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println(titleStyle.Render("schema"))
	fmt.Println()
	for _, f := range schema.Fields() {
		fmt.Printf("  %s  %s\n",
			headerStyle.Render(padRight(f.Name, 20)),
			infoStyle.Render(f.DType.String()))
	}
	fmt.Println()
	return nil
}

func (s *state) cmdSelect(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(".select requires at least one column")
	}
	lf := s.currentLazy()
	exprs := make([]expr.Expr, len(args))
	for i, a := range args {
		exprs[i] = expr.Col(a)
	}
	next := lf.Select(exprs...)
	s.lf = &next
	fmt.Println(successStyle.Render("ok") + " added SELECT to pipeline")
	return nil
}

func (s *state) cmdDrop(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(".drop requires at least one column")
	}
	lf := s.currentLazy().Drop(args...)
	s.lf = &lf
	fmt.Println(successStyle.Render("ok") + " added DROP to pipeline")
	return nil
}

func (s *state) cmdFilter(predicate string) error {
	if predicate == "" {
		return fmt.Errorf(".filter requires a predicate")
	}
	e, err := parsePredicate(predicate)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	next := s.currentLazy().Filter(e)
	s.lf = &next
	fmt.Println(successStyle.Render("ok") + " added FILTER to pipeline: " + dimStyle.Render(e.String()))
	return nil
}

func (s *state) cmdSort(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(".sort requires a column")
	}
	desc := false
	if len(args) >= 2 && strings.EqualFold(args[1], "desc") {
		desc = true
	}
	next := s.currentLazy().Sort(args[0], desc)
	s.lf = &next
	dir := "asc"
	if desc {
		dir = "desc"
	}
	fmt.Println(successStyle.Render("ok") + fmt.Sprintf(" added SORT %s %s to pipeline", args[0], dir))
	return nil
}

func (s *state) cmdLimit(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(".limit requires n")
	}
	n, ok := parseNat(args[0])
	if !ok {
		return fmt.Errorf("invalid n: %q", args[0])
	}
	next := s.currentLazy().Limit(n)
	s.lf = &next
	fmt.Println(successStyle.Render("ok") + fmt.Sprintf(" added LIMIT %d to pipeline", n))
	return nil
}

func (s *state) cmdDescribe() error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()

	fmt.Println()
	fmt.Println(titleStyle.Render("describe"))
	fmt.Println()
	for _, f := range df.Schema().Fields() {
		col, _ := df.Column(f.Name)
		fmt.Printf("  %s  %s  nulls=%d  len=%d",
			headerStyle.Render(padRight(f.Name, 18)),
			infoStyle.Render(padRight(f.DType.String(), 14)),
			col.NullCount(), col.Len())
		if f.DType.IsNumeric() {
			if f.DType.IsFloating() {
				minV, _, _ := compute.MinFloat64(s.ctx, col)
				maxV, _, _ := compute.MaxFloat64(s.ctx, col)
				mean, _, _ := compute.MeanFloat64(s.ctx, col)
				fmt.Printf("  min=%g  max=%g  mean=%g", minV, maxV, mean)
			} else if f.DType.IsInteger() {
				minV, _, _ := compute.MinInt64(s.ctx, col)
				maxV, _, _ := compute.MaxInt64(s.ctx, col)
				mean, _, _ := compute.MeanFloat64(s.ctx, col)
				fmt.Printf("  min=%d  max=%d  mean=%g", minV, maxV, mean)
			}
		}
		fmt.Println()
	}
	fmt.Println()
	return nil
}

func (s *state) cmdExplain() error {
	lf := s.currentLazy()
	out, err := lf.Explain()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(out)
	fmt.Println()
	return nil
}

// cmdExplainTree is the box-drawing variant of cmdExplain. Uses the
// same three-section layout but renders each plan as a tree instead
// of an indented list.
func (s *state) cmdExplainTree() error {
	lf := s.currentLazy()
	out, err := lf.ExplainTree()
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Print(out)
	fmt.Println()
	return nil
}

func (s *state) cmdCollect() error {
	if s.lf == nil {
		return fmt.Errorf("no pending pipeline to collect")
	}
	df, err := s.lf.Collect(s.ctx)
	if err != nil {
		return err
	}
	if s.df != nil {
		s.df.Release()
	}
	s.df = df
	s.lf = nil
	fmt.Printf("%s collected (%d × %d)\n",
		successStyle.Render("ok"), df.Height(), df.Width())
	return nil
}

func (s *state) cmdReset() error {
	s.lf = nil
	fmt.Println(successStyle.Render("ok") + " pipeline reset")
	return nil
}

// cmdSource runs a script file against this REPL session. Each line
// is dispatched through s.handle as if typed at the prompt. Trace
// prints the line before it runs so the REPL transcript stays
// coherent.
func (s *state) cmdSource(path string) error {
	runner := script.Runner{
		Exec: script.ExecutorFunc(s.handle),
		Trace: func(line string) {
			fmt.Println(promptStyle.Render("golars") + dimStyle.Render(" > ") + line)
		},
	}
	if err := runner.RunFile(path); err != nil {
		return err
	}
	fmt.Println(successStyle.Render("ok") + " script complete: " + path)
	return nil
}

// cmdGroupBy accepts: keys=k1,k2,... agg_specs...
// Each agg_spec is <col>:<op>[:<alias>]. Supported ops: sum, mean, min, max,
// count, null_count, first, last.
func (s *state) cmdGroupBy(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: .groupby <key1,key2,...> <col:op[:alias]>")
	}
	keys := strings.Split(args[0], ",")
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
	}
	aggs := make([]expr.Expr, 0, len(args)-1)
	for _, spec := range args[1:] {
		e, err := parseAggSpec(spec)
		if err != nil {
			return fmt.Errorf("agg %q: %w", spec, err)
		}
		aggs = append(aggs, e)
	}
	next := s.currentLazy().GroupBy(keys...).Agg(aggs...)
	s.lf = &next
	fmt.Println(successStyle.Render("ok") + fmt.Sprintf(" added GROUP BY %v with %d aggs", keys, len(aggs)))
	return nil
}

func parseAggSpec(s string) (expr.Expr, error) {
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return expr.Expr{}, fmt.Errorf("expected col:op[:alias]")
	}
	col := strings.TrimSpace(parts[0])
	op := strings.ToLower(strings.TrimSpace(parts[1]))
	alias := col
	if len(parts) == 3 {
		alias = strings.TrimSpace(parts[2])
	}
	e := expr.Col(col)
	switch op {
	case "sum":
		e = e.Sum()
	case "mean", "avg":
		e = e.Mean()
	case "min":
		e = e.Min()
	case "max":
		e = e.Max()
	case "count":
		e = e.Count()
	case "null_count":
		e = e.NullCount()
	case "first":
		e = e.First()
	case "last":
		e = e.Last()
	default:
		return expr.Expr{}, fmt.Errorf("unknown op %q", op)
	}
	return e.Alias(alias), nil
}

// cmdJoin joins the focused pipeline with either a loaded frame by
// NAME or a fresh file loaded from PATH.
// Usage: .join <path|NAME> on <key> [inner|left|cross]
func (s *state) cmdJoin(args []string) error {
	if len(args) < 3 || !strings.EqualFold(args[1], "on") {
		return fmt.Errorf(".join <path|NAME> on <key> [inner|left|cross]")
	}
	target := args[0]
	key := args[2]
	how := dataframe.InnerJoin
	if len(args) >= 4 {
		switch strings.ToLower(args[3]) {
		case "inner":
			how = dataframe.InnerJoin
		case "left":
			how = dataframe.LeftJoin
		case "cross":
			how = dataframe.CrossJoin
		default:
			return fmt.Errorf("unknown join type %q", args[3])
		}
	}

	// Named frame wins over path. This lets scripts stage multiple
	// frames up front and reference them by name.
	var (
		other        *dataframe.DataFrame
		releaseOther bool
		err          error
	)
	if nf, ok := s.frames[target]; ok {
		other = nf.df
		releaseOther = false // owned by the registry
	} else {
		other, err = s.loadFile(target)
		if err != nil {
			return err
		}
		releaseOther = true
	}

	cur, err := s.materialize()
	if err != nil {
		if releaseOther {
			other.Release()
		}
		return err
	}
	joined, err := cur.Join(s.ctx, other, []string{key}, how)
	if releaseOther {
		other.Release()
	}
	cur.Release()
	if err != nil {
		return err
	}
	if s.df != nil {
		s.df.Release()
	}
	s.df = joined
	s.lf = nil
	fmt.Printf("%s joined (%d × %d) via %s on %s\n",
		successStyle.Render("ok"), joined.Height(), joined.Width(), how, key)
	return nil
}

// loadFile is like load but returns the DataFrame instead of storing it.
func (s *state) loadFile(path string) (*dataframe.DataFrame, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv", ".tsv":
		opts := []iocsv.Option{}
		if ext == ".tsv" {
			opts = append(opts, iocsv.WithDelimiter('\t'))
		}
		return iocsv.ReadFile(s.ctx, path, opts...)
	case ".parquet", ".pq":
		return ioparquet.ReadFile(s.ctx, path)
	case ".arrow", ".ipc":
		return ipc.ReadFile(s.ctx, path)
	}
	return nil, fmt.Errorf("unsupported extension %q", ext)
}

// Helpers -------------------------------------------------------------------

func (s *state) currentLazy() lazy.LazyFrame {
	if s.lf != nil {
		return *s.lf
	}
	if s.df == nil {
		// A dummy LazyFrame to avoid nil panics; operations on it will fail
		// when Schema() runs.
		return lazy.FromDataFrame(nil)
	}
	return lazy.FromDataFrame(s.df)
}

func (s *state) materialize() (*dataframe.DataFrame, error) {
	if s.lf != nil {
		return s.lf.Collect(s.ctx)
	}
	if s.df == nil {
		return nil, fmt.Errorf("no source loaded")
	}
	return s.df.Clone(), nil
}

func fields(s string) []string { return strings.Fields(s) }

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func parseNat(s string) (int, bool) {
	n := 0
	if s == "" {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

func printErr(err error) {
	fmt.Println(errStyle.Render("error") + " " + errMsgStyle.Render(err.Error()))
}

func printTiming(d time.Duration) {
	style := successStyle
	switch {
	case d > 100*time.Millisecond:
		style = errStyle
	case d > 10*time.Millisecond:
		style = lipgloss.NewStyle().Foreground(warningColor)
	}
	fmt.Println(style.Render("⏱  " + d.String()))
}
