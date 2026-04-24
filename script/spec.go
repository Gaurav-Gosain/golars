package script

// CommandSpec is the compile-time description of a golars script
// command. Editors and LSPs can consult this to render completion
// labels, signature help, and hover text without re-parsing the
// dispatcher in cmd/golars. The spec list is authoritative: if you
// add a command to cmd/golars, mirror it here.
type CommandSpec struct {
	// Name is the bare command (no leading dot).
	Name string
	// Signature shows arguments: angle-bracketed placeholders for
	// required args, square-bracketed for optional. Used verbatim in
	// completion details.
	Signature string
	// Summary is a one-line description: populated in hover docs.
	Summary string
	// LongDoc is the full man-page style description: multi-line,
	// rendered in hover markdown. May be empty for minor commands.
	LongDoc string
	// Category groups commands for editor UIs. Free-form string.
	Category string
	// ArgKind annotates what the next positional argument is, so
	// editors can offer the right completion after the command. One
	// of: "none", "path", "frame", "column", "count".
	ArgKind string
	// Examples is a list of runnable `.glr` snippets that demonstrate
	// typical uses. Rendered as a fenced code block in LSP hover so a
	// dev can see exactly how to invoke the command. Leave empty if
	// the Signature alone is self-documenting.
	Examples []string
}

// Commands is the authoritative list. Order matches the REPL .help
// table for readability.
var Commands = []CommandSpec{
	{
		Name:      "load",
		Signature: "load <path> [as NAME]",
		Summary:   "Load a csv/parquet/arrow/ipc file as the focused frame (or staged under NAME).",
		LongDoc: "Loads a file by path and makes it the new focused pipeline." +
			" When `as NAME` is appended, the frame is staged in the named-frame registry" +
			" instead of replacing the focus: useful for multi-source scripts.\n\n" +
			"Supported extensions: .csv .tsv .parquet .pq .arrow .ipc. Empty CSV fields" +
			" become nulls (matches polars).",
		Category: "io",
		ArgKind:  "path",
		Examples: []string{
			"load data/people.csv",
			"load data/salaries.parquet as salaries",
		},
	},
	{
		Name: "use", Signature: "use <NAME>", Summary: "Switch focus to a clone of a named frame.",
		LongDoc: "Copy-on-promote: NAME stays in the registry so repeated `use NAME`" +
			" lets scripts branch off the same base. The prior focus is discarded -" +
			" call `stash` first if you need it back.",
		Category: "frames", ArgKind: "frame",
	},
	{
		Name: "stash", Signature: "stash <NAME>",
		Summary: "Snapshot the current focus as NAME for later `.use`.",
		LongDoc: "Materialises any pending lazy pipeline and stores a reference under NAME." +
			" The focus is replaced with the materialised state (lazy pipeline cleared)," +
			" so subsequent ops continue from the snapshot. Combine with `use` to branch:" +
			" `stash base; filter X; stash a; use base; filter Y; stash b; use a; join b on k`.",
		Category: "frames",
	},
	{Name: "frames", Signature: "frames", Summary: "List loaded frames.", Category: "frames"},
	{
		Name: "drop_frame", Signature: "drop_frame <NAME>", Summary: "Release a named frame.",
		Category: "frames", ArgKind: "frame",
	},
	{Name: "save", Signature: "save <path>", Summary: "Collect focused pipeline and write to disk.", Category: "io", ArgKind: "path"},
	{Name: "show", Signature: "show", Summary: "Alias for head 10; prints first rows.", Category: "inspect"},
	{
		Name: "ishow", Signature: "ishow",
		Summary: "Open the focused pipeline in the interactive browse TUI.",
		LongDoc: "Materialises the current lazy pipeline (or focused frame) and hands it to" +
			" the browse TUI on the alt screen. Quit with `q` to return to the REPL; the" +
			" pipeline is untouched. Export the current view (with filter/sort applied) via" +
			" `:export PATH` inside the TUI; format is inferred from the extension.",
		Category: "inspect",
	},
	{Name: "browse", Signature: "browse", Summary: "Alias for `ishow`.", Category: "inspect"},
	{Name: "schema", Signature: "schema", Summary: "Print column names and dtypes.", Category: "inspect"},
	{Name: "describe", Signature: "describe", Summary: "Per-column summary stats (count/null/mean/std/quartiles).", Category: "inspect"},
	{Name: "head", Signature: "head [N]", Summary: "Collect and print first N rows (default 10).", Category: "inspect", ArgKind: "count"},
	{Name: "tail", Signature: "tail [N]", Summary: "Collect and print last N rows (default 10).", Category: "inspect", ArgKind: "count"},
	{Name: "select", Signature: "select <col>[,<col>...]", Summary: "Project columns (lazy).", Category: "pipeline", ArgKind: "column"},
	{Name: "drop", Signature: "drop <col>[,<col>...]", Summary: "Drop columns (lazy).", Category: "pipeline", ArgKind: "column"},
	{
		Name: "filter", Signature: "filter <col> <op> <value> [and|or ...]",
		Summary: "Filter rows by a predicate (lazy).",
		LongDoc: "Accepts `col op value` clauses combined with `and`/`or` (left-to-right, no parens)." +
			" Ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `is_null`, `is_not_null`," +
			" `contains`, `starts_with`, `ends_with`, `like`, `not_like`." +
			" Values: integers, floats, \"double-quoted strings\", `true`, `false`." +
			" A bare boolean column name is also a valid predicate: `filter active`.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"filter age > 25",
			"filter salary >= 100000 and dept == \"eng\"",
			"filter name like \"a%\"",
			"filter discount is_not_null",
		},
	},
	{Name: "sort", Signature: "sort <col> [asc|desc]", Summary: "Sort by one column (lazy).", Category: "pipeline", ArgKind: "column"},
	{Name: "limit", Signature: "limit <N>", Summary: "Keep the first N rows (lazy).", Category: "pipeline", ArgKind: "count"},
	{
		Name: "groupby", Signature: "groupby <k1,k2,...> <col:op[:alias]>...",
		Summary:  "Group by KEYS, aggregate via col:op[:alias].",
		LongDoc:  "Ops: `sum`, `mean`/`avg`, `min`, `max`, `count`, `null_count`, `first`, `last`.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"groupby dept amount:sum:total amount:mean:avg",
			"groupby region,product qty:sum:units price:max:peak",
		},
	},
	{
		Name: "join", Signature: "join <path|NAME> on <key> [inner|left|cross]",
		Summary:  "Join focus with a file or named frame on KEY.",
		Category: "pipeline", ArgKind: "frame",
		Examples: []string{
			"load people.csv as people",
			"load salaries.csv",
			"join people on name inner",
		},
	},
	{Name: "explain", Signature: "explain", Summary: "Print logical plan, optimiser trace, optimised plan.", Category: "pipeline"},
	{Name: "explain_tree", Signature: "explain_tree", Summary: "Explain rendered as a box-drawn tree.", Category: "pipeline"},
	{Name: "tree", Signature: "tree", Summary: "Alias of explain_tree.", Category: "pipeline"},
	{Name: "graph", Signature: "graph", Summary: "Styled plan tree with lipgloss colour coding (alias: show_graph).", Category: "pipeline"},
	{Name: "show_graph", Signature: "show_graph", Summary: "Alias of graph.", Category: "pipeline"},
	{Name: "mermaid", Signature: "mermaid", Summary: "Emit the plan as a Mermaid flowchart.", Category: "pipeline"},
	{Name: "collect", Signature: "collect", Summary: "Materialise lazy pipeline back into focused source.", Category: "pipeline"},
	{Name: "reset", Signature: "reset", Summary: "Discard the lazy pipeline; keep the source.", Category: "pipeline"},
	{Name: "source", Signature: "source <path>", Summary: "Run another .glr script inline.", Category: "meta", ArgKind: "path"},
	{Name: "timing", Signature: "timing", Summary: "Toggle per-statement timing output.", Category: "meta"},
	{Name: "info", Signature: "info", Summary: "Runtime info: Go version, heap, uptime.", Category: "meta"},
	{Name: "clear", Signature: "clear", Summary: "Clear the screen.", Category: "meta"},
	{Name: "help", Signature: "help", Summary: "Print the command reference.", Category: "meta"},
	{Name: "exit", Signature: "exit", Summary: "Quit the REPL.", Category: "meta"},
	{Name: "quit", Signature: "quit", Summary: "Quit the REPL.", Category: "meta"},
	{Name: "reverse", Signature: "reverse", Summary: "Reverse row order of the current focus.", Category: "pipeline"},
	{
		Name: "sample", Signature: "sample <N> [seed]",
		Summary:  "Sample N rows (no replacement).",
		LongDoc:  "Draws N rows uniformly at random without replacement. Optional second argument sets the PCG seed.",
		Category: "pipeline", ArgKind: "count",
	},
	{
		Name: "shuffle", Signature: "shuffle [seed]",
		Summary:  "Randomly reorder every row.",
		Category: "pipeline",
	},
	{
		Name: "unique", Signature: "unique",
		Summary:  "Drop duplicate rows over all columns (group-by-every).",
		Category: "pipeline",
	},
	{
		Name: "null_count", Signature: "null_count",
		Summary:  "Per-column null count as a single-row frame.",
		Category: "inspect",
	},
	{
		Name: "glimpse", Signature: "glimpse [N]",
		Summary:  "Compact peek at the first N rows (default 5).",
		Category: "inspect", ArgKind: "count",
	},
	{
		Name: "size", Signature: "size",
		Summary:  "Estimated Arrow byte size of the current pipeline output.",
		Category: "inspect",
	},
	{
		Name: "cast", Signature: "cast <col> <dtype>",
		Summary:  "Cast a column to the given dtype (i64, i32, f64, f32, bool, str).",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "fill_null", Signature: "fill_null <value>",
		Summary:  "Replace nulls across all compatible columns.",
		Category: "pipeline",
	},
	{
		Name: "drop_null", Signature: "drop_null [col...]",
		Summary:  "Drop rows with nulls in any (or the listed) columns.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "rename", Signature: "rename <old> as <new>",
		Summary:  "Rename a single column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "sum", Signature: "sum <col>",
		Summary:  "Print the sum of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "mean", Signature: "mean <col>",
		Summary:  "Print the mean of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "min", Signature: "min <col>",
		Summary:  "Print the min of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "max", Signature: "max <col>",
		Summary:  "Print the max of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "median", Signature: "median <col>",
		Summary:  "Print the median of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "std", Signature: "std <col>",
		Summary:  "Print the sample standard deviation of the given column.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "write", Signature: "write <path>",
		Summary:  "Alias for `save`: materialise the pipeline and write to disk.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "with_row_index", Signature: "with_row_index <name> [offset]",
		Summary:  "Prepend an int64 row-index column.",
		Category: "pipeline",
	},
	{
		Name: "pwd", Signature: "pwd",
		Summary:  "Print the REPL working directory.",
		Category: "meta",
	},
	{
		Name: "ls", Signature: "ls [path]",
		Summary:  "List files in the given directory (default: cwd).",
		Category: "meta", ArgKind: "path",
	},
	{
		Name: "cd", Signature: "cd [path]",
		Summary:  "Change the REPL working directory (default: home).",
		Category: "meta", ArgKind: "path",
	},
	{
		Name: "sum_horizontal", Signature: "sum_horizontal <out> [col...]",
		Summary: "Append a row-wise sum column named <out> across selected (or all numeric) columns.",
		LongDoc: "Row-wise reduction: for every row, sum the values in the listed columns" +
			" (or every numeric column when none are given). Nulls are skipped by default.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "mean_horizontal", Signature: "mean_horizontal <out> [col...]",
		Summary:  "Append a row-wise mean column (nulls skipped).",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "min_horizontal", Signature: "min_horizontal <out> [col...]",
		Summary:  "Append a row-wise min column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "max_horizontal", Signature: "max_horizontal <out> [col...]",
		Summary:  "Append a row-wise max column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "all_horizontal", Signature: "all_horizontal <out> [col...]",
		Summary:  "Append a row-wise boolean AND column across selected (or all boolean) columns.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "any_horizontal", Signature: "any_horizontal <out> [col...]",
		Summary:  "Append a row-wise boolean OR column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "sum_all", Signature: "sum_all",
		Summary:  "One-row frame with the sum of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "mean_all", Signature: "mean_all",
		Summary:  "One-row frame with the mean of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "min_all", Signature: "min_all",
		Summary:  "One-row frame with the min of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "max_all", Signature: "max_all",
		Summary:  "One-row frame with the max of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "std_all", Signature: "std_all",
		Summary:  "One-row frame with the sample std of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "var_all", Signature: "var_all",
		Summary:  "One-row frame with the sample variance of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "median_all", Signature: "median_all",
		Summary:  "One-row frame with the median of every numeric column.",
		Category: "inspect",
	},
	{
		Name: "count_all", Signature: "count_all",
		Summary:  "One-row frame with the non-null count of every column.",
		Category: "inspect",
	},
	{
		Name: "null_count_all", Signature: "null_count_all",
		Summary:  "One-row frame with the null count of every column.",
		Category: "inspect",
	},
	{
		Name: "with", Signature: "with <name> = <expression>",
		Summary: "Append a derived column via the expression DSL.",
		LongDoc: "Expression grammar covers arithmetic, comparisons, logical ops," +
			" `str.*` methods, aggregates, rolling/EWM windows, casts, and top-level" +
			" `col` / `lit` / `coalesce`. Result appended as a new column; originals untouched.\n\n" +
			"**Supported `str.*` methods**\n" +
			"`upper`, `lower`, `trim`, `reverse`, `title`, `len_bytes`, `len_chars`, `contains`," +
			" `starts_with`, `ends_with`, `like`, `not_like`, `contains_regex`, `replace`," +
			" `replace_all`, `strip_prefix`, `strip_suffix`, `slice`, `head`, `tail`," +
			" `count_matches`, `find`, `split_exact`.\n\n" +
			"**Aggregates**\n" +
			"`sum`, `mean`, `min`, `max`, `count`, `null_count`, `first`, `last`," +
			" `median`, `std`, `var`, `quantile`, `skew`, `kurtosis`, `n_unique`, `approx_n_unique`.\n\n" +
			"**Shape ops**\n" +
			"`abs`, `neg`, `not`, `round`, `floor`, `ceil`, `sqrt`, `exp`, `log`, `log2`, `log10`," +
			" `sign`, `reverse`, `head`, `tail`, `shift`, `diff`, `cum_sum`, `cum_min`, `cum_max`," +
			" `fill_null`, `alias`, `cast`, `between`, `forward_fill`, `backward_fill`.\n\n" +
			"**Windows**\n" +
			"`rolling_sum`, `rolling_mean`, `rolling_min`, `rolling_max`, `rolling_std`," +
			" `rolling_var`, `ewm_mean`, `ewm_std`, `ewm_var`, `over(keys...)`.",
		Category: "pipeline",
		Examples: []string{
			"with bulk = salary > 100000",
			"with uname = name.str.upper()",
			"with revenue = price * qty",
			"with trend = amount.rolling_mean(7, 1)",
			"with smooth = amount.ewm_mean(0.3)",
			"with filled = coalesce(primary, backup).str.trim()",
		},
	},
	{
		Name: "unnest", Signature: "unnest <col>",
		Summary: "Project the fields of a struct-typed column as top-level columns.",
		LongDoc: "Requires COL to have a Struct dtype. Each struct field becomes a" +
			" top-level column whose name is taken from the field. Struct-level" +
			" nulls propagate into every unnested child. Field names must not" +
			" collide with existing columns.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "explode", Signature: "explode <col>",
		Summary: "Fan out each element of a list-typed column into its own row.",
		LongDoc: "Requires COL to have a List dtype. Surrounding columns are repeated" +
			" to match. Null and empty lists each become a single null row, matching" +
			" polars' default explode semantics.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "upsample", Signature: "upsample <col> <every>",
		Summary: "Interpolate a timestamp column at a regular interval.",
		LongDoc: "COL must be sorted-ascending timestamp. EVERY is a shorthand duration:" +
			" ns, us, ms, s, m, h, d, w. Calendar units (mo, y) are not supported." +
			" The source is left-joined onto the dense grid so gaps stay as nulls.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "scan_csv", Signature: "scan_csv <path> [as NAME]",
		Summary: "Register a lazy scan of a CSV file (push-down friendly).",
		LongDoc: "Unlike `load`, scan defers opening the file until Collect," +
			" allowing the optimiser to push projections and filters into the reader.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_parquet", Signature: "scan_parquet <path> [as NAME]",
		Summary:  "Register a lazy scan of a Parquet file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_ipc", Signature: "scan_ipc <path> [as NAME]",
		Summary:  "Register a lazy scan of an Arrow IPC file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_ndjson", Signature: "scan_ndjson <path> [as NAME]",
		Summary:  "Register a lazy scan of an NDJSON file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_json", Signature: "scan_json <path> [as NAME]",
		Summary:  "Register a lazy scan of a JSON (array) file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_auto", Signature: "scan_auto <path> [as NAME]",
		Summary:  "Register a lazy scan inferring the reader from the file extension.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "fill_nan", Signature: "fill_nan <value>",
		Summary:  "Replace NaN with VALUE in every float column (frame-level).",
		Category: "pipeline",
	},
	{
		Name: "forward_fill", Signature: "forward_fill [limit]",
		Summary: "Forward-fill nulls (at most LIMIT consecutive, 0 = unlimited).",
		LongDoc: "Per-column: replace nulls with the most recent non-null value." +
			" Leading nulls stay null. Mirrors polars' lf.fill_null(strategy=\"forward\").",
		Category: "pipeline",
	},
	{
		Name: "backward_fill", Signature: "backward_fill [limit]",
		Summary:  "Backward-fill nulls. Trailing nulls stay null.",
		Category: "pipeline",
	},
	{
		Name: "top_k", Signature: "top_k <K> <col>",
		Summary:  "Collect the K rows with the largest values in COL (descending).",
		Category: "inspect", ArgKind: "count",
	},
	{
		Name: "bottom_k", Signature: "bottom_k <K> <col>",
		Summary:  "Collect the K rows with the smallest values in COL.",
		Category: "inspect", ArgKind: "count",
	},
	{
		Name: "transpose", Signature: "transpose [header_col] [prefix]",
		Summary:  "Transpose the focus (numeric/bool columns only).",
		Category: "pipeline",
	},
	{
		Name: "unpivot", Signature: "unpivot <id_cols> [val_cols]",
		Summary:  "Reshape wide to long (melt). ID_COLS/VAL_COLS are comma-separated lists.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "partition_by", Signature: "partition_by <keys>",
		Summary:  "Split the focus into one frame per distinct key combination; prints a summary.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "skew", Signature: "skew <col>",
		Summary:  "Print the skewness of COL (polars-default, biased).",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "kurtosis", Signature: "kurtosis <col>",
		Summary:  "Print the excess kurtosis of COL.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "approx_n_unique", Signature: "approx_n_unique <col>",
		Summary:  "HyperLogLog estimate of the number of distinct values in COL.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "corr", Signature: "corr <col1> <col2>",
		Summary:  "Print the Pearson correlation between two numeric columns.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "cov", Signature: "cov <col1> <col2>",
		Summary:  "Print the sample covariance (ddof=1) between two numeric columns.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "pivot", Signature: "pivot <index_cols> <on_col> <values_col> [agg]",
		Summary:  "Long-to-wide pivot. agg: first/sum/mean/min/max/count (default first).",
		LongDoc:  "INDEX_COLS may be a comma-separated list.",
		Category: "pipeline", ArgKind: "column",
	},
}

// FindCommand returns the CommandSpec with the given name (case-
// insensitive, leading '.' stripped), or nil if unknown.
func FindCommand(name string) *CommandSpec {
	name = normalizeCommandName(name)
	for i := range Commands {
		if Commands[i].Name == name {
			return &Commands[i]
		}
	}
	return nil
}

func normalizeCommandName(s string) string {
	if len(s) > 0 && s[0] == '.' {
		s = s[1:]
	}
	// ASCII lowercase: command names are all lowercase.
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

// SuggestCommand returns the closest known command name to want by
// edit distance, or "" if nothing is within reasonable range.
// Typos like `filtet`, `groupbyy`, `schma` get a helpful nudge.
func SuggestCommand(want string) string {
	want = normalizeCommandName(want)
	if want == "" {
		return ""
	}
	best, bestDist := "", len(want)+1
	// Cap depends on input length; longer commands tolerate more slop.
	maxDist := 2
	if len(want) >= 8 {
		maxDist = 3
	}
	for _, cmd := range Commands {
		d := editDistance(want, cmd.Name, bestDist+1)
		if d < bestDist && d <= maxDist {
			best, bestDist = cmd.Name, d
		}
	}
	return best
}

// editDistance is the classic Damerau-Levenshtein distance capped
// at maxD. Returning a value >= maxD indicates "too far to matter".
func editDistance(a, b string, maxD int) int {
	la, lb := len(a), len(b)
	if abs(la-lb) >= maxD {
		return maxD
	}
	// Ensure a is the shorter string so we use less memory.
	if la > lb {
		a, b = b, a
		la, lb = lb, la
	}
	prev := make([]int, la+1)
	curr := make([]int, la+1)
	for i := 0; i <= la; i++ {
		prev[i] = i
	}
	for j := 1; j <= lb; j++ {
		curr[0] = j
		rowMin := curr[0]
		for i := 1; i <= la; i++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			ins := curr[i-1] + 1
			del := prev[i] + 1
			sub := prev[i-1] + cost
			m := min(ins, del, sub)
			// Transposition (Damerau): swap two adjacent chars.
			if i >= 2 && j >= 2 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				if t := prev[i-2] + 1; t < m {
					m = t
				}
			}
			curr[i] = m
			if m < rowMin {
				rowMin = m
			}
		}
		if rowMin >= maxD {
			return maxD
		}
		prev, curr = curr, prev
	}
	return prev[la]
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
