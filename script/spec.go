package script

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// CommandSpec is the compile-time description of a golars script
// command. Editors and LSPs can consult this to render completion
// labels, signature help, and hover text without re-parsing the
// dispatcher in cmd/golars. The spec list is authoritative: if you
// add a command to cmd/golars, mirror it here.
type CommandSpec struct {
	// Name is the canonical command (no leading dot).
	Name string
	// Aliases are alternative spellings that dispatch to the same
	// command. FindCommand resolves them to this spec.
	Aliases []string
	// Signature shows arguments: angle-bracketed placeholders for
	// required args, square-bracketed for optional. Used verbatim in
	// completion details.
	Signature string
	// Grammar describes the arguments for the statement parser in
	// script/syntax: a space-separated pattern of slots. Slots are
	// path, frame (a staged name), target (a staged name or a path),
	// newframe, col, newcol, cols (one or more columns, commas or
	// spaces), collist (one comma-joined list), count, int, number,
	// value, dtype, dur and word; 'kw' is a literal keyword, a|b|c is
	// one of the listed words, and [ ] marks an optional group. A
	// grammar starting with @ names a statement with its own parser
	// (@select, @filter, @with, @sort, @groupby, @dynamic, @asof).
	Grammar string
	// Summary is a one-line description: populated in hover docs.
	Summary string
	// LongDoc is the full man-page style description: multi-line,
	// rendered in hover markdown. May be empty for minor commands.
	LongDoc string
	// Category groups commands in `.help` and editor UIs. One of the
	// values in Categories.
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

// Commands is the authoritative list of script commands. The CLI
// dispatcher, the REPL `.help` table and completion, the LSP, the
// Jupyter kernel, the tree-sitter grammar and docs/scripting.md are
// all checked against it by tests, so adding a command here is the
// first step and the failing tests list the places that still need it.
var Commands = []CommandSpec{
	{
		Name:      "load",
		Signature: "load <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary: "Load a csv/tsv/parquet/arrow/json/ndjson file as the focused frame (or staged under NAME).",
		LongDoc: "Loads a file by path and makes it the new focused pipeline." +
			" When `as NAME` is appended, the frame is staged in the named-frame registry" +
			" instead of replacing the focus: useful for multi-source scripts.\n\n" +
			"Supported extensions: .csv .tsv .parquet .pq .arrow .ipc .json .ndjson .jsonl. Empty CSV fields" +
			" become nulls (matches polars).",
		Category: "io",
		ArgKind:  "path",
		Examples: []string{
			"load data/people.csv",
			"load data/salaries.parquet as salaries",
		},
	},
	{
		Name: "use", Signature: "use <NAME>", Grammar: "frame", Summary: "Switch focus to a clone of a named frame.",
		LongDoc: "Copy-on-promote: NAME stays in the registry so repeated `use NAME`" +
			" lets scripts branch off the same base. The prior focus is discarded -" +
			" call `stash` first if you need it back.",
		Category: "frames", ArgKind: "frame",
	},
	{
		Name: "stash", Signature: "stash <NAME>", Grammar: "newframe",
		Summary: "Snapshot the current focus as NAME for later `.use`.",
		LongDoc: "Materialises any pending lazy pipeline and stores a reference under NAME." +
			" The focus is replaced with the materialised state (lazy pipeline cleared)," +
			" so subsequent ops continue from the snapshot. Combine with `use` to branch:" +
			" `stash base; filter X; stash a; use base; filter Y; stash b; use a; join b on k`.",
		Category: "frames",
	},
	{Name: "frames", Signature: "frames", Grammar: "", Summary: "List loaded frames.", Category: "frames"},
	{
		Name: "drop_frame", Signature: "drop_frame <NAME>", Grammar: "frame", Summary: "Release a named frame.",
		Category: "frames", ArgKind: "frame",
	},
	{
		Name: "save", Aliases: []string{"write"}, Signature: "save <path>", Grammar: "path",
		Summary:  "Collect the focused pipeline and write it to disk.",
		LongDoc:  "The format comes from the extension: .csv .tsv .parquet .pq .arrow .ipc .json .ndjson .jsonl.",
		Category: "io", ArgKind: "path",
	},
	{Name: "show", Signature: "show [N]", Grammar: "[count]", Summary: "Collect and print the first N rows (default 10). Same as head.", Category: "inspect", ArgKind: "count"},
	{
		Name: "ishow", Aliases: []string{"browse"}, Signature: "ishow", Grammar: "",
		Summary: "Open the focused pipeline in the interactive browse TUI.",
		LongDoc: "Materialises the current lazy pipeline (or focused frame) and hands it to" +
			" the browse TUI on the alt screen. Quit with `q` to return to the REPL; the" +
			" pipeline is untouched. Export the current view (with filter/sort applied) via" +
			" `:export PATH` inside the TUI; format is inferred from the extension.",
		Category: "inspect",
	},
	{Name: "schema", Signature: "schema", Grammar: "", Summary: "Print column names and dtypes.", Category: "inspect"},
	{
		Name: "describe", Signature: "describe [col...]", Grammar: "[cols]",
		Summary:  "Per-column summary stats (count, null_count, mean, std, min, quartiles, max).",
		LongDoc:  "Describes every column, or only the listed ones. Non-numeric columns get count and null_count.",
		Category: "inspect", ArgKind: "column",
		Examples: []string{"describe", "describe salary, age"},
	},
	{Name: "head", Signature: "head [N]", Grammar: "[count]", Summary: "Collect and print first N rows (default 10).", Category: "inspect", ArgKind: "count"},
	{Name: "tail", Signature: "tail [N]", Grammar: "[count]", Summary: "Collect and print last N rows (default 10).", Category: "inspect", ArgKind: "count"},
	{
		Name: "select", Signature: "select <col|name = expr>[, ...]", Grammar: "@select",
		Summary: "Project columns or expressions (lazy).",
		LongDoc: "Plain column names may be separated by commas or spaces. Items that contain" +
			" an expression are separated by commas and may be named with `name = expr`;" +
			" the expression language is the one `with` uses.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"select name, dept",
			"select name, year = dt.year(hired), salary / 12",
		},
	},
	{Name: "drop", Signature: "drop <col>[,<col>...]", Grammar: "cols", Summary: "Drop columns (lazy). Columns may be separated by commas or spaces.", Category: "pipeline", ArgKind: "column"},
	{
		Name: "filter", Signature: "filter <predicate>", Grammar: "@filter",
		Summary: "Filter rows by a predicate (lazy).",
		LongDoc: "The predicate is any boolean expression in the `with` expression language." +
			" Classic clauses read naturally: `col op value` with `==`, `!=`, `<`, `<=`, `>`, `>=`," +
			" `is_null`, `is_not_null`, `contains`, `starts_with`, `ends_with`, `like`, `not_like`," +
			" and `in [...]`, combined with `and`, `or`, `not` and parentheses (`and` binds tighter)." +
			" Functions work too: `filter dt.year(ts) == 2024`." +
			" A bare boolean column name is also a valid predicate: `filter active`.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"filter age > 25",
			"filter salary >= 100000 and dept == \"eng\"",
			"filter name like \"a%\"",
			"filter discount is_not_null",
			"filter dept in [\"eng\", \"ops\"] and not (age < 21)",
			"filter dt.month(hired) == 12",
		},
	},
	{
		Name: "sort", Signature: "sort <col> [asc|desc] [<col> [asc|desc]]...", Grammar: "@sort",
		Summary:  "Sort by one or more columns (lazy). Nulls sort first, as in polars.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{"sort salary desc", "sort dept asc salary desc"},
	},
	{Name: "limit", Signature: "limit <N>", Grammar: "count", Summary: "Keep the first N rows (lazy).", Category: "pipeline", ArgKind: "count"},
	{
		Name: "groupby", Signature: "groupby <k1,k2,...> <col:op[:alias] | name=expr>...", Grammar: "@groupby",
		Summary: "Group by KEYS and aggregate.",
		LongDoc: "Each aggregation is a shorthand `col:op[:alias]` or a named expression `name=expr`." +
			" Shorthand ops: `sum`, `mean`/`avg`, `min`, `max`, `count`, `null_count`, `first`, `last`," +
			" `median`, `std`, `var`, `n_unique`. Aggregations are separated by spaces, so write" +
			" an expression without spaces or wrap it in parentheses: `share=(amount.sum() / 100)`.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"groupby dept amount:sum:total amount:mean:avg",
			"groupby region,product qty:sum:units price:max:peak",
			"groupby dept n=len() top=salary.max() names=name.n_unique()",
		},
	},
	{
		Name: "group_by_dynamic", Aliases: []string{"groupby_dynamic"},
		Signature: "group_by_dynamic <time_col> every <dur> [period <dur>] [offset <dur>] [by <keys>] [closed <c>] [label <l>] <agg>...", Grammar: "@dynamic",
		Summary: "Group rows into time windows of TIME_COL and aggregate (lazy).",
		LongDoc: "Windows start every DUR (`30m`, `1h`, `1d`, `1w`, `1mo`, `1y`, or `3i` for integer" +
			" columns) and last PERIOD (default: EVERY). TIME_COL must be sorted ascending (within each" +
			" `by` group). `closed` is left (default), right, both or none; `label` is left (default)," +
			" right or datapoint. Aggregations use the `groupby` forms; windowed aggregations take a" +
			" bare column: `col:op[:alias]` or `name=col.op()`.",
		Category: "pipeline", ArgKind: "column",
		Examples: []string{
			"group_by_dynamic ts every 1h amount:sum:total amount:count:n",
			"group_by_dynamic day every 1w by store sales:mean",
		},
	},
	{
		Name: "join", Signature: "join <path|NAME> on <key> [inner|left|cross]", Grammar: "target 'on' col [inner|left|cross]",
		Summary:  "Join focus with a file or named frame on KEY.",
		Category: "pipeline", ArgKind: "frame",
		Examples: []string{
			"load people.csv as people",
			"load salaries.csv",
			"join people on name inner",
		},
	},
	{
		Name: "join_asof", Signature: "join_asof <path|NAME> on <key> [by <cols>] [backward|forward|nearest] [tolerance <t>]", Grammar: "@asof",
		Summary: "Join each row to the nearest earlier (or later) row of another frame by KEY.",
		LongDoc: "An asof join matches on the nearest key rather than an equal one. `backward`" +
			" (default) takes the last right row whose key is <= the left key, `forward` the first" +
			" row >= it, `nearest` the closest. `by` requires exact matches on those columns first." +
			" `tolerance` bounds the distance: a number, or a duration such as `2m` for temporal keys." +
			" Both frames must be sorted by KEY.",
		Category: "pipeline", ArgKind: "frame",
		Examples: []string{
			"load quotes.csv as quotes",
			"load trades.csv",
			"join_asof quotes on time by symbol tolerance 2m",
		},
	},
	{Name: "explain", Signature: "explain", Grammar: "", Summary: "Print logical plan, optimiser trace, optimised plan.", Category: "plan"},
	{Name: "explain_tree", Aliases: []string{"tree"}, Signature: "explain_tree", Grammar: "", Summary: "Explain rendered as a box-drawn tree.", Category: "plan"},
	{Name: "graph", Aliases: []string{"show_graph"}, Signature: "graph", Grammar: "", Summary: "Styled plan tree with colour coding per node kind.", Category: "plan"},
	{Name: "mermaid", Signature: "mermaid", Grammar: "", Summary: "Emit the plan as a Mermaid flowchart.", Category: "plan"},
	{Name: "collect", Signature: "collect", Grammar: "", Summary: "Materialise lazy pipeline back into focused source.", Category: "pipeline"},
	{Name: "reset", Signature: "reset", Grammar: "", Summary: "Discard the lazy pipeline; keep the source.", Category: "pipeline"},
	{Name: "source", Signature: "source <path>", Grammar: "path", Summary: "Run another .glr script inline.", Category: "session", ArgKind: "path"},
	{Name: "timing", Signature: "timing", Grammar: "", Summary: "Toggle per-statement timing output.", Category: "session"},
	{Name: "info", Signature: "info", Grammar: "", Summary: "Runtime info: Go version, heap, uptime.", Category: "session"},
	{Name: "clear", Signature: "clear", Grammar: "", Summary: "Clear the screen.", Category: "session"},
	{Name: "help", Aliases: []string{"h", "?"}, Signature: "help", Grammar: "", Summary: "Print the command reference.", Category: "session"},
	{Name: "exit", Aliases: []string{"quit", "q"}, Signature: "exit", Grammar: "", Summary: "Quit the REPL. In a script, stop running further statements.", Category: "session"},
	{Name: "reverse", Signature: "reverse", Grammar: "", Summary: "Reverse the row order of the focus (lazy).", Category: "pipeline"},
	{
		Name: "sample", Signature: "sample <N> [seed]", Grammar: "count [int]",
		Summary:  "Replace the focus with N rows sampled without replacement.",
		LongDoc:  "Draws N rows uniformly at random without replacement. The optional second argument sets the PCG seed (default 42).",
		Category: "reshape", ArgKind: "count",
	},
	{
		Name: "shuffle", Signature: "shuffle [seed]", Grammar: "[int]",
		Summary:  "Randomly reorder every row of the focus (seed defaults to 42).",
		Category: "reshape",
	},
	{
		Name: "unique", Signature: "unique", Grammar: "",
		Summary:  "Drop duplicate rows over all columns (lazy).",
		Category: "pipeline",
	},
	{
		Name: "null_count", Aliases: []string{"null-count"}, Signature: "null_count", Grammar: "",
		Summary:  "Per-column null count as a single-row frame.",
		Category: "inspect",
	},
	{
		Name: "glimpse", Signature: "glimpse [N]", Grammar: "[count]",
		Summary:  "Compact peek at the first N rows (default 5).",
		Category: "inspect", ArgKind: "count",
	},
	{
		Name: "size", Signature: "size", Grammar: "",
		Summary:  "Estimated Arrow byte size of the current pipeline output.",
		Category: "inspect",
	},
	{
		Name: "cast", Signature: "cast <col> <dtype>", Grammar: "col dtype",
		Summary:  "Cast a column to a dtype: i8 to i64, u8 to u64, f32, f64, bool, str, date, datetime[ms], categorical, ...",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "fill_null", Aliases: []string{"fillnull"}, Signature: "fill_null <value>", Grammar: "value",
		Summary:  "Replace nulls across all compatible columns.",
		Category: "pipeline",
	},
	{
		Name: "drop_null", Aliases: []string{"dropnull"}, Signature: "drop_null [col...]", Grammar: "[cols]",
		Summary:  "Drop rows with nulls in any (or the listed) columns.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "rename", Signature: "rename <old> as <new>", Grammar: "col 'as' newcol",
		Summary:  "Rename a single column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "sum", Signature: "sum <col>", Grammar: "col",
		Summary:  "Print the sum of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "mean", Aliases: []string{"avg"}, Signature: "mean <col>", Grammar: "col",
		Summary:  "Print the mean of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "min", Signature: "min <col>", Grammar: "col",
		Summary:  "Print the min of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "max", Signature: "max <col>", Grammar: "col",
		Summary:  "Print the max of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "median", Signature: "median <col>", Grammar: "col",
		Summary:  "Print the median of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "std", Signature: "std <col>", Grammar: "col",
		Summary:  "Print the sample standard deviation of the given column.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "with_row_index", Signature: "with_row_index <name> [offset]", Grammar: "newcol [int]",
		Summary:  "Prepend an int64 row-index column.",
		Category: "pipeline",
	},
	{
		Name: "pwd", Signature: "pwd", Grammar: "",
		Summary:  "Print the REPL working directory.",
		Category: "session",
	},
	{
		Name: "ls", Signature: "ls [path]", Grammar: "[path]",
		Summary:  "List files in the given directory (default: cwd).",
		Category: "session", ArgKind: "path",
	},
	{
		Name: "cd", Signature: "cd [path]", Grammar: "[path]",
		Summary:  "Change the REPL working directory (default: home).",
		Category: "session", ArgKind: "path",
	},
	{
		Name: "sum_horizontal", Signature: "sum_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary: "Append a row-wise sum column named <out> across selected (or all numeric) columns.",
		LongDoc: "Row-wise reduction: for every row, sum the values in the listed columns" +
			" (or every numeric column when none are given). Nulls are skipped by default.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "mean_horizontal", Signature: "mean_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary:  "Append a row-wise mean column (nulls skipped).",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "min_horizontal", Signature: "min_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary:  "Append a row-wise min column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "max_horizontal", Signature: "max_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary:  "Append a row-wise max column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "all_horizontal", Signature: "all_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary:  "Append a row-wise boolean AND column across selected (or all boolean) columns.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "any_horizontal", Signature: "any_horizontal <out> [col...]", Grammar: "newcol [cols]",
		Summary:  "Append a row-wise boolean OR column.",
		Category: "pipeline", ArgKind: "column",
	},
	{
		Name: "sum_all", Signature: "sum_all", Grammar: "",
		Summary:  "One-row frame with the sum of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "mean_all", Signature: "mean_all", Grammar: "",
		Summary:  "One-row frame with the mean of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "min_all", Signature: "min_all", Grammar: "",
		Summary:  "One-row frame with the min of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "max_all", Signature: "max_all", Grammar: "",
		Summary:  "One-row frame with the max of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "std_all", Signature: "std_all", Grammar: "",
		Summary:  "One-row frame with the sample std of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "var_all", Signature: "var_all", Grammar: "",
		Summary:  "One-row frame with the sample variance of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "median_all", Signature: "median_all", Grammar: "",
		Summary:  "One-row frame with the median of every numeric column.",
		Category: "aggregate",
	},
	{
		Name: "count_all", Signature: "count_all", Grammar: "",
		Summary:  "One-row frame with the non-null count of every column.",
		Category: "aggregate",
	},
	{
		Name: "null_count_all", Signature: "null_count_all", Grammar: "",
		Summary:  "One-row frame with the null count of every column.",
		Category: "aggregate",
	},
	{
		Name: "with", Signature: "with <name> = <expression>", Grammar: "@with",
		Summary: "Append a derived column via the expression language.",
		LongDoc: "Evaluates EXPRESSION for every row and appends the result as column NAME;" +
			" the other columns are untouched. The same expression language drives `filter`.\n\n" +
			"**Operators**\n" +
			"`+ - * /` (integer `/` gives f64), `//` floor division, `%` modulo, `**` power," +
			" comparisons, `and`, `or`, `not`, `x in [1, 2]`, `x not in [...]`, `x is_null`," +
			" and `when c then a [when c2 then b] otherwise d` (no otherwise gives null).\n\n" +
			"**Calls**\n" +
			"Every function works as a method or as a call whose first argument is the value:" +
			" `x.round(2)` is `round(x, 2)`, and `ts.dt.year()` is `dt.year(ts)`." +
			" Namespaces: `str`, `dt`, `list`, `arr`, `struct`, `name`, `bin`, `cat`." +
			" Options go in keyword arguments: `cut(x, [0, 10], labels=[\"lo\", \"mid\", \"hi\"])`." +
			" Lists are written `[a, b]`. Any golars expression method is reachable by its" +
			" snake_case name (`rolling_mean_by`, `rle_id`, `is_nan`, ...).\n\n" +
			"**Common functions**\n" +
			"`dt.year(ts)`, `dt.month(ts)`, `dt.weekday(ts)`, `dt.hour(ts)`, `dt.truncate(ts, \"1h\")`," +
			" `dt.offset_by(ts, \"1mo\")`, `dt.strftime(ts, \"%Y-%m\")`, `dt.convert_time_zone(ts, \"UTC\")`," +
			" `str.to_date(s, \"%Y-%m-%d\")`, `str.to_datetime(s)`, `str.split(s, \",\")`," +
			" `str.to_uppercase(s)`, `str.contains(s, \"x\")`, `str.replace_all(s, \"a\", \"b\")`," +
			" `str.slice(s, 0, 3)`, `str.pad_start(s, 5, \"0\")`, `list.len(tags)`, `list.get(tags, 0)`," +
			" `list.contains(tags, \"a\")`, `list.join(tags, \",\")`, `struct.field(st, \"a\")`," +
			" `cut(x, [0, 10, 100])`, `qcut(x, 4)`, `replace_strict(x, [1, 2], [\"a\", \"b\"], default=\"z\")`," +
			" `replace(x, [1], [10])`, `coalesce(a, b)`, `concat_str(a, b, separator=\"-\")`," +
			" `fill_null(x, 0)`, `is_between(x, 1, 5)`, `rank(x, \"dense\")`, `shift(x, 1)`, `diff(x)`," +
			" `cum_sum(x)`, `round(x, 2)`, `abs(x)`, `log(x, 10)`, `clip(x, 0, 1)`, `cast(x, \"f64\")`," +
			" `rolling_mean(x, 7, 1)`, `rolling_mean_by(x, ts, \"7d\")`, `ewm_mean(x, 0.3)`," +
			" `sort_by(x, y)`, `top_k_by(x, y, 3)`, `sum(x).over(\"k\")`, `interpolate(x)`," +
			" `date(2024, 1, 31)`, `date_range(a, b, \"1d\")`, `sum_horizontal(a, b)`, `len()`.\n\n" +
			"**Dtypes** for `cast`: i8 to i64, u8 to u64, f32, f64, bool, str, binary, date," +
			" datetime, datetime[ms], duration, time, categorical.",
		Category: "pipeline",
		Examples: []string{
			"with bulk = salary > 100000",
			"with uname = name.str.upper()",
			"with year = dt.year(str.to_date(hired, \"%Y-%m-%d\"))",
			"with band = cut(age, [30, 50], labels=[\"young\", \"mid\", \"senior\"])",
			"with ntags = list.len(str.split(tags, \",\"))",
			"with size = when amount > 1000 then \"big\" otherwise \"small\"",
			"with trend = amount.rolling_mean(7, 1)",
			"with filled = coalesce(primary, backup).str.trim()",
		},
	},
	{
		Name: "unnest", Signature: "unnest <col>", Grammar: "col",
		Summary: "Project the fields of a struct-typed column as top-level columns.",
		LongDoc: "Requires COL to have a Struct dtype. Each struct field becomes a" +
			" top-level column whose name is taken from the field. Struct-level" +
			" nulls propagate into every unnested child. Field names must not" +
			" collide with existing columns.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "explode", Signature: "explode <col>[,<col>...]", Grammar: "cols",
		Summary: "Fan out each element of a list-typed column into its own row.",
		LongDoc: "Requires list-typed columns. Surrounding columns are repeated" +
			" to match. Null and empty lists each become a single null row, matching" +
			" polars' default explode semantics. Several columns explode together and" +
			" must hold lists of equal length in each row.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "to_dummies", Signature: "to_dummies [col...] [drop_first]", Grammar: "[cols] ['drop_first']",
		Summary: "One-hot encode columns into u8 indicator columns named COL_VALUE.",
		LongDoc: "Encodes the listed columns, or every column when none are given. Each source" +
			" column is replaced in place by its indicators, sorted by value. `drop_first` drops" +
			" the first indicator of each column.",
		Category: "reshape", ArgKind: "column",
		Examples: []string{"to_dummies dept", "to_dummies dept, level drop_first"},
	},
	{
		Name: "upsample", Signature: "upsample <col> <every>", Grammar: "col dur",
		Summary: "Interpolate a timestamp column at a regular interval.",
		LongDoc: "COL must be sorted-ascending timestamp. EVERY is a shorthand duration:" +
			" ns, us, ms, s, m, h, d, w. Calendar units (mo, y) are not supported." +
			" The source is left-joined onto the dense grid so gaps stay as nulls.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "scan_csv", Signature: "scan_csv <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary: "Register a lazy scan of a CSV file (push-down friendly).",
		LongDoc: "Unlike `load`, scan defers opening the file until Collect," +
			" allowing the optimiser to push projections and filters into the reader.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_parquet", Signature: "scan_parquet <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary:  "Register a lazy scan of a Parquet file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_ipc", Aliases: []string{"scan_arrow"}, Signature: "scan_ipc <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary:  "Register a lazy scan of an Arrow IPC file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_ndjson", Aliases: []string{"scan_jsonl"}, Signature: "scan_ndjson <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary:  "Register a lazy scan of an NDJSON file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_json", Signature: "scan_json <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary:  "Register a lazy scan of a JSON (array) file.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "scan_auto", Signature: "scan_auto <path> [as NAME]", Grammar: "path ['as' newframe]",
		Summary:  "Register a lazy scan inferring the reader from the file extension.",
		Category: "io", ArgKind: "path",
	},
	{
		Name: "fill_nan", Signature: "fill_nan <value>", Grammar: "number",
		Summary:  "Replace NaN with VALUE in every float column (frame-level).",
		Category: "pipeline",
	},
	{
		Name: "forward_fill", Aliases: []string{"ff"}, Signature: "forward_fill [limit]", Grammar: "[count]",
		Summary: "Forward-fill nulls (at most LIMIT consecutive, 0 = unlimited).",
		LongDoc: "Per-column: replace nulls with the most recent non-null value." +
			" Leading nulls stay null. Mirrors polars' lf.fill_null(strategy=\"forward\").",
		Category: "pipeline",
	},
	{
		Name: "backward_fill", Aliases: []string{"bf"}, Signature: "backward_fill [limit]", Grammar: "[count]",
		Summary:  "Backward-fill nulls. Trailing nulls stay null.",
		Category: "pipeline",
	},
	{
		Name: "top_k", Signature: "top_k <K> <col>", Grammar: "count col",
		Summary:  "Replace the focus with the K rows holding the largest values in COL (descending).",
		Category: "reshape", ArgKind: "count",
	},
	{
		Name: "bottom_k", Signature: "bottom_k <K> <col>", Grammar: "count col",
		Summary:  "Replace the focus with the K rows holding the smallest values in COL.",
		Category: "reshape", ArgKind: "count",
	},
	{
		Name: "transpose", Signature: "transpose [header_col] [prefix]", Grammar: "[col] [word]",
		Summary:  "Transpose the focus (numeric/bool columns only).",
		Category: "reshape",
	},
	{
		Name: "unpivot", Aliases: []string{"melt"}, Signature: "unpivot <id_cols> [val_cols]", Grammar: "collist [collist]",
		Summary:  "Reshape wide to long. ID_COLS/VAL_COLS are comma-separated lists.",
		Category: "reshape", ArgKind: "column",
	},
	{
		Name: "partition_by", Signature: "partition_by <keys>", Grammar: "cols",
		Summary:  "Split the focus into one frame per distinct key combination; prints a summary.",
		Category: "inspect", ArgKind: "column",
	},
	{
		Name: "skew", Signature: "skew <col>", Grammar: "col",
		Summary:  "Print the skewness of COL (polars-default, biased).",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "kurtosis", Signature: "kurtosis <col>", Grammar: "col",
		Summary:  "Print the excess kurtosis of COL.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "approx_n_unique", Aliases: []string{"approx_nunique"}, Signature: "approx_n_unique <col>", Grammar: "col",
		Summary:  "HyperLogLog estimate of the number of distinct values in COL.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "corr", Signature: "corr <col1> <col2>", Grammar: "col col",
		Summary:  "Print the Pearson correlation between two numeric columns.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "cov", Signature: "cov <col1> <col2>", Grammar: "col col",
		Summary:  "Print the sample covariance (ddof=1) between two numeric columns.",
		Category: "aggregate", ArgKind: "column",
	},
	{
		Name: "pivot", Signature: "pivot <index_cols> <on_col> <values_col> [agg]", Grammar: "collist col col [first|sum|mean|min|max|count]",
		Summary:  "Long-to-wide pivot. agg: first/sum/mean/min/max/count (default first).",
		LongDoc:  "INDEX_COLS may be a comma-separated list.",
		Category: "reshape", ArgKind: "column",
	},
}

// Categories lists every CommandSpec.Category value in the order the
// REPL `.help` table prints them.
var Categories = []string{
	"io", "frames", "pipeline", "reshape", "inspect", "aggregate", "plan", "session",
}

// ErrUnknownCommand is wrapped by the error UnknownCommand returns, so
// hosts and the Runner can recognise a bad verb with errors.Is.
var ErrUnknownCommand = errors.New("unknown command")

// UnknownCommand returns an error for an unrecognised verb, with a
// "did you mean" hint when a known command is close by edit distance.
func UnknownCommand(name string) error {
	name = normalizeCommandName(name)
	if suggest := SuggestCommand(name); suggest != "" {
		return fmt.Errorf("%w %q (did you mean %q?)", ErrUnknownCommand, name, suggest)
	}
	return fmt.Errorf("%w %q", ErrUnknownCommand, name)
}

// FindCommand returns the CommandSpec whose name or alias matches
// name (case-insensitive, leading '.' stripped), or nil if unknown.
func FindCommand(name string) *CommandSpec {
	name = normalizeCommandName(name)
	for i := range Commands {
		if Commands[i].Name == name || slices.Contains(Commands[i].Aliases, name) {
			return &Commands[i]
		}
	}
	return nil
}

// Names returns every spelling a user can type: canonical names
// followed by aliases, in spec order.
func Names() []string {
	out := make([]string, 0, len(Commands)*2)
	for _, c := range Commands {
		out = append(out, c.Name)
	}
	for _, c := range Commands {
		out = append(out, c.Aliases...)
	}
	return out
}

// CompleteCommand returns every command name or alias that starts
// with prefix (case-insensitive, leading '.' ignored), canonical names
// first.
func CompleteCommand(prefix string) []string {
	prefix = normalizeCommandName(prefix)
	var out []string
	for _, n := range Names() {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	return out
}

// Markdown renders the spec as hover documentation: a title with the
// category, a fenced signature, the summary, the long description,
// aliases, and runnable examples.
func (c *CommandSpec) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "### `%s`", c.Name)
	if c.Category != "" {
		fmt.Fprintf(&b, "  _%s_", c.Category)
	}
	b.WriteString("\n\n")
	fmt.Fprintf(&b, "```glr\n%s\n```\n\n", c.Signature)
	if c.Summary != "" {
		b.WriteString(c.Summary)
		b.WriteString("\n\n")
	}
	if c.LongDoc != "" {
		b.WriteString(c.LongDoc)
		b.WriteString("\n\n")
	}
	if len(c.Aliases) > 0 {
		quoted := make([]string, len(c.Aliases))
		for i, a := range c.Aliases {
			quoted[i] = "`" + a + "`"
		}
		b.WriteString("Aliases: ")
		b.WriteString(strings.Join(quoted, ", "))
		b.WriteString("\n\n")
	}
	if len(c.Examples) > 0 {
		b.WriteString("**Examples**\n\n```glr\n")
		for _, ex := range c.Examples {
			b.WriteString(ex)
			b.WriteString("\n")
		}
		b.WriteString("```\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func normalizeCommandName(s string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s), "."))
}

// SuggestCommand returns the closest known command name to want by
// edit distance, or "" if nothing is within reasonable range.
// Typos like `filtet`, `groupbyy`, `schma` get a helpful nudge.
func SuggestCommand(want string) string {
	want = normalizeCommandName(want)
	if want == "" {
		return ""
	}
	// Cap depends on input length; longer commands tolerate more slop.
	maxDist := 2
	if len(want) >= 8 {
		maxDist = 3
	}
	best, bestDist := "", maxDist+1
	for _, cmd := range Commands {
		if d := editDistance(want, cmd.Name, bestDist); d < bestDist {
			best, bestDist = cmd.Name, d
		}
	}
	return best
}

// Closest returns the candidate nearest to want by edit distance, or
// "" when none is close enough to be a plausible typo. Matching is
// case-insensitive; the candidate is returned as written. Used for
// "did you mean" hints on columns, functions and keywords.
func Closest(want string, candidates []string) string {
	lw := strings.ToLower(want)
	if lw == "" {
		return ""
	}
	maxDist := 2
	switch {
	case len(lw) <= 2:
		maxDist = 1
	case len(lw) >= 8:
		maxDist = 3
	}
	best, bestDist := "", maxDist+1
	for _, c := range candidates {
		lc := strings.ToLower(c)
		if lc == lw {
			if c == want {
				continue
			}
			return c
		}
		if d := editDistance(lw, lc, bestDist); d < bestDist {
			best, bestDist = c, d
		}
	}
	return best
}

// editDistance is the optimal string alignment distance (Levenshtein
// plus adjacent transpositions) between a and b. Any result >= maxD
// is reported as maxD so callers can stop early.
func editDistance(a, b string, maxD int) int {
	la, lb := len(a), len(b)
	if abs(la-lb) >= maxD {
		return maxD
	}
	// Three rows: the transposition step reads the row two back.
	prev2 := make([]int, lb+1)
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := range lb + 1 {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		rowMin := curr[0]
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			m := min(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
			if i >= 2 && j >= 2 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				m = min(m, prev2[j-2]+1)
			}
			curr[j] = m
			rowMin = min(rowMin, m)
		}
		if rowMin >= maxD {
			return maxD
		}
		prev2, prev, curr = prev, curr, prev2
	}
	return min(prev[lb], maxD)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
