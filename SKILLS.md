# SKILLS.md

Invocable skills (slash commands) that the golars project provides.
These are entry points an LLM host or human operator can call against
the codebase or a live dataset.

Surfaced two ways:

1. As CLI subcommands of `golars`, run from a terminal.
2. As MCP tools via `golars-mcp`, exposed to Claude Desktop, Cursor,
   Windsurf, and any other MCP-aware host.

## CLI skills

Every subcommand of `golars` is a skill. `golars help` lists them;
the canonical reference with every flag lives in
[`docs/cookbook.md`](docs/cookbook.md).

### Inspect a file

| Skill | Purpose |
|---|---|
| `golars schema FILE` | Column names + dtypes |
| `golars peek FILE [N]` | Schema + head + shape in one call |
| `golars stats FILE` | describe()-style summary table |
| `golars head FILE [N]` | First N rows |
| `golars tail FILE [N]` | Last N rows |
| `golars sample FILE [--n N] [--frac F]` | Uniform-random sample |
| `golars doctor` | Environment diagnostic for bug reports |
| `golars version` | Print version and exit |

### Transform files

| Skill | Purpose |
|---|---|
| `golars sql QUERY [FILE...]` | Run SQL against one or more files |
| `golars convert SRC DST` | Transcode between csv/tsv/parquet/arrow/json/ndjson |
| `golars cat FILE [FILE...]` | Vstack multiple files with matching schemas |
| `golars diff [--key COL] A B` | Row-level diff between two data files |
| `golars browse FILE` | Interactive TUI table viewer |

### Scripting

| Skill | Purpose |
|---|---|
| `golars run SCRIPT.glr` | Execute a `.glr` pipeline end-to-end |
| `golars transpile SCRIPT.glr [-o OUT.go] [--package NAME]` | Emit a standalone Go program that reproduces the pipeline via the lazy API |
| `golars fmt [-w] FILE.glr` | Canonicalize a `.glr` script |
| `golars lint FILE.glr` | Report common mistakes without running |
| `golars explain SCRIPT.glr` | Print the lazy plan (indented form) |
| `golars explain --tree SCRIPT.glr` | Unicode box-drawing tree |
| `golars explain --graph SCRIPT.glr` | Styled box-drawing tree with lipgloss colouring |
| `golars explain --mermaid SCRIPT.glr` | Mermaid flowchart source (pipe into `mmdc` / `mermaid-ascii`) |
| `golars explain --profile SCRIPT.glr` | Per-node wall-time timings |
| `golars explain --trace PATH.json SCRIPT.glr` | chrome-trace JSON for chrome://tracing |

### REPL

| Skill | Purpose |
|---|---|
| `golars` (no args) | Start the interactive REPL |
| `golars repl` | Alias for the above |
| `golars completion SHELL` | Generate a shell completion script |

## MCP tools

`golars-mcp` exposes the read-only subset of the CLI over the Model
Context Protocol so a host LLM can ask the server to inspect data
files on the user's machine. Every tool returns both a text fallback
and a structured-content payload.

| Tool | Purpose |
|---|---|
| `schema` | Column names + dtypes for a file |
| `head` | First N rows as a structured table |
| `describe` | describe()-style summary stats |
| `sql` | Run a SQL query against one or more files |
| `row_count` | Cheap row + column count probe |
| `null_counts` | Per-column null counts |

See [`docs/mcp.md`](docs/mcp.md) for the tool schemas and the Claude
Desktop / Cursor / Windsurf install walkthrough.

## Library skills (Go API)

The sub-packages are designed to be embedded by other programs. Core
entry points:

- **`lazy.LazyFrame`** with `.Explain()`, `.ExplainTree()`, and
  `.ShowGraph()` for plan introspection. `lazy.MermaidGraph(node)`
  and `lazy.ExplainTree(node)` are the plan-tree renderers used by
  the CLI.
- **`expr` namespaces** — `Expr.Str()`, `Expr.List()`, `Expr.Struct()`
  mirror polars' `.str` / `.list` / `.struct` surfaces. `expr.C[T]`
  plus shorthand `expr.Int` / `Float` / `Str` / `Bool` give typed
  column handles that take bare Go literals.
- **`script.Runner`** accepts an arbitrary `Executor`, so a host can
  layer its own commands on top of the `.glr` grammar.
- **`repl` package** is stand-alone: it ships the interactive prompt
  primitives (history, ghost-text suggestions, suggester callback)
  without any golars coupling.
- **`browse` package** exposes the TUI grid as `browse.Run(df)` so
  downstream apps can reuse the viewer on their own DataFrames.

## `.glr` REPL-only commands

The REPL / `.glr` runner adds commands that don't have a standalone
subcommand equivalent. Highlights:

- `.tree`, `.graph`, `.mermaid` — three views of the current lazy plan
- `.unnest COL`, `.explode COL`, `.upsample COL EVERY` — reshape
- `.sum_all`, `.mean_all`, `.min_all`, `.max_all`, `.std_all`,
  `.var_all`, `.median_all` — frame-wide scalar aggregates
- `.source PATH.glr` — run another script inline
- `.ishow` — open the current pipeline in the browse TUI and return
  to the REPL when the viewer quits

Run `.help` inside the REPL for the complete list.
