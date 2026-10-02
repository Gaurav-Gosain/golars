# MCP: golars as a tool for your LLM host

`golars-mcp` is a Model Context Protocol server that exposes a
read-only subset of golars as tools an LLM host can invoke. Works
with Claude Desktop, Cursor, Windsurf, and any other MCP-aware
client.

## Install

```sh
go install github.com/Gaurav-Gosain/golars/cmd/golars-mcp@latest
```

The binary lives in `$GOBIN` (or `$HOME/go/bin` by default).

## Configure Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(or `%APPDATA%\Claude\claude_desktop_config.json` on Windows; the
Linux equivalent lives under `~/.config/Claude/`) and add a
`mcpServers` entry:

```json
{
  "mcpServers": {
    "golars": {
      "command": "/absolute/path/to/golars-mcp"
    }
  }
}
```

Restart the Claude Desktop app. You should see a hammer icon in the
conversation pane letting you enable the `golars` server.

## Configure Cursor / Windsurf

Both editors read the same JSON format. Add the same snippet to
your workspace MCP config (`~/.cursor/mcp.json` for Cursor). No
restart needed in Cursor; the server picks up automatically.

## Available tools

| Tool | What it does |
|---|---|
| `schema` | Return column names + dtypes for a data file |
| `head` | Return the first N rows (CSV/Parquet/Arrow/JSON/NDJSON) |
| `describe` | Return describe()-style summary stats |
| `sql` | Run a SQL query against one or more files |
| `row_count` | Cheap "how many rows × cols" probe |
| `null_counts` | Per-column null counts |
| `run_glr` | Run a glr script and return its output and the resulting table |
| `check_glr` | Check a glr script without running it; findings plus the schema after each statement |
| `format_glr` | Canonical form of a glr script (`golars fmt`) |
| `glr_reference` | Docs for a glr command or expression function, or the functions of a namespace |

Every tool returns _both_ a plain-text fallback (for hosts that only
render text) and a `structuredContent` payload. The table-shaped
tools (`head`, `describe`, `sql`) put `columns` + `rows` arrays in
it so richer UIs can render a table. The others use their own shape:
`schema` returns `path`, `rows` and a `columns` list of
`{name, dtype}`; `row_count` returns `rows` and `columns` as counts;
`null_counts` returns `counts` (a list of `{column, nulls}`) and
`total_rows`.

File arguments (`path`, and each entry of `files` for `sql`) should
be absolute paths. For `sql`, each file is registered as a table
named after its filename without the extension.

The glr tools take the script as text. Relative paths in it resolve
against `dir` (default: the server's working directory).

- `run_glr` checks the script first; static problems come back as an
  error result with line, column, hint and suggested fix, and nothing
  runs. Otherwise it runs the script through `golars kernel-host`
  (found through `$GOLARS_BIN`, next to `golars-mcp`, or on `$PATH`)
  and returns `output` (what the script printed) and `table`
  (`columns`, `dtypes` and the first `max_rows` rows of the focused
  frame, default 50, every column), plus `truncated` when there may be
  more rows. The text fallback shows the table as CSV.
- `check_glr` returns `ok`, `findings` (each with `line`, `column`,
  `severity`, `code`, `message`, `hint`, `fix`) and `steps`: the shape
  and `name: dtype` columns after every shape-changing statement,
  tracked from the schemas of the files the script loads.
- `glr_reference` with `name` documents a command (`groupby`) or a
  function (`round`, `str.to_date`), suggesting the closest name on a
  typo; with `namespace` (`str`, `dt`, `list`, `arr`, `struct`, `name`,
  `bin`, `cat` or `free`) it lists the functions and their signatures;
  with neither it lists every command.

## Example session

After configuring the server, ask your LLM host something like:

> "What's the schema of `~/data/trades.csv`? If any column has more
> than 10% nulls, summarise it with describe."

The host picks up the tool catalogue from `tools/list`, calls
`schema` then `null_counts` then `describe`, and the model answers
using the structured results.

## Protocol notes

- Protocol version: `2025-06-18`. We only implement the tools
  capability; resources and prompts are not served (yet).
- Transport: stdio JSON-RPC 2.0, one object per line.
- No authentication: `golars-mcp` reads files the user running the
  host process has read access to.

## Security

The MCP server is **read-only**. It cannot write files, start
subprocesses, or reach the network. The tools only accept a path
string and execute a query against its contents; SQL is compiled to
a lazy plan with a whitelist of operators (no arbitrary expressions
or DDL). That said, it _will_ read any file the caller names, so
don't point a host LLM at secrets.

## Extending

`cmd/golars-mcp/tools.go` registers tools into a flat slice. Add a
new `Tool{Name, Description, InputSchema, Run}` entry and the
server picks it up. Keep tools pure (no state outside the local
session) so concurrent calls are safe.
