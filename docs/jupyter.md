# Jupyter integration

golars ships two ways into the notebook ecosystem:

1. **`golars-kernel`** — a native Jupyter kernel for the `.glr`
   scripting language. Each cell is a glr pipeline, the kernel runs
   it, and frames render as HTML tables. No Go knowledge needed.

2. **`jupyter/render` package** — HTML/markdown/text renderers for
   `*dataframe.DataFrame`. Drop into [GoNB](https://github.com/janpfeifer/gonb)
   so DataFrames show up as proper tables in a Go notebook.

## golars-kernel: a kernel for .glr

### Install

After `golars` and `golars-kernel` are on your PATH (release tarball,
`go install`, AUR `golars-bin`):

```sh
golars-kernel install
# installed golars kernel
#   binary: /home/you/.local/bin/golars-kernel
#   spec:   /home/you/.local/share/jupyter/kernels/golars/kernel.json
```

Confirm Jupyter sees it:

```sh
jupyter kernelspec list
# golars  /home/you/.local/share/jupyter/kernels/golars
```

Open JupyterLab, pick **golars (.glr)** from the launcher, and start
typing:

```
load examples/script/data/orders.csv
filter discount is_not_null
groupby region revenue:sum:total
sort total desc
show
```

### What you get

- HTML tables for every materialised frame (`show`, `head`, implicit
  end-of-cell display).
- Persistent state across cells: `load` in cell 1, `filter` in cell 2,
  `groupby` in cell 3 — same model as the REPL.
- `stash NAME` / `use NAME` for branching pipelines.
- Tab completion on command names.
- Hover docs on commands (Shift+Tab in classic notebook, hover panel
  in JupyterLab).
- `interrupt` button kills the in-flight cell by restarting the
  embedded host process.

### How it works

The kernel binary (`cmd/golars-kernel`) speaks the Jupyter v5.3 wire
protocol over ZeroMQ (pure-Go, no `libzmq`). Cell execution is
delegated to a long-lived `golars kernel-host` subprocess via a tiny
NDJSON protocol on stdin/stdout, which means the kernel uses the same
dispatcher (`state.handle`) as the interactive REPL — no second
implementation to drift.

```
JupyterLab ──── ZMQ(5) ───► golars-kernel ──── NDJSON ───► golars kernel-host
                                                              (stateful REPL)
```

The host's `os.Stdout` and `os.Stderr` are piped per cell, so any
table the dispatcher prints lands in the cell output as a `stream`
message. Materialised frames also land as a `display_data` message
with both `text/plain` (ASCII box) and `text/html` (styled table)
mimetypes; Jupyter picks the richest one.

### Install flags

```sh
# default - per-user
golars-kernel install

# venv / conda prefix
golars-kernel install --prefix "$VIRTUAL_ENV"

# rename / customise
golars-kernel install --name golars-py-py --display-name "golars (alt)"
```

### Locating the host binary

`golars-kernel` finds the `golars` binary, in order:

1. `$GOLARS_BIN` if set
2. Sibling of the kernel binary (so release tarballs Just Work)
3. `$PATH`

Set `$GOLARS_BIN` if you want the kernel to use a specific build.

## GoNB: golars in a Go notebook

[GoNB](https://github.com/janpfeifer/gonb) is the reference Go
kernel: each cell is real Go code, recompiled incrementally. golars
provides a multi-mimetype renderer so DataFrames render as proper
HTML tables.

```go
import (
    "github.com/Gaurav-Gosain/golars"
    jrender "github.com/Gaurav-Gosain/golars/jupyter/render"
    "github.com/janpfeifer/gonb/gonbui"
)

df, err := golars.ReadCSV("orders.csv")
if err != nil { panic(err) }
defer df.Release()

gonbui.DisplayHTML(jrender.HTML(df))
```

The `jupyter/render` package exposes:

| Function | Returns |
|----------|---------|
| `HTML(df)` | self-contained HTML fragment with inline styles |
| `Markdown(df)` | GFM pipe-table |
| `Text(df)` | the same ASCII repr `fmt.Println(df)` produces |
| `MimeBundle(df)` | `map[mimetype]string` with all three |

`MimeBundle` is the right call when the consumer takes a multi-format
dict — pass it to `gonbui.DisplayMIMEData` for Jupyter's "richest
available" routing.

### Limits

```go
jrender.HTMLWith(df, jrender.Limits{
    MaxRows:     50,  // -1 to disable
    MaxCols:     -1,
    MaxCellRune: 200,
})
```

Defaults: head 5 + tail 5 rows, 8 columns, 64 runes per cell. Matches
polars-py's default `repr` ergonomics.

## Roadmap

- [ ] Display the *focused* frame (post-pipeline), not just the
      original source. Today the auto-display HTML reflects `s.df`
      which is the loaded source; pipeline ops only mutate `s.lf`.
- [ ] Forward `kernel-host` stdout/stderr line-by-line to iopub during
      a long-running cell instead of buffering until the cell completes.
- [ ] Column-name completion: today only command names are suggested.
      Need an extra `complete` opcode in the NDJSON protocol so the
      kernel can ask the host for live schema names.
- [ ] Inline plotting: `golars` doesn't render charts yet, but once
      it does (e.g. via gg, vegolite, or a wasm hook) the kernel will
      route them as `image/png` or `application/vnd.vega.v5+json`.
