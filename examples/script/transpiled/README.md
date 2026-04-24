# transpiled/

Each subdirectory holds a Go program produced by
`golars transpile examples/script/<name>.glr`. The `.glr` scripts in
the parent directory are the source of truth; the transpiled output
is regenerated to demonstrate the `.glr` to Go lowering rather than
hand-maintained.

Regenerate every example in one go:

```sh
for f in examples/script/*.glr; do
  name=$(basename "$f" .glr)
  mkdir -p examples/script/transpiled/$name
  golars transpile "$f" -o examples/script/transpiled/$name/main.go --package main
done
```

Run any single one:

```sh
go run ./examples/script/transpiled/pipeline
```

Or build everything in one shot:

```sh
go build ./examples/script/transpiled/...
```

## Coverage

All ten bundled scripts round-trip end-to-end:

| Script | What it shows |
|--------|---------------|
| `agg/` | `groupby` + multi-agg spec |
| `branching/` | `stash` / `use` frame switching, sort reversal |
| `demo/` | basic load + filter + show |
| `derived/` | `with NAME = EXPR` arithmetic + string methods |
| `join/` | `load ... as NAME` + `join NAME on KEY` |
| `multisource/` | multiple stashed frames with parked state |
| `nulls/` | `fill_null` + `is_not_null` filter on a CSV with empty fields |
| `pipeline/` | Full load → with → filter → groupby → sort → show chain |
| `regex/` | `filter LIKE`, `contains`, `str.contains_regex` |
| `rolling/` | rolling + EWM windows |

## What the transpiler does NOT cover

* `.tree`, `.graph`, `.mermaid`, `.explain_tree` leave a `TODO(glr):`
  comment in the output because the tree/graph views are REPL
  affordances with no standalone analogue.
* `.reset` is a REPL bookkeeping op; transpiled scripts rebuild the
  focus var from scratch at each `load`.
* `.frames` is informational and intentionally ignored.

Any script using commands outside the supported set still transpiles
to syntactically valid Go; unsupported lines become comments so the
generated file compiles cleanly.
