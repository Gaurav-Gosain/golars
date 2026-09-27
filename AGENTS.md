# AGENTS.md

Project-wide guide for coding agents working on **golars**, a pure-Go
DataFrame library that mirrors polars on top of arrow-go. This file is
the universal agent brief; `CLAUDE.md` adds Claude Code-specific hints
and `SKILLS.md` documents the invocable skills.

## What this codebase is

- **golars**: pure-Go port of polars, built directly on arrow-go. No
  cgo. Single `go build` cross-compiles.
- About 45 Go packages (outside `examples/`). Tests live alongside
  source (`*_test.go`).
- Public API surface is the sub-packages + the root `golars` package
  which re-exports the most common helpers.
- Target: match or beat polars 1.39 on the bench suite at
  `bench/polars-compare`.

## Module layout

```
series/       Named chunked arrow-backed column type
dataframe/    Table of Series with immutable updates
schema/       Arrow schema wrapper
dtype/        Logical dtype descriptors
compute/      Kernel layer (arithmetic, sort, compare, cast, filter, take)
expr/         Expression AST (Col, Lit, BinaryOp, Alias, FunctionNode...)
eval/         Expression evaluator - turns expr.Expr into a *series.Series
lazy/         LazyFrame + logical plan nodes + optimizer + executor
stream/       Morsel-driven streaming engine
selector/     Column-set predicates (Numeric, StartsWith, Matching...)
io/csv        io/parquet  io/ipc  io/json  io/sql  io/clipboard
sql/          Subset SQL parser + compiler to lazy plan
script/       .glr scripting language runner + command specs
repl/         Reusable terminal REPL primitives (prompt, history, ghost)
browse/       Bubble Tea TUI DataFrame viewer
cmd/golars/   The CLI: REPL + subcommands (run, fmt, lint, sql, ...)
cmd/golars-lsp/ .glr Language Server (stdio JSON-RPC)
cmd/golars-mcp/ Model Context Protocol server (stdio JSON-RPC)
cmd/bench/    Perf harness; compare.py sits under bench/polars-compare
editors/      tree-sitter grammar, VS Code extension
docs/         Markdown documentation (architecture, cookbook, scripting, ...)
docs-site/    Fumadocs-based website
```

## Coding style

- No em-dashes. No emojis in code or docs (except `.glr` REPL where
  the user explicitly adds them).
- No AI slop / robotic phrasing. Prefer plain, direct sentences.
- Tests live next to code: `foo.go` + `foo_test.go`. Use
  `testutil.CheckedAllocator` for any test that touches arrow
  buffers - leaks fail the suite.
- Go ≥ 1.23. Use `for i := range n` over `for i := 0; i < n; i++`.
  Use `slices.Contains` over hand-rolled loops.
- Comments: explain the _why_ and the non-obvious. No per-line
  commentary of what code does.
- Public types, methods and functions get doc comments. Internal
  helpers often don't.
- Panic-free public API. Internal invariants go through
  `internal/assert`.

## Performance discipline

- Bench anything that touches a hot kernel before and after. The
  bench harness lives at `bench/polars-compare/compare.py`.
  Add new workloads in `cmd/bench/main.go` _and_
  `bench/polars-compare/bench.py` together so polars parity is
  measured in the same run.
- Hot kernels that fully overwrite their output go through
  `compute.poolingMem(...)` so the allocator sync.Pool kicks in.
- SIMD kernels live under `//go:build goexperiment.simd && amd64`
  guards. A scalar fallback in `compute/simd_fallback.go` must
  compile on every platform.
- Per-row allocations inside hot loops are banned. Use
  `series.BuildInt64Direct` / `BuildFloat64DirectFused` and friends
  to pre-size output buffers.

## Correctness gates

Before declaring work done:

```
go test ./...                             # every package must pass
GOEXPERIMENT=simd go test ./...           # SIMD build path too
go test -race ./compute/ ./eval/ ./series/ ./dataframe/ ./lazy/
go vet ./...
```

Optional but good: `staticcheck ./...` - should be clean except the
pre-existing SA6002 sync.Pool findings.

## Common tasks

- **Add an expression function**: implement on `*series.Series` first
  (plus test), then expose a fluent method on `expr.Expr` as a
  `FunctionNode`, then wire a case arm in `eval/eval_func.go`, then
  add a fluent top-level helper in `golars.go` if appropriate. The
  glr expression language (`script/exprparse`) finds methods on
  `expr.Expr` and its namespaces by reflection, so a new method is
  callable from `.glr` as `x.snake_name(...)` with no parser change.
  Add an entry to `overrides` in `script/exprparse/funcs.go` only for
  polars-style default arguments, aliases, or a signature that needs a
  custom builder, and to `freeFuncs` for a new top-level function.
- **Add a scripting command**: add a `CommandSpec` in
  `script/spec.go` (aliases go in its `Aliases` field), register a
  handler in the `handlers` table in `cmd/golars/dispatch.go` and
  implement it in the matching `cmd/golars/cmds_<category>.go`. The
  REPL help and completion, the LSP and the Jupyter kernel read the
  spec directly. Then run `go test ./script/ ./cmd/golars/`: the drift
  tests name every other place that still needs the command
  (`editors/tree-sitter-golars/grammar.js` plus `tree-sitter generate
  --abi 14`, the VS Code, Vim and JupyterLab keyword lists, and the
  statement reference in `docs/scripting.md`).
- **Add a polars-compat kernel**: check behaviour in
  `polars/crates/...` (the upstream polars source is vendored there
  for reference), reproduce semantics in Go, add parity tests.

## When in doubt

- Look at how an adjacent kernel or method is structured - the code
  style is consistent. Copy its patterns.
- Polars is the reference implementation. When behaviour is
  ambiguous, match polars 1.39.
- Run the REPL: `go run ./cmd/golars` - every feature is exercised
  there.
