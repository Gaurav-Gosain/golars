# golars design docs

This folder holds the architecture and design documentation for golars, a pure-Go DataFrame library modeled on [polars](https://github.com/pola-rs/polars).

Most of these docs are for contributors. User-facing documentation lives at the repository root, on pkg.go.dev, and in the docs site under `docs-site/`. The cookbook, scripting, MCP, Jupyter and API surface pages below are user-facing too.

## Index

1. [Architecture overview](architecture.md). Layered component map and how data flows through the system.
2. [Roadmap](roadmap.md). Phased delivery plan: what has shipped (eager, lazy, streaming MVP, temporal, nested and categorical dtypes), what is pending, and open design threads.
3. [Parallelism model](parallelism.md). How golars uses goroutines and channels for morsel-driven execution.
4. [Memory model](memory-model.md). Buffer ownership, reference counting, GC discipline.
5. [API design](api-design.md). Naming conventions and how polars idioms map to Go.
6. [API surface](api-surface.md). Cross-reference of polars functions to their golars equivalents.
7. [Cookbook](cookbook.md). End-to-end recipes for common tasks.
8. [Scripting](scripting.md). The `.glr` pipe language used by the `golars` REPL and `golars run`.
9. [MCP](mcp.md). The `golars-mcp` server that exposes golars as tools for an LLM host.
10. [Jupyter](jupyter.md). The `golars-kernel` Jupyter kernel and the `jupyter/render` package for Go notebooks.

## Non-goals

- A drop-in replacement for polars' Python or Rust API. We borrow the shape of the API, not the literal surface.
- Bindings to the Rust polars runtime. golars is an independent implementation.
- cgo in any transitive dependency. Cross-compilation must remain a single command.
