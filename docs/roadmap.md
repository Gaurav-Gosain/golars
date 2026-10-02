# Roadmap

Phased delivery plan. Dates are deliberately absent: a phase ships
when it is tested, benchmarked, and documented.

Live throughput ratios vs polars 1.39 live in
[`bench/polars-compare/`](../bench/polars-compare/). Numbers here
would drift; the bench harness is authoritative.

## Status

| Phase | State |
|---|---|
| 0. Foundations | done |
| 1. Eager DataFrame MVP | done |
| 2. Expression API + lazy + optimizer | done |
| 3. Streaming engine | MVP done; parallel file sources, hash exchange and spill pending |
| 4. SQL and temporal | temporal done; subset SQL shipped, joins and CTEs in SQL pending |
| 5. Polish and ecosystem | in progress |

Correctness gates checked on every change:

- `go test ./...`
- `go test -tags noasm ./compute/` (scalar fallback path)
- `GOEXPERIMENT=simd go test ./...`
- `go test -race ./compute/ ./eval/ ./series/ ./dataframe/ ./lazy/ ./sql/ ./cmd/golars-mcp/`
- `go vet ./...`
- Cross-compiles to linux/amd64, linux/arm64, darwin/arm64, windows/amd64
- `CGO_ENABLED=0 go build ./...`

`make test-all` runs `go test`, the SIMD build and the race gate.

## Phase 0: Foundations

Package scaffolding, arrow-go dependency, dtype model, Schema, Series
skeleton, DataFrame skeleton, test infrastructure.

Exit criteria:

- Round-trip every primitive dtype through Series without data loss
- Parity-test harness in place (`internal/framerepr` renders frames in
  a text form that a small polars helper also produces, so expected
  values are pasted from polars output)
- `go test -tags noasm ./compute/` green (scalar fallback path)

## Phase 1: Eager DataFrame MVP

Dtypes: bool, int8/16/32/64, uint8/16/32/64, float32/64, utf8, binary,
date, datetime (s, ms, us, ns, optional time zone), duration, time,
list, fixed-size array, struct, categorical, enum, null.

Kernels: arithmetic, comparison, logical, cast, fill_null, is_null,
is_not_null, is_in, unique, value_counts, n_unique, mode.

DataFrame operations: Select, Filter, WithColumns, Rename, Drop,
Sort, SortBy, Head, Tail, Slice, Sample, HStack, VStack, Concat,
Unique, WithRowIndex, ToDummies, Update, MergeSorted, plus one-row
frame aggregates (`SumAll`, `MeanAll`, `CountAll`, `NullCountAll`, ...).

Aggregations: Sum, Mean, Min, Max, Std, Var, Count, Median, Quantile,
First, Last, NUnique, and GroupBy convenience methods
(`g.Sum(ctx)`, `g.Mean(ctx)`, `g.Len(ctx, name)`, `g.Head(ctx, n)`, ...).

Group-by: hash group-by over dictionary-encoded keys, with
partition-parallel aggregation above a row cutoff. Primitive, string
and categorical keys, multiple keys, multiple aggregates.

Joins: inner, left, right, full, semi, anti and cross, on one or
more keys (hash join with parallel build and probe), with polars'
suffix, coalesce, nulls_equal, validate and maintain_order options;
asof (`JoinAsof`, with `By` groups, tolerance and
backward/forward/nearest strategies) and inequality joins
(`JoinWhere`).

IO: CSV (native parallel reader that parses record-aligned chunks
straight into arrow buffers; arrow-go handles writing and a few reader
options), Parquet (arrow-go's pqarrow, with row groups decoded in
parallel), Arrow IPC file and stream, JSON and NDJSON. Every format
has a read, a write and a lazy scan; CSV, Parquet, IPC and NDJSON also
have lazy sinks.

Parallelism: kernels split work across goroutines above per-kernel row
cutoffs. See [parallelism.md](parallelism.md).

## Phase 2: Expression API and lazy execution

Expression AST: Col, Lit, Cast, Alias, BinaryOp, UnaryOp, FunctionNode,
Agg, When/Then/Otherwise, Over, Rolling. Function nodes cover the
`str`, `dt`, `list`, `arr`, `struct`, `name`, `bin` and `cat`
namespaces, binning (`Cut`, `QCut`), run-length encoding, `ReplaceStrict`,
`SortBy`, `TopKBy`, `Interpolate`, `MapBatches` / `MapElements`,
`Fold` / `Reduce`, and horizontal reductions.

Logical plan nodes: Scan, Projection, WithColumns, Filter, Aggregate,
Join (including asof and join_where), Sort, Slice, Rename, Drop,
Distinct, Cache, Explode, Unnest, Pivot, Unpivot, TopK, dynamic and
rolling temporal group-bys.

Optimizer passes, in order:

1. Simplify (constant folding, boolean simplification, algebraic
   identities), run to a fixed point
2. Type coercion (placeholder: the evaluator promotes types at run time)
3. CSE (placeholder: no rewrites yet)
4. Slice pushdown
5. Predicate pushdown (into scans, including Parquet)
6. Projection pushdown

Pushdowns stop where moving a filter or slice would change results:
below expressions that read other rows (cumulative sums, shifts,
aggregates broadcast to every row) and, for a filter, below a slice.

Pending: real type coercion and CSE passes; join reordering with
cardinality stats.

Executor: pull-based eager evaluation, plus the streaming executor.
Both share kernels. `LazyFrame.ExplainString` and `ExplainTreeString`
expose the plan at each stage, and `lf.Profile` reports per-node
timings.

Typed-column facade (`expr.C[T]`, `expr.Int`, `expr.Float`, etc.)
lives alongside the untyped API and produces identical plans.

## Phase 3: Streaming engine

Morsel-driven parallel executor. See [parallelism.md](parallelism.md)
for the detailed design.

Features:

- Source, Stage, Sink operator kinds
- Morsels as bounded-row DataFrames
- Buffered channels between stages for back-pressure
- Order-preserving parallel map stage
- Hybrid execution: streaming-friendly prefix + pipeline breakers
- Cancellation via context

Pending:

- Streaming file sources (today a scan is read, then sliced into
  morsels)
- Hash exchange for partition-parallel group-by and join
- Spill-to-disk sort via external merge
- Out-of-core join
- Streaming sinks (`Sink*` currently materialise the result first)

## Phase 4: SQL and temporal

Temporal shipped. The shared engine lives in `internal/temporal`
(calendar arithmetic, polars duration strings, time zones via
`time.LoadLocation`, strftime/strptime formats, window bounds,
business days) and backs:

- `Expr.Dt()`: calendar fields, `Truncate`, `Round`, `OffsetBy`,
  `MonthStart` / `MonthEnd`, `Strftime`, `Epoch`, `ReplaceTimeZone`,
  `ConvertTimeZone`, business-day helpers
- `Str().ToDate` / `ToDatetime` / `ToTime` / `Strptime`
- `DateRange`, `DatetimeRange`, `TimeRange`, and literal and
  constructor helpers (`LitDate`, `Datetime`, `Duration`, ...)
- `GroupByDynamic`, `Rolling`, and `RollingSumBy` / `RollingMeanBy` /
  ... on eager and lazy frames
- `JoinAsof` on temporal keys with duration tolerances
- CSV date and datetime inference (`WithTryParseDates`)

Subset SQL shipped: SELECT (DISTINCT), FROM, WHERE, GROUP BY,
ORDER BY, LIMIT. See [scripting.md](scripting.md) for the CLI and
[mcp.md](mcp.md) for the MCP `sql` tool.

Pending:

- JOIN, HAVING, CTE, window functions, subqueries in SQL
- `df.Upsample` with calendar units (`mo`, `q`, `y`); scalar
  intervals (`ns` to `w`) work

## Phase 5: Polish and ecosystem

Shipped:

- Nested dtypes: expression-level `list.*`, `arr.*` and `struct.*`
  namespaces, `List().Eval` with `Element()`, lazy `Explode` and
  `Unnest`
- Categorical and Enum dtypes with casts, comparisons, sorting and
  group-by keys
- Notebook integration: `DataFrame.MimeBundle` / `HTML` and the same on
  `Series`, backed by `internal/reprtable`; structured tables in
  `golars kernel-host` replies. A terminal notebook front end, the
  `golars` branch of
  [github.com/Gaurav-Gosain/gopyter](https://github.com/Gaurav-Gosain/gopyter/tree/golars),
  runs glr and Go cells with shared frames. See
  [jupyter.md](jupyter.md).

Pending:

- Object-store scans (S3, GCS, Azure)
- Delta Lake and Iceberg read paths
- Excel/Avro adapters
- Expanded SIMD coverage beyond the current AVX2/NEON kernel set
- `database/sql` write path (read path exists)

## Deferred indefinitely

- GPU execution. Out of scope for a pure-Go project.
- Python bindings. If demand appears, a separate repo can wrap golars
  over gRPC or CFFI.
- Query languages other than SQL (Substrait consumer). Revisit after
  the SQL surface stabilises.

## Open design threads

Standalone decisions; pick, sequence, or defer independently.

### A. Memory model

Every `Series` / `DataFrame` currently requires `defer x.Release()`.
Correct, zero-leak, but heavy for REPL-style use. Three viable
directions:

1. Finalizer-backed auto-release via `runtime.AddCleanup`. No API
   change. Releases happen on GC, which leaks memory under allocation
   pressure.
2. Arena / session scope:
   `session := golars.NewSession(); defer session.Close()`. Every Series/DataFrame allocated through the
   session releases on close. Ports cleanly to REPL (one session per
   statement) and scripts (one session per `.glr` file).
3. Builder-to-owned-handle: remove `Release` from the public surface;
   wrap the refcount behind a handle only ops can decrement. Biggest
   change; cleanest resulting surface.

Current thinking: combine 2 with 1 as a safety net.

### B. Dtype gaps vs polars

Shipped: bool, int8/16/32/64, uint8/16/32/64, float32/64, date,
datetime (s/ms/us/ns, optional tz), duration, time, utf8, binary,
list, fixed-size array, struct, categorical, enum, null.

Gaps: Float16, Decimal, Int128, Object, Unknown.

Tractable next steps, ranked by effort:

- `series.FromDecimal128`: arrow-go has `Decimal128`; needs dtype
  constructor + format code + From builder. Hours.
- Int128, Float16: low value for DataFrame workloads. Defer.
- Object, Unknown: polars-internal escape hatches. Not needed.

### C. I/O gaps

Shipping: CSV, JSON, NDJSON, Parquet, Arrow IPC/Feather, SQL
(read path), clipboard.

Gap inventory, ranked by usefulness:

- Avro (container + object-container). `github.com/linkedin/goavro`
  is pure-Go. Medium effort.
- Delta Lake. Big. `delta-go` covers readers.
- Iceberg. Pure-Go `iceberg-go` exists.
- Excel / ODS. `xuri/excelize` handles xlsx.
- PyArrow Datasets (partitioned Parquet directories): glob + concat
  over the existing Parquet reader.

Each of these would add a dependency, which needs sign-off first.

### D. Distributed compute

Single-node streaming is Phase 3: done for in-memory sources,
incomplete for file sources and for group-by/join parallel exchange.
Distributed compute is not scoped. Two plausible shapes:

1. Shuffle-based. Split `Scan -> Shuffle -> Aggregate` across workers;
   Arrow IPC over TCP for shuffle payloads. Each worker runs the
   existing streaming engine on its partition.
2. Plan-serialization. `lazy.Node` + `expr.Expr` are exported structs
   already; encode to gob or protobuf and ship to worker pools. No
   new runtime, just a shipping protocol. Function nodes that carry Go
   callbacks (`MapBatches`, `Fold`) cannot be serialized.

Neither is blocking for 1.0. Worth a dedicated RFC.

### E. Behavioural parity

Parity tests live in `*_parity_test.go` files next to the code and
cover filters, sorts, casts, aggregations, group-by (including the
GroupBy convenience methods), joins (hash, asof, join_where), frame
methods (explode, pivot, unpivot and friends), the core expression
methods, the temporal kernels, and the list and array namespaces. The generated expression suite is driven by
`eval/testdata/core_parity_gen.py` and the temporal suite by
`bench/polars-compare/temporal_parity.py`.

Remaining gaps are tracked per method in
[api-surface.md](api-surface.md).
