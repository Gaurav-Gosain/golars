# golars examples

Runnable programs that demonstrate golars usage. Each subdirectory is a
`main` package: run with `go run ./examples/<name>`.

A subset of examples ship a second `main` under `generic/`. That
variant uses the typed `expr.C[T]` / `expr.Int` / `expr.Float` /
`expr.Str` / `expr.Bool` facade introduced in `expr/typed.go` to
eliminate `expr.Lit(int64(X))` boilerplate. The logical plan is
identical; only the caller syntax differs. Run with
`go run ./examples/<name>/generic`.

## Core API

| Directory | What it shows |
|-----------|---------------|
| `basic` | Build a DataFrame from slices; basic operations (head, filter, sort). |
| `describe` | Summary statistics via `DataFrame.Describe(ctx)`. |
| `expressions` | Column expressions (Col, literal, arithmetic) and select/with_columns. |
| `groupby` | GroupBy with sum/mean/count aggregations. |
| `join` | Inner and left joins across two DataFrames. |
| `join_asof` | `df.JoinAsof` of trades against quotes by `symbol`: backward with a `2m` tolerance, forward, and nearest through the lazy `JoinAsof`. |
| `lazy` | The LazyFrame pipeline (filter → groupby → sort) with collect. |
| `streaming` | Streaming execution over large inputs via the morsel engine. |

## Reshape / aggregation

| Directory | What it shows |
|-----------|---------------|
| `horizontal` | `SumHorizontal`, `MeanHorizontal`, `SumAll` - row-wise + frame-scalar aggregates. |
| `pivot` | `df.Pivot(ctx, index, on, values, dataframe.PivotSum)` - long-to-wide reshape. |
| `transpose_unpivot` | `df.Transpose`, `df.Unpivot` (melt). |
| `topk_pipe` | `df.TopK` / `df.BottomK`, `df.Pipe` method-chaining helper. |
| `over_window` | `Expr.Over(keys...)` - window functions broadcast back to rows. |
| `rolling` | `Expr.RollingMean/Std/Min/Max(window, minPeriods)` on a price series. |

## Temporal

| Directory | What it shows |
|-----------|---------------|
| `temporal` | Parse strings with `Str().ToDatetime`, read fields with `Dt().Year/Month/Weekday/Hour`, `Dt().Truncate`, `Dt().Strftime`, time zones with `ReplaceTimeZone` / `ConvertTimeZone`, hourly buckets with `GroupByDynamic`, a 2h `RollingMeanBy`, and `DateRangeSeries` with `Dt().MonthEnd` / `OffsetBy`. |

## Control flow / expressions

| Directory | What it shows |
|-----------|---------------|
| `when_then` | `When(pred).Then(a).Otherwise(b)` with dtype promotion. |
| `coalesce_concat` | `Coalesce`, `ConcatStr`, `IntRange` - polars-style constructors. |
| `fill_strategies` | `ForwardFill`, `BackwardFill`, `FillNan` on a nullable time series. |
| `stats` | `Skew`, `Kurtosis`, `PearsonCorr`, `Covariance`, `ApproxNUnique`, `df.Corr`. |
| `regex_strings` | `Str().Extract`, `ContainsRegex`, `SplitExactNullShort`. |
| `namespaces` | Expression namespaces: list (`Len`, `Contains`, `Unique`, `Sum`, `Get`, `Sort`, `Eval` with `Element()`), struct (`Struct`, `Field`, `RenameFields`, `Unnest`), name (`Prefix`, `Suffix`, `Map`), str (`Extract`, `SplitN`, `ToTitlecase`, `PadStart`) and cat (`Cast(dtype.Categorical())`, `GetCategories`, `StartsWith`). |

## I/O

| Directory | What it shows |
|-----------|---------------|
| `csv` | Read and write CSV files. |
| `csv_url` | Fetch a CSV from an http(s) URL. |
| `parquet` | Read and write Parquet files. |
| `json` | Array-of-objects JSON read/write. |
| `ndjson` | Newline-delimited JSON read/write. |
| `arrow_interop` | DataFrame ↔ `arrow.RecordBatch` / `arrow.Table` round trip. |
| `ipc_streaming` | Arrow IPC stream: write multiple batches, read them back. |
| `scan_pushdown` | `iocsv.Scan()` with predicate + projection pushdown through to the reader. |

## Querying + tooling

| Directory | What it shows |
|-----------|---------------|
| `sql` | **Nested module.** Read a query result into a DataFrame via `modernc.org/sqlite` (pure-Go). Run with `cd examples/sql && go run .`. |
| `sql_session` | `sql.Session` Register + Query to run SQL directly against in-memory DataFrames. |
| `script` | `.glr` script samples plus a host program driving `script.Runner`. `expressions.glr` tours the expression language (namespaces, `cut`, `when`), `join_asof` and `group_by_dynamic`. `script/transpiled/` has the same scripts lowered to Go via `golars transpile`. |
| `profiler` | `lazy.NewProfiler()` + `lazy.WithProfiler(p)` for per-node timings. |

## Notes

Most examples create in-memory DataFrames so no external files are
needed. The `csv_url` example serves its CSV from a local
`httptest` server, so it needs no network access. `scan_pushdown`
seeds a tiny CSV in `os.TempDir()`.
