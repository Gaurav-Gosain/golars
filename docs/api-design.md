# API design

golars borrows the shape of polars' API. The surface is polars-style expressions with Go naming conventions. This document records the conventions so the API stays consistent as it grows.

## Naming

- Exported identifiers are `PascalCase`. Unexported are `camelCase`.
- Acronyms are capitalized consistently: `CSV`, `JSON`, `URL`, `ID`.
- Methods prefer verbs: `Select`, `Filter`, `Join`, `Sort`. Polars uses snake_case verbs; Go uses PascalCase verbs. Direct translation.
- Boolean predicates are prefixed `Is`: `IsNull`, `IsNotNull`, `IsUnique`, `IsIn`.
- Getters do not use `Get`: `df.Schema()`, `s.DType()`, `s.Len()`. This matches Go convention.

## Expressions

Polars in Python and Rust overloads operators. Go does not. We use method chaining:

| Polars (Python)                       | golars                                      |
|---------------------------------------|---------------------------------------------|
| `pl.col("a") + pl.col("b")`           | `expr.Col("a").Add(expr.Col("b"))`          |
| `pl.col("a") * 2`                     | `expr.Col("a").MulLit(2)`                   |
| `pl.col("a") > 3`                     | `expr.Col("a").GtLit(3)`                    |
| `pl.col("a").is_null()`               | `expr.Col("a").IsNull()`                    |
| `pl.col("a").alias("b")`              | `expr.Col("a").Alias("b")`                  |
| `pl.when(c).then(a).otherwise(b)`     | `expr.When(c).Then(a).Otherwise(b)`         |
| `pl.col("a").sum().over("g")`         | `expr.Col("a").Sum().Over("g")`             |

Arithmetic and comparisons with a Go literal use a `Lit` suffix (`MulLit`, `GtLit`), which wraps the value in `expr.Lit`. Arithmetic between two expressions is the plain verb. The `Lit` variants accept `any`, so a literal of the wrong type is only caught at evaluation; the typed facade (`expr.Int("a").Gt(3)`, `expr.Float`, `expr.Str`, `expr.C[T]`) checks literal types at compile time and builds the same plan.

## Namespaces

Polars groups dtype-specific methods under accessors (`.str`, `.dt`, `.list`). golars mirrors them as methods that return a small namespace value:

| Polars (Python)                          | golars                                            |
|------------------------------------------|---------------------------------------------------|
| `pl.col("s").str.to_uppercase()`         | `expr.Col("s").Str().ToUpper()`                   |
| `pl.col("ts").dt.year()`                 | `expr.Col("ts").Dt().Year()`                      |
| `pl.col("l").list.len()`                 | `expr.Col("l").List().Len()`                      |
| `pl.col("a").arr.sum()`                  | `expr.Col("a").Arr().Sum()`                       |
| `pl.col("p").struct.field("x")`          | `expr.Col("p").Struct().Field("x")`               |
| `pl.col("x").name.suffix("_raw")`        | `expr.Col("x").Name().Suffix("_raw")`             |
| `pl.col("b").bin.encode("hex")`          | `expr.Col("b").Bin().Encode("hex")`               |
| `pl.col("c").cat.get_categories()`       | `expr.Col("c").Cat().GetCategories()`             |

Every namespace method returns an `expr.Expr`, so chains cross namespaces freely: `expr.Col("csv").Str().Split(",").List().Len()`. `series.Series` has the same accessors for eager use (`s.Str().Upper()`, `s.List()`, ...).

## Option structs

Operations that mirror a polars call with many keyword arguments take an options struct instead of functional options: `dataframe.AsofOptions`, `dataframe.DynamicGroupOptions`, `dataframe.RollingGroupOptions`, `expr.CutOptions`, `expr.SortByOptions`, `expr.ReplaceStrictOptions`. Field names follow the polars argument names, and the zero value of every field means the polars default. Where the polars default is `true`, the field is phrased negatively so the zero value still matches (`AsofOptions.DisallowExactMatches` for `allow_exact_matches=True`, `NoCoalesce` for `coalesce=True`). Empty strings select defaults for string-valued fields such as `Closed` and `Label`.

## Scalar kernels (`compute.*Lit`)

The `compute` package provides imperative kernels that work directly on `*series.Series`. The `*Lit` comparison variants compare against a scalar literal and skip the allocation a broadcast Series would require. Each returns `(*series.Series, error)`:

| Method                       | Behaviour                                          |
|-----------------------------|----------------------------------------------------|
| `compute.GtLit(ctx, s, 5)`   | mask where s > 5                                   |
| `compute.LtLit(ctx, s, 0)`   | mask where s < 0                                   |
| `compute.EqLit(ctx, s, 42)`  | mask where s == 42                                 |
| `compute.GeLit(ctx, s, 0.5)` | mask where s >= 0.5                                |

These accept any numeric Go literal type and coerce to the series dtype. Fast paths exist for int64 and float64; other dtypes fall back to a broadcast Series internally. Use them in hot loops where the expression compiler's overhead would dominate.

## Top-level re-exports

The root `golars` package re-exports the commonly used names so most user code imports a single package:

```go
import "github.com/Gaurav-Gosain/golars"

df, err := golars.ReadCSV("data.csv")
if err != nil {
    return err
}
defer df.Release()

out, err := golars.Lazy(df).
    Filter(golars.Col("x").GtLit(0)).
    GroupBy("k").
    Agg(golars.Col("v").Sum().Alias("v_sum")).
    Collect(ctx)
```

The root shortcuts (`ReadCSV`, `ReadParquet`, ...) use `context.Background()`. For a cancellable read, call the format package directly, for example `iocsv.ReadFile(ctx, path)`.

Deeper packages (`dataframe`, `series`, `expr`, `lazy`, `compute`, `io/...`) are still importable for users who need them.

## Errors

- Every IO-bound or parse-bound operation returns `(T, error)`. No panics on user input.
- Operations that cannot fail given valid inputs return `T` (for example `df.Head(n)`, `df.Drop(...)`, `s.Len()`). Operations that can hit invalid inputs (wrong dtype, nonexistent column) return a descriptive error prefixed with the package name, for example `dataframe: column not found`.
- Sentinel errors are defined per package for the common cases, and user code can `errors.Is` on them: `dataframe.ErrColumnNotFound`, `dataframe.ErrDuplicateColumn`, `dataframe.ErrHeightMismatch`, `schema.ErrColumnNotFound`, `compute.ErrDTypeMismatch`, `compute.ErrLengthMismatch`, `compute.ErrUnsupportedDType`, `series.ErrLengthMismatch`, `dataframe.ErrJoinKeyDTypeMismatch`.
- Expression build errors are deferred to `Collect()`. `expr.Col("a").Add(expr.Col("b"))` never fails at construction time, even if "a" does not exist in the target frame. The error surfaces when the plan is resolved.

## Context

Every operation that does IO, runs a plan, or runs a compute kernel over the data accepts a `context.Context` as the first argument. Metadata-only operations and plan builders do not. Examples:

- `iocsv.ReadFile(ctx, path)` takes ctx. (The root `golars.ReadCSV(path)` shortcut uses `context.Background()`.)
- `df.Filter(ctx, mask)`, `df.Sort(ctx, col, desc)`, `df.Join(ctx, ...)` and `df.GroupBy(...).Agg(ctx, aggs)` take ctx. They run kernels over every row.
- `df.Select(names...)`, `df.Head(n)`, `df.Rename(old, new)` do not. They only rearrange column references.
- `lf.Collect(ctx)` takes ctx. Collect runs the plan.
- `lf.GroupBy("k")` does not. GroupBy is a plan builder.

The rule is: if a call can run arbitrary user-supplied IO, a plan, or a compute stage over the data, it takes a context. Builder calls do not.

## Options

For operations with many optional parameters (readers and writers, Join, Filter, Sort, GroupBy.Agg), we use functional options:

```go
import iocsv "github.com/Gaurav-Gosain/golars/io/csv"

df, err := iocsv.ReadFile(ctx, "data.csv",
    iocsv.WithDelimiter(','),
    iocsv.WithHasHeader(true),
    iocsv.WithNullValues("", "NA"),
)
```

Options are functions with typed constructors. Option types are scoped to the package or operation (`iocsv.Option`, `parquet.Option`, `dataframe.JoinOption`, `dataframe.SortOption`) so the compiler enforces correct combinations.

## IO packages

Each supported file format lives in its own package so programs that only need one format don't pull the rest:

```
io/csv       // RFC 4180 CSV
io/parquet   // Apache Parquet via pqarrow
io/ipc       // Arrow IPC (feather)
io/json      // JSON array-of-objects, object-of-arrays, and NDJSON
io/sql       // database/sql bridge (any pure-Go driver)
```

Each package exposes `Read` (from an `io.Reader`; parquet takes a `parquet.ReaderAtSeeker`), `ReadFile`, and where it makes sense `ReadURL` (net/http-backed: csv, parquet, json), `ReadString` (json) and `ReadBytes` (parquet). Writers are symmetric: `Write`, `WriteFile`. `io/ipc` also has `NewStreamWriter` / `NewStreamReader` for the Arrow IPC stream format. Each package also has a lazy `Scan(path, opts...)`. The URL loaders accept `WithHTTPClient` so tests can inject a custom transport and production code can wire retry/auth middleware.

JSON type inference promotes numeric columns like polars: mixed int/float → float64; mixed anything/string → string. NaN and Inf round-trip. Nulls in input become null bitmap entries.

`io/sql` is the pragmatic pure-Go path for databases: plug in any `database/sql`-compatible driver (pgx, modernc.org/sqlite, go-sql-driver/mysql, go-mssqldb) and get a typed DataFrame. `ReadSQL(ctx, db, query, args...)` is eager; `NewReader(ctx, db, query, opts, args...)` streams batches of `WithBatchSize(n)` rows for result sets that exceed memory. Null values are preserved via the arrow validity bitmap. Apache ADBC is deliberately not wrapped: its drivers require cgo, which breaks the pure-Go invariant; the nested demo in `examples/sql/` shows the integration pattern with SQLite.

## Scripting (`script/` + `.glr` files)

`script.Runner` runs a tiny pipe-style language against any `Executor`. One statement per line, `#` for comments, the leading `.` on each command is optional:

```glr
# trades.glr
load data/trades.csv
filter volume > 100
groupby symbol amount:sum:total
sort total desc
show
```

`cmd/golars` is the reference host: `golars run path.glr` runs a file one-shot and exits, `.source path.glr` runs one inline from the REPL. Third-party programs plug in via `script.ExecutorFunc`.

Multi-source: `load PATH as NAME` stages a frame in a registry without promoting it to focus; `use NAME` focuses a clone of it and leaves NAME in the registry, discarding the prior focus (run `stash NAME` first to keep it); `join PATH|NAME on KEY` looks up a staged frame by name (it joins a clone, so NAME stays staged) before trying it as a path. See the full language reference in [`docs/scripting.md`](scripting.md). A Tree-sitter grammar + highlight queries ship at [`editors/tree-sitter-golars/`](../editors/tree-sitter-golars/) for editor integrations.


## Nullability and zero values

Go has no null. Arrow has validity bitmaps. The API presents null the same way polars does:

- `df.Row(i)` returns `([]any, error)` with `nil` for null cells. For typed per-cell access, use the underlying Arrow array (`s.Chunk(0)`) and its `IsValid(i)`.
- `s.IsNull()` returns `(*series.Series, error)`: a boolean mask Series. `s.NullCount()` returns the null count.
- Aggregations skip nulls by default. `Sum` over `[1, null, 2]` is 3.
- Comparison operators produce null when either side is null. This matches SQL and polars.

## Iteration

golars does not encourage row-wise iteration. The idiomatic shape of a program is:

```go
result, err := golars.Lazy(df).
    Filter(golars.Col("price").GtLit(0)).
    WithColumns(
        golars.Col("price").Mul(golars.Col("qty")).Alias("total"),
    ).
    GroupBy("region").
    Agg(golars.Col("total").Sum()).
    Collect(ctx)
```

Row access is available as `df.Row(i)` (returns `([]any, error)`) and `df.Rows()` (returns `([][]any, error)`, materialising every row) for cases where it is truly needed (writing to a non-columnar sink, debugging), but it is slow by design and documented as such. For large inputs, `lf.IterBatches(ctx)` yields `*DataFrame` batches instead.

## Versioning

We follow semver. Before v1.0.0 the API is unstable by convention (minor version bumps may break). After v1.0.0 we commit to semver strictly. Deprecations are marked with `// Deprecated:` comments and persist for at least one minor version before removal.

## What we do not export

- Concrete struct fields on `DataFrame`, `Series`, `LazyFrame`. All access is through methods.
- A separate physical plan. The logical plan node types live in package `lazy` (`lazy.Node`, returned by `lf.Plan()`) and are exported for inspection and tooling, and the executor runs the optimised logical plan directly. The supported API is what you build through the fluent `LazyFrame` API. Direct plan construction is not supported.
- Internal hash table and pool types (`internal/intmap`, `internal/pool`, `internal/mempool`).
