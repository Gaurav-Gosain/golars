# Testing golars against polars

golars has three layers of correctness tests beyond the ordinary unit
tests:

1. **Property and fuzz tests** (`*_prop_test.go`, `*_fuzz_test.go`)
   check invariants that need no oracle: sort output is sorted and a
   permutation, filter keeps exactly the masked rows, group sums add up
   to the column total, join cardinalities match a nested loop, casts
   round trip, CSV/Parquet/IPC/NDJSON write-then-read round trips, and
   the IPC reader and glr parser never panic on arbitrary bytes. They
   run with `go test ./...`.
2. **The differential harness** (`internal/difftest`) runs random
   queries through golars and polars 1.39.3 and compares the results.
3. **Regression cases** (`internal/difftest/testdata/regress/*.json`)
   are shrunk mismatches with the polars result baked in, so
   `go test ./internal/difftest/` replays them without Python.

## Running the differential harness

The polars side uses the uv environment under `bench/polars-compare`
(`uv sync` there once). Then:

```sh
go build -o /tmp/difftest ./internal/difftest/cmd/difftest
/tmp/difftest -n 20000 -seed 1 -out /tmp/dt -emit /tmp/dt-emit
```

Useful flags:

- `-n`, `-seed`: case count and first seed (case i uses seed+i, so a
  run is reproducible).
- `-workers`: parallel workers, each with its own long-lived polars
  process (default 6). About 500 to 1000 cases per second.
- `-ops join`: keep only cases whose plan uses one of these ops (comma
  separated), for example to exercise joins alone. Seeds that do not
  qualify are skipped, so `-n` still counts kept cases.
- `-show kinds`: print shrunk examples only for these verdict kinds,
  for example `golars_panic,value,schema,height,missing_error`.
- `-max-shrink`: shrink this many cases per signature (value, schema
  and height mismatches get ten times as many).
- `-emit dir`: write each shrunk mismatch as a regression JSON file.
- `-literal-trees p`: how often column-free subexpressions are kept
  (default 0.1, see the literal semantics note below).
- `DIFFTEST_PYTHON`: the interpreter to use (default
  `bench/polars-compare/.venv/bin/python`).

Replay and re-shrink saved case directories (`<out>/fail/...` or
`<out>/min/...`):

```sh
/tmp/difftest -out /tmp/dt-replay /tmp/dt/min/value/1234
/tmp/difftest -shrink=false -emit internal/difftest/testdata/regress \
  -emit-name my_bug /tmp/dt/min/value/1234
```

Mark regression files that now pass as fixed:

```sh
/tmp/difftest -promote internal/difftest/testdata/regress
```

`DIFFTEST=1 go test ./internal/difftest/` also runs a small smoke test
against the oracle.

## Verdicts

| kind | meaning |
| --- | --- |
| agree / both_error | results match, or both engines raised |
| golars_panic | golars panicked (always a bug) |
| value, schema, height | results differ |
| missing_error | polars raised, golars returned a result |
| golars_error | polars succeeded, golars returned an error (mostly unsupported dtype and feature gaps) |
| unsupported | golars cannot express the plan (glr has no spelling for an expression) |
| polars_panic | polars itself panicked; ignored |
| harness | the harness failed to build or write the case |

## Comparison rules

- Schema: column names and dtypes must match exactly. Large, view and
  plain string/list types compare equal.
- Ints, strings, bools, temporals: exact.
- Floats: NaN equals NaN, infinities exact, otherwise a relative
  tolerance of 1e-9 (f64) or 1e-5 (f32) with a small absolute floor,
  because summation order differs between the engines.
- Row order: exact where polars guarantees it. After a group_by
  without `maintain_order` or a join without a `maintain_order` that
  fixes the order (`left_right`, `right_left`), rows compare as a
  multiset.
  After sorting an unordered frame, the sort keys must match in order
  and whole rows as a multiset; a slice of that compares only the
  keys; a slice of an unordered frame compares only the height. The
  generator only uses order-dependent functions (cum ops, shift,
  first/last, rolling, ...) while the frame order is still exact.

## Known intentional differences

- `cast` is not strict in golars: failed conversions become null where
  polars raises by default.
- glr `str.contains`, `str.replace`, `str.replace_all`,
  `str.count_matches` and `str.find` are literal; polars defaults to
  regex. The harness maps polars `literal=True` to the glr form and
  counts regex forms without a glr spelling as unsupported.

## Known open bug classes

See the `known` files under `internal/difftest/testdata/regress` for
minimal repros.

## Literal semantics

golars follows polars here:

- A literal evaluates to one row. Functions on it (`count`, `shift`,
  `var`, `is_unique`, ...) see that single value, and it broadcasts
  only when combined with a frame-length column or when the output
  frame is built. `select(lit(x))` has one row.
- In `group_by().agg()` an expression built only from literals is one
  scalar per group, not a list.
- An untyped numeric literal (glr `1`, `1.5`; Go `expr.Lit(1)`,
  `expr.LitInt`, `expr.LitFloat` and the `AddLit`-style sugar) is i32
  on its own (i64 when the value does not fit) or f64, and next to a
  numeric column it takes the column's dtype when the value fits:
  `col(i8) + 1` is i8, `col(i8) + 1000` is i16, `col(f32) * 1.5` is
  f32. `expr.LitInt64`, `expr.LitFloat64` and Go `int64`, `int32` and
  `float32` values passed to `expr.Lit` pin the dtype, like
  `pl.lit(1, dtype=pl.Int64)`.
- Two typed numeric operands combine in the polars supertype
  (`dtype.NumericSupertype`): `i16 + i32` is i32, `u8 + i8` is i16,
  `f32 + i16` is f32. Integer arithmetic wraps on overflow.
