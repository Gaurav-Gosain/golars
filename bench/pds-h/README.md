# pds-h: PDS-H / TPC-H bench runner for golars

Upstream: [pola-rs/polars-benchmark](https://github.com/pola-rs/polars-benchmark)
runs the 22 TPC-H queries (rebranded PDS-H for license reasons) across
polars, duckdb, pandas, pyspark, dask, modin. This directory is the
analogous golars runner: a native Go binary that executes the
queries against `.parquet` tables on disk and emits the same
`timings.csv` schema so its rows plug into upstream's
`scripts/plot_bars.py` unchanged.

## Layout

```
bench/pds-h/
  README.md                  this file
  cmd/pdsh/main.go           CLI: -q N -data DIR [-sf F] [-repeats N]
  queries/                   one file per query
    registry.go              name->func table
    q01.go                   Q1: pricing summary report
    q06.go                   Q6: forecasting revenue change
  gen/                       synthetic data for local dev
    gen.go                   lineitem.parquet generator
```

## Running against real TPC-H data

```sh
# One-time: generate SF=1 parquet tables via upstream's Makefile.
cd path/to/polars-benchmark
pip install -r requirements.in
SCALE_FACTOR=1 make data

# Run Q1 with golars.
cd path/to/golars
go run ./bench/pds-h/cmd/pdsh -q 1 -data path/to/polars-benchmark/data/tables/scale-1 -sf 1
```

Each run appends one row to `bench/pds-h/output/timings.csv`:

```
solution,version,query_number,duration[s],io_type,scale_factor
golars,0.1.0,1,0.34,parquet,1.0
```

Drop this into upstream's `output/run/timings.csv` to have its plot
scripts pick us up alongside polars/duckdb/etc.

## Running against synthetic data (local dev, no tpchgen)

```sh
# Emit a small synthetic lineitem.parquet (100k rows by default).
go run ./bench/pds-h/gen -rows 100000 -out /tmp/pdsh

# Run against it.
go run ./bench/pds-h/cmd/pdsh -q 1 -data /tmp/pdsh -sf synthetic
```

The synthetic dataset is schema-compatible with tpchgen but drops
the cross-table referential invariants. Good enough to validate the
query compiles; not a real benchmark.

## Comparing against polars

`compare.py` runs the golars queries and their polars equivalents
(same logical plan, including the stubbed shipdate predicates) on the
same parquet files and prints per-query medians and the ratio:

```sh
go run ./bench/pds-h/gen -rows 6000000 -out /tmp/pdsh   # ~SF1 lineitem size
uv run --project bench/polars-compare python bench/pds-h/compare.py \
    --data /tmp/pdsh --runs 7
```

The pdsh binary is built into the data directory. `pdsh -cpuprofile
FILE` writes a CPU profile covering every repetition.

## Query status

| # | Name                        | Status | Blocker |
|---|-----------------------------|--------|---------|
| 1 | Pricing summary report      | done    | - |
| 6 | Forecasting revenue change  | done    | - |
| 2, 7-22 | other queries         | todo    | date interval arithmetic, semi/anti join, substring, regex |

Porting cadence: add a `q<N>.go`, register in `registry.go`, verify
output matches `data/answers/q<N>.parquet` at SF=1 (the reference
answers upstream ships). Hook missing kernels into golars as we go.
