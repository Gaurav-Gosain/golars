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
    tables.go                scan and expression helpers
    q01.go ... q19.go        one query each
  gen/main.go                TPC-H table generator for local dev
  compare.py                 polars twins, timing, memory, answer check
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

## Running against generated data (local dev, no tpchgen)

```sh
# All eight tables at scale factor 0.1 (600k lineitem rows).
go run ./bench/pds-h/gen -sf 0.1 -out /tmp/pdsh
go run ./bench/pds-h/cmd/pdsh -q all -data /tmp/pdsh -sf 0.1
```

The generator follows the dbgen rules the queries depend on (key
ranges, foreign keys, date offsets, flag rules, value lists), so the
answers are meaningful, but free text comes from word lists and does
not match dbgen byte for byte.

## Comparing against polars

`compare.py` runs the golars queries and the polars-benchmark polars
queries on the same parquet files and prints per-query medians, the
speedup, and peak RSS of each side:

```sh
go run ./bench/pds-h/gen -sf 1 -out /tmp/pdsh
uv run --project bench/polars-compare python bench/pds-h/compare.py \
    --data /tmp/pdsh --runs 5
# Check that every golars answer matches polars:
uv run --project bench/polars-compare python bench/pds-h/compare.py \
    --data /tmp/pdsh --check
```

The pdsh binary is built into the data directory. `pdsh -cpuprofile
FILE` writes a CPU profile covering every repetition and `pdsh -dump
DIR` writes each result to `DIR/q<N>.parquet`.

## Query status

| # | Name                         | Status | Note |
|---|------------------------------|--------|------|
| 1 | Pricing summary report       | done   | - |
| 3 | Shipping priority            | done   | - |
| 4 | Order priority checking      | done   | unique on the selected pair instead of unique(subset=...) |
| 5 | Local supplier volume        | done   | one-key join plus a nation filter instead of a two-key join |
| 6 | Forecasting revenue change   | done   | - |
| 10 | Returned item reporting     | done   | - |
| 12 | Shipping modes and priority | done   | - |
| 14 | Promotion effect            | done   | - |
| 19 | Discounted revenue          | done   | - |
| others |                         | todo   | multi-key joins, semi/anti joins |

Porting cadence: add a `q<N>.go`, register in `registry.go`, add the
polars twin to `compare.py`, and run `compare.py --check`.
