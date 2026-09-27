# polars-compare

4-way head-to-head benchmark: `polars-py` vs the `polars-rs` crate vs
`golars-scalar` vs `golars-simd`.

## Layout

- [`bench.py`](bench.py) - polars-py workloads, prints JSON to stdout.
- [`../polars-rust/`](../polars-rust/) - Rust crate using polars 0.53.
- [`../../cmd/bench/`](../../cmd/bench/) - the same workloads in golars.
- [`compare.py`](compare.py) - runs all four, joins on `(name, rows)`,
  prints a table and per-workload ratios.

## Run

```sh
cd bench/polars-compare
uv run python compare.py
```

To run a subset, pass a regexp matched against workload names. It is
forwarded to all four engines, so filtered-out workloads cost nothing:

```sh
uv run python compare.py --runs 3 --only '^(SortStr|Str|CSVRead)'
```

Each engine also takes the filter directly: `golars-bench -only RE`,
`python bench.py --only RE`, `polars-rust-bench --only RE`. When
`CARGO_TARGET_DIR` is set, `compare.py` looks for the Rust binary there.

`compare.py` builds each binary on the fly (`go build`, `cargo build`).
Make sure you have `uv`, a recent Go, and a Rust toolchain installed.

Output is a fixed-width table with per-workload throughput (MB/s) and
two ratios:

```
workload                           rows      pl-py      pl-rs     scalar       simd    s/py    s/rs
------------------------------------------------------------------------------------------------
SumInt64                       16,384     5,000MB    15,500MB    14,500MB   36,000MB   7.2x   2.3x
...

polars-py 1.39.3  |  polars-rs crate 0.53.0  |  simd wins vs py: NN/NN  |  simd wins vs rs: NN/NN
```

## Workloads

Covers aggregations, arithmetic, filters, joins, groupby /
multi-agg / over, horizontal ops, rolling, sorts, pipelines,
unique / shift / cumsum / fill-null / drop-nulls, Take, and CSV
conversion. See [`bench.py`](bench.py) for the authoritative list.

## Notes

- The harness runs each workload in a fresh subprocess so allocator
  state doesn't bleed between runs. Variance on the Linux i7-10700
  test box is typically ±3 wins across 8 runs; expect the same on
  your hardware.
- Build `golars-bench` with `GOEXPERIMENT=simd` for the AVX2/AVX-512
  fast paths; the harness does this for you.
- The polars-py path uses the installed `polars` package - see
  [`pyproject.toml`](pyproject.toml) for the pinned version.
