"""Run the golars PDS-H queries and their polars equivalents on the same
parquet data and print per-query timings and ratios.

The polars queries mirror bench/pds-h/queries/*.go exactly (including
the stubbed-out shipdate predicates), so the two engines compute the
same answer on the same input.

Run from the repo root, reusing the polars-compare uv project:

    go run ./bench/pds-h/gen -rows 6000000 -out /tmp/pdsh
    uv run --project bench/polars-compare python bench/pds-h/compare.py \
        --data /tmp/pdsh --runs 5
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import polars as pl


def q1(data: Path) -> pl.LazyFrame:
    li = pl.scan_parquet(data / "lineitem.parquet")
    price = pl.col("l_extendedprice")
    disc_price = price * (1.0 - pl.col("l_discount"))
    charge = disc_price * (1.0 + pl.col("l_tax"))
    return (
        li.group_by("l_returnflag", "l_linestatus")
        .agg(
            pl.col("l_quantity").sum().alias("sum_qty"),
            price.sum().alias("sum_base_price"),
            disc_price.sum().alias("sum_disc_price"),
            charge.sum().alias("sum_charge"),
            pl.col("l_quantity").mean().alias("avg_qty"),
            price.mean().alias("avg_price"),
            pl.col("l_discount").mean().alias("avg_disc"),
            pl.col("l_quantity").count().alias("count_order"),
        )
        .sort("l_returnflag", "l_linestatus")
    )


def q6(data: Path) -> pl.LazyFrame:
    li = pl.scan_parquet(data / "lineitem.parquet")
    d = pl.col("l_discount")
    return li.filter(
        (d >= 0.05) & (d <= 0.07) & (pl.col("l_quantity") < 24)
    ).select((pl.col("l_extendedprice") * d).sum().alias("revenue"))


QUERIES = {1: q1, 6: q6}


def time_polars(fn, data: Path, runs: int) -> float:
    fn(data).collect()  # warm the page cache and the thread pool
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn(data).collect()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def time_golars(binary: Path, q: int, data: Path, runs: int) -> float:
    with tempfile.TemporaryDirectory() as tmp:
        out = subprocess.check_output(
            [str(binary), "-q", str(q), "-data", str(data),
             "-repeats", str(runs + 1), "-out", os.path.join(tmp, "t.csv")],
            text=True,
        )
    samples = []
    for line in out.splitlines():
        m = re.search(r"rep=(\d+)\s+(\S+)", line)
        if m and int(m.group(1)) > 0:  # rep 0 is the warmup
            samples.append(parse_go_duration(m.group(2)))
    return statistics.median(samples)


def parse_go_duration(s: str) -> float:
    for suffix, scale in (("ms", 1e-3), ("µs", 1e-6), ("us", 1e-6), ("ns", 1e-9), ("s", 1.0)):
        if s.endswith(suffix):
            return float(s[: -len(suffix)]) * scale
    raise ValueError(s)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, type=Path)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--queries", default="1,6")
    args = p.parse_args()

    repo = Path(__file__).resolve().parents[2]
    # Build next to the data so every artifact of a run lives in one
    # directory the caller controls.
    binary = args.data / "pdsh"
    subprocess.check_call(["go", "build", "-o", str(binary), "./bench/pds-h/cmd/pdsh"], cwd=repo)

    print(f"{'query':<6} {'polars':>10} {'golars':>10} {'golars/polars':>14}")
    for q in (int(x) for x in args.queries.split(",")):
        tp = time_polars(QUERIES[q], args.data, args.runs)
        tg = time_golars(binary, q, args.data, args.runs)
        print(f"q{q:<5} {tp * 1e3:>8.1f}ms {tg * 1e3:>8.1f}ms {tg / tp:>13.2f}x")
    print(f"polars {pl.__version__}; ratio > 1 means golars is slower")
    return 0


if __name__ == "__main__":
    sys.exit(main())
