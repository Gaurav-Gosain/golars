"""Micro-benchmark harness for polars, mirrored by a Go program that runs
the same workloads through golars.

Each benchmark:
  1. Allocates deterministic input of a fixed row count.
  2. Warms up once (JIT and caches).
  3. Measures `repeat` wall-clock runs and reports the median.

Output is JSON on stdout so a runner can parse both results and diff them.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass
class Result:
    name: str
    rows: int
    median_ns: int
    throughput_mbps: float


def time_ns(fn, warmup: int = 3, repeat: int = 25) -> int:
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append(time.perf_counter_ns() - t0)
    return int(statistics.median(samples))


def bench_sum_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.select(pl.col("a").sum()).item()

    t = time_ns(run)
    bytes_in = rows * 8
    return Result("SumInt64", rows, t, bytes_in / t * 1000.0)


def bench_add_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.with_columns((pl.col("a") + pl.col("b")).alias("s"))

    t = time_ns(run)
    bytes_in = rows * 16
    return Result("AddInt64", rows, t, bytes_in / t * 1000.0)


def bench_filter_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.filter(pl.col("a") > (1 << 19))

    t = time_ns(run)
    bytes_in = rows * 8
    return Result("FilterInt64", rows, t, bytes_in / t * 1000.0)


def bench_sort_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.sort("a")

    t = time_ns(run)
    bytes_in = rows * 8
    return Result("SortInt64", rows, t, bytes_in / t * 1000.0)


def bench_groupby_sum(rows: int, groups: int) -> Result:
    rng = np.random.default_rng(42)
    keys = rng.integers(0, groups, size=rows, dtype=np.int64)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = df.group_by("k").agg(pl.col("v").sum().alias("s"))

    t = time_ns(run)
    bytes_in = rows * 16
    return Result(f"GroupBySum(groups={groups})", rows, t, bytes_in / t * 1000.0)


def bench_groupby_sum_multikey(rows: int) -> Result:
    regions = [f"r{i % 8}" for i in range(rows)]
    years = [2020 + i % 5 for i in range(rows)]
    rng = np.random.default_rng(44)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"region": regions, "year": years, "v": vals})

    def run():
        _ = df.group_by("region", "year").agg(pl.col("v").sum().alias("s"))

    t = time_ns(run)
    return Result("GroupBySumMultiKey", rows, t, rows * 24 / t * 1000.0)


def bench_inner_join(rows: int) -> Result:
    rng = np.random.default_rng(42)
    ids = np.arange(rows, dtype=np.int64)
    left = pl.DataFrame({"id": ids, "lv": rng.integers(0, 1 << 20, size=rows, dtype=np.int64)})
    right = pl.DataFrame({"id": ids, "rv": rng.integers(0, 1 << 20, size=rows, dtype=np.int64)})

    def run():
        _ = left.join(right, on="id", how="inner")

    t = time_ns(run)
    bytes_in = rows * 16
    return Result("InnerJoin", rows, t, bytes_in / t * 1000.0)


def bench_sum_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.select(pl.col("a").sum()).item()

    t = time_ns(run)
    return Result("SumFloat64", rows, t, rows * 8 / t * 1000.0)


def bench_mean_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.select(pl.col("a").mean()).item()

    t = time_ns(run)
    return Result("MeanFloat64", rows, t, rows * 8 / t * 1000.0)


def bench_min_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.select(pl.col("a").min()).item()

    t = time_ns(run)
    return Result("MinFloat64", rows, t, rows * 8 / t * 1000.0)


def bench_add_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    a = rng.random(size=rows, dtype=np.float64)
    b = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.with_columns((pl.col("a") + pl.col("b")).alias("s"))

    t = time_ns(run)
    return Result("AddFloat64", rows, t, rows * 16 / t * 1000.0)


def bench_mul_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 10, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 10, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.with_columns((pl.col("a") * pl.col("b")).alias("s"))

    t = time_ns(run)
    return Result("MulInt64", rows, t, rows * 16 / t * 1000.0)


def bench_gt_int64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.with_columns((pl.col("a") > pl.col("b")).alias("s"))

    t = time_ns(run)
    return Result("GtInt64", rows, t, rows * 16 / t * 1000.0)


def bench_filter_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.filter(pl.col("a") > 0.5)

    t = time_ns(run)
    return Result("FilterFloat64", rows, t, rows * 8 / t * 1000.0)


def bench_sort_float64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.sort("a")

    t = time_ns(run)
    return Result("SortFloat64", rows, t, rows * 8 / t * 1000.0)


def bench_sort_two_keys(rows: int) -> Result:
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 16, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 16, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.sort(["a", "b"])

    t = time_ns(run)
    return Result("SortTwoKeys", rows, t, rows * 16 / t * 1000.0)


def bench_groupby_mean(rows: int, groups: int) -> Result:
    rng = np.random.default_rng(42)
    keys = rng.integers(0, groups, size=rows, dtype=np.int64)
    vals = rng.random(size=rows, dtype=np.float64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = df.group_by("k").agg(pl.col("v").mean().alias("s"))

    t = time_ns(run)
    return Result(f"GroupByMean(groups={groups})", rows, t, rows * 16 / t * 1000.0)


def bench_groupby_multi_agg(rows: int, groups: int) -> Result:
    rng = np.random.default_rng(42)
    keys = rng.integers(0, groups, size=rows, dtype=np.int64)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = df.group_by("k").agg(
            pl.col("v").sum().alias("s"),
            pl.col("v").mean().alias("m"),
            pl.col("v").min().alias("lo"),
            pl.col("v").max().alias("hi"),
        )

    t = time_ns(run)
    return Result(f"GroupByMultiAgg(groups={groups})", rows, t, rows * 16 / t * 1000.0)


def bench_left_join(rows: int) -> Result:
    rng = np.random.default_rng(42)
    ids = np.arange(rows, dtype=np.int64)
    right_ids = rng.choice(rows * 2, size=rows, replace=False).astype(np.int64)
    left = pl.DataFrame({"id": ids, "lv": rng.integers(0, 1 << 20, size=rows, dtype=np.int64)})
    right = pl.DataFrame({"id": right_ids, "rv": rng.integers(0, 1 << 20, size=rows, dtype=np.int64)})

    def run():
        _ = left.join(right, on="id", how="left")

    t = time_ns(run)
    return Result("LeftJoin", rows, t, rows * 16 / t * 1000.0)


def bench_take(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    idx = rng.permutation(rows)[: rows // 2].astype(np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df["a"].gather(idx)

    t = time_ns(run)
    return Result("Take", rows, t, rows * 8 / t * 1000.0)


def bench_cast_i64_f64(rows: int) -> Result:
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.with_columns(pl.col("a").cast(pl.Float64).alias("b"))

    t = time_ns(run)
    return Result("CastI64ToF64", rows, t, rows * 8 / t * 1000.0)


def bench_pipeline(rows: int) -> Result:
    """Analytics pipeline: filter -> groupby -> sort. Measures end-to-end
    throughput of the common analytical shape."""
    rng = np.random.default_rng(42)
    keys = rng.integers(0, 64, size=rows, dtype=np.int64)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = (
            df.filter(pl.col("v") > (1 << 19))
            .group_by("k")
            .agg(pl.col("v").sum().alias("s"))
            .sort("s", descending=True)
        )

    t = time_ns(run)
    return Result("Pipeline(filter>gb>sort)", rows, t, rows * 16 / t * 1000.0)


def bench_rolling_sum(rows: int) -> Result:
    """Rolling sum with window=32 on a no-null int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").rolling_sum(window_size=32))

    t = time_ns(run)
    return Result("RollingSum(w=32)", rows, t, rows * 8 / t * 1000.0)


def bench_rolling_min(rows: int) -> Result:
    """Rolling min with window=32 on a no-null int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").rolling_min(window_size=32))

    t = time_ns(run)
    return Result("RollingMin(w=32)", rows, t, rows * 8 / t * 1000.0)


def bench_rolling_max(rows: int) -> Result:
    """Rolling max with window=32 on a no-null int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").rolling_max(window_size=32))

    t = time_ns(run)
    return Result("RollingMax(w=32)", rows, t, rows * 8 / t * 1000.0)


def bench_rolling_mean(rows: int) -> Result:
    """Rolling mean with window=32 on a no-null int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").rolling_mean(window_size=32))

    t = time_ns(run)
    return Result("RollingMean(w=32)", rows, t, rows * 8 / t * 1000.0)


def bench_when_then(rows: int) -> Result:
    """when/then/otherwise picking between two int64 columns."""
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b})

    def run():
        _ = df.select(
            pl.when(pl.col("a") > (1 << 19)).then(pl.col("a")).otherwise(pl.col("b")).alias("r")
        )

    t = time_ns(run)
    return Result("WhenThenOtherwise", rows, t, rows * 16 / t * 1000.0)


def bench_over_sum(rows: int) -> Result:
    """pl.col("v").sum().over("k") with 64 groups."""
    rng = np.random.default_rng(42)
    keys = rng.integers(0, 64, size=rows, dtype=np.int64)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = df.select(pl.col("v").sum().over("k").alias("vs"))

    t = time_ns(run)
    return Result("SumOverGroup", rows, t, rows * 16 / t * 1000.0)


def bench_cumsum_over_group(rows: int) -> Result:
    rng = np.random.default_rng(42)
    keys = rng.integers(0, 64, size=rows, dtype=np.int64)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"k": keys, "v": vals})

    def run():
        _ = df.select(pl.col("v").cum_sum().over("k").alias("cum"))

    t = time_ns(run)
    return Result("CumSumOverGroup", rows, t, rows * 16 / t * 1000.0)


def bench_forward_fill(rows: int) -> Result:
    """Forward-fill on a 30%-null int64 column."""
    rng = np.random.default_rng(7)
    vals = np.arange(rows, dtype=np.int64)
    mask = rng.random(rows) < 0.3
    # Build a nullable polars Series from a Python list with Nones where mask is True.
    data = [None if mask[i] else int(vals[i]) for i in range(rows)]
    df = pl.DataFrame({"x": pl.Series("x", data, dtype=pl.Int64)})

    def run():
        _ = df.fill_null(strategy="forward")

    t = time_ns(run)
    return Result("ForwardFillInt64", rows, t, rows * 8 / t * 1000.0)


def bench_sum_horizontal(rows: int) -> Result:
    """Row-wise sum across 3 int64 columns."""
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    c = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b, "c": c})

    def run():
        _ = df.select(pl.sum_horizontal("a", "b", "c").alias("total"))

    t = time_ns(run)
    return Result("SumHorizontal(3cols)", rows, t, rows * 24 / t * 1000.0)


def bench_max_horizontal(rows: int) -> Result:
    """Row-wise max across 3 int64 columns."""
    rng = np.random.default_rng(42)
    a = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    b = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    c = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": a, "b": b, "c": c})

    def run():
        _ = df.select(pl.max_horizontal("a", "b", "c").alias("m"))

    t = time_ns(run)
    return Result("MaxHorizontal(3cols)", rows, t, rows * 24 / t * 1000.0)


def bench_unique_int64(rows: int) -> Result:
    """Unique over an int64 column with ~25% distinct values (collisions dominate)."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, rows // 4, size=rows, dtype=np.int64)
    df = pl.DataFrame({"a": vals})

    def run():
        _ = df.unique()

    t = time_ns(run)
    return Result("UniqueInt64", rows, t, rows * 8 / t * 1000.0)


def bench_top_k(rows: int, k: int) -> Result:
    """Top-k selection over an int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").top_k(k))

    t = time_ns(run)
    return Result(f"TopK(k={k})", rows, t, rows * 8 / t * 1000.0)


def bench_rank(rows: int) -> Result:
    """Rank over an int64 column (average method)."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").rank())

    t = time_ns(run)
    return Result("RankInt64", rows, t, rows * 8 / t * 1000.0)


def bench_cumsum_int64(rows: int) -> Result:
    """Cumulative sum along an int64 column."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 16, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").cum_sum())

    t = time_ns(run)
    return Result("CumSumInt64", rows, t, rows * 8 / t * 1000.0)


def bench_shift_int64(rows: int) -> Result:
    """Shift by 1 along an int64 column (top value becomes null)."""
    rng = np.random.default_rng(42)
    vals = rng.integers(0, 1 << 20, size=rows, dtype=np.int64)
    df = pl.DataFrame({"x": vals})

    def run():
        _ = df.select(pl.col("x").shift(1))

    t = time_ns(run)
    return Result("ShiftInt64", rows, t, rows * 8 / t * 1000.0)


def bench_fill_null_value(rows: int) -> Result:
    """fill_null with a constant value (not strategy-based), ~30% null input."""
    rng = np.random.default_rng(7)
    vals = np.arange(rows, dtype=np.int64)
    mask = rng.random(rows) < 0.3
    data = [None if mask[i] else int(vals[i]) for i in range(rows)]
    df = pl.DataFrame({"x": pl.Series("x", data, dtype=pl.Int64)})

    def run():
        _ = df.fill_null(0)

    t = time_ns(run)
    return Result("FillNullValue", rows, t, rows * 8 / t * 1000.0)


def bench_drop_nulls(rows: int) -> Result:
    """drop_nulls on a column with ~30% null input."""
    rng = np.random.default_rng(7)
    vals = np.arange(rows, dtype=np.int64)
    mask = rng.random(rows) < 0.3
    data = [None if mask[i] else int(vals[i]) for i in range(rows)]
    df = pl.DataFrame({"x": pl.Series("x", data, dtype=pl.Int64)})

    def run():
        _ = df.drop_nulls()

    t = time_ns(run)
    return Result("DropNulls", rows, t, rows * 8 / t * 1000.0)


def main() -> int:
    SIZES = [16_384, 262_144, 1_048_576]
    out: list[dict] = []

    for n in SIZES:
        for r in (
            bench_sum_int64(n),
            bench_sum_float64(n),
            bench_mean_float64(n),
            bench_min_float64(n),
            bench_add_int64(n),
            bench_add_float64(n),
            bench_mul_int64(n),
            bench_gt_int64(n),
            bench_filter_int64(n),
            bench_filter_float64(n),
            bench_sort_int64(n),
            bench_sort_float64(n),
            bench_cast_i64_f64(n),
            bench_take(n),
        ):
            out.append(vars(r))

    for n in (16_384, 262_144):
        out.append(vars(bench_sort_two_keys(n)))

    for n in (16_384, 262_144):
        for g in (8, 1024):
            out.append(vars(bench_groupby_sum(n, g)))
        for g in (64,):
            out.append(vars(bench_groupby_mean(n, g)))
            out.append(vars(bench_groupby_multi_agg(n, g)))
        out.append(vars(bench_groupby_sum_multikey(n)))

    for n in (16_384, 262_144):
        out.append(vars(bench_inner_join(n)))
        out.append(vars(bench_left_join(n)))

    for n in (16_384, 262_144):
        out.append(vars(bench_pipeline(n)))

    for n in SIZES:
        out.append(vars(bench_sum_horizontal(n)))

    for n in SIZES:
        out.append(vars(bench_max_horizontal(n)))

    for n in (16_384, 262_144):
        out.append(vars(bench_unique_int64(n)))
        out.append(vars(bench_top_k(n, 10)))
        out.append(vars(bench_rank(n)))

    for n in SIZES:
        out.append(vars(bench_cumsum_int64(n)))
        out.append(vars(bench_shift_int64(n)))
        out.append(vars(bench_fill_null_value(n)))
        out.append(vars(bench_drop_nulls(n)))

    for n in SIZES:
        out.append(vars(bench_forward_fill(n)))

    for n in SIZES:
        out.append(vars(bench_rolling_sum(n)))
        out.append(vars(bench_rolling_min(n)))
        out.append(vars(bench_rolling_max(n)))
        out.append(vars(bench_rolling_mean(n)))

    for n in (16_384, 262_144):
        out.append(vars(bench_when_then(n)))
        out.append(vars(bench_over_sum(n)))
        out.append(vars(bench_cumsum_over_group(n)))

    json.dump({"engine": "polars", "version": pl.__version__, "runs": out}, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
