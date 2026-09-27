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


# ---- temporal workloads (mirrored in cmd/bench/temporal.go) ----


def temporal_frame(rows: int) -> pl.DataFrame:
    """Microsecond timestamps spread over ten years, no randomness."""
    base = 1_500_000_000_000_000
    span = 315_360_000_000_000
    i = np.arange(rows, dtype=np.int64)
    vals = base + (i * 7_919_000_003) % span
    return pl.DataFrame({"t": pl.Series(vals).cast(pl.Datetime("us"))})


def bench_dt_year(rows: int) -> Result:
    df = temporal_frame(rows)

    def run():
        _ = df.select(pl.col("t").dt.year())

    t = time_ns(run)
    return Result("DtYear", rows, t, rows * 8 / t * 1000.0)


def bench_dt_truncate(rows: int) -> Result:
    df = temporal_frame(rows)

    def run():
        _ = df.select(pl.col("t").dt.truncate("1h"))

    t = time_ns(run)
    return Result("DtTruncate(1h)", rows, t, rows * 8 / t * 1000.0)


def bench_strptime(rows: int) -> Result:
    df = temporal_frame(rows).select(pl.col("t").dt.strftime("%Y-%m-%d %H:%M:%S").alias("s"))

    def run():
        _ = df.select(pl.col("s").str.to_datetime("%Y-%m-%d %H:%M:%S"))

    t = time_ns(run)
    return Result("Strptime", rows, t, rows * 19 / t * 1000.0)


def bench_strftime(rows: int) -> Result:
    df = temporal_frame(rows)

    def run():
        _ = df.select(pl.col("t").dt.strftime("%Y-%m-%d %H:%M:%S"))

    t = time_ns(run)
    return Result("Strftime", rows, t, rows * 8 / t * 1000.0)


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


# ---- string namespace workloads (mirrors cmd/bench/strings.go) ----


def str_bench_values(rows: int) -> tuple[pl.Series, int]:
    vals = [f"k{i % 997}-v{i % 13}-x{i}" for i in range(rows)]
    return pl.Series("s", vals, dtype=pl.String), sum(len(v) for v in vals)


def bench_str_contains(rows: int) -> Result:
    """Literal substring test on 1M short strings."""
    s, nbytes = str_bench_values(rows)

    def run():
        _ = s.str.contains("v7", literal=True)

    t = time_ns(run)
    return Result("StrContainsShort", rows, t, nbytes / t * 1000.0)


def bench_str_split(rows: int) -> Result:
    """Split into List[String] on a one-byte separator."""
    s, nbytes = str_bench_values(rows)

    def run():
        _ = s.str.split("-")

    t = time_ns(run)
    return Result("StrSplit", rows, t, nbytes / t * 1000.0)


def string_benches() -> list[Result]:
    n = 1_048_576
    return [bench_str_contains(n), bench_str_split(n)]



# --- join_asof / join_where workloads -----------------------------------
# Inputs are arithmetic sequences (no RNG) so the Go harness builds
# byte-identical frames. Mirrored by benchJoinAsof* / benchJoinWhere in
# cmd/bench/main.go.


def _asof_frames(rows: int, by: bool):
    i = np.arange(rows, dtype=np.int64)
    m = rows // 4
    j = np.arange(m, dtype=np.int64)
    left = {"t": i * 3, "lv": i}
    right = {"t": j * 11 + 1, "rv": j}
    if by:
        left["g"] = i % 64
        right["g"] = j % 64
    return pl.DataFrame(left), pl.DataFrame(right)


def bench_join_asof(rows: int) -> Result:
    left, right = _asof_frames(rows, by=False)

    def run():
        _ = left.join_asof(right, on="t")

    t = time_ns(run)
    return Result("JoinAsof", rows, t, rows * 16 / t * 1000.0)


def bench_join_asof_by(rows: int) -> Result:
    left, right = _asof_frames(rows, by=True)

    def run():
        _ = left.join_asof(right, on="t", by="g")

    t = time_ns(run)
    return Result("JoinAsofBy", rows, t, rows * 24 / t * 1000.0)


def bench_join_where(rows: int) -> Result:
    # Range join: each left point falls in one or two right windows, so
    # the output stays near 1.5x the left height.
    i = np.arange(rows, dtype=np.int64)
    m = rows // 64
    j = np.arange(m, dtype=np.int64)
    left = pl.DataFrame({"a": (i * 7919) % rows, "lv": i})
    right = pl.DataFrame({"lo": j * 64, "hi": j * 64 + 96, "rv": j})

    def run():
        _ = left.join_where(right, pl.col("a") >= pl.col("lo"), pl.col("a") < pl.col("hi"))

    t = time_ns(run)
    return Result("JoinWhere", rows, t, rows * 16 / t * 1000.0)


# ---- core expression API (mirrored in cmd/bench/core_exprs.go) ----


def bench_cut(rows: int) -> Result:
    """cut into 10 bins over uniform floats."""
    rng = np.random.default_rng(42)
    df = pl.DataFrame({"x": rng.random(rows) * 100})
    breaks = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]

    def run():
        _ = df.select(pl.col("x").cut(breaks))

    t = time_ns(run)
    return Result("Cut(10 bins)", rows, t, rows * 8 / t * 1000.0)


def bench_qcut(rows: int) -> Result:
    """qcut into 10 quantile bins."""
    rng = np.random.default_rng(43)
    df = pl.DataFrame({"x": rng.random(rows) * 100})

    def run():
        _ = df.select(pl.col("x").qcut(10))

    t = time_ns(run)
    return Result("QCut(10)", rows, t, rows * 8 / t * 1000.0)


def bench_replace_strict(rows: int) -> Result:
    """replace_strict with 500 keys and a default."""
    rng = np.random.default_rng(44)
    df = pl.DataFrame({"k": rng.integers(0, 1000, size=rows, dtype=np.int64)})
    old = [i * 2 for i in range(500)]
    new = [i * 10 for i in range(500)]

    def run():
        _ = df.select(pl.col("k").replace_strict(old, new, default=pl.lit(-1, dtype=pl.Int64)))

    t = time_ns(run)
    return Result("ReplaceStrict(500 keys)", rows, t, rows * 8 / t * 1000.0)


def bench_filter_agg_group(rows: int) -> Result:
    """Per-group filtered sum inside group_by().agg()."""
    rng = np.random.default_rng(45)
    df = pl.DataFrame({
        "k": rng.integers(0, 64, size=rows, dtype=np.int64),
        "v": rng.integers(0, 1 << 20, size=rows, dtype=np.int64),
    })

    def run():
        _ = df.group_by("k").agg(pl.col("v").filter(pl.col("v") > (1 << 19)).sum().alias("s"))

    t = time_ns(run)
    return Result("FilterSumPerGroup(groups=64)", rows, t, rows * 16 / t * 1000.0)


def core_expr_benches() -> list[dict]:
    out = []
    for n in (262_144, 1_048_576):
        out.append(vars(bench_cut(n)))
        out.append(vars(bench_qcut(n)))
        out.append(vars(bench_replace_strict(n)))
    for n in (16_384, 262_144):
        out.append(vars(bench_filter_agg_group(n)))
    return out


# ---------------------------------------------------------------------------
# Workloads beyond the original numeric suite: strings, CSV and Parquet IO,
# string-keyed group-bys and joins, wide aggregations and a lazy pipeline.
# Twins live in cmd/bench/extra.go and bench/polars-rust/src/extra.rs with
# the same names, sizes and data shapes:
#   word(i)   = WORDS8[i % 8] + "_" + hex6((i * 2654435761) mod 2^24)
#   key(i, c) = "key_" + pad7(((i * 2654435761) mod 2^32) mod c)
# Throughput uses a nominal bytes-in figure: 8 per numeric cell and 16 per
# string cell.
# ---------------------------------------------------------------------------

EXTRA_ROWS = 1_048_576
WORDS8 = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]


def bench_words(n: int) -> list[str]:
    return [f"{WORDS8[i % 8]}_{(i * 2654435761) & 0xFFFFFF:06x}" for i in range(n)]


def bench_keys(n: int, card: int) -> list[str]:
    return [f"key_{((i * 2654435761) & 0xFFFFFFFF) % card:07d}" for i in range(n)]


def _numeric_frame(n: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    return pl.DataFrame({
        "a": rng.integers(0, 1_000_000, size=n, dtype=np.int64),
        "b": rng.integers(0, 1_000_000_000, size=n, dtype=np.int64),
        "c": rng.random(n),
        "d": rng.random(n),
    })


def _mixed_frame(n: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    return pl.DataFrame({
        "a": rng.integers(0, 1_000_000, size=n, dtype=np.int64),
        "b": rng.random(n),
        "s": bench_words(n),
        "k": bench_keys(n, 1000),
    })


def _parquet_frame(n: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    return pl.DataFrame({
        "a": rng.integers(0, 1_000_000, size=n, dtype=np.int64),
        "b": rng.integers(0, 1_000_000_000, size=n, dtype=np.int64),
        "c": rng.random(n),
        "s": bench_keys(n, 1000),
    })


def _bench_csv_read(name: str, df: pl.DataFrame, bytes_in: int) -> Result:
    import os
    import shutil
    import tempfile

    d = tempfile.mkdtemp(prefix="polars-bench-")
    try:
        path = os.path.join(d, "in.csv")
        df.write_csv(path)
        rows = df.height

        def run():
            _ = pl.read_csv(path)

        t = time_ns(run)
    finally:
        shutil.rmtree(d, ignore_errors=True)
    return Result(name, rows, t, bytes_in / t * 1000.0)


def bench_csv_read_numeric(n: int) -> Result:
    return _bench_csv_read("CSVReadNumeric", _numeric_frame(n), n * 32)


def bench_csv_read_mixed(n: int) -> Result:
    return _bench_csv_read("CSVReadMixed", _mixed_frame(n), n * 48)


def bench_parquet_read(n: int) -> Result:
    import os
    import shutil
    import tempfile

    d = tempfile.mkdtemp(prefix="polars-bench-")
    try:
        path = os.path.join(d, "in.parquet")
        _parquet_frame(n).write_parquet(path, compression="snappy")

        def run():
            _ = pl.read_parquet(path)

        t = time_ns(run)
    finally:
        shutil.rmtree(d, ignore_errors=True)
    return Result("ParquetRead", n, t, n * 40 / t * 1000.0)


def bench_parquet_write(n: int) -> Result:
    import os
    import shutil
    import tempfile

    d = tempfile.mkdtemp(prefix="polars-bench-")
    try:
        path = os.path.join(d, "out.parquet")
        df = _parquet_frame(n)

        def run():
            df.write_parquet(path, compression="snappy")

        t = time_ns(run)
    finally:
        shutil.rmtree(d, ignore_errors=True)
    return Result("ParquetWrite", n, t, n * 40 / t * 1000.0)


def bench_groupby_str_key(n: int, groups: int) -> Result:
    rng = np.random.default_rng(44)
    df = pl.DataFrame({"k": bench_keys(n, groups), "v": rng.integers(0, 1 << 20, size=n, dtype=np.int64)})

    def run():
        _ = df.group_by("k").agg(pl.col("v").sum().alias("s"))

    t = time_ns(run)
    return Result(f"GroupByStrKey(groups={groups})", n, t, n * 24 / t * 1000.0)


def _bench_str_unary(name: str, n: int, e: pl.Expr) -> Result:
    df = pl.DataFrame({"s": bench_words(n)})

    def run():
        _ = df.select(e)

    t = time_ns(run)
    return Result(name, n, t, n * 16 / t * 1000.0)


def bench_str_contains(n: int) -> Result:
    return _bench_str_unary("StrContains", n, pl.col("s").str.contains("ta", literal=True))


def bench_str_to_uppercase(n: int) -> Result:
    return _bench_str_unary("StrToUppercase", n, pl.col("s").str.to_uppercase())


def bench_str_len_bytes(n: int) -> Result:
    return _bench_str_unary("StrLenBytes", n, pl.col("s").str.len_bytes())


def bench_str_len_chars(n: int) -> Result:
    return _bench_str_unary("StrLenChars", n, pl.col("s").str.len_chars())


def bench_inner_join_str(n: int) -> Result:
    rng = np.random.default_rng(42)
    left = pl.DataFrame({
        "k": [f"k{i:07d}" for i in range(n)],
        "lv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    right = pl.DataFrame({
        "k": [f"k{(i * 7919) % n:07d}" for i in range(n)],
        "rv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })

    def run():
        _ = left.join(right, on="k", how="inner")

    t = time_ns(run)
    return Result("InnerJoinStr", n, t, n * 24 / t * 1000.0)


def bench_groupby_many_aggs(n: int) -> Result:
    rng = np.random.default_rng(41)
    df = pl.DataFrame({
        "k": rng.integers(0, 1024, size=n, dtype=np.int64),
        "v": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
        "w": rng.random(n),
    })
    aggs = [
        pl.col("v").sum().alias("v_sum"),
        pl.col("v").mean().alias("v_mean"),
        pl.col("v").min().alias("v_min"),
        pl.col("v").max().alias("v_max"),
        pl.col("v").count().alias("v_count"),
        pl.col("w").first().alias("w_first"),
        pl.col("w").last().alias("w_last"),
        pl.col("w").mean().alias("w_mean"),
        pl.col("w").min().alias("w_min"),
        pl.col("w").max().alias("w_max"),
    ]

    def run():
        _ = df.group_by("k").agg(aggs)

    t = time_ns(run)
    return Result("GroupByManyAggs(groups=1024)", n, t, n * 24 / t * 1000.0)


def bench_sort_str(n: int) -> Result:
    df = pl.DataFrame({"s": bench_words(n)})

    def run():
        _ = df.sort("s")

    t = time_ns(run)
    return Result("SortStr", n, t, n * 16 / t * 1000.0)


def bench_unique_str(n: int) -> Result:
    df = pl.DataFrame({"k": bench_keys(n, n // 4)})

    def run():
        _ = df.unique()

    t = time_ns(run)
    return Result("UniqueStr", n, t, n * 16 / t * 1000.0)


def bench_lazy_pipeline(n: int) -> Result:
    rng = np.random.default_rng(42)
    df = pl.DataFrame({
        "s": bench_keys(n, 16),
        "a": rng.integers(0, 1_000_000, size=n, dtype=np.int64),
        "b": rng.random(n),
    })

    def run():
        _ = (
            df.lazy()
            .filter(pl.col("a") > 500_000)
            .with_columns((pl.col("a") * 3).alias("a3"), (pl.col("b") * 2.0).alias("b2"))
            .group_by("s")
            .agg(
                pl.col("a3").sum().alias("sa"),
                pl.col("b2").mean().alias("mb"),
                pl.col("a").count().alias("n"),
            )
            .sort("sa", descending=True)
            .collect()
        )

    t = time_ns(run)
    return Result("LazyPipeline", n, t, n * 32 / t * 1000.0)


def add_extra_workloads(add) -> None:
    n = EXTRA_ROWS
    add("CSVReadNumeric", lambda: bench_csv_read_numeric(n))
    add("CSVReadMixed", lambda: bench_csv_read_mixed(n))
    add("ParquetRead", lambda: bench_parquet_read(n))
    add("ParquetWrite", lambda: bench_parquet_write(n))
    add("GroupByStrKey(groups=16)", lambda: bench_groupby_str_key(n, 16))
    add("GroupByStrKey(groups=100000)", lambda: bench_groupby_str_key(n, 100000))
    add("StrContains", lambda: bench_str_contains(n))
    add("StrToUppercase", lambda: bench_str_to_uppercase(n))
    add("StrLenBytes", lambda: bench_str_len_bytes(n))
    add("StrLenChars", lambda: bench_str_len_chars(n))
    add("InnerJoinStr", lambda: bench_inner_join_str(n))
    add("GroupByManyAggs(groups=1024)", lambda: bench_groupby_many_aggs(n))
    add("SortStr", lambda: bench_sort_str(n))
    add("UniqueStr", lambda: bench_unique_str(n))
    add("LazyPipeline", lambda: bench_lazy_pipeline(n))



# Join workloads beyond inner and left joins. cmd/bench/joins_extra.go and
# bench/polars-rust/src/joins_extra.rs build the same frames.


def _join_perm(n: int) -> np.ndarray:
    return (np.arange(n, dtype=np.int64) * 7919) % n


def _run_join(name: str, n: int, bytes_in: int, left, right, on, how: str) -> Result:
    def run():
        _ = left.join(right, on=on, how=how)

    t = time_ns(run)
    return Result(name, n, t, bytes_in / t * 1000.0)


def bench_full_join(n: int) -> Result:
    rng = np.random.default_rng(42)
    left = pl.DataFrame({
        "id": np.arange(n, dtype=np.int64),
        "lv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    right = pl.DataFrame({
        "id": _join_perm(n) + n // 2,
        "rv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    return _run_join("FullJoin", n, n * 32, left, right, "id", "full")


def _semi_anti_frames(n: int):
    rng = np.random.default_rng(44)
    left = pl.DataFrame({
        "id": rng.integers(0, 2 * n, size=n, dtype=np.int64),
        "lv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    right = pl.DataFrame({"id": rng.integers(0, n, size=n, dtype=np.int64)})
    return left, right


def bench_semi_join(n: int) -> Result:
    left, right = _semi_anti_frames(n)
    return _run_join("SemiJoin", n, n * 24, left, right, "id", "semi")


def bench_anti_join(n: int) -> Result:
    left, right = _semi_anti_frames(n)
    return _run_join("AntiJoin", n, n * 24, left, right, "id", "anti")


def bench_inner_join_2key(n: int) -> Result:
    rng = np.random.default_rng(42)
    i = np.arange(n, dtype=np.int64)
    p = _join_perm(n)
    left = pl.DataFrame({
        "a": i % 1024,
        "b": i // 1024,
        "lv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    right = pl.DataFrame({
        "a": p % 1024,
        "b": p // 1024,
        "rv": rng.integers(0, 1 << 20, size=n, dtype=np.int64),
    })
    return _run_join("InnerJoin2Key", n, n * 48, left, right, ["a", "b"], "inner")


def add_join_workloads(add) -> None:
    n = EXTRA_ROWS
    add("FullJoin", lambda: bench_full_join(n))
    add("SemiJoin", lambda: bench_semi_join(n))
    add("AntiJoin", lambda: bench_anti_join(n))
    add("InnerJoin2Key", lambda: bench_inner_join_2key(n))


def main() -> int:
    import argparse
    import re

    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="run only workloads whose name matches this regexp")
    args = ap.parse_args()
    only = re.compile(args.only) if args.only else None

    SIZES = [16_384, 262_144, 1_048_576]
    out: list[dict] = []

    def add(name, fn):
        # fn builds its own inputs, so filtered-out workloads cost nothing.
        if only is not None and not only.search(name):
            return
        out.append(vars(fn()))

    for n in SIZES:
        add("SumInt64", lambda: bench_sum_int64(n))
        add("SumFloat64", lambda: bench_sum_float64(n))
        add("MeanFloat64", lambda: bench_mean_float64(n))
        add("MinFloat64", lambda: bench_min_float64(n))
        add("AddInt64", lambda: bench_add_int64(n))
        add("AddFloat64", lambda: bench_add_float64(n))
        add("MulInt64", lambda: bench_mul_int64(n))
        add("GtInt64", lambda: bench_gt_int64(n))
        add("FilterInt64", lambda: bench_filter_int64(n))
        add("FilterFloat64", lambda: bench_filter_float64(n))
        add("SortInt64", lambda: bench_sort_int64(n))
        add("SortFloat64", lambda: bench_sort_float64(n))
        add("CastI64ToF64", lambda: bench_cast_i64_f64(n))
        add("Take", lambda: bench_take(n))

    for n in (16_384, 262_144):
        add("SortTwoKeys", lambda: bench_sort_two_keys(n))

    for n in (16_384, 262_144):
        for g in (8, 1024):
            add(f"GroupBySum(groups={g})", lambda: bench_groupby_sum(n, g))
        for g in (64,):
            add(f"GroupByMean(groups={g})", lambda: bench_groupby_mean(n, g))
            add(f"GroupByMultiAgg(groups={g})", lambda: bench_groupby_multi_agg(n, g))
        add("GroupBySumMultiKey", lambda: bench_groupby_sum_multikey(n))

    for n in (16_384, 262_144):
        add("InnerJoin", lambda: bench_inner_join(n))
        add("LeftJoin", lambda: bench_left_join(n))

    for n in (16_384, 262_144):
        out.append(vars(bench_join_asof(n)))
        out.append(vars(bench_join_asof_by(n)))
        out.append(vars(bench_join_where(n)))

    for n in (16_384, 262_144):
        out.append(vars(bench_pipeline(n)))
        add("Pipeline(filter>gb>sort)", lambda: bench_pipeline(n))

    for n in SIZES:
        add("SumHorizontal(3cols)", lambda: bench_sum_horizontal(n))

    for n in SIZES:
        add("MaxHorizontal(3cols)", lambda: bench_max_horizontal(n))

    for n in (16_384, 262_144):
        add("UniqueInt64", lambda: bench_unique_int64(n))
        add("TopK(k=10)", lambda: bench_top_k(n, 10))
        add("RankInt64", lambda: bench_rank(n))

    for n in SIZES:
        add("CumSumInt64", lambda: bench_cumsum_int64(n))
        add("ShiftInt64", lambda: bench_shift_int64(n))
        add("FillNullValue", lambda: bench_fill_null_value(n))
        add("DropNulls", lambda: bench_drop_nulls(n))

    for n in SIZES:
        add("ForwardFillInt64", lambda: bench_forward_fill(n))

    for n in SIZES:
        add("RollingSum(w=32)", lambda: bench_rolling_sum(n))
        add("RollingMin(w=32)", lambda: bench_rolling_min(n))
        add("RollingMax(w=32)", lambda: bench_rolling_max(n))
        add("RollingMean(w=32)", lambda: bench_rolling_mean(n))

    for n in (16_384, 262_144):
        add("WhenThenOtherwise", lambda: bench_when_then(n))
        add("SumOverGroup", lambda: bench_over_sum(n))
        add("CumSumOverGroup", lambda: bench_cumsum_over_group(n))

    # Workloads beyond the original numeric suite (strings, IO, lazy).
    add_extra_workloads(add)
    add_join_workloads(add)

    for r in string_benches():
        out.append(vars(r))

    for n in SIZES:
        out.append(vars(bench_dt_year(n)))
        out.append(vars(bench_dt_truncate(n)))
        out.append(vars(bench_strptime(n)))
        out.append(vars(bench_strftime(n)))

    out.extend(core_expr_benches())

    json.dump({"engine": "polars", "version": pl.__version__, "runs": out}, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
