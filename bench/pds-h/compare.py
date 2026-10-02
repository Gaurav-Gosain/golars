"""Run the golars PDS-H queries and their polars equivalents on the same
parquet data, check that the answers agree, and print per-query
timings, ratios and peak memory.

The polars queries are the polars-benchmark ones (queries/polars/q*.py).
Where golars needs a different but equivalent formulation (Q4 unique
subset, Q5 two-key join), the golars side documents it in its q*.go.

Run from the repo root, reusing the polars-compare uv project:

    go run ./bench/pds-h/gen -sf 1 -out /tmp/pdsh
    uv run --project bench/polars-compare python bench/pds-h/compare.py \\
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
from datetime import date
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal


def scan(data: Path, table: str) -> pl.LazyFrame:
    return pl.scan_parquet(data / f"{table}.parquet")


def disc_price() -> pl.Expr:
    return pl.col("l_extendedprice") * (1 - pl.col("l_discount"))


def q1(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "lineitem")
        .filter(pl.col("l_shipdate") <= date(1998, 9, 2))
        .group_by("l_returnflag", "l_linestatus")
        .agg(
            pl.sum("l_quantity").alias("sum_qty"),
            pl.sum("l_extendedprice").alias("sum_base_price"),
            disc_price().sum().alias("sum_disc_price"),
            (disc_price() * (1.0 + pl.col("l_tax"))).sum().alias("sum_charge"),
            pl.mean("l_quantity").alias("avg_qty"),
            pl.mean("l_extendedprice").alias("avg_price"),
            pl.mean("l_discount").alias("avg_disc"),
            pl.len().alias("count_order"),
        )
        .sort("l_returnflag", "l_linestatus")
    )


def q3(d: Path) -> pl.LazyFrame:
    cutoff = date(1995, 3, 15)
    return (
        scan(d, "customer")
        .filter(pl.col("c_mktsegment") == "BUILDING")
        .join(scan(d, "orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan(d, "lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < cutoff)
        .filter(pl.col("l_shipdate") > cutoff)
        .with_columns(disc_price().alias("revenue"))
        .group_by("o_orderkey", "o_orderdate", "o_shippriority")
        .agg(pl.sum("revenue"))
        .select(pl.col("o_orderkey").alias("l_orderkey"), "revenue", "o_orderdate", "o_shippriority")
        .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        .head(10)
    )


def q4(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "lineitem")
        .join(scan(d, "orders"), left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate").is_between(date(1993, 7, 1), date(1993, 10, 1), closed="left"))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort("o_orderpriority")
    )


def q5(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "region")
        .join(scan(d, "nation"), left_on="r_regionkey", right_on="n_regionkey")
        .join(scan(d, "customer"), left_on="n_nationkey", right_on="c_nationkey")
        .join(scan(d, "orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan(d, "lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(scan(d, "supplier"), left_on=["l_suppkey", "n_nationkey"], right_on=["s_suppkey", "s_nationkey"])
        .filter(pl.col("r_name") == "ASIA")
        .filter(pl.col("o_orderdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
        .with_columns(disc_price().alias("revenue"))
        .group_by("n_name")
        .agg(pl.sum("revenue"))
        .sort(by="revenue", descending=True)
    )


def q6(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "lineitem")
        .filter(pl.col("l_shipdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < 24)
        .select((pl.col("l_extendedprice") * pl.col("l_discount")).sum().alias("revenue"))
    )


def q10(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "customer")
        .join(scan(d, "orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan(d, "lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(scan(d, "nation"), left_on="c_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(date(1993, 10, 1), date(1994, 1, 1), closed="left"))
        .filter(pl.col("l_returnflag") == "R")
        .group_by("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
        .agg(disc_price().sum().round(2).alias("revenue"))
        .select("c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
        .sort(by="revenue", descending=True)
        .head(20)
    )


def q12(d: Path) -> pl.LazyFrame:
    high = pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])
    return (
        scan(d, "orders")
        .join(scan(d, "lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("l_shipmode").is_in(["MAIL", "SHIP"]))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
        .filter(pl.col("l_receiptdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
        .with_columns(
            pl.when(high).then(1).otherwise(0).alias("high_line_count"),
            pl.when(high.not_()).then(1).otherwise(0).alias("low_line_count"),
        )
        .group_by("l_shipmode")
        .agg(pl.col("high_line_count").sum(), pl.col("low_line_count").sum())
        .sort("l_shipmode")
    )


def q14(d: Path) -> pl.LazyFrame:
    return (
        scan(d, "lineitem")
        .join(scan(d, "part"), left_on="l_partkey", right_on="p_partkey")
        .filter(pl.col("l_shipdate").is_between(date(1995, 9, 1), date(1995, 10, 1), closed="left"))
        .select(
            (
                100.00
                * pl.when(pl.col("p_type").str.contains("PROMO*")).then(disc_price()).otherwise(0).sum()
                / disc_price().sum()
            )
            .round(2)
            .alias("promo_revenue")
        )
    )


def q19(d: Path) -> pl.LazyFrame:
    def arm(brand, containers, qlo, qhi, smax):
        return (
            (pl.col("p_brand") == brand)
            & pl.col("p_container").is_in(containers)
            & pl.col("l_quantity").is_between(qlo, qhi)
            & pl.col("p_size").is_between(1, smax)
        )

    return (
        scan(d, "part")
        .join(scan(d, "lineitem"), left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_shipmode").is_in(["AIR", "AIR REG"]))
        .filter(pl.col("l_shipinstruct") == "DELIVER IN PERSON")
        .filter(
            arm("Brand#12", ["SM CASE", "SM BOX", "SM PACK", "SM PKG"], 1, 11, 5)
            | arm("Brand#23", ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], 10, 20, 10)
            | arm("Brand#34", ["LG CASE", "LG BOX", "LG PACK", "LG PKG"], 20, 30, 15)
        )
        .select(disc_price().sum().round(2).alias("revenue"))
    )


QUERIES = {1: q1, 3: q3, 4: q4, 5: q5, 6: q6, 10: q10, 12: q12, 14: q14, 19: q19}


def time_polars(fn, data: Path, runs: int) -> float:
    fn(data).collect()  # warm the page cache and the thread pool
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn(data).collect()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def run_golars(binary: Path, q: int, data: Path, runs: int, dump: Path | None = None) -> tuple[float, int]:
    """Returns (median seconds, peak RSS bytes of the pdsh process)."""
    with tempfile.TemporaryDirectory() as tmp:
        cmd = [str(binary), "-q", str(q), "-data", str(data),
               "-repeats", str(runs + 1), "-out", os.path.join(tmp, "t.csv")]
        if dump is not None:
            cmd += ["-dump", str(dump)]
        with tempfile.TemporaryFile() as out:
            proc = subprocess.Popen(cmd, stdout=out)
            _, status, ru = os.wait4(proc.pid, 0)
            if os.waitstatus_to_exitcode(status) != 0:
                raise RuntimeError(f"pdsh q{q} failed")
            out.seek(0)
            text = out.read().decode()
    samples = []
    for line in text.splitlines():
        m = re.search(r"rep=(\d+)\s+(\S+)", line)
        if m and int(m.group(1)) > 0:  # rep 0 is the warmup
            samples.append(parse_go_duration(m.group(2)))
    if not samples and runs > 0:
        raise RuntimeError(f"pdsh q{q} produced no timings:\n{text}")
    return (statistics.median(samples) if samples else 0.0), ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024)


def polars_peak_rss(q: int, data: Path) -> int:
    """Peak RSS of a fresh python process that runs one polars query."""
    code = (
        "import sys; sys.path.insert(0, %r); import compare, pathlib; "
        "compare.QUERIES[%d](pathlib.Path(%r)).collect()" % (str(Path(__file__).parent), q, str(data))
    )
    proc = subprocess.Popen([sys.executable, "-B", "-c", code])
    _, status, ru = os.wait4(proc.pid, 0)
    if os.waitstatus_to_exitcode(status) != 0:
        raise RuntimeError(f"polars q{q} failed")
    return ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024)


def python_baseline_rss() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "import polars"])
    _, _, ru = os.wait4(proc.pid, 0)
    return ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024)


def parse_go_duration(s: str) -> float:
    for suffix, scale in (("ms", 1e-3), ("µs", 1e-6), ("us", 1e-6), ("ns", 1e-9), ("s", 1.0)):
        if s.endswith(suffix):
            return float(s[: -len(suffix)]) * scale
    raise ValueError(s)


def check_answer(q: int, data: Path, got_path: Path) -> str:
    want = QUERIES[q](data).collect()
    got = pl.read_parquet(got_path)
    try:
        # Group-by output order is unspecified unless the query sorts on
        # a unique key; compare sorted on every column for those.
        assert_frame_equal(got, want, check_dtypes=False, rel_tol=1e-9, abs_tol=1e-6)
        return "ok"
    except AssertionError:
        try:
            assert_frame_equal(got.sort(got.columns), want.sort(want.columns),
                               check_dtypes=False, rel_tol=1e-9, abs_tol=1e-6)
            return "ok (row order differs)"
        except AssertionError as e:
            return "MISMATCH: " + str(e).splitlines()[0]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, type=Path)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--queries", default=",".join(str(q) for q in QUERIES))
    p.add_argument("--check", action="store_true", help="only check golars answers against polars")
    args = p.parse_args()

    repo = Path(__file__).resolve().parents[2]
    # Build next to the data so every artifact of a run lives in one
    # directory the caller controls.
    binary = args.data / "pdsh"
    subprocess.check_call(["go", "build", "-o", str(binary), "./bench/pds-h/cmd/pdsh"], cwd=repo)
    qs = [int(x) for x in args.queries.split(",")]

    if args.check:
        with tempfile.TemporaryDirectory() as tmp:
            bad = 0
            for q in qs:
                run_golars(binary, q, args.data, 0, dump=Path(tmp))
                verdict = check_answer(q, args.data, Path(tmp) / f"q{q}.parquet")
                bad += not verdict.startswith("ok")
                print(f"q{q:<3} {verdict}")
        return 1 if bad else 0

    base = python_baseline_rss()
    print(f"{'query':<6} {'polars':>10} {'golars':>10} {'speedup':>8} {'pl-rss':>9} {'g-rss':>9} {'mem':>7}")
    speed, mem = [], []
    for q in qs:
        tp = time_polars(QUERIES[q], args.data, args.runs)
        tg, _ = run_golars(binary, q, args.data, args.runs)
        # Peak RSS from a process that runs the query once, like the
        # polars measurement; repeated runs in one Go process keep the
        # heap high-water mark of every run.
        _, g_rss = run_golars(binary, q, args.data, 0)
        p_rss = polars_peak_rss(q, args.data) - base
        speed.append(tp / tg)
        mem.append(g_rss / max(p_rss, 1))
        print(f"q{q:<5} {tp * 1e3:>8.1f}ms {tg * 1e3:>8.1f}ms {tp / tg:>7.2f}x"
              f" {p_rss / 2**20:>7.0f}MB {g_rss / 2**20:>7.0f}MB {g_rss / max(p_rss, 1):>6.2f}x")
    print(f"median speedup {statistics.median(speed):.2f}x, median memory {statistics.median(mem):.2f}x")
    print(f"polars {pl.__version__}; speedup > 1 means golars is faster; mem = golars peak RSS "
          "over polars peak RSS, one query per process (polars minus its import footprint)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
