"""Compare polars-py vs polars-rs vs golars (scalar and simd) on a
shared workload suite.

Each of the four engines is built on demand:
  go build   -o bin/golars-bench       ./cmd/bench/
  GOEXPERIMENT=simd go build -o bin/golars-bench-simd ./cmd/bench/
  cargo build --release --manifest-path bench/polars-rust/Cargo.toml

By default the script runs each engine 3 times end-to-end, drops the
worst run per engine (outlier rejection), and reports the median ratio
per workload across the remaining runs. This trims most OS-scheduling
noise on sub-millisecond workloads. Engine order rotates (and reverses)
every repetition so thermal/load drift across the session cancels out
instead of penalising whoever runs last.

Run:
  uv run python compare.py              # default: 3 runs, drop 1
  uv run python compare.py --runs 5     # more runs, smoother signal
  uv run python compare.py --runs 1     # quick single-run check
  uv run python compare.py --rss        # peak RSS per workload instead

Memory. golars and polars-rs report the heap bytes one call of each
workload allocates (alloc_bytes; polars-rs also reports peak_bytes,
the high-water mark of live memory the call added). The table prints
golars simd alloc bytes, polars-rs alloc and peak bytes, and m/rs =
golars alloc / polars-rs alloc (above 1 means golars allocates more).
polars-py cannot hook the allocator of its compiled extension, so it
only appears in the --rss view, which runs every workload in its own
process per engine and reads the peak resident set size of each.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def _noise_wrap(cmd: list[str], *, cores: str = "", nice: int = 0) -> list[str]:
    """Wrap a command with taskset + nice to reduce OS-noise variance.

    On Linux, taskset -c PIN pins to physical cores (avoid SMT noise);
    nice -n NICE raises scheduling priority. Both are no-ops if the
    tool is missing - falls through to the original command.
    """
    if sys.platform != "linux":
        return cmd
    wrapped = []
    if nice:
        nice_bin = shutil.which("nice")
        if nice_bin:
            wrapped.extend([nice_bin, "-n", str(nice)])
    if cores:
        taskset_bin = shutil.which("taskset")
        if taskset_bin:
            wrapped.extend([taskset_bin, "-c", cores])
    return wrapped + cmd


def run(cmd, cwd, env=None):
    return subprocess.check_output(cmd, cwd=cwd, text=True, env=env)


def aggregate_runs(runs, how="median_drop_worst"):
    """Collapse a list of per-invocation JSON runs into a single
    result set, keyed on (workload, rows).

    Strategies:
      - "median_drop_worst": drop the slowest run, take median of the
        rest. Cheap IQR-style outlier rejection.
      - "min": take the slowest run. Conservative - "our worst".
      - "max": take the fastest run. Optimistic - "our best".
      - "median": plain median across all runs.

    The ratio printing in main() uses two aggregations: `simd_worst`
    and `polars_best` to compute a *conservative* ratio
    (our_worst_throughput / their_best_throughput). A conservative win
    here means "even on our noisiest run we beat their fastest run".
    """
    if len(runs) == 1:
        return runs[0]
    by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in runs:
        for entry in r["runs"]:
            by_key[(entry["name"], entry["rows"])].append(entry)

    def pick(entries, how):
        entries = sorted(entries, key=lambda e: e["throughput_mbps"])
        match how:
            case "min":
                return entries[0]
            case "max":
                return entries[-1]
            case "median":
                return entries[len(entries) // 2]
            case "median_drop_worst":
                kept = entries[1:] if len(entries) > 1 else entries
                return kept[len(kept) // 2]
        raise ValueError(f"unknown agg: {how}")

    merged = []
    for key, entries in by_key.items():
        e = pick(entries, how)
        row = {
            "name": key[0],
            "rows": key[1],
            "median_ns": int(e["median_ns"]),
            "throughput_mbps": float(e["throughput_mbps"]),
        }
        # Allocation figures barely move between runs; the median over
        # runs discards the odd one that caught a lazy init.
        for field in ("alloc_bytes", "allocs", "peak_bytes"):
            vals = sorted(x[field] for x in entries if x.get(field) is not None)
            if vals:
                row[field] = vals[len(vals) // 2]
        merged.append(row)
    return {
        "engine": runs[0].get("engine", ""),
        "version": runs[0].get("version", ""),
        "runs": merged,
    }


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs", type=int, default=3,
                   help="invocations per engine (default 3)")
    p.add_argument("--skip-build", action="store_true",
                   help="reuse already-built binaries")
    p.add_argument("--pin-cores", default="",
                   help="taskset -c value for pinning (e.g. '0-7' for 8 physical cores). "
                        "Empty = don't pin. Reduces SMT / background-load variance on Intel.")
    p.add_argument("--nice", type=int, default=0,
                   help="nice level for bench processes (negative = higher priority; "
                        "-5 requires root on most systems)")
    p.add_argument("--only", default="",
                   help="regexp; run only workloads whose name matches (passed to all engines)")
    p.add_argument("--rss", action="store_true",
                   help="measure peak RSS per workload (one process per workload and engine) "
                        "instead of throughput")
    return p.parse_args()


def fmt_bytes(n) -> str:
    if n is None:
        return "-"
    n = float(n)
    if abs(n) < 1024:
        return f"{n:.0f}B"
    for unit in ("K", "M", "G"):
        n /= 1024
        if abs(n) < 1024 or unit == "G":
            return f"{n:.1f}{unit}"
    return f"{n:.1f}G"


def run_rss(cmd: list[str], cwd) -> int:
    """Run cmd to completion and return its peak resident set size in
    bytes, taken from the rusage of that one child."""
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _, status, ru = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    return ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024)


def with_only(cmd: list[str], pattern: str) -> list[str]:
    cmd = list(cmd)
    if "--only" in cmd:
        i = cmd.index("--only")
        del cmd[i:i + 2]
    return cmd + ["--only", pattern]


def rss_main(args, engines, base) -> int:
    """Peak RSS view: one process per (workload, engine). Each harness
    runs every size of the named workload, so the peak belongs to the
    largest size. A process that matches no workload gives each
    runtime's idle footprint (interpreter, thread pools), which is
    subtracted so the numbers show what the workload added: its inputs,
    the operation's working memory and, for golars, the GC headroom."""
    import re

    cmd, cwd = engines["simd"]
    listing = json.loads(run(with_only(cmd, args.only or "."), cwd))
    names: dict[str, int] = {}
    for r in listing["runs"]:
        names[r["name"]] = max(names.get(r["name"], 0), r["rows"])

    baseline = {}
    for name in base:
        cmd, cwd = engines[name]
        baseline[name] = min(run_rss(with_only(cmd, "^$nothing"), cwd) for _ in range(2))

    print()
    print(f"{'workload':<28} {'rows':>10} {'pl-py':>9} {'pl-rs':>9} {'simd':>9} {'m/py':>7} {'m/rs':>7}")
    print("(peak RSS above each engine's idle baseline, at the largest size of the workload)")
    print("-" * 86)
    ratios = []
    for wl, rows in names.items():
        pattern = "^" + re.escape(wl) + "$"
        rss = {}
        for name in base:
            cmd, cwd = engines[name]
            rss[name] = max(run_rss(with_only(cmd, pattern), cwd) - baseline[name], 1)
        m_py = rss["simd"] / rss["py"]
        m_rs = rss["simd"] / rss["rs"]
        ratios.append(m_rs)
        print(f"{wl:<28} {rows:>10,} {fmt_bytes(rss['py']):>9} {fmt_bytes(rss['rs']):>9}"
              f" {fmt_bytes(rss['simd']):>9} {m_py:>6.2f}x {m_rs:>6.2f}x")
    print()
    print("idle baseline: " + ", ".join(f"{k}={fmt_bytes(v)}" for k, v in baseline.items()))
    if ratios:
        print(f"median simd/rs peak RSS: {statistics.median(ratios):.2f}x (above 1 means golars uses more)")
    return 0


def main() -> int:
    args = parse_args()
    here = Path(__file__).resolve().parent
    repo = here.parent.parent

    bin_dir = repo / "bin"
    bin_dir.mkdir(exist_ok=True)

    if not args.skip_build:
        subprocess.check_call(
            ["go", "build", "-o", str(bin_dir / "golars-bench"), "./cmd/bench/"],
            cwd=repo,
        )
        subprocess.check_call(
            ["go", "build", "-o", str(bin_dir / "golars-bench-simd"), "./cmd/bench/"],
            cwd=repo,
            env={**os.environ, "GOEXPERIMENT": "simd"},
        )
        rust_dir = repo / "bench" / "polars-rust"
        subprocess.check_call(
            ["cargo", "build", "--release", "--manifest-path", str(rust_dir / "Cargo.toml")],
            cwd=rust_dir,
        )

    rust_dir = repo / "bench" / "polars-rust"
    # cargo honors CARGO_TARGET_DIR; look for the binary where cargo put it.
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR") or (rust_dir / "target"))
    rust_bin = target_dir / "release" / "polars-rust-bench"

    nr = args.runs

    # Optionally pin all four engines to a fixed core set and elevate
    # priority. Dramatically cuts variance on sub-ms workloads at the
    # cost of running everything a bit faster than "realistic" - fine
    # for comparative benchmarking.
    def wrap(cmd: list[str]) -> list[str]:
        return _noise_wrap(cmd, cores=args.pin_cores, nice=args.nice)

    only = ["--only", args.only] if args.only else []
    engines = {
        "py": (wrap(["uv", "run", "python", "bench.py", *only]), here),
        "rs": (wrap([str(rust_bin), *only]), here),
        "scalar": (wrap([str(bin_dir / "golars-bench"), *only]), repo),
        "simd": (wrap([str(bin_dir / "golars-bench-simd"), *only]), repo),
    }
    base = ["py", "rs", "scalar", "simd"]
    if args.rss:
        return rss_main(args, engines, ["py", "rs", "simd"])
    runs: dict[str, list] = {name: [] for name in base}
    for r in range(nr):
        order = base[r:] + base[:r]
        if r % 2 == 1:
            order = order[::-1]
        for name in order:
            cmd, cwd = engines[name]
            print(f"[rep {r + 1}/{nr}] engine={name}", flush=True)
            runs[name].append(json.loads(run(cmd, cwd=cwd)))
    polars_py_runs = runs["py"]
    polars_rs_runs = runs["rs"]
    scalar_runs = runs["scalar"]
    simd_runs = runs["simd"]

    # Two aggregations we report alongside:
    #   typical: median-with-worst-dropped across runs. Stable-case view.
    #   worst:   minimum throughput per workload. "Always" view - if
    #            this wins, we beat them on our noisiest run.
    py_typical = aggregate_runs(polars_py_runs, "median_drop_worst")
    rs_typical = aggregate_runs(polars_rs_runs, "median_drop_worst")
    simd_typical = aggregate_runs(simd_runs, "median_drop_worst")
    scalar_typical = aggregate_runs(scalar_runs, "median_drop_worst")

    # For the conservative "even worst run wins" view, pair our
    # slowest sample against polars' fastest sample.
    py_best = aggregate_runs(polars_py_runs, "max")
    rs_best = aggregate_runs(polars_rs_runs, "max")
    simd_worst = aggregate_runs(simd_runs, "min")

    def idx(frame):
        return {(r["name"], r["rows"]): r for r in frame["runs"]}

    py_t, rs_t, sc_t, v_t = idx(py_typical), idx(rs_typical), idx(scalar_typical), idx(simd_typical)
    py_b, rs_b = idx(py_best), idx(rs_best)
    v_w = idx(simd_worst)
    keys = sorted(py_t.keys() & rs_t.keys() & sc_t.keys() & v_t.keys())

    print()
    header_tag = f"(typical=median of {nr} runs, worst drop; conservative=our worst vs their best)" if nr > 1 else ""
    print(
        f"{'workload':<28} {'rows':>10}"
        f" {'pl-py':>10} {'pl-rs':>10} {'scalar':>10} {'simd':>10}"
        f" {'s/py':>7} {'s/rs':>7} {'c/py':>7} {'c/rs':>7}"
        f" {'g-alloc':>8} {'rs-alloc':>8} {'rs-peak':>8} {'m/rs':>7}"
    )
    print(header_tag)
    print("-" * 168)
    wins_py = wins_rs = 0
    cons_wins_py = cons_wins_rs = 0
    total = 0
    mem_ratios: list[tuple[float, str]] = []
    for name, rows in keys:
        p_py, p_rs, ss, vv = py_t[(name, rows)], rs_t[(name, rows)], sc_t[(name, rows)], v_t[(name, rows)]
        ratio_py = vv["throughput_mbps"] / p_py["throughput_mbps"] if p_py["throughput_mbps"] else 0
        ratio_rs = vv["throughput_mbps"] / p_rs["throughput_mbps"] if p_rs["throughput_mbps"] else 0
        # Conservative: our slowest run vs their fastest run.
        cp_py, cp_rs, cv = py_b[(name, rows)], rs_b[(name, rows)], v_w[(name, rows)]
        cons_py = cv["throughput_mbps"] / cp_py["throughput_mbps"] if cp_py["throughput_mbps"] else 0
        cons_rs = cv["throughput_mbps"] / cp_rs["throughput_mbps"] if cp_rs["throughput_mbps"] else 0
        if ratio_py >= 1.0:
            wins_py += 1
        if ratio_rs >= 1.0:
            wins_rs += 1
        if cons_py >= 1.0:
            cons_wins_py += 1
        if cons_rs >= 1.0:
            cons_wins_rs += 1
        total += 1
        g_alloc, rs_alloc = vv.get("alloc_bytes"), p_rs.get("alloc_bytes")
        # Ratios of tiny figures (a few hundred bytes of bookkeeping) say
        # nothing; below 64 KiB on both sides the column shows "-".
        mem_ratio = None
        if g_alloc is not None and rs_alloc is not None and max(g_alloc, rs_alloc) >= 64 << 10:
            mem_ratio = g_alloc / max(rs_alloc, 1)
            mem_ratios.append((mem_ratio, f"{name}({rows:,})"))
        mem_cell = f"{mem_ratio:>6.2f}x" if mem_ratio is not None else f"{'-':>7}"
        print(
            f"{name:<28} {rows:>10,}"
            f" {p_py['throughput_mbps']:>8,.0f}MB"
            f" {p_rs['throughput_mbps']:>8,.0f}MB"
            f" {ss['throughput_mbps']:>8,.0f}MB"
            f" {vv['throughput_mbps']:>8,.0f}MB"
            f" {ratio_py:>6.2f}x"
            f" {ratio_rs:>6.2f}x"
            f" {cons_py:>6.2f}x"
            f" {cons_rs:>6.2f}x"
            f" {fmt_bytes(g_alloc):>8} {fmt_bytes(rs_alloc):>8} {fmt_bytes(p_rs.get('peak_bytes')):>8}"
            f" {mem_cell}"
        )
    print()
    if mem_ratios:
        mem_ratios.sort(reverse=True)
        print(
            f"alloc bytes per call, golars simd vs polars-rs: median {statistics.median(r for r, _ in mem_ratios):.2f}x, "
            f"golars allocates more on {sum(r > 1.0 for r, _ in mem_ratios)}/{len(mem_ratios)}"
        )
        print("  largest: " + ", ".join(f"{label} {r:.2f}x" for r, label in mem_ratios[:8]))
    print(
        f"polars-py {py_typical['version']}  |  polars-rs crate {rs_typical['version']}  |  runs={nr}"
    )
    print(
        f"typical:      simd vs py {wins_py}/{total}   simd vs rs {wins_rs}/{total}"
    )
    if nr > 1:
        print(
            f"conservative: simd vs py {cons_wins_py}/{total}   simd vs rs {cons_wins_rs}/{total}  "
            f"(our worst vs their best)"
        )

    # Stability breakdown: classify each workload into buckets.
    # - solid: typical >= 1.0x AND conservative >= 1.0x → "always faster"
    # - noise: typical >= 1.0x but conservative < 1.0x, within 10% → can flip on noisy runs
    # - loss:  typical < 1.0x → genuine regression vs polars
    #
    # This tells you which workloads need real work vs which just need
    # a better bench harness.
    print()
    print("stability breakdown vs polars-rs:")
    solid_rs: list[str] = []
    noise_rs: list[str] = []
    loss_rs: list[str] = []
    for name, rows in keys:
        p_rs = rs_t[(name, rows)]
        vv = v_t[(name, rows)]
        cons_rs = v_w[(name, rows)]["throughput_mbps"] / rs_b[(name, rows)]["throughput_mbps"]
        typical_rs = vv["throughput_mbps"] / p_rs["throughput_mbps"]
        label = f"{name}({rows:,})"
        if typical_rs < 1.0:
            loss_rs.append(f"{label}: {typical_rs:.2f}x (stable loss)")
        elif cons_rs < 0.90:
            loss_rs.append(f"{label}: typical {typical_rs:.2f}x, conservative {cons_rs:.2f}x (borderline)")
        elif cons_rs < 1.0:
            noise_rs.append(f"{label}: typical {typical_rs:.2f}x, conservative {cons_rs:.2f}x")
        else:
            solid_rs.append(label)
    print(f"  solid     ({len(solid_rs):>2}/{total}) - win in every run")
    print(f"  noise     ({len(noise_rs):>2}/{total}) - win typically, flip to <1.0 in noisy runs")
    print(f"  loss      ({len(loss_rs):>2}/{total}) - stable loss, needs kernel work")
    if loss_rs:
        print("  stable losses:")
        for entry in loss_rs:
            print(f"    - {entry}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
