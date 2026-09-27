"""Polars side of the golars differential test harness.

Reads case directories written by internal/difftest (plan.json plus
left.arrows and optionally right.arrows, Arrow IPC streams), runs the
plan with polars and writes result.arrows and result.json into the
same directory.

Usage:
    python runner.py CASE_DIR [CASE_DIR ...]   run the given cases
    python runner.py --serve                   read case dirs from stdin,
                                               one per line, and answer
                                               each with a line "done"
"""

from __future__ import annotations

import json
import operator
import sys
import traceback
import warnings

import polars as pl

warnings.simplefilter("ignore")

_SIMPLE = {
    "null": pl.Null,
    "bool": pl.Boolean,
    "i8": pl.Int8,
    "i16": pl.Int16,
    "i32": pl.Int32,
    "i64": pl.Int64,
    "u8": pl.UInt8,
    "u16": pl.UInt16,
    "u32": pl.UInt32,
    "u64": pl.UInt64,
    "f32": pl.Float32,
    "f64": pl.Float64,
    "str": pl.String,
    "date": pl.Date,
    "time": pl.Time,
    "cat": pl.Categorical,
}


def dtype_of(name: str):
    if name in _SIMPLE:
        return _SIMPLE[name]
    if name.startswith("datetime[") and name.endswith("]"):
        inner = name[len("datetime[") : -1]
        unit, _, tz = inner.partition(", ")
        return pl.Datetime(unit, tz or None)
    if name.startswith("duration[") and name.endswith("]"):
        return pl.Duration(name[len("duration[") : -1])
    if name.startswith("list[") and name.endswith("]"):
        return pl.List(dtype_of(name[len("list[") : -1]))
    raise ValueError(f"unknown dtype {name!r}")


_BIN = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "//": operator.floordiv,
    "%": operator.mod,
    "**": operator.pow,
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "&": operator.and_,
    "|": operator.or_,
}


def raw_value(v, is_float: bool):
    if isinstance(v, list):
        return [raw_value(x, is_float) for x in v]
    if is_float and v is not None:
        return float(v)
    return v


def build(e):
    k = e["k"]
    if k == "col":
        return pl.col(e["name"])
    if k == "lit":
        v = raw_value(e.get("val"), e.get("f", False))
        x = pl.lit(v)
        if e.get("dtype"):
            x = x.cast(dtype_of(e["dtype"]))
        return x
    if k == "raw":
        return raw_value(e.get("val"), e.get("f", False))
    if k == "dtype":
        return dtype_of(e["dtype"])
    args = e.get("args") or []
    if k == "bin":
        return _BIN[e["name"]](build(args[0]), build(args[1]))
    if k == "not":
        return ~build(args[0])
    if k == "neg":
        return -build(args[0])
    kw = {a["name"]: build(a["val"]) for a in e.get("kw") or []}
    if k == "call":
        target = build(args[0])
        for part in e["name"].split("."):
            target = getattr(target, part)
        return target(*[build(a) for a in args[1:]], **kw)
    if k == "fn":
        return getattr(pl, e["name"])(*[build(a) for a in args], **kw)
    if k == "when":
        chain = None
        for i in range(0, len(args), 2):
            c, v = build(args[i]), build(args[i + 1])
            chain = pl.when(c).then(v) if chain is None else chain.when(c).then(v)
        if e.get("else") is not None:
            chain = chain.otherwise(build(e["else"]))
        return chain
    raise ValueError(f"unknown expr kind {k!r}")


def apply_op(lf: pl.LazyFrame, op, right):
    name = op["op"]
    keys = op.get("keys") or []
    if name == "filter":
        return lf.filter(build(op["pred"]))
    if name == "select":
        return lf.select([build(x) for x in op["exprs"]])
    if name == "with_columns":
        return lf.with_columns([build(x) for x in op["exprs"]])
    if name == "group_by":
        return lf.group_by(keys, maintain_order=op.get("maintain_order", False)).agg(
            [build(x) for x in op.get("exprs") or []]
        )
    if name == "sort":
        return lf.sort(
            keys,
            descending=op["desc"],
            nulls_last=op["nulls_last"],
            maintain_order=True,
        )
    if name == "join":
        how = op["how"]
        if how == "cross":
            return lf.join(right, how="cross", suffix=op.get("suffix") or "_right")
        kw = {"how": how}
        if op.get("right_on"):
            kw["left_on"] = keys
            kw["right_on"] = op["right_on"]
        else:
            kw["on"] = keys
        if op.get("coalesce"):
            kw["coalesce"] = op["coalesce"] == "true"
        if op.get("nulls_equal"):
            kw["nulls_equal"] = True
        if op.get("validate"):
            kw["validate"] = op["validate"]
        if op.get("join_order"):
            kw["maintain_order"] = op["join_order"]
        if op.get("suffix"):
            kw["suffix"] = op["suffix"]
        return lf.join(right, **kw)
    if name == "join_asof":
        return lf.join_asof(
            right,
            on=keys[0],
            by=op.get("by") or None,
            strategy=op.get("strategy") or "backward",
        )
    if name == "unique":
        return lf.unique(
            subset=keys or None,
            maintain_order=op.get("maintain_order", False),
            keep="first" if op.get("maintain_order") else "any",
        )
    if name == "slice":
        return lf.slice(op.get("offset", 0), op.get("n", 0))
    if name == "head":
        return lf.head(op.get("n", 0))
    if name == "tail":
        return lf.tail(op.get("n", 0))
    if name == "explode":
        return lf.explode(keys)
    if name == "unpivot":
        return lf.unpivot(on=op.get("on") or None, index=op.get("index") or None)
    if name == "pivot":
        return lf.pivot(
            on=op["on"][0],
            on_columns=op.get("on_values") or [],
            index=op.get("index") or None,
            values=op.get("values") or None,
            aggregate_function=op.get("agg") or None,
            maintain_order=True,
        )
    if name == "with_row_index":
        return lf.with_row_index(op.get("name") or "index", op.get("offset", 0))
    if name == "drop_nulls":
        return lf.drop_nulls(keys or None)
    if name == "drop":
        return lf.drop(keys)
    if name == "rename":
        return lf.rename({keys[0]: op["name"]})
    if name == "reverse":
        return lf.reverse()
    raise ValueError(f"unknown op {name!r}")


def run_case(d: str) -> None:
    with open(f"{d}/plan.json") as f:
        plan = json.load(f)
    result = {"ok": False}
    try:
        left = pl.read_ipc_stream(f"{d}/left.arrows")
        right = None
        if plan.get("has_right"):
            right = pl.read_ipc_stream(f"{d}/right.arrows").lazy()
        lf = left.lazy()
        for op in plan.get("ops") or []:
            lf = apply_op(lf, op, right)
        out = lf.collect()
        out.write_ipc_stream(f"{d}/result.arrows", compat_level=pl.CompatLevel.oldest())
        result = {
            "ok": True,
            "schema": [[n, str(t)] for n, t in out.schema.items()],
        }
    except BaseException as exc:  # noqa: BLE001 - polars panics derive from BaseException
        if isinstance(exc, KeyboardInterrupt):
            raise
        result = {
            "ok": False,
            "etype": type(exc).__name__,
            "error": str(exc)[:2000],
            "trace": traceback.format_exc()[-2000:],
        }
    with open(f"{d}/result.json", "w") as f:
        json.dump(result, f)


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--serve":
        for line in sys.stdin:
            d = line.strip()
            if not d:
                continue
            run_case(d)
            sys.stdout.write("done\n")
            sys.stdout.flush()
        return
    for d in sys.argv[1:]:
        run_case(d)


if __name__ == "__main__":
    main()
