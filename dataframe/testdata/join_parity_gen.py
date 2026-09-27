"""Generate dataframe/testdata/join_parity.json from polars 1.39.3.

Run from bench/polars-compare so the pinned polars is used:

    cd bench/polars-compare
    uv run python ../../dataframe/testdata/join_parity_gen.py > ../../dataframe/testdata/join_parity.json

The file holds the input frames (column name, golars dtype, values) and
one entry per join call with the polars result rendered the way
internal/framerepr renders a golars frame, or the polars error text.
dataframe/join_general_parity_test.go rebuilds the frames, runs the
same joins through golars and compares.
"""

import datetime as dt
import itertools
import json
import math
import sys

import polars as pl

NAN = float("nan")

# name -> list of (column, golars dtype, values)
FRAMES = {
    "l1": [
        ("k", "i64", [3, 1, None, 2, 1, 5, 7]),
        ("a", "i64", [10, 11, 12, 13, 14, 15, 16]),
        ("v", "str", ["x", "y", "z", "w", "u", "t", None]),
    ],
    "r1": [
        ("k", "i64", [1, 2, None, 1, 4, 6, 7, 7]),
        ("v", "f64", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, None, 8.5]),
        ("b", "bool", [True, False, True, None, False, True, True, False]),
    ],
    "l2": [
        ("x", "i64", [1, 1, 2, None, 3, 1, None]),
        ("y", "str", ["a", "b", "a", "a", None, "a", None]),
        ("p", "i64", [1, 2, 3, 4, 5, 6, 7]),
    ],
    "r2": [
        ("x", "i64", [1, 2, None, 1, 3, None]),
        ("y", "str", ["a", "a", "a", "a", None, None]),
        ("q", "i64", [9, 8, 7, 6, 5, 4]),
    ],
    # Mixed key dtypes: widths differ between the sides, plus date,
    # categorical and datetime keys; nested payload columns.
    "l3": [
        ("ki", "i8", [1, 2, 2, None, 3]),
        ("kd", "date", [18000, 18001, 18001, 18000, None]),
        ("kc", "cat", ["a", "b", "b", "a", "c"]),
        ("lst", "list[i64]", [[1, 2], None, [], [3], [4, None]]),
    ],
    "r3": [
        ("ki", "i64", [2, 1, 3, 2, None]),
        ("kd", "date", [18001, 18000, None, 18001, 18000]),
        ("kc", "cat", ["b", "a", "c", "b", "a"]),
        ("st", "struct", [{"a": 1, "b": "p"}, None, {"a": 3, "b": None}, {"a": 4, "b": "q"}, {"a": 5, "b": "r"}]),
    ],
    "l4": [
        ("a", "i64", [1, 2, 3, 2]),
        ("v", "i64", [1, 2, 3, 4]),
    ],
    "r4": [
        ("b", "i64", [2, 3, 4, 2]),
        ("a", "i64", [7, 8, 9, 10]),
        ("v", "i64", [5, 6, 7, 8]),
    ],
    "empty": [
        ("k", "i64", []),
        ("e", "str", []),
    ],
    "ls": [
        ("s", "str", ["apple", "pear", None, "fig", "apple", "a-much-longer-string-key"]),
        ("t", "datetime[us]", [0, 1_000_000, 2_000_000, None, 0, 5]),
        ("n", "i32", [1, 2, 3, 4, 5, 6]),
    ],
    "rs": [
        ("s", "str", ["fig", "apple", None, "kiwi", "a-much-longer-string-key"]),
        ("t", "datetime[us]", [None, 0, 2_000_000, 7, 5]),
        ("m", "u32", [10, 20, 30, 40, 50]),
    ],
    "lf": [
        ("f", "f64", [0.0, NAN, -0.0, None, 1.5, 2.5]),
        ("i", "i64", [1, 2, 3, 4, 5, 6]),
    ],
    "rf": [
        ("f", "f32", [-0.0, NAN, None, 1.5, 3.5]),
        ("j", "i64", [10, 20, 30, 40, 50]),
    ],
    "lb": [
        ("b", "bool", [True, False, None, True]),
        ("i", "i64", [1, 2, 3, 4]),
    ],
    "rb": [
        ("b", "bool", [False, None, None]),
        ("j", "i64", [10, 20, 30]),
    ],
    "ln": [
        ("k", "null", [None, None, None]),
        ("i", "i64", [1, 2, 3]),
    ],
    "rn": [
        ("k", "null", [None, None]),
        ("j", "i64", [10, 20]),
    ],
    "lsfx": [
        ("k", "i64", [1, 2]),
        ("v", "i64", [1, 2]),
        ("v_right", "i64", [3, 4]),
    ],
    "rsfx": [
        ("k", "i64", [2, 1]),
        ("v", "i64", [5, 6]),
    ],
    "lu": [
        ("k", "i64", [1, 2, 3, None, None]),
        ("a", "i64", [1, 2, 3, 4, 5]),
    ],
    "ru": [
        ("k", "i64", [1, 2, 2, None]),
        ("b", "i64", [1, 2, 3, 4]),
    ],
}


def pl_dtype(t):
    return {
        "i64": pl.Int64,
        "i32": pl.Int32,
        "i8": pl.Int8,
        "u32": pl.UInt32,
        "f64": pl.Float64,
        "f32": pl.Float32,
        "str": pl.String,
        "bool": pl.Boolean,
        "cat": pl.Categorical,
        "list[i64]": pl.List(pl.Int64),
        "struct": pl.Struct({"a": pl.Int64, "b": pl.String}),
        "null": pl.Null,
    }[t]


def pl_series(name, t, vals):
    if t == "date":
        return pl.Series(name, vals, dtype=pl.Int32).cast(pl.Date)
    if t == "datetime[us]":
        return pl.Series(name, vals, dtype=pl.Int64).cast(pl.Datetime("us"))
    return pl.Series(name, vals, dtype=pl_dtype(t))


def frame(name):
    return pl.DataFrame([pl_series(c, t, v) for c, t, v in FRAMES[name]])


def dtype_repr(d):
    simple = {
        pl.Int8: "i8",
        pl.Int16: "i16",
        pl.Int32: "i32",
        pl.Int64: "i64",
        pl.UInt8: "u8",
        pl.UInt16: "u16",
        pl.UInt32: "u32",
        pl.UInt64: "u64",
        pl.Float32: "f32",
        pl.Float64: "f64",
        pl.String: "str",
        pl.Boolean: "bool",
        pl.Date: "date",
        pl.Null: "null",
        pl.Categorical: "cat",
    }
    for k, v in simple.items():
        if d == k:
            return v
    if isinstance(d, pl.Datetime):
        return f"datetime[{d.time_unit}]"
    if isinstance(d, pl.List):
        return f"list[{dtype_repr(d.inner)}]"
    if isinstance(d, pl.Struct):
        return "struct{" + ", ".join(f"{f.name}: {dtype_repr(f.dtype)}" for f in d.fields) + "}"
    raise ValueError(d)


def fmt_float(v):
    if math.isnan(v):
        return "nan"
    if math.isinf(v):
        return "inf" if v > 0 else "-inf"
    s = repr(float(v))
    if "e" in s:
        m, e = s.split("e")
        s = m.rstrip("0").rstrip(".") + "e" + ("+" if not e.startswith("-") else "-") + e.lstrip("+-").rjust(2, "0")
    return s


def cell(v, d):
    if v is None:
        return "null"
    if d == pl.Boolean:
        return "true" if v else "false"
    if d in (pl.Float32, pl.Float64):
        return fmt_float(v)
    if d in (pl.String, pl.Categorical):
        return '"' + v + '"'
    if d == pl.Date:
        return v.isoformat()
    if isinstance(d, pl.Datetime):
        return v.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if isinstance(d, pl.List):
        return "[" + ",".join(cell(x, d.inner) for x in v) + "]"
    if isinstance(d, pl.Struct):
        return "{" + ",".join(f"{f.name}={cell(v[f.name], f.dtype)}" for f in d.fields) + "}"
    return str(v)


def render(df):
    head = "|".join(f"{n}:{dtype_repr(d)}" for n, d in zip(df.columns, df.dtypes))
    lines = [head]
    for row in df.rows():
        lines.append("|".join(cell(v, d) for v, d in zip(row, df.dtypes)))
    return "\n".join(lines)


def ordered(how, order):
    """Whether polars guarantees the row order of the result."""
    if how in ("semi", "anti"):
        return True
    if how == "cross":
        return True
    if how in ("inner", "full"):
        return order in ("left_right", "right_left")
    if how == "left":
        return order == "left_right"
    if how == "right":
        return order == "right_left"
    return False


CASES = []


def add(name, left, right, how, keys=None, left_on=None, right_on=None, chunked=False, **kw):
    case = {"name": name, "left": left, "right": right, "how": how, "chunked": chunked}
    if keys is not None:
        case["on"] = keys
    if left_on is not None:
        case["left_on"] = left_on
        case["right_on"] = right_on
    for k, v in kw.items():
        if v is not None:
            case[k] = v
    args = {"how": how}
    if how != "cross":
        if keys is not None:
            args["on"] = keys
        else:
            args["left_on"] = left_on
            args["right_on"] = right_on
    for k in ("suffix", "coalesce", "nulls_equal", "validate", "maintain_order"):
        if kw.get(k) is not None:
            args[k] = kw[k]
    case["ordered"] = ordered(how, kw.get("maintain_order") or "none")
    try:
        case["want"] = render(frame(left).join(frame(right), **args))
    except Exception as e:  # noqa: BLE001
        case["error"] = str(e).split("\n")[0]
    CASES.append(case)


HOWS = ["inner", "left", "right", "full", "semi", "anti"]
ORDERS = ["none", "left", "right", "left_right", "right_left"]

for how, co, ne, mo in itertools.product(HOWS, [None, True, False], [False, True], ORDERS):
    if how in ("semi", "anti") and co is not None:
        continue
    tag = f"{how}_co{co}_ne{ne}_{mo}"
    add("basic_" + tag, "l1", "r1", how, ["k"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
    add("multi_" + tag, "l2", "r2", how, ["x", "y"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
    if mo in ("none", "left_right", "right_left"):
        add("mixed_" + tag, "l3", "r3", how, ["ki", "kd", "kc"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("lron_" + tag, "l4", "r4", how, left_on=["a"], right_on=["b"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("str_" + tag, "ls", "rs", how, ["s"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("strdt_" + tag, "ls", "rs", how, ["s", "t"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
    if mo in ("left_right", "right_left"):
        add("chunked_" + tag, "l1", "r1", how, ["k"], chunked=True, coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("chunkedmk_" + tag, "l2", "r2", how, ["x", "y"], chunked=True, coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("float_" + tag, "lf", "rf", how, ["f"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("bool_" + tag, "lb", "rb", how, ["b"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("null_" + tag, "ln", "rn", how, ["k"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("emptyR_" + tag, "l1", "empty", how, ["k"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("emptyL_" + tag, "empty", "r1", how, ["k"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("emptyLR_" + tag, "empty", "empty", how, ["k"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)
        add("lron2_" + tag, "l4", "r4", how, left_on=["a", "v"], right_on=["b", "v"], coalesce=co, nulls_equal=ne or None, maintain_order=mo)

for how in HOWS:
    for sfx in [None, "_x"]:
        add(f"suffix_{how}_{sfx}", "lsfx", "rsfx", how, ["k"], suffix=sfx, maintain_order="left_right")
        add(f"suffix_full_nc_{how}_{sfx}", "lsfx", "rsfx", how, ["k"], suffix=sfx, coalesce=False, maintain_order="left_right")
add("cross", "l4", "r4", "cross")
add("cross_sfx", "lsfx", "rsfx", "cross")
add("cross_sfx_x", "lsfx", "rsfx", "cross", suffix="_x")
add("cross_empty", "l4", "empty", "cross")

for how, v, ne in itertools.product(HOWS, ["1:1", "1:m", "m:1", "m:m"], [False, True]):
    for pair in [("lu", "ru"), ("ru", "lu"), ("l4", "l4"), ("lu", "lu")]:
        add(f"validate_{how}_{v}_ne{ne}_{pair[0]}_{pair[1]}", pair[0], pair[1], how, ["k"] if pair[0] != "l4" else ["a"],
            validate=v, nulls_equal=ne or None, maintain_order="left_right" if how not in ("right",) else "right_left")

# Key errors.
add("err_dtype", "ls", "l1", "inner", left_on=["s"], right_on=["k"])
add("err_missing", "l1", "r1", "inner", ["zz"])
add("err_len", "l2", "r2", "inner", left_on=["x", "y"], right_on=["x"])
add("err_dup", "l2", "r2", "inner", ["x", "x"])
add("err_date_dt", "l3", "ls", "inner", left_on=["kd"], right_on=["t"])

def jsonable(v):
    # JSON has no NaN; the Go side reads the string "nan" back as NaN.
    if isinstance(v, float) and math.isnan(v):
        return "nan"
    return v


json.dump({"frames": {k: [{"name": c, "dtype": t, "values": [jsonable(x) for x in v]} for c, t, v in cols] for k, cols in FRAMES.items()},
           "cases": CASES}, sys.stdout, indent=None, separators=(",", ":"), allow_nan=False)
sys.stdout.write("\n")
