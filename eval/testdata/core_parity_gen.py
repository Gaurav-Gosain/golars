"""Generate eval/core_parity_gen_test.go from polars 1.39.3.

Run from bench/polars-compare so the pinned polars is used:

    cd bench/polars-compare
    uv run python ../../eval/testdata/core_parity_gen.py > ../../eval/core_parity_gen_test.go

Each case pairs a golars expression (Go source) with the polars
expression it mirrors. Frames are declared once here and emitted into
the Go file, so both sides always see the same data.
"""

import json
import math
import sys

import polars as pl

NAN = float("nan")

# name -> list of (column, golars dtype, values)
FRAMES = {
    "main": [
        ("i", "i64", [3, None, 1, 3, 2, None, 5]),
        ("f", "f64", [1.5, NAN, None, -2.0, 1.5, 4.0, NAN]),
        ("s", "str", ["b", None, "a", "b", "c", "a", None]),
        ("b", "bool", [True, False, None, True, False, True, None]),
        ("u", "i64", [5, 1, 4, 1, 3, 9, 2]),
        ("g", "str", ["x", "y", "x", "y", "x", "y", "x"]),
        ("f32", "f32", [5.0, 1.0, 4.0, 1.0, 3.0, 9.0, 2.0]),
        ("i32", "i32", [5, 1, 4, 1, 3, 9, 2]),
    ],
    "h": [
        ("a", "i64", [1, None, 3, None]),
        ("b", "i64", [10, 20, None, None]),
        ("f", "f64", [1.5, None, NAN, None]),
        ("x", "bool", [True, None, False, None]),
        ("y", "bool", [True, True, None, None]),
        ("s", "str", ["p", None, "r", "t"]),
    ],
    "one": [("x", "i64", [5])],
    "empty": [("x", "i64", [])],
    "nul": [("x", "i64", [None, None])],
    "fl": [("x", "f64", [1.0, NAN, 3.0])],
    "runs": [("x", "i64", [1, 1, 2, 2, 2, None, None, 3])],
    "gaps": [("x", "f64", [1.0, None, None, 4.0, None]), ("t", "i64", [0, 1, 3, 4, 5])],
    "ss": [("x", "i64", [None, 1, 2, 2, 3]), ("y", "i64", [0, 2, 5, None, 3])],
    "grp": [
        ("g", "str", ["a", "b", "a", "b", "a", "c", "c"]),
        ("x", "i64", [1, 2, 3, 4, 5, 6, None]),
        ("y", "i64", [7, 1, 5, 3, 2, 8, 9]),
        ("z", "f64", [0.5, NAN, 1.5, None, 2.5, 3.5, 4.5]),
    ],
}

PL_DTYPES = {
    "i64": pl.Int64,
    "i32": pl.Int32,
    "f64": pl.Float64,
    "f32": pl.Float32,
    "str": pl.String,
    "bool": pl.Boolean,
}

GO_ARROW = {
    "i64": "arrow.PrimitiveTypes.Int64",
    "i32": "arrow.PrimitiveTypes.Int32",
    "f64": "arrow.PrimitiveTypes.Float64",
    "f32": "arrow.PrimitiveTypes.Float32",
    "str": "arrow.BinaryTypes.String",
    "bool": "arrow.FixedWidthTypes.Boolean",
}


def frame(name):
    return pl.DataFrame(
        {c: pl.Series(c, vals, dtype=PL_DTYPES[dt]) for c, dt, vals in FRAMES[name]}
    )


def go_dtype(dt):
    """polars dtype -> golars DType.String()."""
    simple = {
        pl.Int8: "i8", pl.Int16: "i16", pl.Int32: "i32", pl.Int64: "i64",
        pl.UInt8: "u8", pl.UInt16: "u16", pl.UInt32: "u32", pl.UInt64: "u64",
        pl.Float32: "f32", pl.Float64: "f64", pl.String: "str",
        pl.Boolean: "bool", pl.Categorical: "str", pl.Null: "null",
    }
    for k, v in simple.items():
        if dt == k:
            return v
    if isinstance(dt, pl.List):
        return f"list[{go_dtype(dt.inner)}]"
    if isinstance(dt, pl.Array):
        return f"list[{go_dtype(dt.inner)}; {dt.size}]"
    if isinstance(dt, pl.Struct):
        inner = ", ".join(f"{f.name}: {go_dtype(f.dtype)}" for f in dt.fields)
        return "struct{" + inner + "}"
    raise ValueError(f"unmapped dtype {dt}")


def go_value(v):
    if v is None:
        return "nil"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        if v > 2**63 - 1:
            return f"uint64({v})"
        return f"int64({v})"
    if isinstance(v, float):
        if math.isnan(v):
            return "math.NaN()"
        if math.isinf(v):
            return "math.Inf(1)" if v > 0 else "math.Inf(-1)"
        return f"float64({v!r})"
    if isinstance(v, str):
        return json.dumps(v)
    if isinstance(v, list):
        return "[]any{" + ", ".join(go_value(x) for x in v) + "}"
    if isinstance(v, dict):
        return "map[string]any{" + ", ".join(f"{json.dumps(k)}: {go_value(x)}" for k, x in v.items()) + "}"
    raise ValueError(f"unmapped value {v!r}")


CASES = []


def case(name, go, py, kind="select", frame="main", key="g"):
    CASES.append((name, go, py, kind, frame, key))


# ---- positional / scalar aggregations ----
for col in ["i", "f", "s", "b"]:
    c = pl.col(col)
    g = f'expr.Col("{col}")'
    case(f"arg_max_{col}", f"{g}.ArgMax()", c.arg_max())
    case(f"arg_min_{col}", f"{g}.ArgMin()", c.arg_min())
    case(f"arg_sort_{col}", f"{g}.ArgSort(false, false)", c.arg_sort())
    case(f"arg_sort_desc_{col}", f"{g}.ArgSort(true, false)", c.arg_sort(descending=True))
    case(f"arg_sort_nl_{col}", f"{g}.ArgSort(false, true)", c.arg_sort(nulls_last=True))
    case(f"arg_unique_{col}", f"{g}.ArgUnique()", c.arg_unique())
    case(f"drop_nulls_{col}", f"{g}.DropNulls()", c.drop_nulls())
    case(f"drop_nans_{col}", f"{g}.DropNans()", c.drop_nans())
    case(f"has_nulls_{col}", f"{g}.HasNulls()", c.has_nulls())
    case(f"len_{col}", f"{g}.Len()", c.len())
    case(f"n_unique_{col}", f"{g}.NUnique()", c.n_unique())
    case(f"approx_n_unique_{col}", f"{g}.ApproxNUnique()", c.approx_n_unique())
    case(f"is_unique_{col}", f"{g}.IsUnique()", c.is_unique())
    case(f"is_duplicated_{col}", f"{g}.IsDuplicated()", c.is_duplicated())
    case(f"is_first_distinct_{col}", f"{g}.IsFirstDistinct()", c.is_first_distinct())
    case(f"is_last_distinct_{col}", f"{g}.IsLastDistinct()", c.is_last_distinct())
    case(f"mode_{col}", f"{g}.Mode()", c.mode(), kind="sorted")
    case(f"unique_counts_{col}", f"{g}.UniqueCounts()", c.unique_counts())
    case(f"value_counts_{col}", f"{g}.ValueCounts(false, false)", c.value_counts(), kind="sorted")
    case(f"nan_max_{col}", f"{g}.NanMax()", c.nan_max())
    case(f"nan_min_{col}", f"{g}.NanMin()", c.nan_min())
    case(f"rle_id_{col}", f"{g}.RleID()", c.rle_id())
    case(f"rle_{col}", f"{g}.Rle()", c.rle())
    case(f"implode_{col}", f"{g}.Implode()", c.implode())
    case(f"gather_every_{col}", f"{g}.GatherEvery(2, 0)", c.gather_every(2))
    case(f"gather_every_off_{col}", f"{g}.GatherEvery(2, 1)", c.gather_every(2, 1))
    case(f"extend_constant_{col}", f"{g}.ExtendConstant(nil, 2)", c.extend_constant(None, 2))
    case(f"index_of_null_{col}", f"{g}.IndexOf(nil)", c.index_of(None))
    case(f"interpolate_{col}", f'{g}.Interpolate("")', c.interpolate())
    case(f"top_k_{col}", f"{g}.TopK(3)", c.top_k(3))
    case(f"bottom_k_{col}", f"{g}.BottomK(3)", c.bottom_k(3))
    case(f"sort_with_{col}", f"{g}.SortWith(false, false)", c.sort())
    case(f"sort_with_desc_nl_{col}", f"{g}.SortWith(true, true)", c.sort(descending=True, nulls_last=True))
    case(f"pct_change_{col}", f"{g}.PctChange(1)", c.pct_change())
    case(f"eq_missing_self_{col}", f"{g}.EqMissing({g}.Reverse())", c.eq_missing(c.reverse()))
    case(f"forward_fill_{col}", f"{g}.ForwardFill(0)", c.forward_fill())
    case(f"forward_fill_1_{col}", f"{g}.ForwardFill(1)", c.forward_fill(1))
    case(f"backward_fill_{col}", f"{g}.BackwardFill(0)", c.backward_fill())
    case(f"backward_fill_1_{col}", f"{g}.BackwardFill(1)", c.backward_fill(1))
    case(f"flatten_{col}", f"{g}.Implode().Flatten()", c.implode().flatten())
    for m in ["average", "min", "max", "dense", "ordinal"]:
        case(f"rank_{m}_{col}", f'{g}.Rank("{m}")', c.rank(m))
    case(f"rank_desc_{col}", f'{g}.RankWith("min", true)', c.rank("min", descending=True))
    case(f"ne_missing_self_{col}", f"{g}.NeMissing({g}.Reverse())", c.ne_missing(c.reverse()))

for col in ["i", "f"]:
    c = pl.col(col)
    g = f'expr.Col("{col}")'
    case(f"is_nan_{col}", f"{g}.IsNaN()", c.is_nan())
    case(f"is_not_nan_{col}", f"{g}.IsNotNaN()", c.is_not_nan())
    case(f"is_finite_{col}", f"{g}.IsFinite()", c.is_finite())
    case(f"is_infinite_{col}", f"{g}.IsInfinite()", c.is_infinite())
    case(f"peak_max_{col}", f"{g}.PeakMax()", c.peak_max())
    case(f"peak_min_{col}", f"{g}.PeakMin()", c.peak_min())
    case(f"entropy_{col}", f"{g}.Entropy(math.E)", c.entropy())
    case(f"quantile_{col}", f"{g}.Quantile(0.5)", c.quantile(0.5))
    case(f"median_{col}", f"{g}.Median()", c.median())
    case(f"std_{col}", f"{g}.Std()", c.std())
    case(f"var_ddof0_{col}", f"{g}.VarDdof(0)", c.var(ddof=0))
    case(f"interpolate_nearest_{col}", f"{g}.Interpolate(expr.InterpolateNearest)", c.interpolate("nearest"))
    case(f"dot_{col}", f'{g}.Dot(expr.Col("u"))', c.dot(pl.col("u")))
    case(f"round_sig_figs_{col}", f"{g}.RoundSigFigs(1)", c.round_sig_figs(1))
    case(f"floordiv_{col}", f"{g}.FloorDiv(expr.Lit(2))", c.floordiv(pl.lit(2, dtype=pl.Int64)))
    case(f"mod_{col}", f"{g}.Mod(expr.Lit(2))", c.mod(pl.lit(2, dtype=pl.Int64)))
    case(f"truediv_{col}", f"{g}.TrueDiv(expr.Lit(2))", c.truediv(pl.lit(2, dtype=pl.Int64)))
    case(f"cbrt_{col}", f"{g}.Cbrt()", c.cbrt())
    case(f"degrees_{col}", f"{g}.Degrees()", c.degrees())
    case(f"log1p_{col}", f"{g}.Log1p()", c.log1p())
    case(f"exp_{col}", f"{g}.Exp()", c.exp())
    case(f"log_base2_{col}", f"{g}.LogBase(2)", c.log(2))
    case(f"sinh_{col}", f"{g}.Sinh()", c.sinh())
    case(f"arctanh_{col}", f"{g}.Arctanh()", c.arctanh())
    case(f"is_between_{col}", f"{g}.IsBetween(expr.Lit(1.5), expr.Lit(3), expr.ClosedBoth)", c.is_between(1.5, 3))
    case(f"is_between_none_{col}", f"{g}.IsBetween(expr.Lit(1.5), expr.Lit(3), expr.ClosedNone)", c.is_between(1.5, 3, "none"))
    case(f"is_between_left_{col}", f"{g}.IsBetween(expr.Lit(1.5), expr.Lit(3), expr.ClosedLeft)", c.is_between(1.5, 3, "left"))
    case(f"is_between_right_{col}", f"{g}.IsBetween(expr.Lit(1.5), expr.Lit(3), expr.ClosedRight)", c.is_between(1.5, 3, "right"))
    case(f"cut_{col}", f"{g}.Cut([]float64{{0, 2}}, expr.CutOptions{{}})", c.cut([0.0, 2.0]))
    case(f"cut_left_{col}", f"{g}.Cut([]float64{{0, 2}}, expr.CutOptions{{LeftClosed: true}})", c.cut([0.0, 2.0], left_closed=True))
    case(f"qcut_{col}", f"{g}.QCutN(2, expr.CutOptions{{}})", c.qcut(2))
    case(f"hist_{col}", f"{g}.Hist(expr.HistOptions{{BinCount: 2}})", c.hist(bin_count=2))

# ---- elementwise on u / mixed ----
u = pl.col("u")
gu = 'expr.Col("u")'
case("bitwise_and_u", f"{gu}.BitwiseAnd()", u.bitwise_and())
case("bitwise_or_i", 'expr.Col("i").BitwiseOr()', pl.col("i").bitwise_or())
case("bitwise_xor_i", 'expr.Col("i").BitwiseXor()', pl.col("i").bitwise_xor())
case("bitwise_and_b", 'expr.Col("b").BitwiseAnd()', pl.col("b").bitwise_and())
case("bitwise_or_b", 'expr.Col("b").BitwiseOr()', pl.col("b").bitwise_or())
for k in ["count_ones", "count_zeros", "leading_ones", "leading_zeros", "trailing_ones", "trailing_zeros"]:
    camel = "".join(p.title() for p in k.split("_"))
    case(f"bitwise_{k}_i", f'expr.Col("i").Bitwise{camel}()', getattr(pl.col("i"), f"bitwise_{k}")())
    case(f"bitwise_{k}_i32", f'expr.Col("i32").Bitwise{camel}()', getattr(pl.col("i32"), f"bitwise_{k}")())
case("bitwise_count_ones_b", 'expr.Col("b").BitwiseCountOnes()', pl.col("b").bitwise_count_ones())
case("and_int", 'expr.Col("i").AndExpr(expr.Col("u"))', pl.col("i").and_(u))
case("or_int", 'expr.Col("i").OrExpr(expr.Col("u"))', pl.col("i").or_(u))
case("xor_int", 'expr.Col("i").Xor(expr.Col("u"))', pl.col("i").xor(u))
case("xor_bool", 'expr.Col("b").Xor(expr.Lit(true))', pl.col("b").xor(pl.lit(True)))
case("and_bool_kleene", 'expr.Col("b").AndExpr(expr.Col("b").Reverse())', pl.col("b").and_(pl.col("b").reverse()))
case("or_bool_kleene", 'expr.Col("b").OrExpr(expr.Col("b").Reverse())', pl.col("b").or_(pl.col("b").reverse()))
case("floordiv_neg", "expr.Lit(-7).FloorDiv(expr.Col(\"u\"))", pl.lit(-7, dtype=pl.Int64).floordiv(u))
case("mod_neg", "expr.Lit(-7).Mod(expr.Col(\"u\"))", pl.lit(-7, dtype=pl.Int64).mod(u))
case("floordiv_zero", 'expr.Col("i").FloorDiv(expr.Lit(0))', pl.col("i").floordiv(pl.lit(0, dtype=pl.Int64)))
case("mod_float", 'expr.Col("f").Mod(expr.Lit(1.0))', pl.col("f") % 1.0)
case("mod_i32", 'expr.Col("i32").Mod(expr.Col("i32").Reverse())', pl.col("i32") % pl.col("i32").reverse())
case("truediv_f32", 'expr.Col("f32").TrueDiv(expr.Col("f32"))', pl.col("f32").truediv(pl.col("f32")))
case("pow_expr", 'expr.Col("i").PowExpr(expr.Lit(2))', pl.col("i").pow(pl.lit(2, dtype=pl.Int64)))
case("eq_missing_lit", 'expr.Col("i").EqMissing(expr.Lit(3))', pl.col("i").eq_missing(3))
case("ne_missing_null", 'expr.Col("i").NeMissing(expr.LitNull(dtype.Int64()))', pl.col("i").ne_missing(pl.lit(None, dtype=pl.Int64)))
case("eq_missing_nan", 'expr.Col("f").EqMissing(expr.Lit(math.NaN()))', pl.col("f").eq_missing(pl.lit(NAN)))
case("is_close", 'expr.Col("f").IsClose(expr.Lit(1.5000001), 0, 1e-9, false)', pl.col("f").is_close(1.5000001))
case("is_close_tol", 'expr.Col("f").IsClose(expr.Lit(1.5000001), 0, 1e-6, false)', pl.col("f").is_close(1.5000001, rel_tol=1e-6))
case("is_close_nan", 'expr.Col("f").IsClose(expr.Lit(math.NaN()), 0, 1e-9, true)', pl.col("f").is_close(pl.lit(NAN), nans_equal=True))
case("round_sig_f32", 'expr.Col("f32").RoundSigFigs(1)', pl.col("f32").round_sig_figs(1))
case("log_f32", 'expr.Col("f32").LogBase(10)', pl.col("f32").log(10))
case("reinterpret", 'expr.Col("i").Reinterpret(false)', pl.col("i").reinterpret(signed=False))
case("to_physical", 'expr.Col("i").ToPhysical()', pl.col("i").to_physical())
case("lower_bound_i", 'expr.Col("i").LowerBound()', pl.col("i").lower_bound())
case("upper_bound_i32", 'expr.Col("i32").UpperBound()', pl.col("i32").upper_bound())
case("upper_bound_f", 'expr.Col("f").UpperBound()', pl.col("f").upper_bound())
case("max_by", 'expr.Col("s").MaxBy(expr.Col("u"))', pl.col("s").max_by("u"))
case("min_by", 'expr.Col("s").MinBy(expr.Col("u"))', pl.col("s").min_by("u"))
case("max_by_null", 'expr.Col("s").MaxBy(expr.Col("i"))', pl.col("s").max_by("i"))
case("min_by_f", 'expr.Col("s").MinBy(expr.Col("f"))', pl.col("s").min_by("f"))
case("quantile_lin", f'{gu}.QuantileWith(0.3, expr.QuantileLinear)', u.quantile(0.3, "linear"))
case("quantile_nearest", f"{gu}.Quantile(0.3)", u.quantile(0.3))
case("quantile_lower", f'{gu}.QuantileWith(0.3, expr.QuantileLower)', u.quantile(0.3, "lower"))
case("quantile_higher", f'{gu}.QuantileWith(0.3, expr.QuantileHigher)', u.quantile(0.3, "higher"))
case("quantile_mid", f'{gu}.QuantileWith(0.3, expr.QuantileMidpoint)', u.quantile(0.3, "midpoint"))
case("quantile_f32", 'expr.Col("f32").Quantile(0.5)', pl.col("f32").quantile(0.5))
case("median_i32", 'expr.Col("i32").Median()', pl.col("i32").median())
case("std_f32", 'expr.Col("f32").Std()', pl.col("f32").std())
case("entropy_nonorm", f"{gu}.EntropyWith(math.E, false)", u.entropy(normalize=False))
case("entropy_b2", f"{gu}.Entropy(2)", u.entropy(base=2))
case("get", f"{gu}.Get(2)", u.get(2))
case("get_neg", f"{gu}.Get(-1)", u.get(-1))
case("item", f"{gu}.First().Item()", u.first().item())
case("index_of", f"{gu}.IndexOf(9)", u.index_of(9))
case("index_of_missing", f"{gu}.IndexOf(100)", u.index_of(100))
case("arg_true", f"{gu}.Gt(expr.Lit(2)).ArgTrue()", (u > 2).arg_true())
case("arg_where", "expr.ArgWhere(expr.Col(\"u\").Gt(expr.Lit(2)))", pl.arg_where(u > 2))
case("top_k_by", 'expr.Col("s").TopKBy([]expr.Expr{expr.Col("u")}, 3, nil)', pl.col("s").top_k_by("u", 3))
case("bottom_k_by", 'expr.Col("s").BottomKBy([]expr.Expr{expr.Col("u")}, 3, nil)', pl.col("s").bottom_k_by("u", 3))
case("top_k_by_multi", 'expr.Col("s").TopKBy([]expr.Expr{expr.Col("i"), expr.Col("u")}, 3, []bool{false, true})', pl.col("s").top_k_by(["i", "u"], 3, reverse=[False, True]))
case("sort_by", 'expr.Col("s").SortBy([]expr.Expr{expr.Col("u")}, expr.SortByOptions{})', pl.col("s").sort_by("u"))
case("sort_by_multi", 'expr.Col("s").SortBy([]expr.Expr{expr.Col("i"), expr.Col("u")}, expr.SortByOptions{Descending: []bool{true, false}})', pl.col("s").sort_by(["i", "u"], descending=[True, False]))
case("sort_by_nl", 'expr.Col("u").SortBy([]expr.Expr{expr.Col("i")}, expr.SortByOptions{NullsLast: true})', pl.col("u").sort_by("i", nulls_last=True))
case("sort_by_i", 'expr.Col("u").SortBy([]expr.Expr{expr.Col("i")}, expr.SortByOptions{})', pl.col("u").sort_by("i"))
case("filter", 'expr.Col("u").Filter(expr.Col("i").Gt(expr.Lit(2)))', pl.col("u").filter(pl.col("i") > 2))
case("gather", 'expr.Col("u").GatherIdx(0, -1, 2)', pl.col("u").gather([0, -1, 2]))
case("gather_expr", 'expr.Col("s").Gather(expr.Col("u").Sub(expr.Lit(1)).Head(3))', pl.col("s").gather((pl.col("u") - 1).head(3)))
case("limit", f"{gu}.Limit(2)", u.limit(2))
case("pct_change2", f"{gu}.PctChange(2)", u.pct_change(2))
case("repeat_by", 'expr.Col("s").RepeatBy(expr.Col("u"))', pl.col("s").repeat_by("u"))
case("repeat_by_null", 'expr.Col("u").RepeatBy(expr.Col("i"))', pl.col("u").repeat_by("i"))
case("reshape_flat", f"{gu}.Reshape(-1)", u.reshape((-1,)))
case("reshape_2d", f"{gu}.Head(6).Reshape(-1, 2)", u.head(6).reshape((-1, 2)))
case("interpolate_by", 'expr.Col("i").InterpolateBy(expr.Col("u"))', pl.col("i").interpolate_by("u"))
case("search_sorted", f"{gu}.SortWith(false, false).SearchSorted(expr.Lit(4), expr.SideAny)", u.sort().search_sorted(4))
case("search_sorted_left", f"{gu}.SortWith(false, false).SearchSorted(expr.Lit(1), expr.SideLeft)", u.sort().search_sorted(1, "left"))
case("search_sorted_right", f"{gu}.SortWith(false, false).SearchSorted(expr.Lit(1), expr.SideRight)", u.sort().search_sorted(1, "right"))
case("replace", 'expr.Col("i").Replace([]any{3}, []any{30})', pl.col("i").replace(3, 30))
case("replace_multi", 'expr.Col("i").Replace([]any{3, 1}, []any{30, 10})', pl.col("i").replace([3, 1], [30, 10]))
case("replace_null", 'expr.Col("i").Replace([]any{nil}, []any{0})', pl.col("i").replace(None, 0))
case("replace_str", 'expr.Col("s").Replace([]any{"a", nil}, []any{"A", "Z"})', pl.col("s").replace(["a", None], ["A", "Z"]))
case("replace_float_nan", 'expr.Col("f").Replace([]any{1.5, math.NaN()}, []any{0.0, -1.0})', pl.col("f").replace([1.5, NAN], [0.0, -1.0]))
case("replace_trunc", 'expr.Col("i").Replace([]any{3}, []any{1.5})', pl.col("i").replace(3, 1.5))
case("replace_strict_default", 'expr.Col("u").ReplaceStrict([]any{1, 5}, []any{10, 50}, expr.ReplaceStrictOptions{Default: ptr(expr.Lit(0))})', u.replace_strict([1, 5], [10, 50], default=pl.lit(0, dtype=pl.Int64)))
case("replace_strict_str", 'expr.Col("u").ReplaceStrict([]any{1, 5}, []any{"a", "b"}, expr.ReplaceStrictOptions{Default: ptr(expr.Lit("z"))})', u.replace_strict([1, 5], ["a", "b"], default="z"))
case("replace_strict_nulls", 'expr.Col("i").ReplaceStrict([]any{3, 1, 2, 5}, []any{"three", "one", "two", "five"}, expr.ReplaceStrictOptions{})', pl.col("i").replace_strict({3: "three", 1: "one", 2: "two", 5: "five"}))
case("replace_strict_null_default", 'expr.Col("i").ReplaceStrict([]any{3}, []any{30}, expr.ReplaceStrictOptions{Default: ptr(expr.Lit(0))})', pl.col("i").replace_strict([3], [30], default=pl.lit(0, dtype=pl.Int64)))
case("replace_strict_null_key", 'expr.Col("i").ReplaceStrict([]any{3, nil}, []any{30, -1}, expr.ReplaceStrictOptions{Default: ptr(expr.Lit(0))})', pl.col("i").replace_strict([3, None], [30, -1], default=pl.lit(0, dtype=pl.Int64)))
case("replace_strict_mixed", 'expr.Col("i").ReplaceStrict([]any{3}, []any{1.5}, expr.ReplaceStrictOptions{Default: ptr(expr.Lit(0))})', pl.col("i").replace_strict([3], [1.5], default=pl.lit(0, dtype=pl.Int64)))
case("replace_strict_expr_default", 'expr.Col("u").ReplaceStrict([]any{1, 5}, []any{10, 50}, expr.ReplaceStrictOptions{Default: ptr(expr.Col("u").Mul(expr.Lit(2)))})', u.replace_strict([1, 5], [10, 50], default=u * 2))
case("replace_strict_return_dtype", 'expr.Col("u").ReplaceStrict([]any{1, 5}, []any{10, 50}, expr.ReplaceStrictOptions{Default: ptr(expr.LitNull(dtype.Int64())), ReturnDType: dtype.Int32()})', u.replace_strict([1, 5], [10, 50], default=None, return_dtype=pl.Int32))
case("cut_labels", 'expr.Col("f").Cut([]float64{0, 2}, expr.CutOptions{Labels: []string{"lo", "mid", "hi"}})', pl.col("f").cut([0.0, 2.0], labels=["lo", "mid", "hi"]))
case("cut_breaks", f'{gu}.Cut([]float64{{2, 4}}, expr.CutOptions{{IncludeBreaks: true}})', u.cut([2, 4], include_breaks=True))
case("cut_left_u", f'{gu}.Cut([]float64{{2, 4}}, expr.CutOptions{{LeftClosed: true}})', u.cut([2, 4], left_closed=True))
case("cut_big", f'{gu}.Cut([]float64{{2.5, 1e21}}, expr.CutOptions{{}})', u.cut([2.5, 1e21]))
case("qcut_q", f'{gu}.QCut([]float64{{0.25, 0.75}}, expr.CutOptions{{}})', u.qcut([0.25, 0.75]))
case("qcut_n3", f'{gu}.QCutN(3, expr.CutOptions{{}})', u.qcut(3))
case("qcut_breaks", f'{gu}.QCutN(2, expr.CutOptions{{IncludeBreaks: true}})', u.qcut(2, include_breaks=True))
case("qcut_dup", f'{gu}.QCut([]float64{{0.1, 0.2, 0.9}}, expr.CutOptions{{AllowDuplicates: true}})', u.qcut([0.1, 0.2, 0.9], allow_duplicates=True))
case("qcut_labels", f'{gu}.QCutN(2, expr.CutOptions{{Labels: []string{{"lo", "hi"}}}})', u.qcut(2, labels=["lo", "hi"]))
case("qcut_left", f'{gu}.QCutN(2, expr.CutOptions{{LeftClosed: true}})', u.qcut(2, left_closed=True))
case("hist_bins", f"{gu}.Hist(expr.HistOptions{{Bins: []float64{{0, 3, 6, 9}}}})", u.hist(bins=[0, 3, 6, 9]))
case("hist_count", f"{gu}.Hist(expr.HistOptions{{BinCount: 3}})", u.hist(bin_count=3))
case("hist_default", f"{gu}.Hist(expr.HistOptions{{}})", u.hist())
case("hist_struct", 'expr.Col("i").Hist(expr.HistOptions{BinCount: 2, IncludeBreakpoint: true, IncludeCategory: true})', pl.col("i").hist(bin_count=2, include_breakpoint=True, include_category=True))
case("hist_cat", f"{gu}.Hist(expr.HistOptions{{BinCount: 3, IncludeCategory: true}})", u.hist(bin_count=3, include_category=True))
case("hist_bp", f"{gu}.Hist(expr.HistOptions{{Bins: []float64{{0, 3, 6, 9}}, IncludeBreakpoint: true}})", u.hist(bins=[0, 3, 6, 9], include_breakpoint=True))
case("value_counts_sorted", 'expr.Col("u").ValueCounts(true, false)', u.value_counts(sort=True))
case("value_counts_norm", 'expr.Col("s").ValueCounts(false, true)', pl.col("s").value_counts(normalize=True), kind="sorted")
case("cumulative_eval", f"{gu}.CumulativeEval(expr.Element().First().Sub(expr.Element().Last().PowExpr(expr.Lit(2))), 1)", u.cumulative_eval(pl.element().first() - pl.element().last() ** 2))
case("cumulative_eval_min", f"{gu}.CumulativeEval(expr.Element().Sum(), 3)", u.cumulative_eval(pl.element().sum(), min_samples=3))
case("explode_scalar", f"{gu}.Explode()", u.explode())
case("explode_list", 'expr.ConcatList(expr.Col("u"), expr.Col("i")).Explode()', pl.concat_list("u", "i").explode())
case("implode_explode", f"{gu}.Implode().Explode()", u.implode().explode())
case("shrink_is_noop", f"{gu}.ShrinkDtype()", u)

# ---- rolling ----
case("rolling_median", f"{gu}.RollingMedian(expr.RollingWindow{{WindowSize: 3}})", u.rolling_median(3))
case("rolling_median_min1", 'expr.Col("i").RollingMedian(expr.RollingWindow{WindowSize: 3, MinSamples: 1})', pl.col("i").rolling_median(3, min_samples=1))
case("rolling_median_center", f"{gu}.RollingMedian(expr.RollingWindow{{WindowSize: 3, Center: true}})", u.rolling_median(3, center=True))
case("rolling_median_center4", f"{gu}.RollingMedian(expr.RollingWindow{{WindowSize: 4, Center: true, MinSamples: 1}})", u.rolling_median(4, center=True, min_samples=1))
case("rolling_median_f", 'expr.Col("f").RollingMedian(expr.RollingWindow{WindowSize: 2})', pl.col("f").rolling_median(2))
case("rolling_median_f32", 'expr.Col("f32").RollingMedian(expr.RollingWindow{WindowSize: 3})', pl.col("f32").rolling_median(3))
case("rolling_quantile", f'{gu}.RollingQuantile(0.25, "", expr.RollingWindow{{WindowSize: 3}})', u.rolling_quantile(0.25, window_size=3))
case("rolling_quantile_lin", f"{gu}.RollingQuantile(0.25, expr.QuantileLinear, expr.RollingWindow{{WindowSize: 3}})", u.rolling_quantile(0.25, "linear", window_size=3))
case("rolling_quantile_f", 'expr.Col("f").RollingQuantile(0.5, "", expr.RollingWindow{WindowSize: 2, MinSamples: 1})', pl.col("f").rolling_quantile(0.5, window_size=2, min_samples=1))
case("rolling_skew", f"{gu}.RollingSkew(true, expr.RollingWindow{{WindowSize: 3}})", u.rolling_skew(3))
case("rolling_skew_unbiased", f"{gu}.RollingSkew(false, expr.RollingWindow{{WindowSize: 4}})", u.rolling_skew(4, bias=False))
case("rolling_skew_nulls", 'expr.Col("i").RollingSkew(true, expr.RollingWindow{WindowSize: 3, MinSamples: 2})', pl.col("i").rolling_skew(3, min_samples=2))
case("rolling_kurtosis", f"{gu}.RollingKurtosis(true, true, expr.RollingWindow{{WindowSize: 4}})", u.rolling_kurtosis(4))
case("rolling_kurtosis_nf", f"{gu}.RollingKurtosis(false, false, expr.RollingWindow{{WindowSize: 4}})", u.rolling_kurtosis(4, fisher=False, bias=False))
case("rolling_rank", f'{gu}.RollingRank("", expr.RollingWindow{{WindowSize: 3}})', u.rolling_rank(3))
case("rolling_rank_min", f'{gu}.RollingRank("min", expr.RollingWindow{{WindowSize: 3}})', u.rolling_rank(3, method="min"))
case("rolling_rank_max", f'{gu}.RollingRank("max", expr.RollingWindow{{WindowSize: 3}})', u.rolling_rank(3, method="max"))
case("rolling_rank_dense", f'{gu}.RollingRank("dense", expr.RollingWindow{{WindowSize: 3}})', u.rolling_rank(3, method="dense"))
case("rolling_rank_nulls", 'expr.Col("i").RollingRank("", expr.RollingWindow{WindowSize: 3, MinSamples: 1})', pl.col("i").rolling_rank(3, min_samples=1))
case("rolling_corr", 'expr.RollingCorr(expr.Col("u"), expr.Col("i"), expr.RollingWindow{WindowSize: 3}, 1)', pl.rolling_corr(u, pl.col("i"), window_size=3))
case("rolling_corr_min1", 'expr.RollingCorr(expr.Col("u"), expr.Col("i"), expr.RollingWindow{WindowSize: 3, MinSamples: 1}, 1)', pl.rolling_corr(u, pl.col("i"), window_size=3, min_samples=1))
case("rolling_cov", 'expr.RollingCov(expr.Col("u"), expr.Col("i"), expr.RollingWindow{WindowSize: 3, MinSamples: 2}, 1)', pl.rolling_cov(u, pl.col("i"), window_size=3, min_samples=2))
case("rolling_cov_ddof0", 'expr.RollingCov(expr.Col("u"), expr.Col("f32"), expr.RollingWindow{WindowSize: 3}, 0)', pl.rolling_cov(u, pl.col("f32"), window_size=3, ddof=0))
case("corr", 'expr.Corr(expr.Col("u"), expr.Col("i"), "")', pl.corr(u, pl.col("i")))
case("corr_spearman", 'expr.Corr(expr.Col("u"), expr.Col("i"), "spearman")', pl.corr(u, pl.col("i"), method="spearman"))
case("cov", 'expr.Cov(expr.Col("u"), expr.Col("i"), 1)', pl.cov(u, pl.col("i")))
case("cov_ddof0", 'expr.Cov(expr.Col("u"), expr.Col("i"), 0)', pl.cov(u, pl.col("i"), ddof=0))

# ---- scalar edge frames ----
for fr in ["one", "empty", "nul", "fl"]:
    x = pl.col("x")
    gx = 'expr.Col("x")'
    for nm, gexpr, pexpr in [
        ("std", f"{gx}.Std()", x.std()),
        ("var", f"{gx}.Var()", x.var()),
        ("median", f"{gx}.Median()", x.median()),
        ("n_unique", f"{gx}.NUnique()", x.n_unique()),
        ("arg_max", f"{gx}.ArgMax()", x.arg_max()),
        ("arg_min", f"{gx}.ArgMin()", x.arg_min()),
        ("has_nulls", f"{gx}.HasNulls()", x.has_nulls()),
        ("len", f"{gx}.Len()", x.len()),
        ("nan_max", f"{gx}.NanMax()", x.nan_max()),
        ("nan_min", f"{gx}.NanMin()", x.nan_min()),
        ("mode", f"{gx}.Mode()", x.mode()),
        ("implode", f"{gx}.Implode()", x.implode()),
        ("quantile", f"{gx}.Quantile(0.5)", x.quantile(0.5)),
        ("dot", f"{gx}.Dot({gx})", x.dot(x)),
        ("first", f"{gx}.First()", x.first()),
        ("last", f"{gx}.Last()", x.last()),
    ]:
        kind = "sorted" if nm == "mode" else "select"
        case(f"{nm}_{fr}", gexpr, pexpr, frame=fr, kind=kind)

case("rle_runs", 'expr.Col("x").Rle()', pl.col("x").rle(), frame="runs")
case("rle_id_runs", 'expr.Col("x").RleID()', pl.col("x").rle_id(), frame="runs")
case("interpolate_gaps", 'expr.Col("x").Interpolate("")', pl.col("x").interpolate(), frame="gaps")
case("interpolate_gaps_nearest", 'expr.Col("x").Interpolate(expr.InterpolateNearest)', pl.col("x").interpolate("nearest"), frame="gaps")
case("interpolate_by_gaps", 'expr.Col("x").InterpolateBy(expr.Col("t"))', pl.col("x").interpolate_by("t"), frame="gaps")
case("search_sorted_nulls", 'expr.Col("x").SearchSorted(expr.Lit(2), expr.SideLeft)', pl.col("x").search_sorted(2, "left"), frame="ss")
case("search_sorted_expr", 'expr.Col("x").SearchSorted(expr.Col("y"), expr.SideRight)', pl.col("x").search_sorted(pl.col("y"), "right"), frame="ss")

# ---- top-level functions ----
H = "h"
case("sum_horizontal", 'expr.SumHorizontal(expr.Col("a"), expr.Col("b"))', pl.sum_horizontal("a", "b"), frame=H)
case("sum_horizontal_f", 'expr.SumHorizontal(expr.Col("a"), expr.Col("f"))', pl.sum_horizontal("a", "f"), frame=H)
case("sum_horizontal_bool", 'expr.SumHorizontal(expr.Col("x"), expr.Col("y"))', pl.sum_horizontal("x", "y"), frame=H)
case("sum_horizontal_str", 'expr.SumHorizontal(expr.Col("s"), expr.Col("s"))', pl.sum_horizontal("s", "s"), frame=H)
case("mean_horizontal", 'expr.MeanHorizontal(expr.Col("a"), expr.Col("b"))', pl.mean_horizontal("a", "b"), frame=H)
case("mean_horizontal_f", 'expr.MeanHorizontal(expr.Col("a"), expr.Col("f"))', pl.mean_horizontal("a", "f"), frame=H)
case("min_horizontal", 'expr.MinHorizontal(expr.Col("a"), expr.Col("b"))', pl.min_horizontal("a", "b"), frame=H)
case("max_horizontal_f", 'expr.MaxHorizontal(expr.Col("a"), expr.Col("f"))', pl.max_horizontal("a", "f"), frame=H)
case("min_horizontal_str", 'expr.MinHorizontal(expr.Col("s"), expr.Lit("q"))', pl.min_horizontal("s", pl.lit("q")), frame=H)
case("all_horizontal", 'expr.AllHorizontal(expr.Col("x"), expr.Col("y"))', pl.all_horizontal("x", "y"), frame=H)
case("any_horizontal", 'expr.AnyHorizontal(expr.Col("x"), expr.Col("y"))', pl.any_horizontal("x", "y"), frame=H)
case("cum_sum_horizontal", 'expr.CumSumHorizontal(expr.Col("a"), expr.Col("b"))', pl.cum_sum_horizontal("a", "b"), frame=H)
case("fold", 'expr.Fold(expr.Col("a").Mul(expr.Lit(0)), addFold, expr.Col("a"), expr.Col("b"))', pl.fold(pl.col("a") * 0, lambda acc, x: acc + x, ["a", "b"]), frame=H)
case("reduce", 'expr.Reduce(addFold, expr.Col("a"), expr.Col("b"))', pl.reduce(lambda acc, x: acc + x, ["a", "b"]), frame=H)
case("cum_reduce", 'expr.CumReduce(addFold, expr.Col("a"), expr.Col("b"))', pl.cum_reduce(lambda acc, x: acc + x, ["a", "b"]), frame=H)
case("arg_sort_by", 'expr.ArgSortBy([]expr.Expr{expr.Col("a"), expr.Col("b")}, []bool{true, false}, false)', pl.arg_sort_by(["a", "b"], descending=[True, False]), frame=H)
case("arg_sort_by_nl", 'expr.ArgSortBy([]expr.Expr{expr.Col("a")}, nil, true)', pl.arg_sort_by("a", nulls_last=True), frame=H)
case("repeat_f", "expr.Repeat(1.5, 2)", pl.repeat(1.5, 2), frame=H)
case("repeat_s", 'expr.Repeat("z", 2)', pl.repeat("z", 2), frame=H)
case("repeat_expr", 'expr.RepeatExpr(expr.Col("b").First(), 2)', pl.repeat(pl.col("b").first(), 2), frame=H)
case("linear_space", "expr.LinearSpace(0, 1, 5, \"\")", pl.linear_space(0, 1, 5), frame=H)
case("linear_space_left", "expr.LinearSpace(0, 1, 5, expr.ClosedLeft)", pl.linear_space(0, 1, 5, closed="left"), frame=H)
case("linear_space_right", "expr.LinearSpace(0, 1, 5, expr.ClosedRight)", pl.linear_space(0, 1, 5, closed="right"), frame=H)
case("linear_space_none", "expr.LinearSpace(0, 1, 5, expr.ClosedNone)", pl.linear_space(0, 1, 5, closed="none"), frame=H)
case("linear_space_one", "expr.LinearSpace(0, 1, 1, \"\")", pl.linear_space(0, 1, 1), frame=H)
case("format", 'expr.Format("{}-{}", expr.Col("a"), expr.Col("s"))', pl.format("{}-{}", "a", "s"), frame=H)
case("format_f", 'expr.Format("v={}", expr.Col("f"))', pl.format("v={}", "f"), frame=H)
case("format_b", 'expr.Format("v={}", expr.Col("x"))', pl.format("v={}", "x"), frame=H)
case("frame_len", "expr.Len()", pl.len(), frame=H)
case("struct", 'expr.Struct(expr.Col("a"), expr.Col("s").Alias("t"))', pl.struct("a", pl.col("s").alias("t")), frame=H)
case("concat_list", 'expr.ConcatList(expr.Col("a"), expr.Col("b"))', pl.concat_list("a", "b"), frame=H)
case("concat_list_mixed", 'expr.ConcatList(expr.Col("a"), expr.Col("f"))', pl.concat_list("a", "f"), frame=H)
case("concat_list_lit", 'expr.ConcatList(expr.Col("s"), expr.Lit("z"))', pl.concat_list("s", pl.lit("z")), frame=H)
case("concat_list_nested", 'expr.ConcatList(expr.ConcatList(expr.Col("a"), expr.Col("b")), expr.Col("a"))', pl.concat_list(pl.concat_list("a", "b"), "a"), frame=H)
case("concat_arr", 'expr.ConcatArr(expr.Col("a"), expr.Col("b"))', pl.concat_arr("a", "b"), frame=H)
case("int_ranges", 'expr.IntRanges(expr.Col("a"), expr.Col("b"), expr.Lit(1))', pl.int_ranges("a", "b"), frame=H)
case("int_ranges_step", 'expr.IntRanges(expr.Col("b"), expr.Lit(0), expr.Lit(-7))', pl.int_ranges(pl.col("b"), 0, -7), frame=H)
case("arctan2", 'expr.Arctan2(expr.Col("b"), expr.Col("a"))', pl.arctan2(pl.col("b"), pl.col("a")), frame=H)
case("nth", "expr.Nth(1)", pl.nth(1), frame=H)
case("exclude_sum", 'expr.AllCols().Exclude("f", "s", "x", "y").Sum()', pl.all().exclude("f", "s", "x", "y").sum(), frame=H)

# ---- aggregation context: group_by().agg() ----
G = "grp"
case("agg_sort_by_first", 'expr.Col("x").SortBy([]expr.Expr{expr.Col("y")}, expr.SortByOptions{}).First()', pl.col("x").sort_by("y").first(), kind="agg", frame=G)
case("agg_sort_by_last_desc", 'expr.Col("x").SortBy([]expr.Expr{expr.Col("y")}, expr.SortByOptions{Descending: []bool{true}}).Last()', pl.col("x").sort_by("y", descending=True).last(), kind="agg", frame=G)
case("agg_filter_sum", 'expr.Col("x").Filter(expr.Col("y").Gt(expr.Lit(2))).Sum()', pl.col("x").filter(pl.col("y") > 2).sum(), kind="agg", frame=G)
case("agg_filter_count", 'expr.Col("x").Filter(expr.Col("y").Gt(expr.Lit(4))).Len()', pl.col("x").filter(pl.col("y") > 4).len(), kind="agg", frame=G)
case("agg_list", 'expr.Col("x")', pl.col("x"), kind="agg", frame=G)
case("agg_head", 'expr.Col("y").Head(2)', pl.col("y").head(2), kind="agg", frame=G)
case("agg_arg_max", 'expr.Col("y").ArgMax()', pl.col("y").arg_max(), kind="agg", frame=G)
case("agg_max_by", 'expr.Col("x").MaxBy(expr.Col("y"))', pl.col("x").max_by("y"), kind="agg", frame=G)
case("agg_median", 'expr.Col("z").Median()', pl.col("z").median(), kind="agg", frame=G)
case("agg_std", 'expr.Col("y").Std()', pl.col("y").std(), kind="agg", frame=G)
case("agg_n_unique", 'expr.Col("x").NUnique()', pl.col("x").n_unique(), kind="agg", frame=G)
case("agg_quantile", 'expr.Col("y").Quantile(0.5)', pl.col("y").quantile(0.5), kind="agg", frame=G)
case("agg_implode", 'expr.Col("y").Implode()', pl.col("y").implode(), kind="agg", frame=G)
case("agg_mean_minus", 'expr.Col("y").Sub(expr.Col("y").Mean()).Max()', (pl.col("y") - pl.col("y").mean()).max(), kind="agg", frame=G)
case("agg_rank", 'expr.Col("y").Rank("dense")', pl.col("y").rank("dense"), kind="agg", frame=G)
case("agg_len", "expr.Len()", pl.len(), kind="agg", frame=G)
case("agg_top_k", 'expr.Col("y").TopK(2)', pl.col("y").top_k(2), kind="agg", frame=G)
case("top_k_all", 'expr.Col("i").TopK(10)', pl.col("i").top_k(10))
case("bottom_k_all", 'expr.Col("f").BottomK(7)', pl.col("f").bottom_k(7))
case("agg_bottom_k_by", 'expr.Col("x").BottomKBy([]expr.Expr{expr.Col("y")}, 1, nil)', pl.col("x").bottom_k_by("y", 1), kind="agg", frame=G)
case("agg_dot", 'expr.Col("x").Dot(expr.Col("y"))', pl.col("x").dot("y"), kind="agg", frame=G)
case("agg_has_nulls", 'expr.Col("x").HasNulls()', pl.col("x").has_nulls(), kind="agg", frame=G)
case("agg_cum_eval", 'expr.Col("y").Filter(expr.Col("y").Gt(expr.Lit(1))).First()', pl.col("y").filter(pl.col("y") > 1).first(), kind="agg", frame=G)
case("agg_all_exclude", 'expr.AllCols().Exclude("z").Max()', pl.all().exclude("z").max(), kind="agg_all", frame=G)

# ---- window context: over() ----
case("over_rank", 'expr.Col("y").Rank("ordinal")', pl.col("y").rank("ordinal"), kind="over", frame=G)
case("over_rank_avg", 'expr.Col("y").Rank("average")', pl.col("y").rank("average"), kind="over", frame=G)
case("over_cum_max", 'expr.Col("y").CumMax()', pl.col("y").cum_max(), kind="over", frame=G)
case("over_arg_max", 'expr.Col("y").ArgMax()', pl.col("y").arg_max(), kind="over", frame=G)
case("over_demean", 'expr.Col("y").Sub(expr.Col("y").Mean())', pl.col("y") - pl.col("y").mean(), kind="over", frame=G)
case("over_sort_by_first", 'expr.Col("x").SortBy([]expr.Expr{expr.Col("y")}, expr.SortByOptions{}).First()', pl.col("x").sort_by("y").first(), kind="over", frame=G)
case("over_filter_sum", 'expr.Col("x").Filter(expr.Col("y").Gt(expr.Lit(2))).Sum()', pl.col("x").filter(pl.col("y") > 2).sum(), kind="over", frame=G)
case("over_interpolate", 'expr.Col("z").Interpolate("")', pl.col("z").interpolate(), kind="over", frame=G)
case("over_rle_id", 'expr.Col("y").RleID()', pl.col("y").rle_id(), kind="over", frame=G)
case("over_is_first", 'expr.Col("x").IsFirstDistinct()', pl.col("x").is_first_distinct(), kind="over", frame=G)
case("over_shift", 'expr.Col("y").Shift(1)', pl.col("y").shift(1), kind="over", frame=G)
case("over_implode", 'expr.Col("y").Implode()', pl.col("y").implode(), kind="over", frame=G)


def run(c):
    name, go, py, kind, fr, key = c
    df = frame(fr)
    if kind in ("select", "sorted"):
        s = df.select(py).to_series()
    elif kind == "agg":
        s = df.group_by(key).agg(py).sort(key).to_series(1)
    elif kind == "agg_all":
        out = df.group_by(key).agg(py).sort(key)
        s = pl.Series("rows", [list(r) for r in out.drop(key).rows()])
        return name, go, kind, fr, key, "rows", [list(r) for r in out.drop(key).rows()], list(out.drop(key).columns)
    elif kind == "over":
        s = df.select(py.over(key)).to_series()
    else:
        raise ValueError(kind)
    vals = s.to_list()
    return name, go, kind, fr, key, go_dtype(s.dtype), vals, None


def main():
    out = sys.stdout
    out.write("// Code produced by eval/testdata/core_parity_gen.py from polars\n")
    out.write(f"// {pl.__version__}. Edit the generator, not this file.\n\n")
    out.write("package eval_test\n\n")
    out.write('import (\n\t"math"\n\n\t"github.com/apache/arrow-go/v18/arrow"\n\n')
    out.write('\t"github.com/Gaurav-Gosain/golars/dtype"\n\t"github.com/Gaurav-Gosain/golars/expr"\n)\n\n')
    out.write("var _ = dtype.Int64\n\n")
    out.write("var coreParityFrames = map[string][]parityColumn{\n")
    for fname, cols in FRAMES.items():
        out.write(f"\t{json.dumps(fname)}: {{\n")
        for cname, dt, vals in cols:
            out.write(f"\t\t{{Name: {json.dumps(cname)}, Type: {GO_ARROW[dt]}, Values: {go_value(vals)}}},\n")
        out.write("\t},\n")
    out.write("}\n\n")
    out.write("var coreParityCases = []parityCase{\n")
    for c in CASES:
        try:
            name, go, kind, fr, key, dt, vals, cols = run(c)
        except Exception as ex:  # noqa: BLE001
            out.write(f"\t// {c[0]}: polars error {type(ex).__name__}\n")
            out.write(f"\t{{Name: {json.dumps(c[0])}, Frame: {json.dumps(c[4])}, Kind: {json.dumps(c[3])}, Key: {json.dumps(c[5])}, Expr: {c[1]}, WantErr: true}},\n")
            continue
        extra = f", Cols: {go_value(cols)}" if cols is not None else ""
        out.write(
            f"\t{{Name: {json.dumps(name)}, Frame: {json.dumps(fr)}, Kind: {json.dumps(kind)}, Key: {json.dumps(key)}, "
            f"Expr: {go}, DType: {json.dumps(dt)}, Want: {go_value(vals)}{extra}}},\n"
        )
    out.write("}\n")


if __name__ == "__main__":
    main()
