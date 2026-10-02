"""Generate docs/api-surface.md from the real polars and golars APIs.

Run from this directory:

    uv run python api_coverage.py

The script needs only the stdlib and polars. It does not touch the
network. It:

1. Lists the public polars API with dir() on pl.Expr, pl.DataFrame,
   pl.LazyFrame, pl.Series, their namespaces, the GroupBy objects and
   top-level pl.* functions. Private and deprecated names are skipped.
2. Parses golars' Go source with a regex for exported methods per
   receiver type and exported package-level functions.
3. Maps every polars name to a golars name: a case-insensitive match
   of the snake_case name with underscores removed, or an entry in the
   RENAMES table below. Every rename is checked against the parsed Go
   source, so a stale entry falls back to "todo" instead of lying.
4. Writes docs/api-surface.md and the docs-site MDX copy (keeping its
   frontmatter).

Edit RENAMES, PARTIAL and NOT_APPLICABLE to fix the mapping. Do not
hand-edit the generated markdown.
"""

from __future__ import annotations

import inspect
import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
OUT_MD = ROOT / "docs" / "api-surface.md"
OUT_MDX = ROOT / "docs-site" / "content" / "docs" / "api-surface.mdx"

# ---------------------------------------------------------------------------
# Go source parsing
# ---------------------------------------------------------------------------

METHOD_RE = re.compile(r"^func \(\w+ \*?([A-Z]\w*)(?:\[[^\]]*\])?\) ([A-Z]\w*)(?:\[[^\]]*\])?\(", re.M)
FUNC_RE = re.compile(r"^func ([A-Z]\w*)(?:\[[^\]]*\])?\(", re.M)
PKG_RE = re.compile(r"^package (\w+)", re.M)


@dataclass
class GoPkg:
    name: str
    methods: dict[str, set[str]] = field(default_factory=dict)
    funcs: set[str] = field(default_factory=set)


def parse_go(rel: str, only: str | None = None) -> GoPkg:
    """Parse exported methods and funcs of the Go package in ROOT/rel.

    With `only`, restrict to files whose name matches that glob (used
    for the root package, whose files are golars*.go).
    """
    d = ROOT / rel
    pattern = only or "*.go"
    pkg = GoPkg(name="")
    for f in sorted(d.glob(pattern)):
        if f.name.endswith("_test.go"):
            continue
        src = f.read_text()
        m = PKG_RE.search(src)
        if m and not pkg.name:
            pkg.name = m.group(1)
        for recv, meth in METHOD_RE.findall(src):
            pkg.methods.setdefault(recv, set()).add(meth)
        pkg.funcs.update(FUNC_RE.findall(src))
    return pkg


GO = {
    "golars": parse_go(".", "golars*.go"),
    "expr": parse_go("expr"),
    "series": parse_go("series"),
    "dataframe": parse_go("dataframe"),
    "lazy": parse_go("lazy"),
    "dtype": parse_go("dtype"),
    "compute": parse_go("compute"),
    "selector": parse_go("selector"),
    "csv": parse_go("io/csv"),
    "parquet": parse_go("io/parquet"),
    "ipc": parse_go("io/ipc"),
    "json": parse_go("io/json"),
    "sql": parse_go("io/sql"),
    "clipboard": parse_go("io/clipboard"),
}


def go_methods(pkg: str, recv: str) -> set[str]:
    return GO[pkg].methods.get(recv, set())


# ---------------------------------------------------------------------------
# polars API listing
# ---------------------------------------------------------------------------


def is_deprecated(obj) -> bool:
    for o in (obj, getattr(obj, "fget", None)):
        if o is None:
            continue
        if isinstance(getattr(o, "__deprecated__", None), str):
            return True
        doc = getattr(o, "__doc__", None)
        if isinstance(doc, str) and ".. deprecated::" in doc:
            return True
    return False


def public_names(cls) -> list[str]:
    out = []
    for n in dir(cls):
        if n.startswith("_"):
            continue
        try:
            obj = getattr(cls, n)
        except Exception:
            obj = inspect.getattr_static(cls, n)
        if is_deprecated(obj):
            continue
        out.append(n)
    return sorted(out)


def toplevel_names() -> list[str]:
    out = []
    for n in dir(pl):
        if n.startswith("_") or not n[0].islower():
            continue
        obj = getattr(pl, n)
        if not inspect.isfunction(obj):
            continue
        mod = getattr(obj, "__module__", "") or ""
        if not mod.startswith("polars"):
            continue
        if is_deprecated(obj):
            continue
        out.append(n)
    return sorted(out)


_df = pl.DataFrame({"a": [1]})
_s = pl.Series("a", [1])
_e = pl.col("a")
NS = ["str", "dt", "list", "arr", "struct", "name", "bin", "cat"]


def expr_ns(ns: str):
    return type(getattr(_e, ns))


def series_ns(ns: str):
    return type(getattr(_s, ns))


# ---------------------------------------------------------------------------
# Mapping tables
# ---------------------------------------------------------------------------

# Names that make no sense in a Go library. Excluded from the
# denominator. A direct Go match still wins over this list.
NOT_APPLICABLE_COMMON = {
    "pipe": "Python callable chaining; call the Go function directly",
    "to_pandas": "no pandas in Go",
    "to_numpy": "no numpy in Go",
    "to_jax": "no jax in Go",
    "to_torch": "no torch in Go",
    "to_init_repr": "Python repr round-trip",
    "plot": "Python plotting backend",
    "style": "Python great_tables styling",
    "deserialize": "Python plan serialization format",
    "serialize": "Python plan serialization format",
    "map_batches": "Python UDF",
    "map_elements": "Python UDF",
    "map_rows": "Python UDF",
    "map_groups": "Python UDF",
    "map_columns": "Python UDF",
    "register_plugin_function": "Python plugin loader",
    "collect_async": "Python asyncio",
    "remote": "Polars Cloud",
    "inspect": "Python print debugging",
    "ext": "unstable extension-type API",
    "shrink_to_fit": "memory tuning hint; arrow buffers are sized on build",
    "pipe_with_schema": "Python callable chaining",
}
NOT_APPLICABLE_COMMON = {k: v for k, v in NOT_APPLICABLE_COMMON.items() if v}

# Per-surface not-applicable additions: {surface_key: {name: reason}}
NOT_APPLICABLE: dict[str, dict[str, str]] = {
    "lazyframe": {
        "lazy": "identity on a LazyFrame",
        "sink_batches": "Python callback per batch",
    },
    "series.cat": {"to_local": "golars has no global string cache"},
    "dtype": {
        "Object": "polars escape hatch for Python objects",
        "Unknown": "polars-internal placeholder",
        "Extension": "unstable extension-type API",
        "BaseExtension": "unstable extension-type API",
    },
    "toplevel": {
        "build_info": "Python build metadata",
        "show_versions": "Python environment report",
        "thread_pool_size": "Go runtime schedules goroutines; see compute.WithParallelism",
        "collect_all_async": "Python asyncio",
        "defer": "Python callable source",
        "enable_string_cache": "golars has no global string cache",
        "disable_string_cache": "golars has no global string cache",
        "using_string_cache": "golars has no global string cache",
        "get_extension_type": "unstable extension-type API",
        "register_extension_type": "unstable extension-type API",
        "unregister_extension_type": "unstable extension-type API",
        "from_numpy": "no numpy in Go",
        "from_pandas": "no pandas in Go",
        "from_torch": "no torch in Go",
        "from_dataframe": "Python dataframe interchange protocol",
        "scan_pyarrow_dataset": "pyarrow-specific",
        "wrap_df": "internal Python helper",
        "wrap_s": "internal Python helper",
        "set_random_seed": "golars sampling takes an explicit seed argument",
        "get_index_type": "golars indices are Go int; counts are u32 like polars",
    },
}

# Per-surface renames: {surface_key: {polars_name: go_name}}.
# go_name may list alternatives separated by " / "; each is checked
# against the parsed Go source and the first one that exists wins.
# A name is either a member of the surface's receiver type (`Upper`),
# a package function (`compute.Sort`) or a method on another type
# (`series.Series.Head`). Text after the identifier (for example call
# arguments) is shown as-is and not checked.
RENAMES: dict[str, dict[str, str]] = {
    "expr": {},
    "dtype": {
        "Boolean": "Bool",
        "Utf8": "String",
        "Array": "FixedList",
    },
    "expr.str": {
        "to_lowercase": "ToLower",
        "to_uppercase": "ToUpper",
    },
    "dataframe": {
        "count": "CountAll",
        "max": "MaxAll",
        "min": "MinAll",
        "mean": "MeanAll",
        "median": "MedianAll",
        "product": "ProductAll",
        "quantile": "QuantileAll",
        "std": "StdAll",
        "var": "VarAll",
        "sum": "SumAll",
        "to_dict": "ToMap",
        "lazy": "golars.Lazy / lazy.FromDataFrame",
        "select_seq": "golars.SelectSeq",
        "with_columns_seq": "golars.WithColumnsSeq",
        "sql": "golars.SQL",
        "write_json": "golars.WriteJSON",
        "write_ndjson": "golars.WriteNDJSON",
        "write_ipc_stream": "golars.WriteIPCStream",
        "write_clipboard": "clipboard.Write",
        "write_csv": "golars.WriteCSV",
        "write_parquet": "golars.WriteParquet",
        "write_ipc": "golars.WriteIPC",
        "show": "String",
    },
    "lazyframe": {
        "sql": "golars.SQLLazy",
        "sink_csv": "golars.SinkCSV",
        "sink_parquet": "golars.SinkParquet",
        "sink_ipc": "golars.SinkIPC",
        "sink_ndjson": "golars.SinkNDJSON",
    },
    "series": {
        "alias": "Rename",
        "bitwise_count_ones": 'BitCount("count_ones")',
        "bitwise_count_zeros": 'BitCount("count_zeros")',
        "bitwise_leading_ones": 'BitCount("leading_ones")',
        "bitwise_leading_zeros": 'BitCount("leading_zeros")',
        "bitwise_trailing_ones": 'BitCount("trailing_ones")',
        "bitwise_trailing_zeros": 'BitCount("trailing_zeros")',
        "cast": "compute.Cast",
        "count": "compute.Count",
        "describe": "golars.DescribeSeries",
        "eq": "compute.Eq",
        "ne": "compute.Ne",
        "lt": "compute.Lt",
        "le": "compute.Le",
        "gt": "compute.Gt",
        "ge": "compute.Ge",
        "not_": "compute.Not",
        "filter": "compute.Filter",
        "get_chunks": "Chunks",
        "limit": "Head",
        "replace_strict": "ReplaceStrictSeries",
        "rolling_median": "RollingMedianWindow",
        "rolling_max_by": 'RollingBy("max", ...)',
        "rolling_mean_by": 'RollingBy("mean", ...)',
        "rolling_median_by": 'RollingBy("median", ...)',
        "rolling_min_by": 'RollingBy("min", ...)',
        "rolling_quantile_by": 'RollingBy("quantile", ...)',
        "rolling_std_by": 'RollingBy("std", ...)',
        "rolling_sum_by": 'RollingBy("sum", ...)',
        "rolling_var_by": 'RollingBy("var", ...)',
        "set": "Scatter",
        "sort": "SortWith / compute.Sort",
        "to_frame": "golars.ToFrame",
        "map_elements": "series.MapValues / golars.MapElements",
    },
    "series.list": {
        "set_difference": "SetOperation(other, SetDifference)",
        "set_intersection": "SetOperation(other, SetIntersection)",
        "set_symmetric_difference": "SetOperation(other, SetSymmetricDifference)",
        "set_union": "SetOperation(other, SetUnion)",
    },
    "series.struct": {"fields": "FieldNames"},
    "series.str": {
        "to_lowercase": "ToLower",
        "to_uppercase": "ToUpper",
    },
    "toplevel": {
        "arange": "golars.IntRange",
        "from_arrow": "dataframe.FromArrowTable / series.FromArrowArray",
        "from_dict": "golars.FromMap",
        "read_clipboard": "clipboard.Read",
        "read_database": "sql.ReadSQL",
        "read_parquet_schema": "parquet.ReadSchema",
        "read_ipc_stream": "golars.NewIPCStreamReader",
        "all": "golars.All",
        "struct": "golars.Struct",
        "escape_regex": "expr.StrOps.EscapeRegex",
    },
}

# Entries that exist but cover less than polars: {surface: {name: (go_name, note)}}.
PARTIAL: dict[str, dict[str, tuple[str, str]]] = {
    "expr": {
        "meta": (
            "expr.OutputName / expr.ReferencedColumns",
            "package functions, not a namespace: OutputName, ReferencedColumns, IsMultiColumn, Equal",
        ),
    },
    "dataframe": {
        "join": ("Join", "keys are column names (WithJoinKeys for left_on/right_on), not expressions"),
        "select": ("Select", "takes column names; use golars.SelectExpr for expressions"),
        "with_columns": ("WithColumns", "takes Series; use golars.WithColumnsExpr for expressions"),
    },
    "lazyframe": {
        "join": ("Join", "keys are column names (WithJoinKeys for left_on/right_on), not expressions"),
    },
    "toplevel": {
        "union": ("golars.Concat", "vertical concat; polars union does not keep order"),
        "cum_sum": ("golars.CumSumHorizontal", "horizontal form; for one column use Col(x).CumSum()"),
        "implode": ("expr.Expr.Implode", "method only: Col(x).Implode()"),
        "cum_count": ("expr.Expr.CumCount", "method only: Col(x).CumCount()"),
        "from_dicts": ("dataframe.DataFrame.ToDicts", "reverse direction only; build frames with FromMap"),
        "from_records": ("dataframe.FromRecord", "takes an arrow RecordBatch, not Python rows"),
        "row_index": ("dataframe.DataFrame.WithRowIndex", "frame method only: df.WithRowIndex / lf.WithRowIndex"),
        "time": ("expr.LitTime", "literal only; no expression-built time"),
    },
}

# Notes attached to a done row: {surface_key: {polars_name: note}}.
NOTES: dict[str, dict[str, str]] = {
    "series": {
        "sum": "returns float64; empty or all-null gives NaN, not None",
        "mean": "returns float64; empty or all-null gives NaN, not None",
        "min": "returns float64; empty or all-null gives NaN, not None",
        "max": "returns float64; empty or all-null gives NaN, not None",
    },
    "dataframe": {
        "show": "String() renders the polars-style table",
    },
}


# ---------------------------------------------------------------------------
# Surfaces
# ---------------------------------------------------------------------------


@dataclass
class Surface:
    key: str
    title: str
    polars_prefix: str  # e.g. "df." shown before the polars name
    go_prefix: str  # e.g. "df." shown before the golars name
    polars_names: list[str]
    go_names: set[str]
    # Top-level surfaces search several Go packages; values are "pkg.Name".
    qualified: bool = False


def build_surfaces() -> list[Surface]:
    s: list[Surface] = []
    s.append(Surface("expr", "Expr", "", "", public_names(pl.Expr), go_methods("expr", "Expr")))
    exprns = {
        "str": "StrOps",
        "dt": "DtOps",
        "list": "ListOps",
        "arr": "ArrOps",
        "struct": "StructOps",
        "name": "NameOps",
        "bin": "BinOps",
        "cat": "CatOps",
    }
    goacc = {
        "str": "Str",
        "dt": "Dt",
        "list": "List",
        "arr": "Arr",
        "struct": "Struct",
        "name": "Name",
        "bin": "Bin",
        "cat": "Cat",
    }
    for ns, recv in exprns.items():
        s.append(
            Surface(
                f"expr.{ns}",
                f"Expr.{ns}",
                f"{ns}.",
                f"{goacc[ns]}().",
                public_names(expr_ns(ns)),
                go_methods("expr", recv),
            )
        )
    s.append(Surface("dataframe", "DataFrame", "df.", "df.", public_names(pl.DataFrame), go_methods("dataframe", "DataFrame")))
    s.append(Surface("lazyframe", "LazyFrame", "lf.", "lf.", public_names(pl.LazyFrame), go_methods("lazy", "LazyFrame")))
    s.append(
        Surface(
            "groupby",
            "GroupBy",
            "gb.",
            "gb.",
            public_names(type(_df.group_by("a"))),
            go_methods("dataframe", "GroupBy"),
        )
    )
    s.append(
        Surface(
            "lazygroupby",
            "LazyGroupBy",
            "lgb.",
            "lgb.",
            public_names(type(_df.lazy().group_by("a"))),
            go_methods("lazy", "LazyGroupBy"),
        )
    )
    s.append(Surface("series", "Series", "s.", "s.", public_names(pl.Series), go_methods("series", "Series")))
    serns = {
        "str": "StrOps",
        "dt": "DtOps",
        "list": "ListOps",
        "arr": "ArrOps",
        "struct": "StructOps",
        "bin": "BinOps",
        "cat": "CatOps",
    }
    for ns, recv in serns.items():
        s.append(
            Surface(
                f"series.{ns}",
                f"Series.{ns}",
                f"s.{ns}.",
                f"s.{goacc[ns]}().",
                public_names(series_ns(ns)),
                go_methods("series", recv),
            )
        )
    dtypes = sorted(
        n
        for n in dir(pl)
        if inspect.isclass(getattr(pl, n)) and issubclass(getattr(pl, n), pl.DataType) and n != "DataType"
    )
    s.append(Surface("dtype", "Data types", "pl.", "dtype.", dtypes, GO["dtype"].funcs))
    top: set[str] = set()
    for pkg in ["golars", "expr", "dataframe", "lazy", "series", "csv", "parquet", "ipc", "json", "sql", "clipboard"]:
        top.update(f"{pkg}.{f}" for f in GO[pkg].funcs)
    s.append(Surface("toplevel", "Top-level functions", "pl.", "", toplevel_names(), top, qualified=True))
    return s


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def norm(name: str) -> str:
    return name.replace("_", "").replace(".", "").lower()


@dataclass
class Row:
    polars: str
    golars: str
    status: str  # done, partial, todo, n/a
    note: str = ""


IDENT_RE = re.compile(r"[A-Za-z_][\w.]*")


def resolve(sf: Surface, candidate: str) -> str | None:
    """Return the display form of the first alternative that exists."""
    for alt in candidate.split(" / "):
        alt = alt.strip()
        base = IDENT_RE.match(alt).group(0)
        rest = alt[len(base):]
        parts = base.split(".")
        if len(parts) > 1 and parts[0] in GO:
            pkg = GO[parts[0]]
            if len(parts) == 2 and parts[1] in pkg.funcs:
                return alt
            if len(parts) == 3 and parts[2] in pkg.methods.get(parts[1], set()):
                return alt
            continue
        if sf.qualified:
            for q in sorted(sf.go_names):
                if q.split(".", 1)[1] == base:
                    return q + rest
        elif base in sf.go_names:
            return alt
    return None


def direct(sf: Surface, name: str) -> str | None:
    n = norm(name)
    if sf.qualified:
        # Prefer the root package, then expr, then the rest.
        order = ["golars", "expr", "dataframe", "lazy", "series"]
        hits = [g for g in sf.go_names if norm(g.split(".", 1)[1]) == n]
        hits.sort(key=lambda g: (order.index(g.split(".")[0]) if g.split(".")[0] in order else 99, g))
        return hits[0] if hits else None
    for g in sorted(sf.go_names):
        if norm(g) == n:
            return g
    return None


def classify(sf: Surface) -> list[Row]:
    rows = []
    na = {**NOT_APPLICABLE_COMMON, **NOT_APPLICABLE.get(sf.key, {})}
    renames = RENAMES.get(sf.key, {})
    partial = PARTIAL.get(sf.key, {})
    notes = NOTES.get(sf.key, {})
    for name in sf.polars_names:
        if name in na and not direct(sf, name):
            rows.append(Row(name, "", "n/a", na[name]))
            continue
        if name in partial:
            cand, note = partial[name]
            hit = resolve(sf, cand)
            if hit:
                rows.append(Row(name, hit, "partial", note))
            else:
                rows.append(Row(name, "", "todo", f"expected {cand}, not found"))
            continue
        hit = direct(sf, name)
        if not hit and name in renames:
            hit = resolve(sf, renames[name])
        if hit:
            rows.append(Row(name, hit, "done", notes.get(name, "")))
        else:
            rows.append(Row(name, "", "todo", notes.get(name, "")))
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

INTENTIONAL_DIFFERENCES = """\
These are deliberate, checked against polars {version} and the golars
source. Most of them are "same as polars" confirmations for behaviour
people often ask about.

- **Null order in sorts matches polars.** Nulls sort first by default,
  in both ascending and descending order (`nulls_last=False` in
  polars). `compute.NullsFirst` is the zero value of
  `compute.NullPosition`; pass `NullsLast` to flip it.
- **NaN order matches polars.** NaN sorts as the largest float: last
  when ascending, first when descending. `min` and `max` skip NaN
  unless every non-null value is NaN.
- **Integer `/` in expressions is true division, like polars.** It
  returns f64 and `x / 0` gives `inf` or `NaN`. `FloorDiv` keeps
  integer semantics. The low-level kernel `compute.Div` on two integer
  Series is different: it stays integer and a zero divisor gives null.
- **Counts are u32, like polars.** `count`, `len`, `n_unique`,
  `null_count`, `arg_*`, group sizes and boolean sums come back as
  u32.
- **Eager scalar aggregations return Go values.** `Series.Sum`, `Mean`,
  `Min`, `Max`, `Std` and `Median` return `(float64, error)`. An empty
  or all-null input returns `NaN` where polars returns `None`. The
  expression forms (`Col(x).Sum()`) keep the input dtype and return
  null like polars.
- **Errors instead of exceptions.** Eager operations take a
  `context.Context` and return `error`. The public API does not panic.
- **Expression entry points on DataFrame.** `df.Select` takes column
  names and `df.WithColumns` takes Series. Use `golars.SelectExpr` and
  `golars.WithColumnsExpr`, or a LazyFrame, for expressions.
- **Hash values differ.** `hash` and `hash_rows` are stable within
  golars but do not reproduce polars' hash values.
- **No global string cache.** Categoricals carry their own dictionary,
  so `enable_string_cache` and `cat.to_local` have nothing to do.
- **Group order.** Without `MaintainOrder`, the output order of
  `group_by` is an implementation detail, as in polars. With it, groups
  come out in first-appearance order.
"""


def md_escape_cell(text: str) -> str:
    return text.replace("|", "\\|")


def code(text: str) -> str:
    return f"`{text}`" if text else ""


def pct(a: int, b: int) -> str:
    return f"{100 * a / b:.0f}%" if b else "n/a"


def render(results, version: str) -> str:
    out: list[str] = []
    w = out.append
    w("# golars / polars API surface")
    w("")
    w("This file is generated. Do not edit it by hand. Regenerate with:")
    w("")
    w("```sh")
    w("cd bench/polars-compare && uv run python api_coverage.py")
    w("```")
    w("")
    intro = (
        f"The script lists the public API of polars {version} with `dir()`, parses "
        "the golars Go source for exported methods and functions, and maps "
        "the names. A polars name counts as `done` when a golars name matches it "
        "case-insensitively with underscores removed (`n_unique` and `NUnique`), "
        "or when the rename table in the script points at a name that exists in "
        "the Go source. `partial` means golars covers a narrower form; the note "
        "says how. `todo` means no match was found. Names that have no meaning in "
        "Go (pandas, numpy, Python UDFs, plotting) are listed under "
        "[Not applicable](#not-applicable) and left out of the totals. Deprecated "
        "polars names are skipped."
    )
    w(textwrap.fill(intro, width=72, break_on_hyphens=False, break_long_words=False))
    w("")
    w("A `done` row means the name exists with the same intent. It does not")
    w("promise identical arguments; Go signatures use typed options instead of")
    w("keyword arguments.")
    w("")
    w("## Coverage summary")
    w("")
    w("| Surface | Covered | Done | Partial | Todo | Not applicable | Coverage |")
    w("|---|---|---|---|---|---|---|")
    tot = [0, 0, 0, 0]
    for sf, rows in results:
        d = sum(r.status == "done" for r in rows)
        p = sum(r.status == "partial" for r in rows)
        t = sum(r.status == "todo" for r in rows)
        n = sum(r.status == "n/a" for r in rows)
        tot = [tot[0] + d, tot[1] + p, tot[2] + t, tot[3] + n]
        w(f"| [{sf.title}](#{anchor(sf.title)}) | {d + p} of {d + p + t} | {d} | {p} | {t} | {n} | {pct(d + p, d + p + t)} |")
    d, p, t, n = tot
    w(f"| **Total** | **{d + p} of {d + p + t}** | {d} | {p} | {t} | {n} | {pct(d + p, d + p + t)} |")
    w("")
    w("Series namespaces (`s.str`, `s.dt`, ...) are counted against the")
    w("eager `series.*Ops` types.")
    w("")
    for sf, rows in results:
        w(f"## {sf.title}")
        w("")
        w(f"{surface_blurb(sf)}")
        w("")
        w("| polars | golars | Status | Notes |")
        w("|---|---|---|---|")
        for r in rows:
            if r.status == "n/a":
                continue
            pol = code(sf.polars_prefix + r.polars)
            if not r.golars:
                gol = ""
            elif sf.qualified or "." in r.golars.split("(")[0]:
                gol = code(r.golars)
            else:
                gol = code(sf.go_prefix + r.golars)
            w(f"| {pol} | {md_escape_cell(gol)} | {r.status} | {md_escape_cell(r.note)} |")
        w("")
    w("## Intentional differences")
    w("")
    w(INTENTIONAL_DIFFERENCES.format(version=version).rstrip())
    w("")
    w("## Known gaps")
    w("")
    w("Every `todo` row from the tables above, grouped by surface.")
    w("")
    for sf, rows in results:
        todo = [r.polars for r in rows if r.status == "todo"]
        if todo:
            w(f"- **{sf.title}**: " + ", ".join(code(x) for x in todo))
    w("")
    w("Partial rows, where golars covers a narrower form:")
    w("")
    for sf, rows in results:
        for r in rows:
            if r.status == "partial":
                w(f"- {code(sf.polars_prefix + r.polars)}: {r.note}")
    w("")
    w("## Not applicable")
    w("")
    w("Excluded from the totals because they only make sense in Python.")
    w("")
    w("| Surface | polars | Reason |")
    w("|---|---|---|")
    for sf, rows in results:
        for r in rows:
            if r.status == "n/a":
                w(f"| {sf.title} | {code(sf.polars_prefix + r.polars)} | {md_escape_cell(r.note)} |")
    w("")
    return "\n".join(out)


def anchor(title: str) -> str:
    a = title.lower().replace(" ", "-")
    return re.sub(r"[^a-z0-9-]", "", a)


BLURBS = {
    "expr": "`pl.Expr` against `expr.Expr`.",
    "dataframe": "`pl.DataFrame` against `*dataframe.DataFrame`. Some rows point at root package helpers (`golars.*`) or I/O packages.",
    "lazyframe": "`pl.LazyFrame` against `lazy.LazyFrame`.",
    "groupby": "`df.group_by(...)` against `*dataframe.GroupBy`.",
    "lazygroupby": "`lf.group_by(...)` against `lazy.LazyGroupBy`.",
    "series": "`pl.Series` against `*series.Series`. Some rows point at `compute.*` kernels or root helpers.",
    "dtype": "polars data type classes against the `dtype` package constructors.",
    "toplevel": "`pl.*` functions against the root `golars` package, then `expr`, `dataframe`, `lazy`, `series` and the `io/*` packages.",
}


def surface_blurb(sf) -> str:
    if sf.key in BLURBS:
        return BLURBS[sf.key]
    base, ns = sf.key.split(".")
    if base == "expr":
        return f"`pl.Expr.{ns}` against `expr.{type_name(ns)}`, reached with `.{sf.go_prefix.rstrip('.')}`."
    return f"`pl.Series.{ns}` against `series.{type_name(ns)}`, reached with `s.{sf.go_prefix[2:].rstrip('.')}`."


def type_name(ns: str) -> str:
    return {"str": "StrOps", "dt": "DtOps", "list": "ListOps", "arr": "ArrOps", "struct": "StructOps", "name": "NameOps", "bin": "BinOps", "cat": "CatOps"}[ns]


MDX_FOOTER = """
See [roadmap.md](roadmap.md) for the perf side of the same picture
(throughput ratios vs polars 1.39 on the bench suite).
"""

MD_FOOTER = """
See [roadmap.md](roadmap.md) for the perf side of the same picture and
[`bench/polars-compare/`](../bench/polars-compare/) for throughput
ratios vs polars.
"""

DEFAULT_FRONTMATTER = """---
title: golars / polars API surface
description: Map between polars' Python API and golars' Go surface.
icon: FileText
---
"""


def to_mdx(md: str) -> str:
    """Escape MDX-significant characters outside code spans and fences."""
    lines = md.split("\n")
    # Drop the H1: the frontmatter title renders it.
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
    out = []
    in_fence = False
    for line in lines:
        if line.startswith("```"):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        parts = line.split("`")
        for i in range(0, len(parts), 2):
            parts[i] = (
                parts[i]
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("{", "&#123;")
                .replace("}", "&#125;")
            )
        out.append("`".join(parts))
    return "\n".join(out)


def read_frontmatter(path: Path) -> str:
    if not path.exists():
        return DEFAULT_FRONTMATTER
    text = path.read_text()
    if not text.startswith("---\n"):
        return DEFAULT_FRONTMATTER
    end = text.index("\n---\n", 4)
    return text[: end + 5]


def main() -> None:
    surfaces = build_surfaces()
    results = [(sf, classify(sf)) for sf in surfaces]
    md = render(results, pl.__version__)
    OUT_MD.write_text(md + MD_FOOTER)
    fm = read_frontmatter(OUT_MDX)
    OUT_MDX.write_text(fm + "\n" + to_mdx(md) + MDX_FOOTER)
    for sf, rows in results:
        d = sum(r.status == "done" for r in rows)
        p = sum(r.status == "partial" for r in rows)
        t = sum(r.status == "todo" for r in rows)
        print(f"{sf.title:22} {d + p:4} of {d + p + t:4}  (done {d}, partial {p}, todo {t})")
    print(f"wrote {OUT_MD.relative_to(ROOT)} and {OUT_MDX.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
