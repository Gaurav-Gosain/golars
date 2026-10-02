# Generates the polars-written parquet fixtures in this directory.
# Each fixture names an Arrow IPC file of the same frame that the Go
# tests use as the expected value.
#
#   cd bench/polars-compare && uv run python ../../io/parquet/testdata/polars/gen.py
import datetime as dt
import os

import polars as pl

here = os.path.dirname(os.path.abspath(__file__))


def frame(n: int) -> pl.DataFrame:
    def nulls(values, every):
        return [None if every and i % every == 0 else v for i, v in enumerate(values)]

    r = range(n)
    return pl.DataFrame(
        {
            "i8": pl.Series(nulls([(i * 7) % 256 - 128 for i in r], 5), dtype=pl.Int8),
            "i16": pl.Series(nulls([(i * 31) % 65536 - 32768 for i in r], 0), dtype=pl.Int16),
            "i32": pl.Series(nulls([i * 1000003 % 2**31 for i in r], 3), dtype=pl.Int32),
            "i64": pl.Series(nulls([(i * 6364136223846793005) % 2**63 - 2**62 for i in r], 0), dtype=pl.Int64),
            "u8": pl.Series(nulls([i % 256 for i in r], 4), dtype=pl.UInt8),
            "u16": pl.Series(nulls([i * 13 % 65536 for i in r], 0), dtype=pl.UInt16),
            "u32": pl.Series(nulls([(i * 2654435761) % 2**32 for i in r], 6), dtype=pl.UInt32),
            "u64": pl.Series(nulls([(i * 11400714819323198485) % 2**64 for i in r], 0), dtype=pl.UInt64),
            "f32": pl.Series(nulls([i / 3 for i in r], 7), dtype=pl.Float32),
            "f64": pl.Series(nulls([float("nan") if i % 11 == 5 else i * 1.25 - 40 for i in r], 9), dtype=pl.Float64),
            "bool": pl.Series(nulls([i % 3 == 0 for i in r], 8), dtype=pl.Boolean),
            "str_low": pl.Series(nulls([f"k{i % 5}" for i in r], 4)),
            "str_high": pl.Series(nulls([f"value-{i * 17}" * (1 + i % 3) for i in r], 0)),
            "str_empty": pl.Series(nulls(["" if i % 2 else "x" for i in r], 5)),
            "bin": pl.Series(nulls([bytes([i % 256, (i * 3) % 256]) for i in r], 6), dtype=pl.Binary),
            "date": pl.Series(nulls([dt.date(2020, 1, 1) + dt.timedelta(days=i) for i in r], 10)),
            "dt_ms": pl.Series(nulls([dt.datetime(2021, 3, 4) + dt.timedelta(seconds=i * 37) for i in r], 0)).cast(pl.Datetime("ms")),
            "dt_us": pl.Series(nulls([dt.datetime(2021, 3, 4) + dt.timedelta(microseconds=i * 12345) for i in r], 4)).cast(pl.Datetime("us")),
            "dt_ns": pl.Series(nulls([dt.datetime(2021, 3, 4) + dt.timedelta(microseconds=i) for i in r], 0)).cast(pl.Datetime("ns")),
            "dt_tz": pl.Series(nulls([dt.datetime(2022, 1, 1) + dt.timedelta(hours=i) for i in r], 3)).cast(pl.Datetime("us")).dt.replace_time_zone("Asia/Dubai"),
            "time": pl.Series(nulls([dt.time(i % 24, i % 60, i % 60) for i in r], 5)),
            "all_null": pl.Series([None] * n, dtype=pl.Int64),
        }
    )


written = set()


def write(name: str, df: pl.DataFrame, expect: str, **kw) -> None:
    df.write_parquet(os.path.join(here, name + ".parquet"), **kw)
    if expect not in written:
        written.add(expect)
        path = os.path.join(here, expect + ".arrow")
        df.write_ipc(path, compression="zstd", compat_level=pl.CompatLevel.oldest())


base = frame(200)
for codec in ["uncompressed", "snappy", "zstd", "gzip", "lz4", "brotli"]:
    write(f"all_{codec}", base, "base", compression=codec)
write("multi_rg", base, "base", compression="snappy", row_group_size=64)
write("no_stats", base, "base", compression="zstd", statistics=False)
write("one_row", frame(1), "one_row", compression="snappy")
write("empty", frame(0), "empty", compression="snappy")
nested = pl.DataFrame(
    {
        "id": pl.Series(list(range(50)), dtype=pl.Int64),
        "lst": [[i, i + 1] if i % 4 else None for i in range(50)],
        "s": [f"n{i % 7}" for i in range(50)],
        "dec": pl.Series([f"{i}.25" for i in range(50)]).cast(pl.Decimal(10, 2)),
        "cat": pl.Series([f"c{i % 3}" for i in range(50)], dtype=pl.Categorical),
    }
)
write("nested", nested, "nested", compression="snappy")
