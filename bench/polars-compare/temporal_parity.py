"""Generate golars temporal parity cases from polars.

Run from bench/polars-compare:

    uv run python temporal_parity.py > ../../eval/temporal_parity_data_test.go
    gofmt -w ../../eval/temporal_parity_data_test.go

Each case pairs a polars expression with the equivalent golars Go
expression. The script evaluates the polars side, renders every value
canonically (temporal values as their physical integer) and emits a Go
table the harness in eval/temporal_parity_test.go replays.
"""

from datetime import date, datetime, time, timedelta

import polars as pl

UTC_TZ = "America/New_York"


def base_frame():
    ts = [
        datetime(2020, 2, 29, 13, 45, 30, 123456),
        None,
        datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime(1900, 1, 1),
        datetime(2024, 12, 30, 1, 2, 3),
        datetime(2021, 1, 3, 23, 30),
        datetime(2000, 6, 15, 12, 0, 0, 500000),
        datetime(1970, 1, 1),
    ]
    dur = [
        timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5),
        None,
        timedelta(seconds=-90),
        timedelta(0),
        timedelta(days=-3, hours=5),
        timedelta(microseconds=1),
        timedelta(hours=36),
        timedelta(microseconds=-1),
    ]
    utc = [
        datetime(2024, 3, 10, 6, 30),
        datetime(2024, 3, 10, 7, 30),
        datetime(2024, 11, 3, 5, 30),
        datetime(2024, 11, 3, 6, 30),
        None,
        datetime(1960, 6, 1, 12, 0),
        datetime(2024, 7, 1, 16, 0),
        datetime(2024, 1, 1, 5, 0),
    ]
    df = pl.DataFrame(
        {
            "ts": ts,
            "dur": dur,
            "i": [1, -1, 2, None, -3, 5, 100, 7],
            "f": [1.5, None, -2.25, 0.0, 3.0, 1e6, -0.5, 2.0],
            "s": [
                "2021-01-02 03:04:05",
                None,
                "1969-12-31 23:59:59.999",
                "2024-02-29T12:00:00",
                "garbage",
                "2021-01-02",
                "1900-01-01 00:00:00.123456",
                "2000-06-15 12:00",
            ],
            "sd": [
                "2021-01-02",
                None,
                "1969-12-31",
                "2024/02/29",
                "2021-02-30",
                "2000-06-15",
                "1900-01-01",
                "2024-01-01",
            ],
            "st": ["03:04:05", None, "23:59:59.123456", "00:00:00", "12:34:56.5", "01:02:03", "10:00:00", "23:00:00"],
            "utc": utc,
        }
    )
    return df.with_columns(
        pl.col("ts").dt.date().alias("d"),
        pl.col("ts").dt.time().alias("t"),
        pl.col("ts").dt.cast_time_unit("ms").alias("ts_ms"),
        pl.col("ts").dt.cast_time_unit("ns").alias("ts_ns"),
        pl.col("utc").dt.replace_time_zone("UTC").dt.convert_time_zone(UTC_TZ).alias("tz"),
        pl.col("utc").dt.replace_time_zone("UTC").alias("utc"),
    )


def window_frame():
    return pl.DataFrame(
        {
            "t": [
                datetime(2021, 12, 16, 0, 0),
                datetime(2021, 12, 16, 0, 30),
                datetime(2021, 12, 16, 1, 0),
                datetime(2021, 12, 16, 1, 30),
                datetime(2021, 12, 16, 2, 0),
                datetime(2021, 12, 16, 2, 30),
                datetime(2021, 12, 16, 3, 0),
            ],
            "n": [0, 1, 2, 3, 4, 5, 6],
            "g": ["b", "a", "b", "a", "a", "b", "b"],
            "f": [1.0, None, 2.5, 3.0, None, 4.0, 5.5],
        }
    ).with_columns(pl.col("t").dt.date().alias("d"))


def unsorted_frame():
    return pl.DataFrame(
        {
            "t": [
                datetime(2021, 1, 1, 3),
                datetime(2021, 1, 1, 0),
                datetime(2021, 1, 1, 2),
                datetime(2021, 1, 1, 1),
                datetime(2021, 1, 1, 1),
            ],
            "v": [3.0, 0.0, 2.0, 1.0, 10.0],
        }
    )


def int_frame():
    return pl.DataFrame({"i": [1, 2, 3, 5, 8, 8, 13], "v": [1, 2, 3, 4, 5, 6, 7], "g": ["a", "b", "a", "a", "b", "b", "a"]})


def tz_window_frame():
    utc = [datetime(2024, 11, 2, 22) + timedelta(hours=h) for h in range(0, 12)]
    return pl.DataFrame({"t": utc, "v": list(range(12))}).with_columns(
        pl.col("t").dt.replace_time_zone("UTC").dt.convert_time_zone(UTC_TZ)
    )


FRAMES = {
    "base": base_frame(),
    "win": window_frame(),
    "unsorted": unsorted_frame(),
    "ints": int_frame(),
    "wtz": tz_window_frame(),
}

C = pl.col

# (name, frame, polars expression, golars Go expression)
CASES = [
    # ---- calendar fields ----
    ("year", "base", C("ts").dt.year(), 'col("ts").Dt().Year()'),
    ("month", "base", C("ts").dt.month(), 'col("ts").Dt().Month()'),
    ("day", "base", C("ts").dt.day(), 'col("ts").Dt().Day()'),
    ("hour", "base", C("ts").dt.hour(), 'col("ts").Dt().Hour()'),
    ("minute", "base", C("ts").dt.minute(), 'col("ts").Dt().Minute()'),
    ("second", "base", C("ts").dt.second(), 'col("ts").Dt().Second()'),
    ("millisecond", "base", C("ts").dt.millisecond(), 'col("ts").Dt().Millisecond()'),
    ("microsecond", "base", C("ts").dt.microsecond(), 'col("ts").Dt().Microsecond()'),
    ("nanosecond", "base", C("ts").dt.nanosecond(), 'col("ts").Dt().Nanosecond()'),
    ("quarter", "base", C("ts").dt.quarter(), 'col("ts").Dt().Quarter()'),
    ("week", "base", C("ts").dt.week(), 'col("ts").Dt().Week()'),
    ("weekday", "base", C("ts").dt.weekday(), 'col("ts").Dt().Weekday()'),
    ("ordinal_day", "base", C("ts").dt.ordinal_day(), 'col("ts").Dt().OrdinalDay()'),
    ("iso_year", "base", C("ts").dt.iso_year(), 'col("ts").Dt().IsoYear()'),
    ("is_leap_year", "base", C("ts").dt.is_leap_year(), 'col("ts").Dt().IsLeapYear()'),
    ("days_in_month", "base", C("ts").dt.days_in_month(), 'col("ts").Dt().DaysInMonth()'),
    ("century", "base", C("ts").dt.century(), 'col("ts").Dt().Century()'),
    ("millennium", "base", C("ts").dt.millennium(), 'col("ts").Dt().Millennium()'),
    ("date_year", "base", C("d").dt.year(), 'col("d").Dt().Year()'),
    ("date_weekday", "base", C("d").dt.weekday(), 'col("d").Dt().Weekday()'),
    ("date_week", "base", C("d").dt.week(), 'col("d").Dt().Week()'),
    ("date_hour_err", "base", C("d").dt.hour(), 'col("d").Dt().Hour()'),
    ("ms_year", "base", C("ts_ms").dt.year(), 'col("ts_ms").Dt().Year()'),
    ("ns_nanosecond", "base", C("ts_ns").dt.nanosecond(), 'col("ts_ns").Dt().Nanosecond()'),
    ("time_hour", "base", C("t").dt.hour(), 'col("t").Dt().Hour()'),
    ("time_microsecond", "base", C("t").dt.microsecond(), 'col("t").Dt().Microsecond()'),
    ("tz_hour", "base", C("tz").dt.hour(), 'col("tz").Dt().Hour()'),
    ("tz_day", "base", C("tz").dt.day(), 'col("tz").Dt().Day()'),
    ("tz_date", "base", C("tz").dt.date(), 'col("tz").Dt().Date()'),
    # ---- conversions ----
    ("date", "base", C("ts").dt.date(), 'col("ts").Dt().Date()'),
    ("time", "base", C("ts").dt.time(), 'col("ts").Dt().Time()'),
    ("epoch_s", "base", C("ts").dt.epoch("s"), 'col("ts").Dt().Epoch("s")'),
    ("epoch_d", "base", C("ts").dt.epoch("d"), 'col("ts").Dt().Epoch("d")'),
    ("epoch_ms", "base", C("ts").dt.epoch("ms"), 'col("ts").Dt().Epoch("ms")'),
    ("epoch_ns", "base", C("ts").dt.epoch("ns"), 'col("ts").Dt().Epoch("ns")'),
    ("date_epoch_s", "base", C("d").dt.epoch("s"), 'col("d").Dt().Epoch("s")'),
    ("timestamp_ms", "base", C("ts").dt.timestamp("ms"), 'col("ts").Dt().Timestamp("ms")'),
    ("timestamp_ms_floor", "base", C("ts_ns").dt.timestamp("ms"), 'col("ts_ns").Dt().Timestamp("ms")'),
    ("date_timestamp", "base", C("d").dt.timestamp(), 'col("d").Dt().Timestamp("us")'),
    ("tz_epoch", "base", C("tz").dt.epoch("s"), 'col("tz").Dt().Epoch("s")'),
    ("cast_time_unit_ms", "base", C("ts").dt.cast_time_unit("ms"), 'col("ts").Dt().CastTimeUnit(dtype.Millisecond)'),
    ("cast_time_unit_ns", "base", C("ts").dt.cast_time_unit("ns"), 'col("ts").Dt().CastTimeUnit(dtype.Nanosecond)'),
    ("dur_cast_time_unit_ms", "base", C("dur").dt.cast_time_unit("ms"), 'col("dur").Dt().CastTimeUnit(dtype.Millisecond)'),
    ("with_time_unit", "base", C("ts").dt.with_time_unit("ms"), 'col("ts").Dt().WithTimeUnit(dtype.Millisecond)'),
    ("total_days", "base", C("dur").dt.total_days(), 'col("dur").Dt().TotalDays()'),
    ("total_hours", "base", C("dur").dt.total_hours(), 'col("dur").Dt().TotalHours()'),
    ("total_minutes", "base", C("dur").dt.total_minutes(), 'col("dur").Dt().TotalMinutes()'),
    ("total_seconds", "base", C("dur").dt.total_seconds(), 'col("dur").Dt().TotalSeconds()'),
    ("total_milliseconds", "base", C("dur").dt.total_milliseconds(), 'col("dur").Dt().TotalMilliseconds()'),
    ("total_microseconds", "base", C("dur").dt.total_microseconds(), 'col("dur").Dt().TotalMicroseconds()'),
    ("total_nanoseconds", "base", C("dur").dt.total_nanoseconds(), 'col("dur").Dt().TotalNanoseconds()'),
    ("total_seconds_frac", "base", C("dur").dt.total_seconds(fractional=True), 'col("dur").Dt().TotalSecondsFractional()'),
    # ---- truncate / round / offset ----
    ("truncate_1h", "base", C("ts").dt.truncate("1h"), 'col("ts").Dt().Truncate("1h")'),
    ("truncate_15m", "base", C("ts").dt.truncate("15m"), 'col("ts").Dt().Truncate("15m")'),
    ("truncate_1d", "base", C("ts").dt.truncate("1d"), 'col("ts").Dt().Truncate("1d")'),
    ("truncate_2d", "base", C("ts").dt.truncate("2d"), 'col("ts").Dt().Truncate("2d")'),
    ("truncate_1w", "base", C("ts").dt.truncate("1w"), 'col("ts").Dt().Truncate("1w")'),
    ("truncate_2w", "base", C("ts").dt.truncate("2w"), 'col("ts").Dt().Truncate("2w")'),
    ("truncate_1mo", "base", C("ts").dt.truncate("1mo"), 'col("ts").Dt().Truncate("1mo")'),
    ("truncate_3mo", "base", C("ts").dt.truncate("3mo"), 'col("ts").Dt().Truncate("3mo")'),
    ("truncate_1q", "base", C("ts").dt.truncate("1q"), 'col("ts").Dt().Truncate("1q")'),
    ("truncate_1y", "base", C("ts").dt.truncate("1y"), 'col("ts").Dt().Truncate("1y")'),
    ("truncate_1d2h", "base", C("ts").dt.truncate("1d2h"), 'col("ts").Dt().Truncate("1d2h")'),
    ("truncate_date_1mo", "base", C("d").dt.truncate("1mo"), 'col("d").Dt().Truncate("1mo")'),
    ("truncate_date_1w", "base", C("d").dt.truncate("1w"), 'col("d").Dt().Truncate("1w")'),
    ("truncate_tz_1d", "base", C("tz").dt.truncate("1d"), 'col("tz").Dt().Truncate("1d")'),
    ("truncate_tz_1h", "base", C("tz").dt.truncate("1h"), 'col("tz").Dt().Truncate("1h")'),
    ("truncate_tz_1mo", "base", C("tz").dt.truncate("1mo"), 'col("tz").Dt().Truncate("1mo")'),
    ("round_1h", "base", C("ts").dt.round("1h"), 'col("ts").Dt().Round("1h")'),
    ("round_1d", "base", C("ts").dt.round("1d"), 'col("ts").Dt().Round("1d")'),
    ("round_1mo", "base", C("ts").dt.round("1mo"), 'col("ts").Dt().Round("1mo")'),
    ("round_1y", "base", C("ts").dt.round("1y"), 'col("ts").Dt().Round("1y")'),
    ("round_1w", "base", C("ts").dt.round("1w"), 'col("ts").Dt().Round("1w")'),
    ("round_date_1mo", "base", C("d").dt.round("1mo"), 'col("d").Dt().Round("1mo")'),
    ("round_tz_1d", "base", C("tz").dt.round("1d"), 'col("tz").Dt().Round("1d")'),
    ("offset_1mo", "base", C("ts").dt.offset_by("1mo"), 'col("ts").Dt().OffsetBy("1mo")'),
    ("offset_neg_1mo", "base", C("ts").dt.offset_by("-1mo"), 'col("ts").Dt().OffsetBy("-1mo")'),
    ("offset_1y", "base", C("ts").dt.offset_by("1y"), 'col("ts").Dt().OffsetBy("1y")'),
    ("offset_mixed", "base", C("ts").dt.offset_by("1y2mo3d4h"), 'col("ts").Dt().OffsetBy("1y2mo3d4h")'),
    ("offset_neg_3d", "base", C("ts").dt.offset_by("-3d"), 'col("ts").Dt().OffsetBy("-3d")'),
    ("offset_1q", "base", C("ts").dt.offset_by("1q"), 'col("ts").Dt().OffsetBy("1q")'),
    ("offset_date_1mo", "base", C("d").dt.offset_by("1mo"), 'col("d").Dt().OffsetBy("1mo")'),
    ("offset_date_25h", "base", C("d").dt.offset_by("25h"), 'col("d").Dt().OffsetBy("25h")'),
    ("offset_date_neg_1h", "base", C("d").dt.offset_by("-1h"), 'col("d").Dt().OffsetBy("-1h")'),
    ("offset_tz_1d", "base", C("tz").dt.offset_by("1d"), 'col("tz").Dt().OffsetBy("1d")'),
    ("offset_tz_24h", "base", C("tz").dt.offset_by("24h"), 'col("tz").Dt().OffsetBy("24h")'),
    ("offset_tz_1mo", "base", C("tz").dt.offset_by("1mo"), 'col("tz").Dt().OffsetBy("1mo")'),
    ("month_start", "base", C("ts").dt.month_start(), 'col("ts").Dt().MonthStart()'),
    ("month_end", "base", C("ts").dt.month_end(), 'col("ts").Dt().MonthEnd()'),
    ("month_start_date", "base", C("d").dt.month_start(), 'col("d").Dt().MonthStart()'),
    ("month_end_date", "base", C("d").dt.month_end(), 'col("d").Dt().MonthEnd()'),
    ("month_start_tz", "base", C("tz").dt.month_start(), 'col("tz").Dt().MonthStart()'),
    # ---- time zones ----
    ("convert_tz", "base", C("utc").dt.convert_time_zone("Asia/Kolkata"), 'col("utc").Dt().ConvertTimeZone("Asia/Kolkata")'),
    ("replace_tz_none", "base", C("tz").dt.replace_time_zone(None), 'col("tz").Dt().ReplaceTimeZone("")'),
    ("replace_tz_other", "base", C("tz").dt.replace_time_zone("Europe/London"), 'col("tz").Dt().ReplaceTimeZone("Europe/London")'),
    ("replace_tz_naive", "base", C("ts").dt.replace_time_zone("Asia/Tokyo"), 'col("ts").Dt().ReplaceTimeZone("Asia/Tokyo")'),
    ("replace_tz_dst_raise", "base", C("utc").dt.replace_time_zone(None).dt.replace_time_zone(UTC_TZ),
     'col("utc").Dt().ReplaceTimeZone("").Dt().ReplaceTimeZone("America/New_York")'),
    ("replace_tz_dst_null", "base",
     C("tz").dt.replace_time_zone(None).dt.offset_by("1h").dt.replace_time_zone(UTC_TZ, ambiguous="null", non_existent="null"),
     'col("tz").Dt().ReplaceTimeZone("").Dt().OffsetBy("1h").Dt().ReplaceTimeZoneWith("America/New_York", "null", "null")'),
    ("replace_tz_dst_earliest", "base",
     C("tz").dt.replace_time_zone(None).dt.replace_time_zone(UTC_TZ, ambiguous="earliest", non_existent="null"),
     'col("tz").Dt().ReplaceTimeZone("").Dt().ReplaceTimeZoneWith("America/New_York", "earliest", "null")'),
    ("replace_tz_dst_latest", "base",
     C("tz").dt.replace_time_zone(None).dt.replace_time_zone(UTC_TZ, ambiguous="latest", non_existent="null"),
     'col("tz").Dt().ReplaceTimeZone("").Dt().ReplaceTimeZoneWith("America/New_York", "latest", "null")'),
    ("datetime_drop_tz", "base", C("tz").dt.replace_time_zone(None), 'col("tz").Dt().Datetime()'),
    ("convert_tz_naive_err", "base", C("ts").dt.convert_time_zone("UTC"), 'col("ts").Dt().ConvertTimeZone("UTC")'),
    # ---- formatting ----
    ("to_string", "base", C("ts").dt.to_string(), 'col("ts").Dt().ToString("")'),
    ("to_string_ms", "base", C("ts_ms").dt.to_string(), 'col("ts_ms").Dt().ToString("")'),
    ("to_string_ns", "base", C("ts_ns").dt.to_string(), 'col("ts_ns").Dt().ToString("")'),
    ("to_string_date", "base", C("d").dt.to_string(), 'col("d").Dt().ToString("")'),
    ("to_string_time", "base", C("t").dt.to_string(), 'col("t").Dt().ToString("")'),
    ("to_string_tz", "base", C("tz").dt.to_string(), 'col("tz").Dt().ToString("")'),
    ("to_string_dur", "base", C("dur").dt.to_string(), 'col("dur").Dt().ToString("")'),
    ("to_string_dur_polars", "base", C("dur").dt.to_string("polars"), 'col("dur").Dt().ToString("polars")'),
    ("strftime_full", "base", C("ts").dt.strftime("%Y-%m-%dT%H:%M:%S%.3f %p %I %y %e %A %a %B %b %j %U %W %V %G %u %w %C %s"),
     'col("ts").Dt().Strftime("%Y-%m-%dT%H:%M:%S%.3f %p %I %y %e %A %a %B %b %j %U %W %V %G %u %w %C %s")'),
    ("strftime_frac", "base", C("ts").dt.strftime("%H:%M:%S%.f|%.6f|%.9f|%3f|%f|%-d|%-m|%_H|%k|%l|%P|%D|%F|%R|%T|%%"),
     'col("ts").Dt().Strftime("%H:%M:%S%.f|%.6f|%.9f|%3f|%f|%-d|%-m|%_H|%k|%l|%P|%D|%F|%R|%T|%%")'),
    ("strftime_date", "base", C("d").dt.strftime("%Y/%m/%d %a %b %j"), 'col("d").Dt().Strftime("%Y/%m/%d %a %b %j")'),
    ("strftime_tz", "base", C("tz").dt.strftime("%Y-%m-%d %H:%M:%S %Z %z %:z"), 'col("tz").Dt().Strftime("%Y-%m-%d %H:%M:%S %Z %z %:z")'),
    ("strftime_utc", "base", C("utc").dt.strftime("%H:%M %Z %z"), 'col("utc").Dt().Strftime("%H:%M %Z %z")'),
    ("strftime_time", "base", C("t").dt.strftime("%H-%M %p"), 'col("t").Dt().Strftime("%H-%M %p")'),
    ("cast_string_dt", "base", C("ts").cast(pl.String), 'col("ts").Cast(dtype.String())'),
    ("cast_string_date", "base", C("d").cast(pl.String), 'col("d").Cast(dtype.String())'),
    ("cast_string_time", "base", C("t").cast(pl.String), 'col("t").Cast(dtype.String())'),
    ("cast_string_tz", "base", C("tz").cast(pl.String), 'col("tz").Cast(dtype.String())'),
    # ---- casts ----
    ("cast_date_dt", "base", C("d").cast(pl.Datetime("us")), 'col("d").Cast(dtype.Datetime(dtype.Microsecond, ""))'),
    ("cast_dt_date", "base", C("ts").cast(pl.Date), 'col("ts").Cast(dtype.Date())'),
    ("cast_dt_i64", "base", C("ts").cast(pl.Int64), 'col("ts").Cast(dtype.Int64())'),
    ("cast_date_i64", "base", C("d").cast(pl.Int64), 'col("d").Cast(dtype.Int64())'),
    ("cast_date_i32", "base", C("d").cast(pl.Int32), 'col("d").Cast(dtype.Int32())'),
    ("cast_dt_ms", "base", C("ts").cast(pl.Datetime("ms")), 'col("ts").Cast(dtype.Datetime(dtype.Millisecond, ""))'),
    ("cast_dt_time", "base", C("ts").cast(pl.Time), 'col("ts").Cast(dtype.Time(dtype.Nanosecond))'),
    ("cast_tz_naive", "base", C("tz").cast(pl.Datetime("us")), 'col("tz").Cast(dtype.Datetime(dtype.Microsecond, ""))'),
    ("cast_tz_date", "base", C("tz").cast(pl.Date), 'col("tz").Cast(dtype.Date())'),
    ("cast_dur_i64", "base", C("dur").cast(pl.Int64), 'col("dur").Cast(dtype.Int64())'),
    ("cast_i64_dt", "base", C("i").cast(pl.Datetime("ms")), 'col("i").Cast(dtype.Datetime(dtype.Millisecond, ""))'),
    ("cast_i64_date", "base", C("i").cast(pl.Date), 'col("i").Cast(dtype.Date())'),
    ("cast_str_date", "base", C("sd").cast(pl.Date, strict=False), 'col("sd").Cast(dtype.Date())'),
    ("cast_str_dt", "base", C("s").cast(pl.Datetime("us"), strict=False), 'col("s").Cast(dtype.Datetime(dtype.Microsecond, ""))'),
    ("cast_month_i64", "base", C("ts").dt.month().cast(pl.Int64), 'col("ts").Dt().Month().Cast(dtype.Int64())'),
    # ---- parsing ----
    ("to_datetime_infer", "base", C("s").str.to_datetime(strict=False), 'col("s").Str().ToDatetimeWith("", lenient)'),
    ("to_datetime_strict_err", "base", C("s").str.to_datetime(), 'col("s").Str().ToDatetime("")'),
    ("to_datetime_fmt", "base", C("s").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
     'col("s").Str().ToDatetimeWith("%Y-%m-%d %H:%M:%S", lenient)'),
    ("to_datetime_fmt_frac", "base", C("s").str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
     'col("s").Str().ToDatetimeWith("%Y-%m-%d %H:%M:%S%.f", lenient)'),
    ("to_datetime_fmt_3f", "base", C("s").str.to_datetime("%Y-%m-%d %H:%M:%S%.3f", strict=False),
     'col("s").Str().ToDatetimeWith("%Y-%m-%d %H:%M:%S%.3f", lenient)'),
    ("to_datetime_not_exact", "base", C("s").str.to_datetime("%Y-%m-%d", strict=False, exact=False),
     'col("s").Str().ToDatetimeWith("%Y-%m-%d", notExact)'),
    ("to_datetime_ns", "base", C("s").str.to_datetime(strict=False, time_unit="ns"),
     'col("s").Str().ToDatetimeWith("", expr.StrptimeOptions{Exact: true, TimeUnit: "ns"})'),
    ("to_datetime_tz", "base", C("s").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_zone="Asia/Tokyo"),
     'col("s").Str().ToDatetimeWith("%Y-%m-%d %H:%M:%S", expr.StrptimeOptions{Exact: true, TimeZone: "Asia/Tokyo"})'),
    ("to_date_infer", "base", C("sd").str.to_date(strict=False), 'col("sd").Str().ToDateWith("", lenient)'),
    ("to_date_strict_err", "base", C("sd").str.to_date(), 'col("sd").Str().ToDate("")'),
    ("to_date_fmt", "base", C("sd").str.to_date("%Y-%m-%d", strict=False), 'col("sd").Str().ToDateWith("%Y-%m-%d", lenient)'),
    ("to_date_from_dt", "base", C("s").str.to_date("%Y-%m-%d %H:%M:%S", strict=False),
     'col("s").Str().ToDateWith("%Y-%m-%d %H:%M:%S", lenient)'),
    ("to_time_infer", "base", C("st").str.to_time(strict=False), 'col("st").Str().ToTimeWith("", lenient)'),
    ("to_time_fmt", "base", C("st").str.to_time("%H:%M:%S%.f", strict=False), 'col("st").Str().ToTimeWith("%H:%M:%S%.f", lenient)'),
    ("strptime_date", "base", C("sd").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
     'col("sd").Str().StrptimeWith(dtype.Date(), "%Y-%m-%d", lenient)'),
    ("strptime_dt_ms", "base", C("s").str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S", strict=False),
     'col("s").Str().StrptimeWith(dtype.Datetime(dtype.Millisecond, ""), "%Y-%m-%d %H:%M:%S", lenient)'),
    # ---- arithmetic ----
    ("dt_minus_dt", "base", C("ts") - C("ts").dt.offset_by("1d3h"), 'col("ts").Sub(col("ts").Dt().OffsetBy("1d3h"))'),
    ("dt_minus_dt_ms", "base", C("ts_ms") - C("ts"), 'col("ts_ms").Sub(col("ts"))'),
    ("date_minus_date", "base", C("d") - C("d").dt.offset_by("-40d"), 'col("d").Sub(col("d").Dt().OffsetBy("-40d"))'),
    ("dt_minus_date", "base", C("ts") - C("d"), 'col("ts").Sub(col("d"))'),
    ("dt_plus_dur", "base", C("ts") + C("dur"), 'col("ts").Add(col("dur"))'),
    ("dur_plus_dt", "base", C("dur") + C("ts"), 'col("dur").Add(col("ts"))'),
    ("dt_minus_dur", "base", C("ts") - C("dur"), 'col("ts").Sub(col("dur"))'),
    ("dt_ms_plus_dur", "base", C("ts_ms") + C("dur"), 'col("ts_ms").Add(col("dur"))'),
    ("date_plus_dur", "base", C("d") + C("dur"), 'col("d").Add(col("dur"))'),
    ("date_minus_dur", "base", C("d") - C("dur"), 'col("d").Sub(col("dur"))'),
    ("dur_plus_dur", "base", C("dur") + C("dur"), 'col("dur").Add(col("dur"))'),
    ("dur_times_int", "base", C("dur") * C("i"), 'col("dur").Mul(col("i"))'),
    ("int_times_dur", "base", C("i") * C("dur"), 'col("i").Mul(col("dur"))'),
    ("dur_times_lit", "base", C("dur") * 3, 'col("dur").MulLit(3)'),
    ("dur_times_float", "base", C("dur") * 1.5, 'col("dur").MulLit(1.5)'),
    ("dur_div_int", "base", C("dur") / 3, 'col("dur").DivLit(3)'),
    ("dur_div_float", "base", C("dur") / 2.0, 'col("dur").DivLit(2.0)'),
    ("dur_div_dur", "base", C("dur") / C("dur").dt.cast_time_unit("ms"), 'col("dur").Div(col("dur").Dt().CastTimeUnit(dtype.Millisecond))'),
    ("tz_minus_tz", "base", C("tz") - C("tz").dt.offset_by("-1d"), 'col("tz").Sub(col("tz").Dt().OffsetBy("-1d"))'),
    ("tz_plus_dur", "base", C("tz") + C("dur"), 'col("tz").Add(col("dur"))'),
    ("dt_plus_int_err", "base", C("ts") + C("i"), 'col("ts").Add(col("i"))'),
    ("dt_plus_lit", "base", C("ts") + timedelta(days=1, microseconds=7), 'col("ts").Add(expr.Lit(24*time.Hour + 7*time.Microsecond))'),
    ("date_plus_lit", "base", C("d") + timedelta(hours=36), 'col("d").Add(expr.Lit(36*time.Hour))'),
    ("dt_minus_lit", "base", C("ts") - datetime(2000, 1, 1), 'col("ts").Sub(expr.Lit(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)))'),
    ("lit_minus_dt", "base", datetime(2000, 1, 1) - C("ts"), 'expr.Lit(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Sub(col("ts"))'),
    # ---- comparisons ----
    ("dt_gt_lit", "base", C("ts") > datetime(2000, 1, 1), 'col("ts").Gt(expr.Lit(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)))'),
    ("dt_le_lit", "base", C("ts") <= datetime(1970, 1, 1), 'col("ts").Le(expr.Lit(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)))'),
    ("lit_lt_dt", "base", datetime(2000, 1, 1) < C("ts"), 'expr.Lit(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Lt(col("ts"))'),
    ("dt_gt_date_lit", "base", C("ts") > date(2000, 1, 1), 'col("ts").Gt(expr.LitDate(2000, 1, 1))'),
    ("date_eq_lit", "base", C("d") == date(1970, 1, 1), 'col("d").Eq(expr.LitDate(1970, 1, 1))'),
    ("date_gt_dt_lit", "base", C("d") > datetime(2000, 6, 15, 0, 0, 1), 'col("d").Gt(expr.Lit(time.Date(2000, 6, 15, 0, 0, 1, 0, time.UTC)))'),
    ("dt_eq_date", "base", C("ts") == C("d"), 'col("ts").Eq(col("d"))'),
    ("dt_ms_lt_dt", "base", C("ts_ms") < C("ts"), 'col("ts_ms").Lt(col("ts"))'),
    ("dur_gt_lit", "base", C("dur") > timedelta(0), 'col("dur").Gt(expr.Lit(time.Duration(0)))'),
    ("dur_ge_dur", "base", C("dur") >= C("dur").dt.cast_time_unit("ms"), 'col("dur").Ge(col("dur").Dt().CastTimeUnit(dtype.Millisecond))'),
    ("time_gt_lit", "base", C("t") > time(12), 'col("t").Gt(expr.LitTime(12, 0, 0, 0))'),
    ("tz_gt_aware_lit", "base", C("tz") > pl.lit(datetime(2024, 3, 10, 7, 0, tzinfo=__import__("datetime").timezone.utc)).dt.convert_time_zone(UTC_TZ),
     'col("tz").Gt(expr.Lit(time.Date(2024, 3, 10, 7, 0, 0, 0, time.UTC)))'),
    ("month_eq_int", "base", C("ts").dt.month() == 12, 'col("ts").Dt().Month().EqLit(12)'),
    ("year_gt_int", "base", C("ts").dt.year() > 1999, 'col("ts").Dt().Year().GtLit(1999)'),
    ("month_plus_int", "base", C("ts").dt.month() + 1, 'col("ts").Dt().Month().AddLit(1)'),
    # ---- aggregations ----
    ("agg_min_ts", "base", C("ts").min(), 'col("ts").Min()'),
    ("agg_max_ts", "base", C("ts").max(), 'col("ts").Max()'),
    ("agg_mean_ts", "base", C("ts").mean(), 'col("ts").Mean()'),
    ("agg_mean_date", "base", C("d").mean(), 'col("d").Mean()'),
    ("agg_max_date", "base", C("d").max(), 'col("d").Max()'),
    ("agg_sum_dur", "base", C("dur").sum(), 'col("dur").Sum()'),
    ("agg_mean_dur", "base", C("dur").mean(), 'col("dur").Mean()'),
    ("agg_max_tz", "base", C("tz").max(), 'col("tz").Max()'),
    ("neg_dur", "base", -C("dur"), 'col("dur").Neg()'),
    # ---- constructors ----
    ("date_ctor", "base", pl.date(C("ts").dt.year(), 2, 28), 'expr.Date(col("ts").Dt().Year(), expr.Lit(2), expr.Lit(28))'),
    ("date_ctor_err", "base", pl.date(2021, C("ts").dt.month() + 1, 1), 'expr.Date(expr.Lit(2021), col("ts").Dt().Month().AddLit(1), expr.Lit(1))'),
    ("datetime_ctor", "base", pl.datetime(2020, C("ts").dt.month(), 3, C("ts").dt.hour(), 4, 5, 6),
     'expr.Datetime(expr.Lit(2020), col("ts").Dt().Month(), expr.Lit(3), expr.DatetimeArgs{Hour: col("ts").Dt().Hour(), Minute: expr.Lit(4), Second: expr.Lit(5), Microsecond: expr.Lit(6)})'),
    ("datetime_ctor_tz", "base", pl.datetime(2020, C("ts").dt.month(), 3, 12, time_zone="Asia/Kolkata"),
     'expr.Datetime(expr.Lit(2020), col("ts").Dt().Month(), expr.Lit(3), expr.DatetimeArgs{Hour: expr.Lit(12), TimeZone: "Asia/Kolkata"})'),
    ("duration_ctor", "base", pl.duration(days=C("i"), hours=2, minutes=C("i")),
     'expr.Duration(expr.DurationArgs{Days: col("i"), Hours: expr.Lit(2), Minutes: col("i")})'),
    ("duration_ctor_ns", "base", pl.duration(weeks=1, nanoseconds=C("i")),
     'expr.Duration(expr.DurationArgs{Weeks: expr.Lit(1), Nanoseconds: col("i")})'),
    # ---- combine / replace / business days ----
    ("combine", "base", C("ts").dt.combine(time(1, 2, 3, 456789)), 'col("ts").Dt().Combine(expr.LitTime(1, 2, 3, 456789000), dtype.Microsecond)'),
    ("combine_date_ms", "base", C("d").dt.combine(time(23, 59), "ms"), 'col("d").Dt().Combine(expr.LitTime(23, 59, 0, 0), dtype.Millisecond)'),
    ("replace_hour", "base", C("ts").dt.replace(hour=5, microsecond=7), 'col("ts").Dt().Replace(expr.ReplaceArgs{Hour: expr.Lit(5), Microsecond: expr.Lit(7)})'),
    ("replace_year", "base", C("ts").dt.replace(year=2023), 'col("ts").Dt().Replace(expr.ReplaceArgs{Year: expr.Lit(2023)})'),
    ("replace_day_err", "base", C("ts").dt.replace(day=31), 'col("ts").Dt().Replace(expr.ReplaceArgs{Day: expr.Lit(31)})'),
    ("replace_date_month", "base", C("d").dt.replace(month=3), 'col("d").Dt().Replace(expr.ReplaceArgs{Month: expr.Lit(3)})'),
    ("is_business_day", "base", C("d").dt.is_business_day(), 'col("d").Dt().IsBusinessDay(expr.BusinessDayArgs{})'),
    ("is_business_day_hol", "base", C("d").dt.is_business_day(holidays=[date(2020, 2, 28), date(1969, 12, 31)]),
     'col("d").Dt().IsBusinessDay(expr.BusinessDayArgs{Holidays: []expr.DateValue{{Year: 2020, Month: 2, Day: 28}, {Year: 1969, Month: 12, Day: 31}}})'),
    ("add_business_days_fwd", "base", C("d").dt.add_business_days(C("i"), roll="forward"),
     'col("d").Dt().AddBusinessDays(col("i"), expr.BusinessDayArgs{Roll: "forward"})'),
    ("add_business_days_back_mask", "base",
     C("d").dt.add_business_days(C("i"), roll="backward", week_mask=[True, True, False, True, True, True, False]),
     'col("d").Dt().AddBusinessDays(col("i"), expr.BusinessDayArgs{Roll: "backward", WeekMask: &[7]bool{true, true, false, true, true, true, false}})'),
    ("add_business_days_hol", "base",
     C("d").dt.add_business_days(12, roll="forward", holidays=[date(2020, 3, 2), date(2025, 1, 1), date(1970, 1, 1)]),
     'col("d").Dt().AddBusinessDays(expr.Lit(12), expr.BusinessDayArgs{Roll: "forward", Holidays: []expr.DateValue{{Year: 2020, Month: 3, Day: 2}, {Year: 2025, Month: 1, Day: 1}, {Year: 1970, Month: 1, Day: 1}}})'),
    ("add_business_days_dt", "base", C("ts").dt.add_business_days(-4, roll="backward"),
     'col("ts").Dt().AddBusinessDays(expr.Lit(-4), expr.BusinessDayArgs{Roll: "backward"})'),
    ("add_business_days_raise", "base", C("d").dt.add_business_days(1), 'col("d").Dt().AddBusinessDays(expr.Lit(1), expr.BusinessDayArgs{})'),
    # ---- rolling by ----
    ("rolling_sum_by", "win", C("n").rolling_sum_by("t", "1h"), 'col("n").RollingSumBy(col("t"), "1h")'),
    ("rolling_mean_by_both", "win", C("f").rolling_mean_by("t", "90m", closed="both"), 'col("f").RollingMeanBy(col("t"), "90m", expr.WithClosed("both"))'),
    ("rolling_max_by_mp", "win", C("f").rolling_max_by("t", "1h", min_periods=2), 'col("f").RollingMaxBy(col("t"), "1h", expr.WithMinPeriods(2))'),
    ("rolling_min_by", "win", C("n").rolling_min_by("t", "1h"), 'col("n").RollingMinBy(col("t"), "1h")'),
    ("rolling_std_by", "win", C("n").rolling_std_by("t", "2h"), 'col("n").RollingStdBy(col("t"), "2h")'),
    ("rolling_var_by_ddof0", "win", C("n").rolling_var_by("t", "2h", ddof=0), 'col("n").RollingVarBy(col("t"), "2h", expr.WithDdof(0))'),
    ("rolling_median_by", "win", C("n").rolling_median_by("t", "2h"), 'col("n").RollingMedianBy(col("t"), "2h")'),
    ("rolling_quantile_by", "win", C("n").rolling_quantile_by("t", "2h", quantile=0.3), 'col("n").RollingQuantileBy(col("t"), 0.3, "2h")'),
    ("rolling_quantile_by_linear", "win", C("n").rolling_quantile_by("t", "2h", quantile=0.3, interpolation="linear"),
     'col("n").RollingQuantileBy(col("t"), 0.3, "2h", expr.WithInterpolation("linear"))'),
    ("rolling_sum_by_float_nulls", "win", C("f").rolling_sum_by("t", "1h"), 'col("f").RollingSumBy(col("t"), "1h")'),
    ("rolling_min_by_left", "win", C("f").rolling_min_by("t", "1h", closed="left"), 'col("f").RollingMinBy(col("t"), "1h", expr.WithClosed("left"))'),
    ("rolling_sum_by_left_int", "win", C("n").rolling_sum_by("t", "1h", closed="left"), 'col("n").RollingSumBy(col("t"), "1h", expr.WithClosed("left"))'),
    ("rolling_sum_by_date", "win", C("n").rolling_sum_by("d", "2d"), 'col("n").RollingSumBy(col("d"), "2d")'),
    ("rolling_mean_by_unsorted", "unsorted", C("v").rolling_mean_by("t", "2h"), 'col("v").RollingMeanBy(col("t"), "2h")'),
    ("rolling_sum_by_unsorted_both", "unsorted", C("v").rolling_sum_by("t", "1h", closed="both"),
     'col("v").RollingSumBy(col("t"), "1h", expr.WithClosed("both"))'),
    # ---- ranges ----
    ("date_range", "base", pl.date_range(date(2020, 1, 29), date(2020, 5, 1), "1mo"),
     'expr.DateRange(expr.LitDate(2020, 1, 29), expr.LitDate(2020, 5, 1), "1mo", "both")'),
    ("date_range_left", "base", pl.date_range(date(2020, 1, 1), date(2020, 1, 5), "2d", closed="left"),
     'expr.DateRange(expr.LitDate(2020, 1, 1), expr.LitDate(2020, 1, 5), "2d", "left")'),
    ("datetime_range_mo_end", "base", pl.datetime_range(datetime(2020, 1, 31), datetime(2020, 6, 1), "1mo"),
     'expr.DatetimeRange(expr.Lit(time.Date(2020, 1, 31, 0, 0, 0, 0, time.UTC)), expr.Lit(time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)), "1mo", "both", "", "")'),
    ("datetime_range_tz", "base", pl.datetime_range(datetime(2024, 3, 9, 12), datetime(2024, 3, 11, 12), "1d", time_zone=UTC_TZ),
     'expr.DatetimeRange(expr.Lit(time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)), expr.Lit(time.Date(2024, 3, 11, 12, 0, 0, 0, time.UTC)), "1d", "both", "", "America/New_York")'),
    ("datetime_range_tz_24h", "base", pl.datetime_range(datetime(2024, 3, 9, 12), datetime(2024, 3, 11, 12), "24h", time_zone=UTC_TZ),
     'expr.DatetimeRange(expr.Lit(time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)), expr.Lit(time.Date(2024, 3, 11, 12, 0, 0, 0, time.UTC)), "24h", "both", "", "America/New_York")'),
    ("datetime_range_ms", "base", pl.datetime_range(datetime(2020, 1, 1), datetime(2020, 1, 1, 0, 0, 1), "250ms", time_unit="ms", closed="right"),
     'expr.DatetimeRange(expr.Lit(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)), expr.Lit(time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC)), "250ms", "right", "ms", "")'),
    ("time_range", "base", pl.time_range(time(9), time(12), "1h30m"),
     'expr.TimeRange(expr.LitTime(9, 0, 0, 0), expr.LitTime(12, 0, 0, 0), "1h30m", "both")'),
]


# (name, frame, polars lazy transform, golars LazyFrame transform body)
FRAME_CASES = [
    ("dyn_1h", "win", lambda lf: lf.group_by_dynamic("t", every="1h").agg(C("n").sum(), C("f").mean().alias("fm"), C("n").count().alias("c")),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "1h"}).Agg(col("n").Sum(), col("f").Mean().Alias("fm"), col("n").Count().Alias("c"))'),
    ("dyn_period_both_bounds_right", "win",
     lambda lf: lf.group_by_dynamic("t", every="1h", period="2h", closed="both", include_boundaries=True, label="right").agg(
         C("n").max(), C("n").first().alias("nf"), C("t").last().alias("tl")),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "1h", Period: "2h", Closed: "both", IncludeBoundaries: true, Label: "right"}).Agg(col("n").Max(), col("n").First().Alias("nf"), col("t").Last().Alias("tl"))'),
    ("dyn_group_offset", "win",
     lambda lf: lf.group_by_dynamic("t", every="1h", group_by="g", offset="30m").agg(C("n").min(), C("f").std().alias("fs"), C("n").median().alias("med")),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "1h", GroupBy: []string{"g"}, Offset: "30m"}).Agg(col("n").Min(), col("f").Std().Alias("fs"), col("n").Median().Alias("med"))'),
    ("dyn_group_bounds", "win",
     lambda lf: lf.group_by_dynamic("t", every="2h", group_by="g", include_boundaries=True).agg(C("n").sum()),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "2h", GroupBy: []string{"g"}, IncludeBoundaries: true}).Agg(col("n").Sum())'),
    ("dyn_datapoint_right", "win",
     lambda lf: lf.group_by_dynamic("t", every="1h", label="datapoint", closed="right").agg(C("n").sum()),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "1h", Label: "datapoint", Closed: "right"}).Agg(col("n").Sum())'),
    ("dyn_start_datapoint", "win",
     lambda lf: lf.group_by_dynamic("t", every="45m", start_by="datapoint").agg(C("n").sum()),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "45m", StartBy: "datapoint"}).Agg(col("n").Sum())'),
    ("dyn_date_week", "win",
     lambda lf: lf.group_by_dynamic("d", every="1w", include_boundaries=True).agg(C("n").sum()),
     'lf.GroupByDynamic("d", dataframe.DynamicGroupOptions{Every: "1w", IncludeBoundaries: true}).Agg(col("n").Sum())'),
    ("dyn_base_month", "base",
     lambda lf: lf.drop_nulls("ts").sort("ts").group_by_dynamic("ts", every="1mo", period="3mo").agg(C("i").sum(), C("f").max()),
     'lf.DropNulls("ts").Sort("ts", false).GroupByDynamic("ts", dataframe.DynamicGroupOptions{Every: "1mo", Period: "3mo"}).Agg(col("i").Sum(), col("f").Max())'),
    ("dyn_int", "ints",
     lambda lf: lf.group_by_dynamic("i", every="3i").agg(C("v").sum()),
     'lf.GroupByDynamic("i", dataframe.DynamicGroupOptions{Every: "3i"}).Agg(col("v").Sum())'),
    ("dyn_tz_1d", "wtz",
     lambda lf: lf.group_by_dynamic("t", every="1d", include_boundaries=True).agg(C("v").sum(), C("v").count().alias("c")),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "1d", IncludeBoundaries: true}).Agg(col("v").Sum(), col("v").Count().Alias("c"))'),
    ("dyn_tz_2h", "wtz",
     lambda lf: lf.group_by_dynamic("t", every="2h").agg(C("v").sum()),
     'lf.GroupByDynamic("t", dataframe.DynamicGroupOptions{Every: "2h"}).Agg(col("v").Sum())'),
    ("rolling_1h", "win",
     lambda lf: lf.rolling("t", period="1h").agg(C("n").sum().alias("s"), C("f").mean().alias("fm"), C("n").count().alias("c")),
     'lf.Rolling("t", dataframe.RollingGroupOptions{Period: "1h"}).Agg(col("n").Sum().Alias("s"), col("f").Mean().Alias("fm"), col("n").Count().Alias("c"))'),
    ("rolling_group_both", "win",
     lambda lf: lf.rolling("t", period="1h", offset="0h", closed="both", group_by="g").agg(C("n").sum().alias("s")),
     'lf.Rolling("t", dataframe.RollingGroupOptions{Period: "1h", Offset: "0h", Closed: "both", GroupBy: []string{"g"}}).Agg(col("n").Sum().Alias("s"))'),
    ("rolling_left_empty", "win",
     lambda lf: lf.rolling("t", period="1h", closed="left").agg(
         C("f").sum().alias("s"), C("f").count().alias("c"), C("f").mean().alias("m"), C("f").min().alias("mn"), C("f").first().alias("fi")),
     'lf.Rolling("t", dataframe.RollingGroupOptions{Period: "1h", Closed: "left"}).Agg(col("f").Sum().Alias("s"), col("f").Count().Alias("c"), col("f").Mean().Alias("m"), col("f").Min().Alias("mn"), col("f").First().Alias("fi"))'),
    ("rolling_int", "ints",
     lambda lf: lf.rolling("i", period="2i").agg(C("v").sum()),
     'lf.Rolling("i", dataframe.RollingGroupOptions{Period: "2i"}).Agg(col("v").Sum())'),
    ("rolling_date", "win",
     lambda lf: lf.rolling("d", period="2d").agg(C("n").max()),
     'lf.Rolling("d", dataframe.RollingGroupOptions{Period: "2d"}).Agg(col("n").Max())'),
    ("rolling_quantile_std", "win",
     lambda lf: lf.rolling("t", period="2h").agg(C("n").quantile(0.3).alias("q"), C("n").var().alias("v"), C("n").median().alias("m")),
     'lf.Rolling("t", dataframe.RollingGroupOptions{Period: "2h"}).Agg(col("n").Quantile(0.3).Alias("q"), col("n").Var().Alias("v"), col("n").Median().Alias("m"))'),
]


def go_dtype(dt):
    if dt == pl.Datetime:
        s = f"datetime[{dt.time_unit}"
        if dt.time_zone:
            s += f", {dt.time_zone}"
        return s + "]"
    if dt == pl.Duration:
        return f"duration[{dt.time_unit}]"
    return {
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
        pl.Boolean: "bool",
        pl.String: "str",
        pl.Date: "date",
        pl.Time: "time64[ns]",
    }[dt]


def render(s: pl.Series):
    dt = s.dtype
    if dt.is_temporal():
        s = s.to_physical()
    out = []
    for v in s.to_list():
        if v is None:
            out.append(None)
        elif isinstance(v, bool):
            out.append("true" if v else "false")
        elif isinstance(v, float):
            out.append(f"{v:.12g}".lower().replace("+", ""))
        else:
            out.append(str(v))
    return out


def go_str(v):
    if v is None:
        return "nullStr"
    return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'


def emit_frame(name, df):
    print(f"\t{go_str(name)}: {{")
    for col in df.columns:
        s = df.get_column(col)
        dt = go_dtype(s.dtype)
        vals = render(s)
        print(f"\t\t{{Name: {go_str(col)}, DType: {go_str(dt)}, Values: []string{{{', '.join(go_str(v) for v in vals)}}}}},")
    print("\t},")


def main():
    print("// Code generated by bench/polars-compare/temporal_parity.py from polars", pl.__version__ + ". DO NOT EDIT.")
    print()
    print("package eval_test")
    print()
    print(
        'import (\n\t"time"\n\n\t"github.com/Gaurav-Gosain/golars/dataframe"\n\t"github.com/Gaurav-Gosain/golars/dtype"\n'
        '\t"github.com/Gaurav-Gosain/golars/expr"\n\t"github.com/Gaurav-Gosain/golars/lazy"\n)'
    )
    print()
    print("var _ = time.UTC")
    print("var _ = dtype.Int64")
    print()
    print("var temporalParityFrames = map[string][]temporalParityColumn{")
    for name, df in FRAMES.items():
        emit_frame(name, df)
    print("}")
    print()
    print("var temporalParityCases = []temporalParityCase{")
    for name, frame, pe, ge in CASES:
        df = FRAMES[frame]
        try:
            out = df.select(pe.alias("__out")).get_column("__out")
            err = False
        except Exception:
            err = True
        if err:
            print(f"\t{{Name: {go_str(name)}, Frame: {go_str(frame)}, Expr: {ge}, Err: true}},")
            continue
        dt = go_dtype(out.dtype)
        vals = render(out)
        print(
            f"\t{{Name: {go_str(name)}, Frame: {go_str(frame)}, Expr: {ge}, DType: {go_str(dt)}, "
            f"Want: []string{{{', '.join(go_str(v) for v in vals)}}}}},"
        )
    print("}")
    print()
    print("var temporalFrameCases = []parityFrameCase{")
    for name, frame, pf, gf in FRAME_CASES:
        plan = f"func(lf lazy.LazyFrame) lazy.LazyFrame {{ return {gf} }}"
        try:
            out = pf(FRAMES[frame].lazy()).collect()
        except Exception:
            print(f"\t{{Name: {go_str(name)}, Frame: {go_str(frame)}, Plan: {plan}, Err: true}},")
            continue
        print(f"\t{{Name: {go_str(name)}, Frame: {go_str(frame)}, Plan: {plan}, Want: []temporalParityColumn{{")
        for c in out.columns:
            ser = out.get_column(c)
            vals = ", ".join(go_str(v) for v in render(ser))
            print(f"\t\t{{Name: {go_str(c)}, DType: {go_str(go_dtype(ser.dtype))}, Values: []string{{{vals}}}}},")
        print("\t}},")
    print("}")


if __name__ == "__main__":
    main()
