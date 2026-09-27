# golars / polars API surface

This file is generated. Do not edit it by hand. Regenerate with:

```sh
cd bench/polars-compare && uv run python api_coverage.py
```

The script lists the public API of polars 1.39.3 with `dir()`, parses
the golars Go source for exported methods and functions, and maps the
names. A polars name counts as `done` when a golars name matches it
case-insensitively with underscores removed (`n_unique` and `NUnique`),
or when the rename table in the script points at a name that exists in
the Go source. `partial` means golars covers a narrower form; the note
says how. `todo` means no match was found. Names that have no meaning in
Go (pandas, numpy, Python UDFs, plotting) are listed under [Not
applicable](#not-applicable) and left out of the totals. Deprecated
polars names are skipped.

A `done` row means the name exists with the same intent. It does not
promise identical arguments; Go signatures use typed options instead of
keyword arguments.

## Coverage summary

| Surface | Covered | Done | Partial | Todo | Not applicable | Coverage |
|---|---|---|---|---|---|---|
| [Expr](#expr) | 201 of 206 | 200 | 1 | 5 | 3 | 98% |
| [Expr.str](#exprstr) | 45 of 47 | 45 | 0 | 2 | 0 | 96% |
| [Expr.dt](#exprdt) | 43 of 45 | 43 | 0 | 2 | 0 | 96% |
| [Expr.list](#exprlist) | 42 of 43 | 42 | 0 | 1 | 0 | 98% |
| [Expr.arr](#exprarr) | 29 of 31 | 29 | 0 | 2 | 0 | 94% |
| [Expr.struct](#exprstruct) | 5 of 5 | 5 | 0 | 0 | 0 | 100% |
| [Expr.name](#exprname) | 10 of 10 | 10 | 0 | 0 | 0 | 100% |
| [Expr.bin](#exprbin) | 11 of 11 | 11 | 0 | 0 | 0 | 100% |
| [Expr.cat](#exprcat) | 6 of 6 | 6 | 0 | 0 | 0 | 100% |
| [DataFrame](#dataframe) | 109 of 118 | 106 | 3 | 9 | 10 | 92% |
| [LazyFrame](#lazyframe) | 64 of 70 | 63 | 1 | 6 | 6 | 91% |
| [GroupBy](#groupby) | 15 of 16 | 15 | 0 | 1 | 0 | 94% |
| [LazyGroupBy](#lazygroupby) | 15 of 16 | 15 | 0 | 1 | 0 | 94% |
| [Series](#series) | 201 of 212 | 201 | 0 | 11 | 8 | 95% |
| [Series.str](#seriesstr) | 45 of 47 | 45 | 0 | 2 | 0 | 96% |
| [Series.dt](#seriesdt) | 43 of 47 | 43 | 0 | 4 | 0 | 91% |
| [Series.list](#serieslist) | 41 of 43 | 41 | 0 | 2 | 0 | 95% |
| [Series.arr](#seriesarr) | 29 of 31 | 29 | 0 | 2 | 0 | 94% |
| [Series.struct](#seriesstruct) | 5 of 6 | 5 | 0 | 1 | 0 | 83% |
| [Series.bin](#seriesbin) | 11 of 11 | 11 | 0 | 0 | 0 | 100% |
| [Series.cat](#seriescat) | 6 of 6 | 6 | 0 | 0 | 1 | 100% |
| [Data types](#data-types) | 24 of 28 | 24 | 0 | 4 | 4 | 86% |
| [Top-level functions](#top-level-functions) | 81 of 105 | 73 | 8 | 24 | 21 | 77% |
| **Total** | **1081 of 1160** | 1068 | 13 | 79 | 53 | 93% |

Series namespaces (`s.str`, `s.dt`, ...) are counted against the
eager `series.*Ops` types.

## Expr

`pl.Expr` against `expr.Expr`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `abs` | `Abs` | done |  |
| `add` | `Add` | done |  |
| `alias` | `Alias` | done |  |
| `all` | `All` | done |  |
| `and_` | `And` | done |  |
| `any` | `Any` | done |  |
| `append` |  | todo |  |
| `approx_n_unique` | `ApproxNUnique` | done |  |
| `arccos` | `Arccos` | done |  |
| `arccosh` | `Arccosh` | done |  |
| `arcsin` | `Arcsin` | done |  |
| `arcsinh` | `Arcsinh` | done |  |
| `arctan` | `Arctan` | done |  |
| `arctanh` | `Arctanh` | done |  |
| `arg_max` | `ArgMax` | done |  |
| `arg_min` | `ArgMin` | done |  |
| `arg_sort` | `ArgSort` | done |  |
| `arg_true` | `ArgTrue` | done |  |
| `arg_unique` | `ArgUnique` | done |  |
| `arr` | `Arr` | done |  |
| `backward_fill` | `BackwardFill` | done |  |
| `bin` | `Bin` | done |  |
| `bitwise_and` | `BitwiseAnd` | done |  |
| `bitwise_count_ones` | `BitwiseCountOnes` | done |  |
| `bitwise_count_zeros` | `BitwiseCountZeros` | done |  |
| `bitwise_leading_ones` | `BitwiseLeadingOnes` | done |  |
| `bitwise_leading_zeros` | `BitwiseLeadingZeros` | done |  |
| `bitwise_or` | `BitwiseOr` | done |  |
| `bitwise_trailing_ones` | `BitwiseTrailingOnes` | done |  |
| `bitwise_trailing_zeros` | `BitwiseTrailingZeros` | done |  |
| `bitwise_xor` | `BitwiseXor` | done |  |
| `bottom_k` | `BottomK` | done |  |
| `bottom_k_by` | `BottomKBy` | done |  |
| `cast` | `Cast` | done |  |
| `cat` | `Cat` | done |  |
| `cbrt` | `Cbrt` | done |  |
| `ceil` | `Ceil` | done |  |
| `clip` | `Clip` | done |  |
| `cos` | `Cos` | done |  |
| `cosh` | `Cosh` | done |  |
| `cot` | `Cot` | done |  |
| `count` | `Count` | done |  |
| `cum_count` | `CumCount` | done |  |
| `cum_max` | `CumMax` | done |  |
| `cum_min` | `CumMin` | done |  |
| `cum_prod` | `CumProd` | done |  |
| `cum_sum` | `CumSum` | done |  |
| `cumulative_eval` | `CumulativeEval` | done |  |
| `cut` | `Cut` | done |  |
| `degrees` | `Degrees` | done |  |
| `diff` | `Diff` | done |  |
| `dot` | `Dot` | done |  |
| `drop_nans` | `DropNans` | done |  |
| `drop_nulls` | `DropNulls` | done |  |
| `dt` | `Dt` | done |  |
| `entropy` | `Entropy` | done |  |
| `eq` | `Eq` | done |  |
| `eq_missing` | `EqMissing` | done |  |
| `ewm_mean` | `EWMMean` | done |  |
| `ewm_mean_by` |  | todo |  |
| `ewm_std` | `EWMStd` | done |  |
| `ewm_var` | `EWMVar` | done |  |
| `exclude` | `Exclude` | done |  |
| `exp` | `Exp` | done |  |
| `explode` | `Explode` | done |  |
| `extend_constant` | `ExtendConstant` | done |  |
| `fill_nan` | `FillNan` | done |  |
| `fill_null` | `FillNull` | done |  |
| `filter` | `Filter` | done |  |
| `first` | `First` | done |  |
| `floor` | `Floor` | done |  |
| `floordiv` | `FloorDiv` | done |  |
| `forward_fill` | `ForwardFill` | done |  |
| `gather` | `Gather` | done |  |
| `gather_every` | `GatherEvery` | done |  |
| `ge` | `Ge` | done |  |
| `get` | `Get` | done |  |
| `gt` | `Gt` | done |  |
| `has_nulls` | `HasNulls` | done |  |
| `hash` | `Hash` | done |  |
| `head` | `Head` | done |  |
| `hist` | `Hist` | done |  |
| `implode` | `Implode` | done |  |
| `index_of` | `IndexOf` | done |  |
| `interpolate` | `Interpolate` | done |  |
| `interpolate_by` | `InterpolateBy` | done |  |
| `is_between` | `IsBetween` | done |  |
| `is_close` | `IsClose` | done |  |
| `is_duplicated` | `IsDuplicated` | done |  |
| `is_finite` | `IsFinite` | done |  |
| `is_first_distinct` | `IsFirstDistinct` | done |  |
| `is_in` | `IsIn` | done |  |
| `is_infinite` | `IsInfinite` | done |  |
| `is_last_distinct` | `IsLastDistinct` | done |  |
| `is_nan` | `IsNaN` | done |  |
| `is_not_nan` | `IsNotNaN` | done |  |
| `is_not_null` | `IsNotNull` | done |  |
| `is_null` | `IsNull` | done |  |
| `is_unique` | `IsUnique` | done |  |
| `item` | `Item` | done |  |
| `kurtosis` | `Kurtosis` | done |  |
| `last` | `Last` | done |  |
| `le` | `Le` | done |  |
| `len` | `Len` | done |  |
| `limit` | `Limit` | done |  |
| `list` | `List` | done |  |
| `log` | `Log` | done |  |
| `log10` | `Log10` | done |  |
| `log1p` | `Log1p` | done |  |
| `lower_bound` | `LowerBound` | done |  |
| `lt` | `Lt` | done |  |
| `max` | `Max` | done |  |
| `max_by` | `MaxBy` | done |  |
| `mean` | `Mean` | done |  |
| `median` | `Median` | done |  |
| `meta` | `expr.OutputName` | partial | package functions, not a namespace: OutputName, ReferencedColumns, IsMultiColumn, Equal |
| `min` | `Min` | done |  |
| `min_by` | `MinBy` | done |  |
| `mod` | `Mod` | done |  |
| `mode` | `Mode` | done |  |
| `mul` | `Mul` | done |  |
| `n_unique` | `NUnique` | done |  |
| `name` | `Name` | done |  |
| `nan_max` | `NanMax` | done |  |
| `nan_min` | `NanMin` | done |  |
| `ne` | `Ne` | done |  |
| `ne_missing` | `NeMissing` | done |  |
| `neg` | `Neg` | done |  |
| `not_` | `Not` | done |  |
| `null_count` | `NullCount` | done |  |
| `or_` | `Or` | done |  |
| `over` | `Over` | done |  |
| `pct_change` | `PctChange` | done |  |
| `peak_max` | `PeakMax` | done |  |
| `peak_min` | `PeakMin` | done |  |
| `pipe` | `Pipe` | done |  |
| `pow` | `Pow` | done |  |
| `product` | `Product` | done |  |
| `qcut` | `QCut` | done |  |
| `quantile` | `Quantile` | done |  |
| `radians` | `Radians` | done |  |
| `rank` | `Rank` | done |  |
| `rechunk` | `Rechunk` | done |  |
| `reinterpret` | `Reinterpret` | done |  |
| `repeat_by` | `RepeatBy` | done |  |
| `replace_strict` | `ReplaceStrict` | done |  |
| `reshape` | `Reshape` | done |  |
| `reverse` | `Reverse` | done |  |
| `rle` | `Rle` | done |  |
| `rle_id` | `RleID` | done |  |
| `rolling` |  | todo |  |
| `rolling_kurtosis` | `RollingKurtosis` | done |  |
| `rolling_map` | `RollingMap` | done |  |
| `rolling_max` | `RollingMax` | done |  |
| `rolling_max_by` | `RollingMaxBy` | done |  |
| `rolling_mean` | `RollingMean` | done |  |
| `rolling_mean_by` | `RollingMeanBy` | done |  |
| `rolling_median` | `RollingMedian` | done |  |
| `rolling_median_by` | `RollingMedianBy` | done |  |
| `rolling_min` | `RollingMin` | done |  |
| `rolling_min_by` | `RollingMinBy` | done |  |
| `rolling_quantile` | `RollingQuantile` | done |  |
| `rolling_quantile_by` | `RollingQuantileBy` | done |  |
| `rolling_rank` | `RollingRank` | done |  |
| `rolling_rank_by` |  | todo |  |
| `rolling_skew` | `RollingSkew` | done |  |
| `rolling_std` | `RollingStd` | done |  |
| `rolling_std_by` | `RollingStdBy` | done |  |
| `rolling_sum` | `RollingSum` | done |  |
| `rolling_sum_by` | `RollingSumBy` | done |  |
| `rolling_var` | `RollingVar` | done |  |
| `rolling_var_by` | `RollingVarBy` | done |  |
| `round` | `Round` | done |  |
| `round_sig_figs` | `RoundSigFigs` | done |  |
| `sample` | `Sample` | done |  |
| `search_sorted` | `SearchSorted` | done |  |
| `set_sorted` | `SetSorted` | done |  |
| `shift` | `Shift` | done |  |
| `shuffle` | `Shuffle` | done |  |
| `sign` | `Sign` | done |  |
| `sin` | `Sin` | done |  |
| `sinh` | `Sinh` | done |  |
| `skew` | `Skew` | done |  |
| `slice` | `Slice` | done |  |
| `sort` | `Sort` | done |  |
| `sort_by` | `SortBy` | done |  |
| `sqrt` | `Sqrt` | done |  |
| `std` | `Std` | done |  |
| `str` | `Str` | done |  |
| `struct` | `Struct` | done |  |
| `sub` | `Sub` | done |  |
| `sum` | `Sum` | done |  |
| `tail` | `Tail` | done |  |
| `tan` | `Tan` | done |  |
| `tanh` | `Tanh` | done |  |
| `to_physical` | `ToPhysical` | done |  |
| `top_k` | `TopK` | done |  |
| `top_k_by` | `TopKBy` | done |  |
| `truediv` | `TrueDiv` | done |  |
| `truncate` |  | todo |  |
| `unique` | `Unique` | done |  |
| `unique_counts` | `UniqueCounts` | done |  |
| `upper_bound` | `UpperBound` | done |  |
| `value_counts` | `ValueCounts` | done |  |
| `var` | `Var` | done |  |
| `xor` | `Xor` | done |  |

## Expr.str

`pl.Expr.str` against `expr.StrOps`, reached with `.Str()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `str.contains` | `Str().Contains` | done |  |
| `str.contains_any` | `Str().ContainsAny` | done |  |
| `str.count_matches` | `Str().CountMatches` | done |  |
| `str.decode` | `Str().Decode` | done |  |
| `str.encode` | `Str().Encode` | done |  |
| `str.ends_with` | `Str().EndsWith` | done |  |
| `str.escape_regex` | `Str().EscapeRegex` | done |  |
| `str.extract` | `Str().Extract` | done |  |
| `str.extract_all` | `Str().ExtractAll` | done |  |
| `str.extract_groups` | `Str().ExtractGroups` | done |  |
| `str.extract_many` | `Str().ExtractMany` | done |  |
| `str.find` | `Str().Find` | done |  |
| `str.find_many` | `Str().FindMany` | done |  |
| `str.head` | `Str().Head` | done |  |
| `str.join` | `Str().Join` | done |  |
| `str.json_decode` | `Str().JSONDecode` | done |  |
| `str.json_path_match` | `Str().JSONPathMatch` | done |  |
| `str.len_bytes` | `Str().LenBytes` | done |  |
| `str.len_chars` | `Str().LenChars` | done |  |
| `str.normalize` |  | todo |  |
| `str.pad_end` | `Str().PadEnd` | done |  |
| `str.pad_start` | `Str().PadStart` | done |  |
| `str.replace` | `Str().Replace` | done |  |
| `str.replace_all` | `Str().ReplaceAll` | done |  |
| `str.replace_many` | `Str().ReplaceMany` | done |  |
| `str.reverse` | `Str().Reverse` | done |  |
| `str.slice` | `Str().Slice` | done |  |
| `str.split` | `Str().Split` | done |  |
| `str.split_exact` | `Str().SplitExact` | done |  |
| `str.splitn` | `Str().SplitN` | done |  |
| `str.starts_with` | `Str().StartsWith` | done |  |
| `str.strip_chars` | `Str().StripChars` | done |  |
| `str.strip_chars_end` | `Str().StripCharsEnd` | done |  |
| `str.strip_chars_start` | `Str().StripCharsStart` | done |  |
| `str.strip_prefix` | `Str().StripPrefix` | done |  |
| `str.strip_suffix` | `Str().StripSuffix` | done |  |
| `str.strptime` | `Str().Strptime` | done |  |
| `str.tail` | `Str().Tail` | done |  |
| `str.to_date` | `Str().ToDate` | done |  |
| `str.to_datetime` | `Str().ToDatetime` | done |  |
| `str.to_decimal` |  | todo |  |
| `str.to_integer` | `Str().ToInteger` | done |  |
| `str.to_lowercase` | `Str().ToLower` | done |  |
| `str.to_time` | `Str().ToTime` | done |  |
| `str.to_titlecase` | `Str().ToTitlecase` | done |  |
| `str.to_uppercase` | `Str().ToUpper` | done |  |
| `str.zfill` | `Str().ZFill` | done |  |

## Expr.dt

`pl.Expr.dt` against `expr.DtOps`, reached with `.Dt()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `dt.add_business_days` | `Dt().AddBusinessDays` | done |  |
| `dt.base_utc_offset` |  | todo |  |
| `dt.cast_time_unit` | `Dt().CastTimeUnit` | done |  |
| `dt.century` | `Dt().Century` | done |  |
| `dt.combine` | `Dt().Combine` | done |  |
| `dt.convert_time_zone` | `Dt().ConvertTimeZone` | done |  |
| `dt.date` | `Dt().Date` | done |  |
| `dt.day` | `Dt().Day` | done |  |
| `dt.days_in_month` | `Dt().DaysInMonth` | done |  |
| `dt.dst_offset` |  | todo |  |
| `dt.epoch` | `Dt().Epoch` | done |  |
| `dt.hour` | `Dt().Hour` | done |  |
| `dt.is_business_day` | `Dt().IsBusinessDay` | done |  |
| `dt.is_leap_year` | `Dt().IsLeapYear` | done |  |
| `dt.iso_year` | `Dt().IsoYear` | done |  |
| `dt.microsecond` | `Dt().Microsecond` | done |  |
| `dt.millennium` | `Dt().Millennium` | done |  |
| `dt.millisecond` | `Dt().Millisecond` | done |  |
| `dt.minute` | `Dt().Minute` | done |  |
| `dt.month` | `Dt().Month` | done |  |
| `dt.month_end` | `Dt().MonthEnd` | done |  |
| `dt.month_start` | `Dt().MonthStart` | done |  |
| `dt.nanosecond` | `Dt().Nanosecond` | done |  |
| `dt.offset_by` | `Dt().OffsetBy` | done |  |
| `dt.ordinal_day` | `Dt().OrdinalDay` | done |  |
| `dt.quarter` | `Dt().Quarter` | done |  |
| `dt.replace` | `Dt().Replace` | done |  |
| `dt.replace_time_zone` | `Dt().ReplaceTimeZone` | done |  |
| `dt.round` | `Dt().Round` | done |  |
| `dt.second` | `Dt().Second` | done |  |
| `dt.strftime` | `Dt().Strftime` | done |  |
| `dt.time` | `Dt().Time` | done |  |
| `dt.timestamp` | `Dt().Timestamp` | done |  |
| `dt.to_string` | `Dt().ToString` | done |  |
| `dt.total_days` | `Dt().TotalDays` | done |  |
| `dt.total_hours` | `Dt().TotalHours` | done |  |
| `dt.total_microseconds` | `Dt().TotalMicroseconds` | done |  |
| `dt.total_milliseconds` | `Dt().TotalMilliseconds` | done |  |
| `dt.total_minutes` | `Dt().TotalMinutes` | done |  |
| `dt.total_nanoseconds` | `Dt().TotalNanoseconds` | done |  |
| `dt.total_seconds` | `Dt().TotalSeconds` | done |  |
| `dt.truncate` | `Dt().Truncate` | done |  |
| `dt.week` | `Dt().Week` | done |  |
| `dt.weekday` | `Dt().Weekday` | done |  |
| `dt.year` | `Dt().Year` | done |  |

## Expr.list

`pl.Expr.list` against `expr.ListOps`, reached with `.List()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `list.agg` |  | todo |  |
| `list.all` | `List().All` | done |  |
| `list.any` | `List().Any` | done |  |
| `list.arg_max` | `List().ArgMax` | done |  |
| `list.arg_min` | `List().ArgMin` | done |  |
| `list.concat` | `List().Concat` | done |  |
| `list.contains` | `List().Contains` | done |  |
| `list.count_matches` | `List().CountMatches` | done |  |
| `list.diff` | `List().Diff` | done |  |
| `list.drop_nulls` | `List().DropNulls` | done |  |
| `list.eval` | `List().Eval` | done |  |
| `list.explode` | `List().Explode` | done |  |
| `list.filter` | `List().Filter` | done |  |
| `list.first` | `List().First` | done |  |
| `list.gather` | `List().Gather` | done |  |
| `list.gather_every` | `List().GatherEvery` | done |  |
| `list.get` | `List().Get` | done |  |
| `list.head` | `List().Head` | done |  |
| `list.item` | `List().Item` | done |  |
| `list.join` | `List().Join` | done |  |
| `list.last` | `List().Last` | done |  |
| `list.len` | `List().Len` | done |  |
| `list.max` | `List().Max` | done |  |
| `list.mean` | `List().Mean` | done |  |
| `list.median` | `List().Median` | done |  |
| `list.min` | `List().Min` | done |  |
| `list.n_unique` | `List().NUnique` | done |  |
| `list.reverse` | `List().Reverse` | done |  |
| `list.sample` | `List().Sample` | done |  |
| `list.set_difference` | `List().SetDifference` | done |  |
| `list.set_intersection` | `List().SetIntersection` | done |  |
| `list.set_symmetric_difference` | `List().SetSymmetricDifference` | done |  |
| `list.set_union` | `List().SetUnion` | done |  |
| `list.shift` | `List().Shift` | done |  |
| `list.slice` | `List().Slice` | done |  |
| `list.sort` | `List().Sort` | done |  |
| `list.std` | `List().Std` | done |  |
| `list.sum` | `List().Sum` | done |  |
| `list.tail` | `List().Tail` | done |  |
| `list.to_array` | `List().ToArray` | done |  |
| `list.to_struct` | `List().ToStruct` | done |  |
| `list.unique` | `List().Unique` | done |  |
| `list.var` | `List().Var` | done |  |

## Expr.arr

`pl.Expr.arr` against `expr.ArrOps`, reached with `.Arr()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `arr.agg` |  | todo |  |
| `arr.all` | `Arr().All` | done |  |
| `arr.any` | `Arr().Any` | done |  |
| `arr.arg_max` | `Arr().ArgMax` | done |  |
| `arr.arg_min` | `Arr().ArgMin` | done |  |
| `arr.contains` | `Arr().Contains` | done |  |
| `arr.count_matches` | `Arr().CountMatches` | done |  |
| `arr.eval` |  | todo |  |
| `arr.explode` | `Arr().Explode` | done |  |
| `arr.first` | `Arr().First` | done |  |
| `arr.get` | `Arr().Get` | done |  |
| `arr.head` | `Arr().Head` | done |  |
| `arr.join` | `Arr().Join` | done |  |
| `arr.last` | `Arr().Last` | done |  |
| `arr.len` | `Arr().Len` | done |  |
| `arr.max` | `Arr().Max` | done |  |
| `arr.mean` | `Arr().Mean` | done |  |
| `arr.median` | `Arr().Median` | done |  |
| `arr.min` | `Arr().Min` | done |  |
| `arr.n_unique` | `Arr().NUnique` | done |  |
| `arr.reverse` | `Arr().Reverse` | done |  |
| `arr.shift` | `Arr().Shift` | done |  |
| `arr.slice` | `Arr().Slice` | done |  |
| `arr.sort` | `Arr().Sort` | done |  |
| `arr.std` | `Arr().Std` | done |  |
| `arr.sum` | `Arr().Sum` | done |  |
| `arr.tail` | `Arr().Tail` | done |  |
| `arr.to_list` | `Arr().ToList` | done |  |
| `arr.to_struct` | `Arr().ToStruct` | done |  |
| `arr.unique` | `Arr().Unique` | done |  |
| `arr.var` | `Arr().Var` | done |  |

## Expr.struct

`pl.Expr.struct` against `expr.StructOps`, reached with `.Struct()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `struct.field` | `Struct().Field` | done |  |
| `struct.json_encode` | `Struct().JSONEncode` | done |  |
| `struct.rename_fields` | `Struct().RenameFields` | done |  |
| `struct.unnest` | `Struct().Unnest` | done |  |
| `struct.with_fields` | `Struct().WithFields` | done |  |

## Expr.name

`pl.Expr.name` against `expr.NameOps`, reached with `.Name()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `name.keep` | `Name().Keep` | done |  |
| `name.map` | `Name().Map` | done |  |
| `name.map_fields` | `Name().MapFields` | done |  |
| `name.prefix` | `Name().Prefix` | done |  |
| `name.prefix_fields` | `Name().PrefixFields` | done |  |
| `name.replace` | `Name().Replace` | done |  |
| `name.suffix` | `Name().Suffix` | done |  |
| `name.suffix_fields` | `Name().SuffixFields` | done |  |
| `name.to_lowercase` | `Name().ToLowercase` | done |  |
| `name.to_uppercase` | `Name().ToUppercase` | done |  |

## Expr.bin

`pl.Expr.bin` against `expr.BinOps`, reached with `.Bin()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `bin.contains` | `Bin().Contains` | done |  |
| `bin.decode` | `Bin().Decode` | done |  |
| `bin.encode` | `Bin().Encode` | done |  |
| `bin.ends_with` | `Bin().EndsWith` | done |  |
| `bin.get` | `Bin().Get` | done |  |
| `bin.head` | `Bin().Head` | done |  |
| `bin.reinterpret` | `Bin().Reinterpret` | done |  |
| `bin.size` | `Bin().Size` | done |  |
| `bin.slice` | `Bin().Slice` | done |  |
| `bin.starts_with` | `Bin().StartsWith` | done |  |
| `bin.tail` | `Bin().Tail` | done |  |

## Expr.cat

`pl.Expr.cat` against `expr.CatOps`, reached with `.Cat()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `cat.ends_with` | `Cat().EndsWith` | done |  |
| `cat.get_categories` | `Cat().GetCategories` | done |  |
| `cat.len_bytes` | `Cat().LenBytes` | done |  |
| `cat.len_chars` | `Cat().LenChars` | done |  |
| `cat.slice` | `Cat().Slice` | done |  |
| `cat.starts_with` | `Cat().StartsWith` | done |  |

## DataFrame

`pl.DataFrame` against `*dataframe.DataFrame`. Some rows point at root package helpers (`golars.*`) or I/O packages.

| polars | golars | Status | Notes |
|---|---|---|---|
| `df.bottom_k` | `df.BottomK` | done |  |
| `df.cast` | `df.Cast` | done |  |
| `df.clear` | `df.Clear` | done |  |
| `df.clone` | `df.Clone` | done |  |
| `df.collect_schema` | `df.CollectSchema` | done |  |
| `df.columns` | `df.Columns` | done |  |
| `df.corr` | `df.Corr` | done |  |
| `df.count` | `df.CountAll` | done |  |
| `df.describe` | `df.Describe` | done |  |
| `df.drop` | `df.Drop` | done |  |
| `df.drop_in_place` |  | todo |  |
| `df.drop_nans` | `df.DropNans` | done |  |
| `df.drop_nulls` | `df.DropNulls` | done |  |
| `df.dtypes` | `df.DTypes` | done |  |
| `df.equals` | `df.Equals` | done |  |
| `df.estimated_size` | `df.EstimatedSize` | done |  |
| `df.explode` | `df.Explode` | done |  |
| `df.extend` | `df.Extend` | done |  |
| `df.fill_nan` | `df.FillNan` | done |  |
| `df.fill_null` | `df.FillNull` | done |  |
| `df.filter` | `df.Filter` | done |  |
| `df.flags` |  | todo |  |
| `df.fold` | `df.Fold` | done |  |
| `df.gather_every` | `df.GatherEvery` | done |  |
| `df.get_column` | `df.GetColumn` | done |  |
| `df.get_column_index` | `df.GetColumnIndex` | done |  |
| `df.get_columns` | `df.GetColumns` | done |  |
| `df.glimpse` | `df.Glimpse` | done |  |
| `df.group_by` | `df.GroupBy` | done |  |
| `df.group_by_dynamic` | `df.GroupByDynamic` | done |  |
| `df.hash_rows` | `df.HashRows` | done |  |
| `df.head` | `df.Head` | done |  |
| `df.height` | `df.Height` | done |  |
| `df.hstack` | `df.HStack` | done |  |
| `df.insert_column` | `df.InsertColumn` | done |  |
| `df.interpolate` |  | todo |  |
| `df.is_duplicated` | `df.IsDuplicated` | done |  |
| `df.is_empty` | `df.IsEmpty` | done |  |
| `df.is_unique` | `df.IsUnique` | done |  |
| `df.item` | `df.Item` | done |  |
| `df.iter_columns` | `df.IterColumns` | done |  |
| `df.iter_rows` | `df.IterRows` | done |  |
| `df.iter_slices` | `df.IterSlices` | done |  |
| `df.join` | `df.Join` | partial | single key; inner, left and cross only (no right, full, semi, anti) |
| `df.join_asof` | `df.JoinAsof` | done |  |
| `df.join_where` | `df.JoinWhere` | done |  |
| `df.lazy` | `golars.Lazy` | done |  |
| `df.limit` | `df.Limit` | done |  |
| `df.map_rows` | `df.MapRows` | done |  |
| `df.match_to_schema` | `df.MatchToSchema` | done |  |
| `df.max` | `df.MaxAll` | done |  |
| `df.max_horizontal` | `df.MaxHorizontal` | done |  |
| `df.mean` | `df.MeanAll` | done |  |
| `df.mean_horizontal` | `df.MeanHorizontal` | done |  |
| `df.median` | `df.MedianAll` | done |  |
| `df.merge_sorted` | `df.MergeSorted` | done |  |
| `df.min` | `df.MinAll` | done |  |
| `df.min_horizontal` | `df.MinHorizontal` | done |  |
| `df.n_chunks` | `df.NChunks` | done |  |
| `df.n_unique` | `df.NUnique` | done |  |
| `df.null_count` | `df.NullCount` | done |  |
| `df.partition_by` | `df.PartitionBy` | done |  |
| `df.pipe` | `df.Pipe` | done |  |
| `df.pivot` | `df.Pivot` | done |  |
| `df.product` | `df.ProductAll` | done |  |
| `df.quantile` | `df.QuantileAll` | done |  |
| `df.rechunk` | `df.Rechunk` | done |  |
| `df.remove` |  | todo |  |
| `df.rename` | `df.Rename` | done |  |
| `df.replace_column` | `df.ReplaceColumn` | done |  |
| `df.reverse` | `df.Reverse` | done |  |
| `df.rolling` | `df.Rolling` | done |  |
| `df.row` | `df.Row` | done |  |
| `df.rows` | `df.Rows` | done |  |
| `df.rows_by_key` | `df.RowsByKey` | done |  |
| `df.sample` | `df.Sample` | done |  |
| `df.schema` | `df.Schema` | done |  |
| `df.select` | `df.Select` | partial | takes column names; use golars.SelectExpr for expressions |
| `df.select_seq` | `golars.SelectSeq` | done |  |
| `df.set_sorted` |  | todo |  |
| `df.shape` | `df.Shape` | done |  |
| `df.shift` | `df.Shift` | done |  |
| `df.show` | `df.String` | done | String() renders the polars-style table |
| `df.slice` | `df.Slice` | done |  |
| `df.sort` | `df.Sort` | done |  |
| `df.sql` | `golars.SQL` | done |  |
| `df.std` | `df.StdAll` | done |  |
| `df.sum` | `df.SumAll` | done |  |
| `df.sum_horizontal` | `df.SumHorizontal` | done |  |
| `df.tail` | `df.Tail` | done |  |
| `df.to_arrow` | `df.ToArrow` | done |  |
| `df.to_dict` | `df.ToMap` | done |  |
| `df.to_dicts` | `df.ToDicts` | done |  |
| `df.to_dummies` | `df.ToDummies` | done |  |
| `df.to_series` | `df.ToSeries` | done |  |
| `df.to_struct` | `df.ToStruct` | done |  |
| `df.top_k` | `df.TopK` | done |  |
| `df.transpose` | `df.Transpose` | done |  |
| `df.unique` | `df.Unique` | done |  |
| `df.unnest` | `df.Unnest` | done |  |
| `df.unpivot` | `df.Unpivot` | done |  |
| `df.unstack` | `df.Unstack` | done |  |
| `df.update` | `df.Update` | done |  |
| `df.upsample` | `df.Upsample` | done |  |
| `df.var` | `df.VarAll` | done |  |
| `df.vstack` | `df.VStack` | done |  |
| `df.width` | `df.Width` | done |  |
| `df.with_columns` | `df.WithColumns` | partial | takes Series; use golars.WithColumnsExpr for expressions |
| `df.with_columns_seq` | `golars.WithColumnsSeq` | done |  |
| `df.with_row_index` | `df.WithRowIndex` | done |  |
| `df.write_avro` |  | todo |  |
| `df.write_clipboard` | `clipboard.Write` | done |  |
| `df.write_database` |  | todo |  |
| `df.write_excel` |  | todo |  |
| `df.write_iceberg` |  | todo |  |
| `df.write_ipc_stream` | `golars.WriteIPCStream` | done |  |
| `df.write_json` | `golars.WriteJSON` | done |  |
| `df.write_ndjson` | `golars.WriteNDJSON` | done |  |

## LazyFrame

`pl.LazyFrame` against `lazy.LazyFrame`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `lf.bottom_k` | `lf.BottomK` | done |  |
| `lf.cache` | `lf.Cache` | done |  |
| `lf.cast` | `lf.Cast` | done |  |
| `lf.clear` | `lf.Clear` | done |  |
| `lf.clone` | `lf.Clone` | done |  |
| `lf.collect_async` | `lf.CollectAsync` | done |  |
| `lf.collect_batches` | `lf.CollectBatches` | done |  |
| `lf.collect_schema` | `lf.CollectSchema` | done |  |
| `lf.columns` | `lf.Columns` | done |  |
| `lf.count` | `lf.Count` | done |  |
| `lf.describe` | `lf.Describe` | done |  |
| `lf.drop` | `lf.Drop` | done |  |
| `lf.drop_nans` | `lf.DropNans` | done |  |
| `lf.drop_nulls` | `lf.DropNulls` | done |  |
| `lf.dtypes` | `lf.DTypes` | done |  |
| `lf.explode` | `lf.Explode` | done |  |
| `lf.fill_nan` | `lf.FillNan` | done |  |
| `lf.fill_null` | `lf.FillNull` | done |  |
| `lf.filter` | `lf.Filter` | done |  |
| `lf.first` | `lf.First` | done |  |
| `lf.gather_every` | `lf.GatherEvery` | done |  |
| `lf.group_by` | `lf.GroupBy` | done |  |
| `lf.group_by_dynamic` | `lf.GroupByDynamic` | done |  |
| `lf.head` | `lf.Head` | done |  |
| `lf.inspect` | `lf.Inspect` | done |  |
| `lf.interpolate` |  | todo |  |
| `lf.join` | `lf.Join` | partial | single key; inner, left and cross only (no right, full, semi, anti) |
| `lf.join_asof` | `lf.JoinAsof` | done |  |
| `lf.join_where` | `lf.JoinWhere` | done |  |
| `lf.last` | `lf.Last` | done |  |
| `lf.limit` | `lf.Limit` | done |  |
| `lf.map_batches` | `lf.MapBatches` | done |  |
| `lf.match_to_schema` | `lf.MatchToSchema` | done |  |
| `lf.max` | `lf.Max` | done |  |
| `lf.mean` | `lf.Mean` | done |  |
| `lf.median` | `lf.Median` | done |  |
| `lf.merge_sorted` | `lf.MergeSorted` | done |  |
| `lf.min` | `lf.Min` | done |  |
| `lf.null_count` | `lf.NullCount` | done |  |
| `lf.pipe` | `lf.Pipe` | done |  |
| `lf.pivot` | `lf.Pivot` | done |  |
| `lf.quantile` | `lf.Quantile` | done |  |
| `lf.remove` |  | todo |  |
| `lf.rename` | `lf.Rename` | done |  |
| `lf.reverse` | `lf.Reverse` | done |  |
| `lf.rolling` | `lf.Rolling` | done |  |
| `lf.schema` | `lf.Schema` | done |  |
| `lf.select` | `lf.Select` | done |  |
| `lf.select_seq` | `lf.SelectSeq` | done |  |
| `lf.set_sorted` |  | todo |  |
| `lf.shift` | `lf.Shift` | done |  |
| `lf.show` |  | todo |  |
| `lf.sink_delta` |  | todo |  |
| `lf.sink_iceberg` |  | todo |  |
| `lf.slice` | `lf.Slice` | done |  |
| `lf.sort` | `lf.Sort` | done |  |
| `lf.sql` | `golars.SQLLazy` | done |  |
| `lf.std` | `lf.Std` | done |  |
| `lf.sum` | `lf.Sum` | done |  |
| `lf.tail` | `lf.Tail` | done |  |
| `lf.top_k` | `lf.TopK` | done |  |
| `lf.unique` | `lf.Unique` | done |  |
| `lf.unnest` | `lf.Unnest` | done |  |
| `lf.unpivot` | `lf.Unpivot` | done |  |
| `lf.update` | `lf.Update` | done |  |
| `lf.var` | `lf.Var` | done |  |
| `lf.width` | `lf.Width` | done |  |
| `lf.with_columns` | `lf.WithColumns` | done |  |
| `lf.with_columns_seq` | `lf.WithColumnsSeq` | done |  |
| `lf.with_row_index` | `lf.WithRowIndex` | done |  |

## GroupBy

`df.group_by(...)` against `*dataframe.GroupBy`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `gb.agg` | `gb.Agg` | done |  |
| `gb.all` | `gb.All` | done |  |
| `gb.first` | `gb.First` | done |  |
| `gb.having` |  | todo |  |
| `gb.head` | `gb.Head` | done |  |
| `gb.last` | `gb.Last` | done |  |
| `gb.len` | `gb.Len` | done |  |
| `gb.map_groups` | `gb.MapGroups` | done |  |
| `gb.max` | `gb.Max` | done |  |
| `gb.mean` | `gb.Mean` | done |  |
| `gb.median` | `gb.Median` | done |  |
| `gb.min` | `gb.Min` | done |  |
| `gb.n_unique` | `gb.NUnique` | done |  |
| `gb.quantile` | `gb.Quantile` | done |  |
| `gb.sum` | `gb.Sum` | done |  |
| `gb.tail` | `gb.Tail` | done |  |

## LazyGroupBy

`lf.group_by(...)` against `lazy.LazyGroupBy`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `lgb.agg` | `lgb.Agg` | done |  |
| `lgb.all` | `lgb.All` | done |  |
| `lgb.first` | `lgb.First` | done |  |
| `lgb.having` |  | todo |  |
| `lgb.head` | `lgb.Head` | done |  |
| `lgb.last` | `lgb.Last` | done |  |
| `lgb.len` | `lgb.Len` | done |  |
| `lgb.map_groups` | `lgb.MapGroups` | done |  |
| `lgb.max` | `lgb.Max` | done |  |
| `lgb.mean` | `lgb.Mean` | done |  |
| `lgb.median` | `lgb.Median` | done |  |
| `lgb.min` | `lgb.Min` | done |  |
| `lgb.n_unique` | `lgb.NUnique` | done |  |
| `lgb.quantile` | `lgb.Quantile` | done |  |
| `lgb.sum` | `lgb.Sum` | done |  |
| `lgb.tail` | `lgb.Tail` | done |  |

## Series

`pl.Series` against `*series.Series`. Some rows point at `compute.*` kernels or root helpers.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.abs` | `s.Abs` | done |  |
| `s.alias` | `s.Rename` | done |  |
| `s.all` | `s.All` | done |  |
| `s.any` | `s.Any` | done |  |
| `s.append` | `s.Append` | done |  |
| `s.approx_n_unique` | `s.ApproxNUnique` | done |  |
| `s.arccos` | `s.Arccos` | done |  |
| `s.arccosh` | `s.Arccosh` | done |  |
| `s.arcsin` | `s.Arcsin` | done |  |
| `s.arcsinh` | `s.Arcsinh` | done |  |
| `s.arctan` | `s.Arctan` | done |  |
| `s.arctanh` | `s.Arctanh` | done |  |
| `s.arg_max` | `s.ArgMax` | done |  |
| `s.arg_min` | `s.ArgMin` | done |  |
| `s.arg_sort` | `s.ArgSort` | done |  |
| `s.arg_true` | `s.ArgTrue` | done |  |
| `s.arg_unique` | `s.ArgUnique` | done |  |
| `s.arr` | `s.Arr` | done |  |
| `s.backward_fill` | `s.BackwardFill` | done |  |
| `s.bin` | `s.Bin` | done |  |
| `s.bitwise_and` | `s.BitwiseAnd` | done |  |
| `s.bitwise_count_ones` | `s.BitCount("count_ones")` | done |  |
| `s.bitwise_count_zeros` | `s.BitCount("count_zeros")` | done |  |
| `s.bitwise_leading_ones` | `s.BitCount("leading_ones")` | done |  |
| `s.bitwise_leading_zeros` | `s.BitCount("leading_zeros")` | done |  |
| `s.bitwise_or` | `s.BitwiseOr` | done |  |
| `s.bitwise_trailing_ones` | `s.BitCount("trailing_ones")` | done |  |
| `s.bitwise_trailing_zeros` | `s.BitCount("trailing_zeros")` | done |  |
| `s.bitwise_xor` | `s.BitwiseXor` | done |  |
| `s.bottom_k` | `s.BottomK` | done |  |
| `s.bottom_k_by` | `s.BottomKBy` | done |  |
| `s.cast` | `compute.Cast` | done |  |
| `s.cat` | `s.Cat` | done |  |
| `s.cbrt` | `s.Cbrt` | done |  |
| `s.ceil` | `s.Ceil` | done |  |
| `s.chunk_lengths` | `s.ChunkLengths` | done |  |
| `s.clear` | `s.Clear` | done |  |
| `s.clip` | `s.Clip` | done |  |
| `s.clone` | `s.Clone` | done |  |
| `s.cos` | `s.Cos` | done |  |
| `s.cosh` | `s.Cosh` | done |  |
| `s.cot` | `s.Cot` | done |  |
| `s.count` | `compute.Count` | done |  |
| `s.cum_count` | `s.CumCount` | done |  |
| `s.cum_max` | `s.CumMax` | done |  |
| `s.cum_min` | `s.CumMin` | done |  |
| `s.cum_prod` | `s.CumProd` | done |  |
| `s.cum_sum` | `s.CumSum` | done |  |
| `s.cumulative_eval` |  | todo |  |
| `s.cut` | `s.Cut` | done |  |
| `s.describe` | `golars.DescribeSeries` | done |  |
| `s.diff` | `s.Diff` | done |  |
| `s.dot` | `s.Dot` | done |  |
| `s.drop_nans` | `s.DropNans` | done |  |
| `s.drop_nulls` | `s.DropNulls` | done |  |
| `s.dt` | `s.Dt` | done |  |
| `s.dtype` | `s.DType` | done |  |
| `s.entropy` | `s.Entropy` | done |  |
| `s.eq` | `compute.Eq` | done |  |
| `s.eq_missing` | `s.EqMissing` | done |  |
| `s.equals` | `s.Equals` | done |  |
| `s.estimated_size` |  | todo |  |
| `s.ewm_mean` | `s.EWMMean` | done |  |
| `s.ewm_mean_by` |  | todo |  |
| `s.ewm_std` | `s.EWMStd` | done |  |
| `s.ewm_var` | `s.EWMVar` | done |  |
| `s.exp` | `s.Exp` | done |  |
| `s.explode` | `s.Explode` | done |  |
| `s.extend` | `s.Extend` | done |  |
| `s.extend_constant` | `s.ExtendConstant` | done |  |
| `s.fill_nan` | `s.FillNan` | done |  |
| `s.fill_null` | `s.FillNull` | done |  |
| `s.filter` | `compute.Filter` | done |  |
| `s.first` | `s.First` | done |  |
| `s.flags` |  | todo |  |
| `s.floor` | `s.Floor` | done |  |
| `s.forward_fill` | `s.ForwardFill` | done |  |
| `s.gather` | `s.Gather` | done |  |
| `s.gather_every` | `s.GatherEvery` | done |  |
| `s.ge` | `compute.Ge` | done |  |
| `s.get_chunks` | `s.Chunks` | done |  |
| `s.gt` | `compute.Gt` | done |  |
| `s.has_nulls` | `s.HasNulls` | done |  |
| `s.hash` | `s.Hash` | done |  |
| `s.head` | `s.Head` | done |  |
| `s.hist` | `s.Hist` | done |  |
| `s.implode` | `s.Implode` | done |  |
| `s.index_of` | `s.IndexOf` | done |  |
| `s.interpolate` | `s.Interpolate` | done |  |
| `s.interpolate_by` | `s.InterpolateBy` | done |  |
| `s.is_between` | `s.IsBetween` | done |  |
| `s.is_close` | `s.IsClose` | done |  |
| `s.is_duplicated` | `s.IsDuplicated` | done |  |
| `s.is_empty` | `s.IsEmpty` | done |  |
| `s.is_finite` | `s.IsFinite` | done |  |
| `s.is_first_distinct` | `s.IsFirstDistinct` | done |  |
| `s.is_in` |  | todo |  |
| `s.is_infinite` | `s.IsInfinite` | done |  |
| `s.is_last_distinct` | `s.IsLastDistinct` | done |  |
| `s.is_nan` | `s.IsNaN` | done |  |
| `s.is_not_nan` |  | todo |  |
| `s.is_not_null` | `s.IsNotNull` | done |  |
| `s.is_null` | `s.IsNull` | done |  |
| `s.is_sorted` | `s.IsSorted` | done |  |
| `s.is_unique` | `s.IsUnique` | done |  |
| `s.item` | `s.Item` | done |  |
| `s.kurtosis` | `s.Kurtosis` | done |  |
| `s.last` | `s.Last` | done |  |
| `s.le` | `compute.Le` | done |  |
| `s.len` | `s.Len` | done |  |
| `s.limit` | `s.Head` | done |  |
| `s.list` | `s.List` | done |  |
| `s.log` | `s.Log` | done |  |
| `s.log10` | `s.Log10` | done |  |
| `s.log1p` | `s.Log1p` | done |  |
| `s.lower_bound` | `s.LowerBound` | done |  |
| `s.lt` | `compute.Lt` | done |  |
| `s.max` | `s.Max` | done | returns float64; empty or all-null gives NaN, not None |
| `s.max_by` | `s.MaxBy` | done |  |
| `s.mean` | `s.Mean` | done | returns float64; empty or all-null gives NaN, not None |
| `s.median` | `s.Median` | done |  |
| `s.min` | `s.Min` | done | returns float64; empty or all-null gives NaN, not None |
| `s.min_by` | `s.MinBy` | done |  |
| `s.mode` | `s.Mode` | done |  |
| `s.n_chunks` | `s.NChunks` | done |  |
| `s.n_unique` | `s.NUnique` | done |  |
| `s.name` | `s.Name` | done |  |
| `s.nan_max` | `s.NanMax` | done |  |
| `s.nan_min` | `s.NanMin` | done |  |
| `s.ne` | `compute.Ne` | done |  |
| `s.ne_missing` | `s.NeMissing` | done |  |
| `s.new_from_index` | `s.NewFromIndex` | done |  |
| `s.not_` | `compute.Not` | done |  |
| `s.null_count` | `s.NullCount` | done |  |
| `s.pct_change` | `s.PctChange` | done |  |
| `s.peak_max` | `s.PeakMax` | done |  |
| `s.peak_min` | `s.PeakMin` | done |  |
| `s.pow` | `s.Pow` | done |  |
| `s.product` | `s.Product` | done |  |
| `s.qcut` | `s.QCut` | done |  |
| `s.quantile` | `s.Quantile` | done |  |
| `s.rank` | `s.Rank` | done |  |
| `s.rechunk` | `s.Rechunk` | done |  |
| `s.reinterpret` | `s.Reinterpret` | done |  |
| `s.rename` | `s.Rename` | done |  |
| `s.repeat_by` | `s.RepeatBy` | done |  |
| `s.replace_strict` | `s.ReplaceStrictSeries` | done |  |
| `s.reshape` | `s.Reshape` | done |  |
| `s.reverse` | `s.Reverse` | done |  |
| `s.rle` | `s.Rle` | done |  |
| `s.rle_id` | `s.RleID` | done |  |
| `s.rolling_kurtosis` | `s.RollingKurtosis` | done |  |
| `s.rolling_map` | `s.RollingMap` | done |  |
| `s.rolling_max` | `s.RollingMax` | done |  |
| `s.rolling_max_by` | `s.RollingBy("max", ...)` | done |  |
| `s.rolling_mean` | `s.RollingMean` | done |  |
| `s.rolling_mean_by` | `s.RollingBy("mean", ...)` | done |  |
| `s.rolling_median` | `s.RollingMedianWindow` | done |  |
| `s.rolling_median_by` | `s.RollingBy("median", ...)` | done |  |
| `s.rolling_min` | `s.RollingMin` | done |  |
| `s.rolling_min_by` | `s.RollingBy("min", ...)` | done |  |
| `s.rolling_quantile` | `s.RollingQuantile` | done |  |
| `s.rolling_quantile_by` | `s.RollingBy("quantile", ...)` | done |  |
| `s.rolling_rank` | `s.RollingRank` | done |  |
| `s.rolling_rank_by` |  | todo |  |
| `s.rolling_skew` | `s.RollingSkew` | done |  |
| `s.rolling_std` | `s.RollingStd` | done |  |
| `s.rolling_std_by` | `s.RollingBy("std", ...)` | done |  |
| `s.rolling_sum` | `s.RollingSum` | done |  |
| `s.rolling_sum_by` | `s.RollingBy("sum", ...)` | done |  |
| `s.rolling_var` | `s.RollingVar` | done |  |
| `s.rolling_var_by` | `s.RollingBy("var", ...)` | done |  |
| `s.round` | `s.Round` | done |  |
| `s.round_sig_figs` | `s.RoundSigFigs` | done |  |
| `s.sample` | `s.Sample` | done |  |
| `s.scatter` | `s.Scatter` | done |  |
| `s.search_sorted` | `s.SearchSorted` | done |  |
| `s.set` | `s.Scatter` | done |  |
| `s.set_sorted` |  | todo |  |
| `s.shape` | `s.Shape` | done |  |
| `s.shift` | `s.Shift` | done |  |
| `s.shrink_dtype` | `s.ShrinkDtype` | done |  |
| `s.shuffle` | `s.Shuffle` | done |  |
| `s.sign` | `s.Sign` | done |  |
| `s.sin` | `s.Sin` | done |  |
| `s.sinh` | `s.Sinh` | done |  |
| `s.skew` | `s.Skew` | done |  |
| `s.slice` | `s.Slice` | done |  |
| `s.sort` | `s.SortWith` | done |  |
| `s.sql` |  | todo |  |
| `s.sqrt` | `s.Sqrt` | done |  |
| `s.std` | `s.Std` | done |  |
| `s.str` | `s.Str` | done |  |
| `s.struct` | `s.Struct` | done |  |
| `s.sum` | `s.Sum` | done | returns float64; empty or all-null gives NaN, not None |
| `s.tail` | `s.Tail` | done |  |
| `s.tan` | `s.Tan` | done |  |
| `s.tanh` | `s.Tanh` | done |  |
| `s.to_arrow` | `s.ToArrow` | done |  |
| `s.to_dummies` |  | todo |  |
| `s.to_frame` | `golars.ToFrame` | done |  |
| `s.to_list` | `s.ToList` | done |  |
| `s.to_physical` | `s.ToPhysical` | done |  |
| `s.top_k` | `s.TopK` | done |  |
| `s.top_k_by` | `s.TopKBy` | done |  |
| `s.truncate` |  | todo |  |
| `s.unique` | `s.Unique` | done |  |
| `s.unique_counts` | `s.UniqueCounts` | done |  |
| `s.upper_bound` | `s.UpperBound` | done |  |
| `s.value_counts` | `s.ValueCounts` | done |  |
| `s.var` | `s.Var` | done |  |
| `s.zip_with` | `s.ZipWith` | done |  |

## Series.str

`pl.Series.str` against `series.StrOps`, reached with `s.Str()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.str.contains` | `s.Str().Contains` | done |  |
| `s.str.contains_any` | `s.Str().ContainsAny` | done |  |
| `s.str.count_matches` | `s.Str().CountMatches` | done |  |
| `s.str.decode` | `s.Str().Decode` | done |  |
| `s.str.encode` | `s.Str().Encode` | done |  |
| `s.str.ends_with` | `s.Str().EndsWith` | done |  |
| `s.str.escape_regex` | `s.Str().EscapeRegex` | done |  |
| `s.str.extract` | `s.Str().Extract` | done |  |
| `s.str.extract_all` | `s.Str().ExtractAll` | done |  |
| `s.str.extract_groups` | `s.Str().ExtractGroups` | done |  |
| `s.str.extract_many` | `s.Str().ExtractMany` | done |  |
| `s.str.find` | `s.Str().Find` | done |  |
| `s.str.find_many` | `s.Str().FindMany` | done |  |
| `s.str.head` | `s.Str().Head` | done |  |
| `s.str.join` | `s.Str().Join` | done |  |
| `s.str.json_decode` | `s.Str().JSONDecode` | done |  |
| `s.str.json_path_match` | `s.Str().JSONPathMatch` | done |  |
| `s.str.len_bytes` | `s.Str().LenBytes` | done |  |
| `s.str.len_chars` | `s.Str().LenChars` | done |  |
| `s.str.normalize` |  | todo |  |
| `s.str.pad_end` | `s.Str().PadEnd` | done |  |
| `s.str.pad_start` | `s.Str().PadStart` | done |  |
| `s.str.replace` | `s.Str().Replace` | done |  |
| `s.str.replace_all` | `s.Str().ReplaceAll` | done |  |
| `s.str.replace_many` | `s.Str().ReplaceMany` | done |  |
| `s.str.reverse` | `s.Str().Reverse` | done |  |
| `s.str.slice` | `s.Str().Slice` | done |  |
| `s.str.split` | `s.Str().Split` | done |  |
| `s.str.split_exact` | `s.Str().SplitExact` | done |  |
| `s.str.splitn` | `s.Str().SplitN` | done |  |
| `s.str.starts_with` | `s.Str().StartsWith` | done |  |
| `s.str.strip_chars` | `s.Str().StripChars` | done |  |
| `s.str.strip_chars_end` | `s.Str().StripCharsEnd` | done |  |
| `s.str.strip_chars_start` | `s.Str().StripCharsStart` | done |  |
| `s.str.strip_prefix` | `s.Str().StripPrefix` | done |  |
| `s.str.strip_suffix` | `s.Str().StripSuffix` | done |  |
| `s.str.strptime` | `s.Str().Strptime` | done |  |
| `s.str.tail` | `s.Str().Tail` | done |  |
| `s.str.to_date` | `s.Str().ToDate` | done |  |
| `s.str.to_datetime` | `s.Str().ToDatetime` | done |  |
| `s.str.to_decimal` |  | todo |  |
| `s.str.to_integer` | `s.Str().ToInteger` | done |  |
| `s.str.to_lowercase` | `s.Str().ToLowercase` | done |  |
| `s.str.to_time` | `s.Str().ToTime` | done |  |
| `s.str.to_titlecase` | `s.Str().ToTitlecase` | done |  |
| `s.str.to_uppercase` | `s.Str().ToUppercase` | done |  |
| `s.str.zfill` | `s.Str().ZFill` | done |  |

## Series.dt

`pl.Series.dt` against `series.DtOps`, reached with `s.Dt()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.dt.add_business_days` | `s.Dt().AddBusinessDays` | done |  |
| `s.dt.base_utc_offset` |  | todo |  |
| `s.dt.cast_time_unit` | `s.Dt().CastTimeUnit` | done |  |
| `s.dt.century` | `s.Dt().Century` | done |  |
| `s.dt.combine` | `s.Dt().Combine` | done |  |
| `s.dt.convert_time_zone` | `s.Dt().ConvertTimeZone` | done |  |
| `s.dt.date` | `s.Dt().Date` | done |  |
| `s.dt.day` | `s.Dt().Day` | done |  |
| `s.dt.days_in_month` | `s.Dt().DaysInMonth` | done |  |
| `s.dt.dst_offset` |  | todo |  |
| `s.dt.epoch` | `s.Dt().Epoch` | done |  |
| `s.dt.hour` | `s.Dt().Hour` | done |  |
| `s.dt.is_business_day` | `s.Dt().IsBusinessDay` | done |  |
| `s.dt.is_leap_year` | `s.Dt().IsLeapYear` | done |  |
| `s.dt.iso_year` | `s.Dt().IsoYear` | done |  |
| `s.dt.max` |  | todo |  |
| `s.dt.microsecond` | `s.Dt().Microsecond` | done |  |
| `s.dt.millennium` | `s.Dt().Millennium` | done |  |
| `s.dt.millisecond` | `s.Dt().Millisecond` | done |  |
| `s.dt.min` |  | todo |  |
| `s.dt.minute` | `s.Dt().Minute` | done |  |
| `s.dt.month` | `s.Dt().Month` | done |  |
| `s.dt.month_end` | `s.Dt().MonthEnd` | done |  |
| `s.dt.month_start` | `s.Dt().MonthStart` | done |  |
| `s.dt.nanosecond` | `s.Dt().Nanosecond` | done |  |
| `s.dt.offset_by` | `s.Dt().OffsetBy` | done |  |
| `s.dt.ordinal_day` | `s.Dt().OrdinalDay` | done |  |
| `s.dt.quarter` | `s.Dt().Quarter` | done |  |
| `s.dt.replace` | `s.Dt().Replace` | done |  |
| `s.dt.replace_time_zone` | `s.Dt().ReplaceTimeZone` | done |  |
| `s.dt.round` | `s.Dt().Round` | done |  |
| `s.dt.second` | `s.Dt().Second` | done |  |
| `s.dt.strftime` | `s.Dt().Strftime` | done |  |
| `s.dt.time` | `s.Dt().Time` | done |  |
| `s.dt.timestamp` | `s.Dt().Timestamp` | done |  |
| `s.dt.to_string` | `s.Dt().ToString` | done |  |
| `s.dt.total_days` | `s.Dt().TotalDays` | done |  |
| `s.dt.total_hours` | `s.Dt().TotalHours` | done |  |
| `s.dt.total_microseconds` | `s.Dt().TotalMicroseconds` | done |  |
| `s.dt.total_milliseconds` | `s.Dt().TotalMilliseconds` | done |  |
| `s.dt.total_minutes` | `s.Dt().TotalMinutes` | done |  |
| `s.dt.total_nanoseconds` | `s.Dt().TotalNanoseconds` | done |  |
| `s.dt.total_seconds` | `s.Dt().TotalSeconds` | done |  |
| `s.dt.truncate` | `s.Dt().Truncate` | done |  |
| `s.dt.week` | `s.Dt().Week` | done |  |
| `s.dt.weekday` | `s.Dt().Weekday` | done |  |
| `s.dt.year` | `s.Dt().Year` | done |  |

## Series.list

`pl.Series.list` against `series.ListOps`, reached with `s.List()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.list.agg` |  | todo |  |
| `s.list.all` | `s.List().All` | done |  |
| `s.list.any` | `s.List().Any` | done |  |
| `s.list.arg_max` | `s.List().ArgMax` | done |  |
| `s.list.arg_min` | `s.List().ArgMin` | done |  |
| `s.list.concat` | `s.List().Concat` | done |  |
| `s.list.contains` | `s.List().Contains` | done |  |
| `s.list.count_matches` | `s.List().CountMatches` | done |  |
| `s.list.diff` | `s.List().Diff` | done |  |
| `s.list.drop_nulls` | `s.List().DropNulls` | done |  |
| `s.list.eval` |  | todo |  |
| `s.list.explode` | `s.List().Explode` | done |  |
| `s.list.filter` | `s.List().Filter` | done |  |
| `s.list.first` | `s.List().First` | done |  |
| `s.list.gather` | `s.List().Gather` | done |  |
| `s.list.gather_every` | `s.List().GatherEvery` | done |  |
| `s.list.get` | `s.List().Get` | done |  |
| `s.list.head` | `s.List().Head` | done |  |
| `s.list.item` | `s.List().Item` | done |  |
| `s.list.join` | `s.List().Join` | done |  |
| `s.list.last` | `s.List().Last` | done |  |
| `s.list.len` | `s.List().Len` | done |  |
| `s.list.max` | `s.List().Max` | done |  |
| `s.list.mean` | `s.List().Mean` | done |  |
| `s.list.median` | `s.List().Median` | done |  |
| `s.list.min` | `s.List().Min` | done |  |
| `s.list.n_unique` | `s.List().NUnique` | done |  |
| `s.list.reverse` | `s.List().Reverse` | done |  |
| `s.list.sample` | `s.List().Sample` | done |  |
| `s.list.set_difference` | `s.List().SetOperation(other, SetDifference)` | done |  |
| `s.list.set_intersection` | `s.List().SetOperation(other, SetIntersection)` | done |  |
| `s.list.set_symmetric_difference` | `s.List().SetOperation(other, SetSymmetricDifference)` | done |  |
| `s.list.set_union` | `s.List().SetOperation(other, SetUnion)` | done |  |
| `s.list.shift` | `s.List().Shift` | done |  |
| `s.list.slice` | `s.List().Slice` | done |  |
| `s.list.sort` | `s.List().Sort` | done |  |
| `s.list.std` | `s.List().Std` | done |  |
| `s.list.sum` | `s.List().Sum` | done |  |
| `s.list.tail` | `s.List().Tail` | done |  |
| `s.list.to_array` | `s.List().ToArray` | done |  |
| `s.list.to_struct` | `s.List().ToStruct` | done |  |
| `s.list.unique` | `s.List().Unique` | done |  |
| `s.list.var` | `s.List().Var` | done |  |

## Series.arr

`pl.Series.arr` against `series.ArrOps`, reached with `s.Arr()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.arr.agg` |  | todo |  |
| `s.arr.all` | `s.Arr().All` | done |  |
| `s.arr.any` | `s.Arr().Any` | done |  |
| `s.arr.arg_max` | `s.Arr().ArgMax` | done |  |
| `s.arr.arg_min` | `s.Arr().ArgMin` | done |  |
| `s.arr.contains` | `s.Arr().Contains` | done |  |
| `s.arr.count_matches` | `s.Arr().CountMatches` | done |  |
| `s.arr.eval` |  | todo |  |
| `s.arr.explode` | `s.Arr().Explode` | done |  |
| `s.arr.first` | `s.Arr().First` | done |  |
| `s.arr.get` | `s.Arr().Get` | done |  |
| `s.arr.head` | `s.Arr().Head` | done |  |
| `s.arr.join` | `s.Arr().Join` | done |  |
| `s.arr.last` | `s.Arr().Last` | done |  |
| `s.arr.len` | `s.Arr().Len` | done |  |
| `s.arr.max` | `s.Arr().Max` | done |  |
| `s.arr.mean` | `s.Arr().Mean` | done |  |
| `s.arr.median` | `s.Arr().Median` | done |  |
| `s.arr.min` | `s.Arr().Min` | done |  |
| `s.arr.n_unique` | `s.Arr().NUnique` | done |  |
| `s.arr.reverse` | `s.Arr().Reverse` | done |  |
| `s.arr.shift` | `s.Arr().Shift` | done |  |
| `s.arr.slice` | `s.Arr().Slice` | done |  |
| `s.arr.sort` | `s.Arr().Sort` | done |  |
| `s.arr.std` | `s.Arr().Std` | done |  |
| `s.arr.sum` | `s.Arr().Sum` | done |  |
| `s.arr.tail` | `s.Arr().Tail` | done |  |
| `s.arr.to_list` | `s.Arr().ToList` | done |  |
| `s.arr.to_struct` | `s.Arr().ToStruct` | done |  |
| `s.arr.unique` | `s.Arr().Unique` | done |  |
| `s.arr.var` | `s.Arr().Var` | done |  |

## Series.struct

`pl.Series.struct` against `series.StructOps`, reached with `s.Struct()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.struct.field` | `s.Struct().Field` | done |  |
| `s.struct.fields` | `s.Struct().FieldNames` | done |  |
| `s.struct.json_encode` | `s.Struct().JSONEncode` | done |  |
| `s.struct.rename_fields` | `s.Struct().RenameFields` | done |  |
| `s.struct.schema` |  | todo |  |
| `s.struct.unnest` | `s.Struct().Unnest` | done |  |

## Series.bin

`pl.Series.bin` against `series.BinOps`, reached with `s.Bin()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.bin.contains` | `s.Bin().Contains` | done |  |
| `s.bin.decode` | `s.Bin().Decode` | done |  |
| `s.bin.encode` | `s.Bin().Encode` | done |  |
| `s.bin.ends_with` | `s.Bin().EndsWith` | done |  |
| `s.bin.get` | `s.Bin().Get` | done |  |
| `s.bin.head` | `s.Bin().Head` | done |  |
| `s.bin.reinterpret` | `s.Bin().Reinterpret` | done |  |
| `s.bin.size` | `s.Bin().Size` | done |  |
| `s.bin.slice` | `s.Bin().Slice` | done |  |
| `s.bin.starts_with` | `s.Bin().StartsWith` | done |  |
| `s.bin.tail` | `s.Bin().Tail` | done |  |

## Series.cat

`pl.Series.cat` against `series.CatOps`, reached with `s.Cat()`.

| polars | golars | Status | Notes |
|---|---|---|---|
| `s.cat.ends_with` | `s.Cat().EndsWith` | done |  |
| `s.cat.get_categories` | `s.Cat().GetCategories` | done |  |
| `s.cat.len_bytes` | `s.Cat().LenBytes` | done |  |
| `s.cat.len_chars` | `s.Cat().LenChars` | done |  |
| `s.cat.slice` | `s.Cat().Slice` | done |  |
| `s.cat.starts_with` | `s.Cat().StartsWith` | done |  |

## Data types

polars data type classes against the `dtype` package constructors.

| polars | golars | Status | Notes |
|---|---|---|---|
| `pl.Array` | `dtype.FixedList` | done |  |
| `pl.Binary` | `dtype.Binary` | done |  |
| `pl.Boolean` | `dtype.Bool` | done |  |
| `pl.Categorical` | `dtype.Categorical` | done |  |
| `pl.Date` | `dtype.Date` | done |  |
| `pl.Datetime` | `dtype.Datetime` | done |  |
| `pl.Decimal` |  | todo |  |
| `pl.Duration` | `dtype.Duration` | done |  |
| `pl.Enum` | `dtype.Enum` | done |  |
| `pl.Float16` |  | todo |  |
| `pl.Float32` | `dtype.Float32` | done |  |
| `pl.Float64` | `dtype.Float64` | done |  |
| `pl.Int128` |  | todo |  |
| `pl.Int16` | `dtype.Int16` | done |  |
| `pl.Int32` | `dtype.Int32` | done |  |
| `pl.Int64` | `dtype.Int64` | done |  |
| `pl.Int8` | `dtype.Int8` | done |  |
| `pl.List` | `dtype.List` | done |  |
| `pl.Null` | `dtype.Null` | done |  |
| `pl.String` | `dtype.String` | done |  |
| `pl.Struct` | `dtype.Struct` | done |  |
| `pl.Time` | `dtype.Time` | done |  |
| `pl.UInt128` |  | todo |  |
| `pl.UInt16` | `dtype.Uint16` | done |  |
| `pl.UInt32` | `dtype.Uint32` | done |  |
| `pl.UInt64` | `dtype.Uint64` | done |  |
| `pl.UInt8` | `dtype.Uint8` | done |  |
| `pl.Utf8` | `dtype.String` | done |  |

## Top-level functions

`pl.*` functions against the root `golars` package, then `expr`, `dataframe`, `lazy`, `series` and the `io/*` packages.

| polars | golars | Status | Notes |
|---|---|---|---|
| `pl.align_frames` |  | todo |  |
| `pl.all` | `golars.All` | done |  |
| `pl.all_horizontal` | `golars.AllHorizontal` | done |  |
| `pl.any` | `golars.Any` | done |  |
| `pl.any_horizontal` | `golars.AnyHorizontal` | done |  |
| `pl.approx_n_unique` | `golars.ApproxNUnique` | done |  |
| `pl.arange` | `golars.IntRange` | done |  |
| `pl.arctan2` | `golars.Arctan2` | done |  |
| `pl.arg_sort_by` | `golars.ArgSortBy` | done |  |
| `pl.arg_where` | `golars.ArgWhere` | done |  |
| `pl.business_day_count` |  | todo |  |
| `pl.coalesce` | `golars.Coalesce` | done |  |
| `pl.concat` | `golars.Concat` | done |  |
| `pl.concat_arr` | `golars.ConcatArr` | done |  |
| `pl.concat_list` | `golars.ConcatList` | done |  |
| `pl.concat_str` | `golars.ConcatStr` | done |  |
| `pl.count` | `golars.Count` | done |  |
| `pl.cov` | `golars.Cov` | done |  |
| `pl.cum_count` | `expr.Expr.CumCount` | partial | method only: Col(x).CumCount() |
| `pl.cum_fold` | `golars.CumFold` | done |  |
| `pl.cum_reduce` | `golars.CumReduce` | done |  |
| `pl.cum_sum` | `golars.CumSumHorizontal` | partial | horizontal form; for one column use Col(x).CumSum() |
| `pl.cum_sum_horizontal` | `golars.CumSumHorizontal` | done |  |
| `pl.date` | `golars.Date` | done |  |
| `pl.date_range` | `golars.DateRange` | done |  |
| `pl.date_ranges` |  | todo |  |
| `pl.datetime` | `golars.Datetime` | done |  |
| `pl.datetime_range` | `golars.DatetimeRange` | done |  |
| `pl.datetime_ranges` |  | todo |  |
| `pl.dtype_of` |  | todo |  |
| `pl.duration` | `golars.Duration` | done |  |
| `pl.element` | `golars.Element` | done |  |
| `pl.escape_regex` | `expr.StrOps.EscapeRegex` | done |  |
| `pl.exclude` | `golars.Exclude` | done |  |
| `pl.explain_all` |  | todo |  |
| `pl.field` | `golars.Field` | done |  |
| `pl.first` | `golars.First` | done |  |
| `pl.fold` | `golars.Fold` | done |  |
| `pl.format` | `golars.Format` | done |  |
| `pl.from_arrow` | `dataframe.FromArrowTable` | done |  |
| `pl.from_dataframe` | `lazy.FromDataFrame` | done |  |
| `pl.from_dict` | `golars.FromMap` | done |  |
| `pl.from_dicts` | `dataframe.DataFrame.ToDicts` | partial | reverse direction only; build frames with FromMap |
| `pl.from_epoch` |  | todo |  |
| `pl.from_records` | `dataframe.FromRecord` | partial | takes an arrow RecordBatch, not Python rows |
| `pl.from_repr` |  | todo |  |
| `pl.head` | `golars.Head` | done |  |
| `pl.implode` | `expr.Expr.Implode` | partial | method only: Col(x).Implode() |
| `pl.int_range` | `golars.IntRange` | done |  |
| `pl.int_ranges` | `golars.IntRanges` | done |  |
| `pl.json_normalize` |  | todo |  |
| `pl.last` | `golars.Last` | done |  |
| `pl.len` | `golars.Len` | done |  |
| `pl.linear_space` | `golars.LinearSpace` | done |  |
| `pl.linear_spaces` |  | todo |  |
| `pl.lit` | `golars.Lit` | done |  |
| `pl.max` | `golars.Max` | done |  |
| `pl.max_horizontal` | `golars.MaxHorizontal` | done |  |
| `pl.mean` | `golars.Mean` | done |  |
| `pl.mean_horizontal` | `golars.MeanHorizontal` | done |  |
| `pl.median` | `golars.Median` | done |  |
| `pl.min` | `golars.Min` | done |  |
| `pl.min_horizontal` | `golars.MinHorizontal` | done |  |
| `pl.n_unique` | `golars.NUnique` | done |  |
| `pl.nth` | `golars.Nth` | done |  |
| `pl.ones` | `golars.Ones` | done |  |
| `pl.quantile` | `golars.Quantile` | done |  |
| `pl.read_avro` |  | todo |  |
| `pl.read_clipboard` | `clipboard.Read` | done |  |
| `pl.read_database` | `sql.ReadSQL` | done |  |
| `pl.read_database_uri` |  | todo |  |
| `pl.read_delta` |  | todo |  |
| `pl.read_excel` |  | todo |  |
| `pl.read_ipc` | `golars.ReadIPC` | done |  |
| `pl.read_ipc_schema` |  | todo |  |
| `pl.read_ipc_stream` | `golars.NewIPCStreamReader` | done |  |
| `pl.read_json` | `golars.ReadJSON` | done |  |
| `pl.read_lines` |  | todo |  |
| `pl.read_ods` |  | todo |  |
| `pl.read_parquet_schema` | `parquet.ReadSchema` | done |  |
| `pl.reduce` | `golars.Reduce` | done |  |
| `pl.repeat` | `golars.Repeat` | done |  |
| `pl.rolling_corr` | `golars.RollingCorr` | done |  |
| `pl.rolling_cov` | `golars.RollingCov` | done |  |
| `pl.row_index` | `dataframe.DataFrame.WithRowIndex` | partial | frame method only: df.WithRowIndex / lf.WithRowIndex |
| `pl.scan_delta` |  | todo |  |
| `pl.scan_iceberg` |  | todo |  |
| `pl.scan_lines` |  | todo |  |
| `pl.select` | `golars.Select` | done |  |
| `pl.self_dtype` |  | todo |  |
| `pl.sql` | `golars.SQL` | done |  |
| `pl.sql_expr` |  | todo |  |
| `pl.std` | `golars.Std` | done |  |
| `pl.struct` | `golars.Struct` | done |  |
| `pl.struct_with_fields` |  | todo |  |
| `pl.sum` | `golars.Sum` | done |  |
| `pl.sum_horizontal` | `golars.SumHorizontal` | done |  |
| `pl.tail` | `golars.Tail` | done |  |
| `pl.time` | `expr.LitTime` | partial | literal only; no expression-built time |
| `pl.time_range` | `golars.TimeRange` | done |  |
| `pl.time_ranges` |  | todo |  |
| `pl.union` | `golars.Concat` | partial | vertical concat; polars union does not keep order |
| `pl.var` | `golars.Var` | done |  |
| `pl.when` | `golars.When` | done |  |
| `pl.zeros` | `golars.Zeros` | done |  |

## Intentional differences

These are deliberate, checked against polars 1.39.3 and the golars
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

## Known gaps

Every `todo` row from the tables above, grouped by surface.

- **Expr**: `append`, `ewm_mean_by`, `rolling`, `rolling_rank_by`, `truncate`
- **Expr.str**: `normalize`, `to_decimal`
- **Expr.dt**: `base_utc_offset`, `dst_offset`
- **Expr.list**: `agg`
- **Expr.arr**: `agg`, `eval`
- **DataFrame**: `drop_in_place`, `flags`, `interpolate`, `remove`, `set_sorted`, `write_avro`, `write_database`, `write_excel`, `write_iceberg`
- **LazyFrame**: `interpolate`, `remove`, `set_sorted`, `show`, `sink_delta`, `sink_iceberg`
- **GroupBy**: `having`
- **LazyGroupBy**: `having`
- **Series**: `cumulative_eval`, `estimated_size`, `ewm_mean_by`, `flags`, `is_in`, `is_not_nan`, `rolling_rank_by`, `set_sorted`, `sql`, `to_dummies`, `truncate`
- **Series.str**: `normalize`, `to_decimal`
- **Series.dt**: `base_utc_offset`, `dst_offset`, `max`, `min`
- **Series.list**: `agg`, `eval`
- **Series.arr**: `agg`, `eval`
- **Series.struct**: `schema`
- **Data types**: `Decimal`, `Float16`, `Int128`, `UInt128`
- **Top-level functions**: `align_frames`, `business_day_count`, `date_ranges`, `datetime_ranges`, `dtype_of`, `explain_all`, `from_epoch`, `from_repr`, `json_normalize`, `linear_spaces`, `read_avro`, `read_database_uri`, `read_delta`, `read_excel`, `read_ipc_schema`, `read_lines`, `read_ods`, `scan_delta`, `scan_iceberg`, `scan_lines`, `self_dtype`, `sql_expr`, `struct_with_fields`, `time_ranges`

Partial rows, where golars covers a narrower form:

- `meta`: package functions, not a namespace: OutputName, ReferencedColumns, IsMultiColumn, Equal
- `df.join`: single key; inner, left and cross only (no right, full, semi, anti)
- `df.select`: takes column names; use golars.SelectExpr for expressions
- `df.with_columns`: takes Series; use golars.WithColumnsExpr for expressions
- `lf.join`: single key; inner, left and cross only (no right, full, semi, anti)
- `pl.cum_count`: method only: Col(x).CumCount()
- `pl.cum_sum`: horizontal form; for one column use Col(x).CumSum()
- `pl.from_dicts`: reverse direction only; build frames with FromMap
- `pl.from_records`: takes an arrow RecordBatch, not Python rows
- `pl.implode`: method only: Col(x).Implode()
- `pl.row_index`: frame method only: df.WithRowIndex / lf.WithRowIndex
- `pl.time`: literal only; no expression-built time
- `pl.union`: vertical concat; polars union does not keep order

## Not applicable

Excluded from the totals because they only make sense in Python.

| Surface | polars | Reason |
|---|---|---|
| Expr | `deserialize` | Python plan serialization format |
| Expr | `ext` | unstable extension-type API |
| Expr | `inspect` | Python print debugging |
| DataFrame | `df.deserialize` | Python plan serialization format |
| DataFrame | `df.map_columns` | Python UDF |
| DataFrame | `df.plot` | Python plotting backend |
| DataFrame | `df.serialize` | Python plan serialization format |
| DataFrame | `df.shrink_to_fit` | memory tuning hint; arrow buffers are sized on build |
| DataFrame | `df.style` | Python great_tables styling |
| DataFrame | `df.to_init_repr` | Python repr round-trip |
| DataFrame | `df.to_jax` | no jax in Go |
| DataFrame | `df.to_pandas` | no pandas in Go |
| DataFrame | `df.to_torch` | no torch in Go |
| LazyFrame | `lf.deserialize` | Python plan serialization format |
| LazyFrame | `lf.lazy` | identity on a LazyFrame |
| LazyFrame | `lf.pipe_with_schema` | Python callable chaining |
| LazyFrame | `lf.remote` | Polars Cloud |
| LazyFrame | `lf.serialize` | Python plan serialization format |
| LazyFrame | `lf.sink_batches` | Python callback per batch |
| Series | `s.ext` | unstable extension-type API |
| Series | `s.map_elements` | Python UDF |
| Series | `s.plot` | Python plotting backend |
| Series | `s.shrink_to_fit` | memory tuning hint; arrow buffers are sized on build |
| Series | `s.to_init_repr` | Python repr round-trip |
| Series | `s.to_jax` | no jax in Go |
| Series | `s.to_pandas` | no pandas in Go |
| Series | `s.to_torch` | no torch in Go |
| Series.cat | `s.cat.to_local` | golars has no global string cache |
| Data types | `pl.BaseExtension` | unstable extension-type API |
| Data types | `pl.Extension` | unstable extension-type API |
| Data types | `pl.Object` | polars escape hatch for Python objects |
| Data types | `pl.Unknown` | polars-internal placeholder |
| Top-level functions | `pl.build_info` | Python build metadata |
| Top-level functions | `pl.collect_all_async` | Python asyncio |
| Top-level functions | `pl.defer` | Python callable source |
| Top-level functions | `pl.disable_string_cache` | golars has no global string cache |
| Top-level functions | `pl.enable_string_cache` | golars has no global string cache |
| Top-level functions | `pl.from_numpy` | no numpy in Go |
| Top-level functions | `pl.from_pandas` | no pandas in Go |
| Top-level functions | `pl.from_torch` | no torch in Go |
| Top-level functions | `pl.get_extension_type` | unstable extension-type API |
| Top-level functions | `pl.get_index_type` | golars indices are Go int; counts are u32 like polars |
| Top-level functions | `pl.map_batches` | Python UDF |
| Top-level functions | `pl.map_groups` | Python UDF |
| Top-level functions | `pl.register_extension_type` | unstable extension-type API |
| Top-level functions | `pl.scan_pyarrow_dataset` | pyarrow-specific |
| Top-level functions | `pl.set_random_seed` | golars sampling takes an explicit seed argument |
| Top-level functions | `pl.show_versions` | Python environment report |
| Top-level functions | `pl.thread_pool_size` | Go runtime schedules goroutines; see compute.WithParallelism |
| Top-level functions | `pl.unregister_extension_type` | unstable extension-type API |
| Top-level functions | `pl.using_string_cache` | golars has no global string cache |
| Top-level functions | `pl.wrap_df` | internal Python helper |
| Top-level functions | `pl.wrap_s` | internal Python helper |

See [roadmap.md](roadmap.md) for the perf side of the same picture and
[`bench/polars-compare/`](../bench/polars-compare/) for throughput
ratios vs polars.
