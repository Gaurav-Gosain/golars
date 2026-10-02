//! Workloads beyond the original numeric suite: strings, CSV and Parquet IO,
//! string-keyed group-bys and joins, wide aggregations and a lazy pipeline.
//! Twins live in cmd/bench/extra.go and bench/polars-compare/bench.py with
//! the same names, sizes and data shapes:
//!   word(i)   = WORDS8[i % 8] + "_" + hex6((i * 2654435761) mod 2^24)
//!   key(i, c) = "key_" + pad7(((i * 2654435761) mod 2^32) mod c)
//! Throughput uses a nominal bytes-in figure: 8 per numeric cell and 16 per
//! string cell.

use super::{add, eager_flags, mbps, rng, time_ns, Result};
use polars::prelude::*;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::fs::File;
use std::path::PathBuf;

const EXTRA_ROWS: usize = 1_048_576;
const WORDS8: [&str; 8] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
];

fn bench_words(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            let h = (i as u64).wrapping_mul(2654435761) & 0xFFFFFF;
            format!("{}_{:06x}", WORDS8[i % 8], h)
        })
        .collect()
}

fn bench_keys(n: usize, card: u64) -> Vec<String> {
    (0..n)
        .map(|i| {
            let h = (i as u64).wrapping_mul(2654435761) & 0xFFFF_FFFF;
            format!("key_{:07}", h % card)
        })
        .collect()
}

fn ri64(n: usize, seed: u64, hi: i64) -> Vec<i64> {
    let mut r = rng(seed);
    let d = Uniform::new(0i64, hi);
    (0..n).map(|_| r.sample(d)).collect()
}

fn rf64(n: usize, seed: u64) -> Vec<f64> {
    let mut r = rng(seed);
    (0..n).map(|_| r.gen::<f64>()).collect()
}

/// A fresh temp directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("polars-rs-bench-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn numeric_frame(n: usize) -> DataFrame {
    df!(
        "a" => ri64(n, 42, 1_000_000),
        "b" => ri64(n, 43, 1_000_000_000),
        "c" => rf64(n, 44),
        "d" => rf64(n, 45)
    )
    .unwrap()
}

fn mixed_frame(n: usize) -> DataFrame {
    df!(
        "a" => ri64(n, 42, 1_000_000),
        "b" => rf64(n, 44),
        "s" => bench_words(n),
        "k" => bench_keys(n, 1000)
    )
    .unwrap()
}

fn parquet_frame(n: usize) -> DataFrame {
    df!(
        "a" => ri64(n, 42, 1_000_000),
        "b" => ri64(n, 43, 1_000_000_000),
        "c" => rf64(n, 44),
        "s" => bench_keys(n, 1000)
    )
    .unwrap()
}

fn bench_csv_read(name: &str, mut df: DataFrame, bytes_in: usize) -> Result {
    let dir = TempDir::new();
    let path = dir.file("in.csv");
    let rows = df.height();
    {
        let mut f = File::create(&path).unwrap();
        CsvWriter::new(&mut f).finish(&mut df).unwrap();
    }
    drop(df);
    let t = time_ns(|| {
        let _ = CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(path.clone()))
            .unwrap()
            .finish()
            .unwrap();
    });
    Result {
        name: name.into(),
        rows,
        median_ns: t,
        throughput_mbps: mbps(bytes_in, t),
    }
}

fn bench_parquet_read(n: usize) -> Result {
    let dir = TempDir::new();
    let path = dir.file("in.parquet");
    {
        let mut df = parquet_frame(n);
        let f = File::create(&path).unwrap();
        ParquetWriter::new(f)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df)
            .unwrap();
    }
    let t = time_ns(|| {
        let f = File::open(&path).unwrap();
        let _ = ParquetReader::new(f).finish().unwrap();
    });
    Result {
        name: "ParquetRead".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 40, t),
    }
}

fn bench_parquet_write(n: usize) -> Result {
    let dir = TempDir::new();
    let path = dir.file("out.parquet");
    let mut df = parquet_frame(n);
    let t = time_ns(|| {
        let f = File::create(&path).unwrap();
        ParquetWriter::new(f)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df)
            .unwrap();
    });
    Result {
        name: "ParquetWrite".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 40, t),
    }
}

fn bench_groupby_str_key(n: usize, groups: u64) -> Result {
    let df = df!("k" => bench_keys(n, groups), "v" => ri64(n, 44, 1 << 20)).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .group_by([col("k")])
            .agg([col("v").sum().alias("s")])
            .collect()
            .unwrap();
    });
    Result {
        name: format!("GroupByStrKey(groups={})", groups),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_str_unary(name: &str, n: usize, e: Expr) -> Result {
    let df = df!("s" => bench_words(n)).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([e.clone()])
            .collect()
            .unwrap();
    });
    Result {
        name: name.into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_inner_join_str(n: usize) -> Result {
    let lk: Vec<String> = (0..n).map(|i| format!("k{:07}", i)).collect();
    let rk: Vec<String> = (0..n).map(|i| format!("k{:07}", (i * 7919) % n)).collect();
    let left = df!("k" => lk, "lv" => ri64(n, 42, 1 << 20)).unwrap();
    let right = df!("k" => rk, "rv" => ri64(n, 43, 1 << 20)).unwrap();
    let t = time_ns(|| {
        let _ = left
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .join(
                right.clone().lazy().with_optimizations(eager_flags()),
                [col("k")],
                [col("k")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: "InnerJoinStr".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_groupby_many_aggs(n: usize) -> Result {
    let df = df!(
        "k" => ri64(n, 41, 1024),
        "v" => ri64(n, 44, 1 << 20),
        "w" => rf64(n, 45)
    )
    .unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .group_by([col("k")])
            .agg([
                col("v").sum().alias("v_sum"),
                col("v").mean().alias("v_mean"),
                col("v").min().alias("v_min"),
                col("v").max().alias("v_max"),
                col("v").count().alias("v_count"),
                col("w").first().alias("w_first"),
                col("w").last().alias("w_last"),
                col("w").mean().alias("w_mean"),
                col("w").min().alias("w_min"),
                col("w").max().alias("w_max"),
            ])
            .collect()
            .unwrap();
    });
    Result {
        name: "GroupByManyAggs(groups=1024)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_sort_str(n: usize) -> Result {
    let df = df!("s" => bench_words(n)).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .sort(["s"], SortMultipleOptions::default())
            .collect()
            .unwrap();
    });
    Result {
        name: "SortStr".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_unique_str(n: usize) -> Result {
    let df = df!("k" => bench_keys(n, (n / 4) as u64)).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .unique::<&[&str], &str>(None, UniqueKeepStrategy::Any, None)
            .unwrap();
    });
    Result {
        name: "UniqueStr".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_lazy_pipeline(n: usize) -> Result {
    let df = df!(
        "s" => bench_keys(n, 16),
        "a" => ri64(n, 42, 1_000_000),
        "b" => rf64(n, 44)
    )
    .unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .filter(col("a").gt(lit(500_000i64)))
            .with_columns([
                (col("a") * lit(3i64)).alias("a3"),
                (col("b") * lit(2.0f64)).alias("b2"),
            ])
            .group_by([col("s")])
            .agg([
                col("a3").sum().alias("sa"),
                col("b2").mean().alias("mb"),
                col("a").count().alias("n"),
            ])
            .sort(
                ["sa"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: "LazyPipeline".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 32, t),
    }
}

pub(crate) fn add_extra_workloads(out: &mut Vec<Result>, only: &Option<regex::Regex>) {
    let n = EXTRA_ROWS;
    add(out, only, "CSVReadNumeric", || {
        bench_csv_read("CSVReadNumeric", numeric_frame(n), n * 32)
    });
    add(out, only, "CSVReadMixed", || {
        bench_csv_read("CSVReadMixed", mixed_frame(n), n * 48)
    });
    add(out, only, "ParquetRead", || bench_parquet_read(n));
    add(out, only, "ParquetWrite", || bench_parquet_write(n));
    add(out, only, "GroupByStrKey(groups=16)", || bench_groupby_str_key(n, 16));
    add(out, only, "GroupByStrKey(groups=100000)", || {
        bench_groupby_str_key(n, 100000)
    });
    add(out, only, "StrContains", || {
        bench_str_unary("StrContains", n, col("s").str().contains_literal(lit("ta")))
    });
    add(out, only, "StrToUppercase", || {
        bench_str_unary("StrToUppercase", n, col("s").str().to_uppercase())
    });
    add(out, only, "StrLenBytes", || {
        bench_str_unary("StrLenBytes", n, col("s").str().len_bytes())
    });
    add(out, only, "StrLenChars", || {
        bench_str_unary("StrLenChars", n, col("s").str().len_chars())
    });
    add(out, only, "InnerJoinStr", || bench_inner_join_str(n));
    add(out, only, "GroupByManyAggs(groups=1024)", || bench_groupby_many_aggs(n));
    add(out, only, "SortStr", || bench_sort_str(n));
    add(out, only, "UniqueStr", || bench_unique_str(n));
    add(out, only, "LazyPipeline", || bench_lazy_pipeline(n));
}
