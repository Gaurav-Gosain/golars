//! Direct Rust polars benchmarks, mirroring bench/polars-compare/bench.py.
//!
//! Output is JSON on stdout in the same shape as bench.py so compare4.py can
//! diff it alongside the python/scalar/simd runs.

use polars::lazy::frame::OptFlags;
use polars::prelude::*;
use polars_ops::series::{RankMethod, RankOptions};
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::Serialize;
use std::time::Instant;

const WARMUP: usize = 3;
const REPEAT: usize = 25;
const SEED: u64 = 42;

// Eager optimizer flags. Python polars' `df.select()` etc. internally call
// `self.lazy().select_seq(...).collect(optimizations=QueryOptFlags._eager())`
// which sets EAGER + SIMPLIFY_EXPR and retains TYPE_COERCION + TYPE_CHECK
// (the two flags with clear=false in optflags.rs). Everything else is
// cleared, so projection/predicate pushdown, CSE, slice pushdown etc. do not
// run. For single-op benchmarks this shaves tens of microseconds that
// otherwise dominate small-N timings.
fn eager_flags() -> OptFlags {
    OptFlags::TYPE_COERCION | OptFlags::TYPE_CHECK | OptFlags::EAGER | OptFlags::SIMPLIFY_EXPR
}

#[derive(Serialize)]
struct Result {
    name: String,
    rows: usize,
    median_ns: u128,
    throughput_mbps: f64,
}

fn time_ns<F: FnMut()>(mut f: F) -> u128 {
    for _ in 0..WARMUP {
        f();
    }
    let mut samples: Vec<u128> = Vec::with_capacity(REPEAT);
    for _ in 0..REPEAT {
        let t0 = Instant::now();
        f();
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    samples[REPEAT / 2]
}

fn mbps(bytes: usize, ns: u128) -> f64 {
    bytes as f64 / ns as f64 * 1000.0
}

fn rng(seed: u64) -> Pcg64 {
    Pcg64::seed_from_u64(seed)
}

fn rand_i64(n: usize, hi: i64) -> Vec<i64> {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, hi);
    (0..n).map(|_| r.sample(d)).collect()
}

fn rand_f64(n: usize) -> Vec<f64> {
    let mut r = rng(SEED);
    (0..n).map(|_| r.gen::<f64>()).collect()
}

// ---------- benchmarks ----------

fn bench_sum_int64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("a").sum()])
            .collect()
            .unwrap();
    });
    Result {
        name: "SumInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_sum_float64(n: usize) -> Result {
    let vals = rand_f64(n);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("a").sum()])
            .collect()
            .unwrap();
    });
    Result {
        name: "SumFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_mean_float64(n: usize) -> Result {
    let vals = rand_f64(n);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("a").mean()])
            .collect()
            .unwrap();
    });
    Result {
        name: "MeanFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_min_float64(n: usize) -> Result {
    let vals = rand_f64(n);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("a").min()])
            .collect()
            .unwrap();
    });
    Result {
        name: "MinFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_add_int64(n: usize) -> Result {
    let a = rand_i64(n, 1 << 20);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 20);
    let b: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .with_column((col("a") + col("b")).alias("s"))
            .collect()
            .unwrap();
    });
    Result {
        name: "AddInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_add_float64(n: usize) -> Result {
    let a = rand_f64(n);
    let mut r = rng(SEED ^ 1);
    let b: Vec<f64> = (0..n).map(|_| r.gen::<f64>()).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .with_column((col("a") + col("b")).alias("s"))
            .collect()
            .unwrap();
    });
    Result {
        name: "AddFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_mul_int64(n: usize) -> Result {
    let a = rand_i64(n, 1 << 10);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 10);
    let b: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .with_column((col("a") * col("b")).alias("s"))
            .collect()
            .unwrap();
    });
    Result {
        name: "MulInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_gt_int64(n: usize) -> Result {
    let a = rand_i64(n, 1 << 20);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 20);
    let b: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .with_column(col("a").gt(col("b")).alias("s"))
            .collect()
            .unwrap();
    });
    Result {
        name: "GtInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_filter_int64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .filter(col("a").gt(lit(1i64 << 19)))
            .collect()
            .unwrap();
    });
    Result {
        name: "FilterInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_filter_float64(n: usize) -> Result {
    let vals = rand_f64(n);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .filter(col("a").gt(lit(0.5f64)))
            .collect()
            .unwrap();
    });
    Result {
        name: "FilterFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_sort_int64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .sort(["a"], SortMultipleOptions::default())
            .collect()
            .unwrap();
    });
    Result {
        name: "SortInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_sort_float64(n: usize) -> Result {
    let vals = rand_f64(n);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .sort(["a"], SortMultipleOptions::default())
            .collect()
            .unwrap();
    });
    Result {
        name: "SortFloat64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_sort_two_keys(n: usize) -> Result {
    let a = rand_i64(n, 1 << 16);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 16);
    let b: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .sort(["a", "b"], SortMultipleOptions::default())
            .collect()
            .unwrap();
    });
    Result {
        name: "SortTwoKeys".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_cast_i64_f64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .with_column(col("a").cast(DataType::Float64).alias("b"))
            .collect()
            .unwrap();
    });
    Result {
        name: "CastI64ToF64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_take(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let mut perm: Vec<u32> = (0..n as u32).collect();
    let mut r = rng(SEED ^ 2);
    perm.shuffle(&mut r);
    let idx: Vec<u32> = perm.into_iter().take(n / 2).collect();
    let s = Series::new("a".into(), vals);
    let idx_s = Series::new("i".into(), idx);
    let t = time_ns(|| {
        let _ = s.take(idx_s.u32().unwrap()).unwrap();
    });
    Result {
        name: "Take".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_groupby_sum(n: usize, groups: i64) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, groups);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("k" => keys, "v" => vals).unwrap();
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
        name: format!("GroupBySum(groups={})", groups),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_groupby_sum_multikey(n: usize) -> Result {
    const REGIONS: [&str; 8] = ["n", "s", "e", "w", "ne", "nw", "se", "sw"];
    let regions: Vec<&str> = (0..n).map(|i| REGIONS[i % 8]).collect();
    let years: Vec<i64> = (0..n).map(|i| 2020 + (i % 5) as i64).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("region" => regions, "year" => years, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .group_by([col("region"), col("year")])
            .agg([col("v").sum().alias("s")])
            .collect()
            .unwrap();
    });
    Result {
        name: "GroupBySumMultiKey".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_groupby_mean(n: usize, groups: i64) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, groups);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_f64(n);
    let df = df!("k" => keys, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .group_by([col("k")])
            .agg([col("v").mean().alias("s")])
            .collect()
            .unwrap();
    });
    Result {
        name: format!("GroupByMean(groups={})", groups),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_groupby_multi_agg(n: usize, groups: i64) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, groups);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("k" => keys, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .group_by([col("k")])
            .agg([
                col("v").sum().alias("s"),
                col("v").mean().alias("m"),
                col("v").min().alias("lo"),
                col("v").max().alias("hi"),
            ])
            .collect()
            .unwrap();
    });
    Result {
        name: format!("GroupByMultiAgg(groups={})", groups),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_inner_join(n: usize) -> Result {
    let ids: Vec<i64> = (0..n as i64).collect();
    let lv = rand_i64(n, 1 << 20);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 20);
    let rv: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let left = df!("id" => ids.clone(), "lv" => lv).unwrap();
    let right = df!("id" => ids, "rv" => rv).unwrap();
    let t = time_ns(|| {
        let _ = left
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .join(
                right.clone().lazy().with_optimizations(eager_flags()),
                [col("id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: "InnerJoin".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_left_join(n: usize) -> Result {
    let ids: Vec<i64> = (0..n as i64).collect();
    // Right keys: random subset from 0..2n without replacement, size n.
    let mut pool: Vec<i64> = (0..2 * n as i64).collect();
    let mut rseed = rng(SEED ^ 3);
    pool.shuffle(&mut rseed);
    let right_ids: Vec<i64> = pool.into_iter().take(n).collect();
    let lv = rand_i64(n, 1 << 20);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 20);
    let rv: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let left = df!("id" => ids, "lv" => lv).unwrap();
    let right = df!("id" => right_ids, "rv" => rv).unwrap();
    let t = time_ns(|| {
        let _ = left
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .join(
                right.clone().lazy().with_optimizations(eager_flags()),
                [col("id")],
                [col("id")],
                JoinArgs::new(JoinType::Left),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: "LeftJoin".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_pipeline(n: usize) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, 64);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("k" => keys, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .filter(col("v").gt(lit(1i64 << 19)))
            .group_by([col("k")])
            .agg([col("v").sum().alias("s")])
            .sort(
                ["s"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: "Pipeline(filter>gb>sort)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_rolling_sum(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let opts = RollingOptionsFixedWindow {
        window_size: 32,
        min_periods: 32,
        weights: None,
        center: false,
        fn_params: None,
    };
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").rolling_sum(opts.clone())])
            .collect()
            .unwrap();
    });
    Result {
        name: "RollingSum(w=32)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_rolling_min(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let opts = RollingOptionsFixedWindow {
        window_size: 32,
        min_periods: 32,
        weights: None,
        center: false,
        fn_params: None,
    };
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").rolling_min(opts.clone())])
            .collect()
            .unwrap();
    });
    Result {
        name: "RollingMin(w=32)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_rolling_max(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let opts = RollingOptionsFixedWindow {
        window_size: 32,
        min_periods: 32,
        weights: None,
        center: false,
        fn_params: None,
    };
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").rolling_max(opts.clone())])
            .collect()
            .unwrap();
    });
    Result {
        name: "RollingMax(w=32)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_rolling_mean(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let opts = RollingOptionsFixedWindow {
        window_size: 32,
        min_periods: 32,
        weights: None,
        center: false,
        fn_params: None,
    };
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").rolling_mean(opts.clone())])
            .collect()
            .unwrap();
    });
    Result {
        name: "RollingMean(w=32)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_when_then(n: usize) -> Result {
    let a = rand_i64(n, 1 << 20);
    let mut r = rng(SEED ^ 1);
    let d = Uniform::new(0i64, 1 << 20);
    let b: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => a, "b" => b).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([when(col("a").gt(lit(1i64 << 19)))
                .then(col("a"))
                .otherwise(col("b"))
                .alias("r")])
            .collect()
            .unwrap();
    });
    Result {
        name: "WhenThenOtherwise".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_over_sum(n: usize) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, 64);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("k" => keys, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("v").sum().over([col("k")]).alias("vs")])
            .collect()
            .unwrap();
    });
    Result {
        name: "SumOverGroup".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_cumsum_over_group(n: usize) -> Result {
    let mut r = rng(SEED);
    let d = Uniform::new(0i64, 64);
    let keys: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let vals = rand_i64(n, 1 << 20);
    let df = df!("k" => keys, "v" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("v").cum_sum(false).over([col("k")]).alias("cum")])
            .collect()
            .unwrap();
    });
    Result {
        name: "CumSumOverGroup".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 16, t),
    }
}

fn bench_forward_fill(n: usize) -> Result {
    let mut r = rng(7);
    let vals: Vec<Option<i64>> = (0..n as i64)
        .map(|i| if r.gen::<f64>() < 0.3 { None } else { Some(i) })
        .collect();
    let s = Series::new("x".into(), vals);
    let df = DataFrame::new(n, vec![s.into()]).unwrap();
    let t = time_ns(|| {
        let _ = df.clone().fill_null(FillNullStrategy::Forward(None)).unwrap();
    });
    Result {
        name: "ForwardFillInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_sum_horizontal(n: usize) -> Result {
    let a = rand_i64(n, 1 << 20);
    let mut r1 = rng(SEED ^ 1);
    let mut r2 = rng(SEED ^ 2);
    let d = Uniform::new(0i64, 1 << 20);
    let b: Vec<i64> = (0..n).map(|_| r1.sample(d)).collect();
    let c: Vec<i64> = (0..n).map(|_| r2.sample(d)).collect();
    let df = df!("a" => a, "b" => b, "c" => c).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([(col("a") + col("b") + col("c")).alias("total")])
            .collect()
            .unwrap();
    });
    Result {
        name: "SumHorizontal(3cols)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_max_horizontal(n: usize) -> Result {
    let a = rand_i64(n, 1 << 20);
    let mut r1 = rng(SEED ^ 1);
    let mut r2 = rng(SEED ^ 2);
    let d = Uniform::new(0i64, 1 << 20);
    let b: Vec<i64> = (0..n).map(|_| r1.sample(d)).collect();
    let c: Vec<i64> = (0..n).map(|_| r2.sample(d)).collect();
    let df = df!("a" => a, "b" => b, "c" => c).unwrap();
    // `max_horizontal` isn't re-exported from polars::prelude in 0.53;
    // reproduce via two chained when/thens (semantically identical:
    // max(a,b,c) = if a>b then (if a>c then a else c) else (if b>c then b else c)).
    let ab = when(col("a").gt(col("b")))
        .then(col("a"))
        .otherwise(col("b"));
    let abc = when(ab.clone().gt(col("c"))).then(ab).otherwise(col("c"));
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([abc.clone().alias("m")])
            .collect()
            .unwrap();
    });
    Result {
        name: "MaxHorizontal(3cols)".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 24, t),
    }
}

fn bench_unique_int64(n: usize) -> Result {
    let mut r = rng(SEED);
    let hi = (n / 4) as i64;
    let d = Uniform::new(0i64, hi);
    let vals: Vec<i64> = (0..n).map(|_| r.sample(d)).collect();
    let df = df!("a" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .unique::<&[&str], &str>(None, UniqueKeepStrategy::Any, None)
            .unwrap();
    });
    Result {
        name: "UniqueInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_top_k(n: usize, k: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").top_k(lit(k as u32))])
            .collect()
            .unwrap();
    });
    Result {
        name: format!("TopK(k={})", k),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_rank(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").rank(
                RankOptions {
                    method: RankMethod::Average,
                    descending: false,
                },
                None,
            )])
            .collect()
            .unwrap();
    });
    Result {
        name: "RankInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_cumsum_int64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 16);
    let df = df!("x" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").cum_sum(false)])
            .collect()
            .unwrap();
    });
    Result {
        name: "CumSumInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_shift_int64(n: usize) -> Result {
    let vals = rand_i64(n, 1 << 20);
    let df = df!("x" => vals).unwrap();
    let t = time_ns(|| {
        let _ = df
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .select_seq([col("x").shift(lit(1i64))])
            .collect()
            .unwrap();
    });
    Result {
        name: "ShiftInt64".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_fill_null_value(n: usize) -> Result {
    let mut r = rng(7);
    let vals: Vec<Option<i64>> = (0..n as i64)
        .map(|i| if r.gen::<f64>() < 0.3 { None } else { Some(i) })
        .collect();
    let s = Series::new("x".into(), vals);
    let df = DataFrame::new(n, vec![s.into()]).unwrap();
    let t = time_ns(|| {
        let _ = df.clone().fill_null(FillNullStrategy::Zero).unwrap();
    });
    Result {
        name: "FillNullValue".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

fn bench_drop_nulls(n: usize) -> Result {
    let mut r = rng(7);
    let vals: Vec<Option<i64>> = (0..n as i64)
        .map(|i| if r.gen::<f64>() < 0.3 { None } else { Some(i) })
        .collect();
    let s = Series::new("x".into(), vals);
    let df = DataFrame::new(n, vec![s.into()]).unwrap();
    let t = time_ns(|| {
        let _ = df.clone().drop_nulls::<String>(None).unwrap();
    });
    Result {
        name: "DropNulls".into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(n * 8, t),
    }
}

// ---------- driver ----------

fn main() {
    let sizes = [16_384usize, 262_144, 1_048_576];
    let mut out: Vec<Result> = Vec::new();

    for &n in &sizes {
        out.push(bench_sum_int64(n));
        out.push(bench_sum_float64(n));
        out.push(bench_mean_float64(n));
        out.push(bench_min_float64(n));
        out.push(bench_add_int64(n));
        out.push(bench_add_float64(n));
        out.push(bench_mul_int64(n));
        out.push(bench_gt_int64(n));
        out.push(bench_filter_int64(n));
        out.push(bench_filter_float64(n));
        out.push(bench_sort_int64(n));
        out.push(bench_sort_float64(n));
        out.push(bench_cast_i64_f64(n));
        out.push(bench_take(n));
    }

    for &n in &[16_384usize, 262_144] {
        out.push(bench_sort_two_keys(n));
    }

    for &n in &[16_384usize, 262_144] {
        for &g in &[8i64, 1024] {
            out.push(bench_groupby_sum(n, g));
        }
        for &g in &[64i64] {
            out.push(bench_groupby_mean(n, g));
            out.push(bench_groupby_multi_agg(n, g));
        }
        out.push(bench_groupby_sum_multikey(n));
    }

    for &n in &[16_384usize, 262_144] {
        out.push(bench_inner_join(n));
        out.push(bench_left_join(n));
    }

    for &n in &[16_384usize, 262_144] {
        out.push(bench_pipeline(n));
    }

    for &n in &sizes {
        out.push(bench_sum_horizontal(n));
    }

    for &n in &sizes {
        out.push(bench_max_horizontal(n));
    }

    for &n in &[16_384usize, 262_144] {
        out.push(bench_unique_int64(n));
        out.push(bench_top_k(n, 10));
        out.push(bench_rank(n));
    }

    for &n in &sizes {
        out.push(bench_cumsum_int64(n));
        out.push(bench_shift_int64(n));
        out.push(bench_fill_null_value(n));
        out.push(bench_drop_nulls(n));
    }

    for &n in &sizes {
        out.push(bench_forward_fill(n));
    }

    for &n in &sizes {
        out.push(bench_rolling_sum(n));
        out.push(bench_rolling_min(n));
        out.push(bench_rolling_max(n));
        out.push(bench_rolling_mean(n));
    }

    for &n in &[16_384usize, 262_144] {
        out.push(bench_when_then(n));
        out.push(bench_over_sum(n));
        out.push(bench_cumsum_over_group(n));
    }

    #[derive(Serialize)]
    struct Envelope<'a> {
        engine: &'a str,
        version: &'a str,
        runs: &'a [Result],
    }
    // Pin the reported version to the polars crate version we link against.
    // Kept in sync with Cargo.toml; bump when the polars dep bumps.
    let env = Envelope {
        engine: "polars-rust",
        version: "0.53.0",
        runs: &out,
    };
    println!("{}", serde_json::to_string_pretty(&env).unwrap());
}
