//! Join workloads beyond inner and left joins. cmd/bench/joins_extra.go
//! and bench/polars-compare/bench.py build the same frames:
//!
//! - FullJoin: left keys 0..n-1, right keys a permutation of n/2..3n/2-1.
//! - SemiJoin, AntiJoin: left keys uniform in [0, 2n), right keys uniform
//!   in [0, n) with duplicates.
//! - InnerJoin2Key: keys (i mod 1024, i / 1024) and a permutation of them.

use super::{add, eager_flags, mbps, rng, time_ns, Result};
use polars::prelude::*;
use rand::distributions::Uniform;
use rand::prelude::*;

const JOIN_ROWS: usize = 1_048_576;

fn ri64(n: usize, seed: u64, hi: i64) -> Vec<i64> {
    let mut r = rng(seed);
    let d = Uniform::new(0i64, hi);
    (0..n).map(|_| r.sample(d)).collect()
}

fn perm(n: usize) -> Vec<i64> {
    (0..n).map(|i| ((i * 7919) % n) as i64).collect()
}

fn run_join(
    name: &str,
    n: usize,
    bytes_in: usize,
    left: DataFrame,
    right: DataFrame,
    on: &[&str],
    how: JoinType,
) -> Result {
    let keys: Vec<Expr> = on.iter().map(|k| col(*k)).collect();
    let t = time_ns(|| {
        let _ = left
            .clone()
            .lazy()
            .with_optimizations(eager_flags())
            .join(
                right.clone().lazy().with_optimizations(eager_flags()),
                keys.clone(),
                keys.clone(),
                JoinArgs::new(how.clone()),
            )
            .collect()
            .unwrap();
    });
    Result {
        name: name.into(),
        rows: n,
        median_ns: t,
        throughput_mbps: mbps(bytes_in, t),
    }
}

fn bench_full_join(n: usize) -> Result {
    let lk: Vec<i64> = (0..n as i64).collect();
    let rk: Vec<i64> = perm(n).into_iter().map(|v| v + (n / 2) as i64).collect();
    let left = df!("id" => lk, "lv" => ri64(n, 42, 1 << 20)).unwrap();
    let right = df!("id" => rk, "rv" => ri64(n, 43, 1 << 20)).unwrap();
    run_join("FullJoin", n, n * 32, left, right, &["id"], JoinType::Full)
}

fn semi_anti_frames(n: usize) -> (DataFrame, DataFrame) {
    let left = df!("id" => ri64(n, 44, 2 * n as i64), "lv" => ri64(n, 42, 1 << 20)).unwrap();
    let right = df!("id" => ri64(n, 45, n as i64)).unwrap();
    (left, right)
}

fn bench_semi_join(n: usize) -> Result {
    let (left, right) = semi_anti_frames(n);
    run_join("SemiJoin", n, n * 24, left, right, &["id"], JoinType::Semi)
}

fn bench_anti_join(n: usize) -> Result {
    let (left, right) = semi_anti_frames(n);
    run_join("AntiJoin", n, n * 24, left, right, &["id"], JoinType::Anti)
}

fn bench_inner_join_2key(n: usize) -> Result {
    let la: Vec<i64> = (0..n as i64).map(|i| i % 1024).collect();
    let lb: Vec<i64> = (0..n as i64).map(|i| i / 1024).collect();
    let p = perm(n);
    let ra: Vec<i64> = p.iter().map(|v| v % 1024).collect();
    let rb: Vec<i64> = p.iter().map(|v| v / 1024).collect();
    let left = df!("a" => la, "b" => lb, "lv" => ri64(n, 42, 1 << 20)).unwrap();
    let right = df!("a" => ra, "b" => rb, "rv" => ri64(n, 43, 1 << 20)).unwrap();
    run_join("InnerJoin2Key", n, n * 48, left, right, &["a", "b"], JoinType::Inner)
}

pub(crate) fn add_join_workloads(out: &mut Vec<Result>, only: &Option<regex::Regex>) {
    let n = JOIN_ROWS;
    add(out, only, "FullJoin", || bench_full_join(n));
    add(out, only, "SemiJoin", || bench_semi_join(n));
    add(out, only, "AntiJoin", || bench_anti_join(n));
    add(out, only, "InnerJoin2Key", || bench_inner_join_2key(n));
}
