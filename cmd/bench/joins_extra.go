package main

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dataframe"
)

// Join workloads beyond the inner and left joins of the core suite.
// bench/polars-compare/bench.py and bench/polars-rust/src/joins_extra.rs
// build the same frames:
//
//   - FullJoin: left keys 0..n-1, right keys a permutation of n/2..3n/2-1,
//     so half of each side matches; one i64 payload per side.
//   - SemiJoin, AntiJoin: left keys uniform in [0, 2n), right keys
//     uniform in [0, n) with duplicates; one payload column on the left.
//   - InnerJoin2Key: keys (i mod 1024, i / 1024) on the left and a
//     permutation of them on the right; one payload per side.

// joinPerm returns (i*7919) mod n for i in [0, n): a permutation when n
// is not a multiple of 7919.
func joinPerm(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64((i * 7919) % n)
	}
	return out
}

func runJoinBench(ctx context.Context, name string, n, bytesIn int, left, right *dataframe.DataFrame, on []string, how dataframe.JoinType) result {
	defer left.Release()
	defer right.Release()
	fn := func() {
		out, err := left.Join(ctx, right, on, how)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: name, Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(bytesIn, t)}
}

func benchFullJoin(ctx context.Context, n int) result {
	lk := make([]int64, n)
	for i := range lk {
		lk[i] = int64(i)
	}
	rk := joinPerm(n)
	for i := range rk {
		rk[i] += int64(n / 2)
	}
	left := mustDF(mustI64("id", lk), mustI64("lv", randInt64s(n, 42, 1<<20)))
	right := mustDF(mustI64("id", rk), mustI64("rv", randInt64s(n, 43, 1<<20)))
	return runJoinBench(ctx, "FullJoin", n, n*32, left, right, []string{"id"}, dataframe.FullJoin)
}

func semiAntiFrames(n int) (*dataframe.DataFrame, *dataframe.DataFrame) {
	left := mustDF(mustI64("id", randInt64s(n, 44, int64(2*n))), mustI64("lv", randInt64s(n, 42, 1<<20)))
	right := mustDF(mustI64("id", randInt64s(n, 45, int64(n))))
	return left, right
}

func benchSemiJoin(ctx context.Context, n int) result {
	left, right := semiAntiFrames(n)
	return runJoinBench(ctx, "SemiJoin", n, n*24, left, right, []string{"id"}, dataframe.SemiJoin)
}

func benchAntiJoin(ctx context.Context, n int) result {
	left, right := semiAntiFrames(n)
	return runJoinBench(ctx, "AntiJoin", n, n*24, left, right, []string{"id"}, dataframe.AntiJoin)
}

func benchInnerJoin2Key(ctx context.Context, n int) result {
	la := make([]int64, n)
	lb := make([]int64, n)
	for i := range n {
		la[i], lb[i] = int64(i%1024), int64(i/1024)
	}
	perm := joinPerm(n)
	ra := make([]int64, n)
	rb := make([]int64, n)
	for i, p := range perm {
		ra[i], rb[i] = p%1024, p/1024
	}
	left := mustDF(mustI64("a", la), mustI64("b", lb), mustI64("lv", randInt64s(n, 42, 1<<20)))
	right := mustDF(mustI64("a", ra), mustI64("b", rb), mustI64("rv", randInt64s(n, 43, 1<<20)))
	return runJoinBench(ctx, "InnerJoin2Key", n, n*48, left, right, []string{"a", "b"}, dataframe.InnerJoin)
}

func addJoinWorkloads(ctx context.Context, add func(string, func() result)) {
	n := extraRows
	add("FullJoin", func() result { return benchFullJoin(ctx, n) })
	add("SemiJoin", func() result { return benchSemiJoin(ctx, n) })
	add("AntiJoin", func() result { return benchAntiJoin(ctx, n) })
	add("InnerJoin2Key", func() result { return benchInnerJoin2Key(ctx, n) })
}
