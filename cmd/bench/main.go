// Command bench runs the same workloads the polars harness runs and emits
// JSON with matching schema. Used to compare golars against polars
// side-by-side.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"time"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

type result struct {
	Name           string  `json:"name"`
	Rows           int     `json:"rows"`
	MedianNs       int64   `json:"median_ns"`
	ThroughputMBps float64 `json:"throughput_mbps"`
}

func timeNs(fn func(), warmup, repeat int) int64 {
	// Match the Python polars harness's intent (3 warmup, 25 measured)
	// but use testing.B-style bulk timing: each sample runs the target
	// many times so the time.Now() / time.Since() overhead is
	// amortized. For sub-microsecond operations the previous
	// per-iteration timing was up to 4x pessimistic on small inputs.
	//
	// Variance reduction pass: target 5 ms per sample instead of 1 ms.
	// Cuts timer-quantisation + OS-interruption noise ~5x for small-N
	// workloads where the difference between 0.98x and 1.02x is all
	// jitter. Total bench-suite wall time grows proportionally; worth
	// the cost since ratios stabilise visibly.
	_ = warmup
	_ = repeat
	for range 5 {
		fn()
	}
	t0 := time.Now()
	fn()
	oneNs := max(time.Since(t0).Nanoseconds(), 1)
	// Aim for 5 ms per sample. Cap at 1M inner iters so absurdly fast
	// ops don't spin for seconds.
	const sampleTargetNs = 5_000_000
	inner := min(max(int(sampleTargetNs/oneNs), 1), 1_000_000)

	const iter = 25
	samples := make([]int64, iter)
	for i := range samples {
		t0 := time.Now()
		for range inner {
			fn()
		}
		samples[i] = time.Since(t0).Nanoseconds() / int64(inner)
	}
	slices.Sort(samples)
	return samples[len(samples)/2]
}

func randInt64s(n int, seed uint64, bound int64) []int64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int64N(bound)
	}
	return out
}

func benchSumInt64(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("a", vals, nil)
	defer s.Release()
	fn := func() {
		_, err := compute.SumInt64(ctx, s)
		if err != nil {
			panic(err)
		}
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SumInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchAddInt64(ctx context.Context, rows int) result {
	a := randInt64s(rows, 42, 1<<20)
	b := randInt64s(rows, 43, 1<<20)
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	defer sa.Release()
	defer sb.Release()
	fn := func() {
		out, err := compute.Add(ctx, sa, sb)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "AddInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchFilterInt64(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("a", vals, nil)
	defer s.Release()
	threshold, _ := compute.Gt(ctx, s, mustLit(rows))
	defer threshold.Release()
	fn := func() {
		out, err := compute.Filter(ctx, s, threshold)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "FilterInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

// mustLit builds a broadcast Int64 Series filled with 1<<19 for the filter
// benchmark (mirrors the polars `pl.col("a") > (1 << 19)` predicate).
func mustLit(rows int) *series.Series {
	vals := make([]int64, rows)
	for i := range vals {
		vals[i] = 1 << 19
	}
	s, _ := series.FromInt64("t", vals, nil)
	return s
}

func benchSortInt64(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("a", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := compute.Sort(ctx, s, compute.SortOptions{})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SortInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchGroupBySum(ctx context.Context, rows, groups int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(int64(groups))
	}
	vals := randInt64s(rows, 44, 1<<20)
	kSeries, _ := series.FromInt64("k", keys, nil)
	vSeries, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(kSeries, vSeries)
	defer df.Release()

	aggs := []expr.Expr{expr.Col("v").Sum().Alias("s")}
	fn := func() {
		out, err := df.GroupBy("k").Agg(ctx, aggs)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: fmt.Sprintf("GroupBySum(groups=%d)", groups), Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

var benchRegions = []string{"n", "s", "e", "w", "ne", "nw", "se", "sw"}

func benchGroupBySumMultiKey(ctx context.Context, rows int) result {
	regions := make([]string, rows)
	years := make([]int64, rows)
	for i := range regions {
		regions[i] = benchRegions[i%len(benchRegions)]
		years[i] = 2020 + int64(i%5)
	}
	vals := randInt64s(rows, 44, 1<<20)
	rSeries, _ := series.FromString("region", regions, nil)
	ySeries, _ := series.FromInt64("year", years, nil)
	vSeries, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(rSeries, ySeries, vSeries)
	defer df.Release()

	aggs := []expr.Expr{expr.Col("v").Sum().Alias("s")}
	fn := func() {
		out, err := df.GroupBy("region", "year").Agg(ctx, aggs)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "GroupBySumMultiKey", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*24) / float64(t) * 1000.0}
}

func benchInnerJoin(ctx context.Context, rows int) result {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	lv := randInt64s(rows, 42, 1<<20)
	rv := randInt64s(rows, 43, 1<<20)
	lk, _ := series.FromInt64("id", ids, nil)
	lvs, _ := series.FromInt64("lv", lv, nil)
	left, _ := dataframe.New(lk, lvs)
	defer left.Release()
	rk, _ := series.FromInt64("id", ids, nil)
	rvs, _ := series.FromInt64("rv", rv, nil)
	right, _ := dataframe.New(rk, rvs)
	defer right.Release()

	fn := func() {
		out, err := left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "InnerJoin", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func randFloat64s(n int, seed uint64) []float64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]float64, n)
	for i := range out {
		out[i] = r.Float64()
	}
	return out
}

func benchSumFloat64(ctx context.Context, rows int) result {
	vals := randFloat64s(rows, 42)
	s, _ := series.FromFloat64("a", vals, nil)
	defer s.Release()
	fn := func() {
		_, err := compute.SumFloat64(ctx, s)
		if err != nil {
			panic(err)
		}
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SumFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchMeanFloat64(ctx context.Context, rows int) result {
	vals := randFloat64s(rows, 42)
	s, _ := series.FromFloat64("a", vals, nil)
	defer s.Release()
	fn := func() {
		_, _, err := compute.MeanFloat64(ctx, s)
		if err != nil {
			panic(err)
		}
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "MeanFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchMinFloat64(ctx context.Context, rows int) result {
	vals := randFloat64s(rows, 42)
	s, _ := series.FromFloat64("a", vals, nil)
	defer s.Release()
	fn := func() {
		_, _, err := compute.MinFloat64(ctx, s)
		if err != nil {
			panic(err)
		}
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "MinFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchAddFloat64(ctx context.Context, rows int) result {
	a := randFloat64s(rows, 42)
	b := randFloat64s(rows, 43)
	sa, _ := series.FromFloat64("a", a, nil)
	sb, _ := series.FromFloat64("b", b, nil)
	defer sa.Release()
	defer sb.Release()
	fn := func() {
		out, err := compute.Add(ctx, sa, sb)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "AddFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchMulInt64(ctx context.Context, rows int) result {
	a := randInt64s(rows, 42, 1<<10)
	b := randInt64s(rows, 43, 1<<10)
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	defer sa.Release()
	defer sb.Release()
	fn := func() {
		out, err := compute.Mul(ctx, sa, sb)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "MulInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchGtInt64(ctx context.Context, rows int) result {
	a := randInt64s(rows, 42, 1<<20)
	b := randInt64s(rows, 43, 1<<20)
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	defer sa.Release()
	defer sb.Release()
	fn := func() {
		out, err := compute.Gt(ctx, sa, sb)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "GtInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchFilterFloat64(ctx context.Context, rows int) result {
	vals := randFloat64s(rows, 42)
	s, _ := series.FromFloat64("a", vals, nil)
	defer s.Release()
	mem := make([]float64, rows)
	for i := range mem {
		mem[i] = 0.5
	}
	thr, _ := series.FromFloat64("t", mem, nil)
	defer thr.Release()
	mask, _ := compute.Gt(ctx, s, thr)
	defer mask.Release()
	fn := func() {
		out, err := compute.Filter(ctx, s, mask)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "FilterFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchSortFloat64(ctx context.Context, rows int) result {
	vals := randFloat64s(rows, 42)
	s, _ := series.FromFloat64("a", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := compute.Sort(ctx, s, compute.SortOptions{})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SortFloat64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchSortTwoKeys(ctx context.Context, rows int) result {
	a := randInt64s(rows, 42, 1<<16)
	b := randInt64s(rows, 43, 1<<16)
	sa, _ := series.FromInt64("a", a, nil)
	sb, _ := series.FromInt64("b", b, nil)
	df, _ := dataframe.New(sa, sb)
	defer df.Release()

	fn := func() {
		out, err := df.SortBy(ctx, []string{"a", "b"},
			[]compute.SortOptions{{}, {}})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SortTwoKeys", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchGroupByMean(ctx context.Context, rows, groups int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(int64(groups))
	}
	vals := randFloat64s(rows, 44)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromFloat64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()

	aggs := []expr.Expr{expr.Col("v").Mean().Alias("s")}
	fn := func() {
		out, err := df.GroupBy("k").Agg(ctx, aggs)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: fmt.Sprintf("GroupByMean(groups=%d)", groups), Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchGroupByMultiAgg(ctx context.Context, rows, groups int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(int64(groups))
	}
	vals := randInt64s(rows, 44, 1<<20)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()

	aggs := []expr.Expr{
		expr.Col("v").Sum().Alias("s"),
		expr.Col("v").Mean().Alias("m"),
		expr.Col("v").Min().Alias("lo"),
		expr.Col("v").Max().Alias("hi"),
	}
	fn := func() {
		out, err := df.GroupBy("k").Agg(ctx, aggs)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: fmt.Sprintf("GroupByMultiAgg(groups=%d)", groups), Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchLeftJoin(ctx context.Context, rows int) result {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	// Match the rust polars harness: right keys are a random size-n subset
	// of 0..2n without replacement (shuffle-and-take). The prior
	// with-replacement form inflated output rows via duplicate matches,
	// which skewed the comparison in the rust harness's favor.
	r := rand.New(rand.NewPCG(42, 100))
	pool := make([]int64, 2*rows)
	for i := range pool {
		pool[i] = int64(i)
	}
	r.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	rightIDs := pool[:rows]
	lv := randInt64s(rows, 42, 1<<20)
	rv := randInt64s(rows, 43, 1<<20)
	lk, _ := series.FromInt64("id", ids, nil)
	lvs, _ := series.FromInt64("lv", lv, nil)
	left, _ := dataframe.New(lk, lvs)
	defer left.Release()
	rk, _ := series.FromInt64("id", rightIDs, nil)
	rvs, _ := series.FromInt64("rv", rv, nil)
	right, _ := dataframe.New(rk, rvs)
	defer right.Release()

	fn := func() {
		out, err := left.Join(ctx, right, []string{"id"}, dataframe.LeftJoin)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "LeftJoin", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchTake(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("a", vals, nil)
	defer s.Release()
	r := rand.New(rand.NewPCG(42, 43))
	// Random permutation of indices, half the rows.
	perm := r.Perm(rows)[:rows/2]
	fn := func() {
		out, err := compute.Take(ctx, s, perm)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "Take", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchCastI64F64(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("a", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := compute.Cast(ctx, s, dtype.Float64())
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "CastI64ToF64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchPipeline(ctx context.Context, rows int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(64)
	}
	vals := randInt64s(rows, 44, 1<<20)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()

	fn := func() {
		// Filter v > (1<<19), GroupBy k summing v, Sort by s desc.
		// Uses compute.GtLit to compare against a scalar, mirroring polars'
		// `pl.col("v") > (1 << 19)` lazy expression. Avoids a 2MB broadcast.
		vCol, _ := df.Column("v")
		mask, _ := compute.GtLit(ctx, vCol, int64(1<<19))
		filtered, _ := df.Filter(ctx, mask)
		mask.Release()
		grouped, _ := filtered.GroupBy("k").Agg(ctx,
			[]expr.Expr{expr.Col("v").Sum().Alias("s")})
		filtered.Release()
		sorted, _ := grouped.Sort(ctx, "s", true)
		grouped.Release()
		sorted.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "Pipeline(filter>gb>sort)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchRollingSum(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.RollingSum(series.RollingOptions{WindowSize: 32})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "RollingSum(w=32)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchRollingMin(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.RollingMin(series.RollingOptions{WindowSize: 32})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "RollingMin(w=32)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchRollingMax(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.RollingMax(series.RollingOptions{WindowSize: 32})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "RollingMax(w=32)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchRank(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.Rank(series.RankAverage)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "RankInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchTopK(ctx context.Context, rows, k int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.TopK(k)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: fmt.Sprintf("TopK(k=%d)", k), Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchRollingMean(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	s, _ := series.FromInt64("x", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.RollingMean(series.RollingOptions{WindowSize: 32})
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "RollingMean(w=32)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchWhenThen(ctx context.Context, rows int) result {
	vals := randInt64s(rows, 42, 1<<20)
	a, _ := series.FromInt64("a", vals, nil)
	b, _ := series.FromInt64("b", vals, nil)
	df, _ := dataframe.New(a, b)
	defer df.Release()
	fn := func() {
		out, err := lazy.FromDataFrame(df).
			Select(expr.When(expr.Col("a").Gt(expr.Lit(int64(1 << 19)))).
				Then(expr.Col("a")).
				Otherwise(expr.Col("b")).Alias("r")).
			Collect(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "WhenThenOtherwise", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchOverSum(ctx context.Context, rows int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(64)
	}
	vals := randInt64s(rows, 44, 1<<20)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()
	fn := func() {
		out, err := lazy.FromDataFrame(df).
			Select(expr.Col("v").Sum().Over("k").Alias("vs")).
			Collect(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SumOverGroup", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchOverCumSum(ctx context.Context, rows int) result {
	keys := make([]int64, rows)
	r := rand.New(rand.NewPCG(42, 43))
	for i := range keys {
		keys[i] = r.Int64N(64)
	}
	vals := randInt64s(rows, 44, 1<<20)
	k, _ := series.FromInt64("k", keys, nil)
	v, _ := series.FromInt64("v", vals, nil)
	df, _ := dataframe.New(k, v)
	defer df.Release()
	fn := func() {
		out, err := lazy.FromDataFrame(df).
			Select(expr.Col("v").CumSum().Over("k").Alias("cum")).
			Collect(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "CumSumOverGroup", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*16) / float64(t) * 1000.0}
}

func benchForwardFill(ctx context.Context, rows int) result {
	vals := make([]int64, rows)
	valid := make([]bool, rows)
	r := rand.New(rand.NewPCG(7, 8))
	for i := range vals {
		vals[i] = int64(i)
		// ~30% null
		valid[i] = r.Float64() >= 0.3
	}
	s, _ := series.FromInt64("x", vals, valid)
	defer s.Release()
	fn := func() {
		out, err := s.ForwardFill(0)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "ForwardFillInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchSumHorizontal(ctx context.Context, rows int) result {
	a, _ := series.FromInt64("a", randInt64s(rows, 42, 1<<20), nil)
	b, _ := series.FromInt64("b", randInt64s(rows, 43, 1<<20), nil)
	c, _ := series.FromInt64("c", randInt64s(rows, 44, 1<<20), nil)
	df, _ := dataframe.New(a, b, c)
	defer df.Release()
	fn := func() {
		out, err := df.SumHorizontal(ctx, dataframe.IgnoreNulls)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SumHorizontal(3cols)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*24) / float64(t) * 1000.0}
}

func benchMaxHorizontal(ctx context.Context, rows int) result {
	a, _ := series.FromInt64("a", randInt64s(rows, 42, 1<<20), nil)
	b, _ := series.FromInt64("b", randInt64s(rows, 43, 1<<20), nil)
	c, _ := series.FromInt64("c", randInt64s(rows, 44, 1<<20), nil)
	df, _ := dataframe.New(a, b, c)
	defer df.Release()
	fn := func() {
		out, err := df.MaxHorizontal(ctx, dataframe.IgnoreNulls)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "MaxHorizontal(3cols)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*24) / float64(t) * 1000.0}
}

func benchUniqueInt64(ctx context.Context, rows int) result {
	// ~25% distinct values so hashing hits dense collisions.
	vals := randInt64s(rows, 42, int64(rows/4))
	s, _ := series.FromInt64("a", vals, nil)
	df, _ := dataframe.New(s)
	defer df.Release()
	fn := func() {
		out, err := df.Unique(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "UniqueInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchCumSumInt64(ctx context.Context, rows int) result {
	s, _ := series.FromInt64("x", randInt64s(rows, 42, 1<<16), nil)
	defer s.Release()
	fn := func() {
		out, err := s.CumSum()
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "CumSumInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchShiftInt64(ctx context.Context, rows int) result {
	s, _ := series.FromInt64("x", randInt64s(rows, 42, 1<<20), nil)
	defer s.Release()
	fn := func() {
		out, err := s.Shift(1)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "ShiftInt64", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchFillNullValue(ctx context.Context, rows int) result {
	vals := make([]int64, rows)
	valid := make([]bool, rows)
	r := rand.New(rand.NewPCG(7, 8))
	for i := range vals {
		vals[i] = int64(i)
		valid[i] = r.Float64() >= 0.3
	}
	s, _ := series.FromInt64("x", vals, valid)
	df, _ := dataframe.New(s)
	defer df.Release()
	fn := func() {
		out, err := df.FillNull(int64(0))
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "FillNullValue", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchDropNulls(ctx context.Context, rows int) result {
	vals := make([]int64, rows)
	valid := make([]bool, rows)
	r := rand.New(rand.NewPCG(7, 8))
	for i := range vals {
		vals[i] = int64(i)
		valid[i] = r.Float64() >= 0.3
	}
	s, _ := series.FromInt64("x", vals, valid)
	df, _ := dataframe.New(s)
	defer df.Release()
	fn := func() {
		out, err := df.DropNulls(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "DropNulls", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func main() {
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile to this file")
	memProfile := flag.String("memprofile", "", "write allocation profile to this file")
	flag.Parse()

	// Tune GC a touch for the microbenchmark; polars does its own allocator
	// tuning so this is a fair comparison attempt, not a cheat.
	debug.SetGCPercent(200)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cpu profile:", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "cpu profile:", err)
			os.Exit(1)
		}
		// Defer order: Close runs after StopCPUProfile so the profile
		// is flushed before the file is closed.
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}
	if *memProfile != "" {
		defer func() {
			f, err := os.Create(*memProfile)
			if err != nil {
				fmt.Fprintln(os.Stderr, "mem profile:", err)
				return
			}
			runtime.GC()
			_ = pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	ctx := context.Background()
	var runs []result

	sizes := []int{16 * 1024, 256 * 1024, 1024 * 1024}
	for _, n := range sizes {
		runs = append(runs,
			benchSumInt64(ctx, n),
			benchSumFloat64(ctx, n),
			benchMeanFloat64(ctx, n),
			benchMinFloat64(ctx, n),
			benchAddInt64(ctx, n),
			benchAddFloat64(ctx, n),
			benchMulInt64(ctx, n),
			benchGtInt64(ctx, n),
			benchFilterInt64(ctx, n),
			benchFilterFloat64(ctx, n),
			benchSortInt64(ctx, n),
			benchSortFloat64(ctx, n),
			benchCastI64F64(ctx, n),
			benchTake(ctx, n),
		)
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs, benchSortTwoKeys(ctx, n))
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs,
			benchGroupBySum(ctx, n, 8),
			benchGroupBySum(ctx, n, 1024),
			benchGroupByMean(ctx, n, 64),
			benchGroupByMultiAgg(ctx, n, 64),
			benchGroupBySumMultiKey(ctx, n),
		)
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs,
			benchInnerJoin(ctx, n),
			benchLeftJoin(ctx, n),
		)
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs, benchPipeline(ctx, n))
	}
	for _, n := range sizes {
		runs = append(runs, benchSumHorizontal(ctx, n))
	}
	for _, n := range sizes {
		runs = append(runs, benchMaxHorizontal(ctx, n))
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs, benchUniqueInt64(ctx, n), benchTopK(ctx, n, 10), benchRank(ctx, n))
	}
	for _, n := range sizes {
		runs = append(runs,
			benchCumSumInt64(ctx, n),
			benchShiftInt64(ctx, n),
			benchFillNullValue(ctx, n),
			benchDropNulls(ctx, n),
		)
	}
	for _, n := range sizes {
		runs = append(runs, benchForwardFill(ctx, n))
	}
	for _, n := range sizes {
		runs = append(runs, benchRollingSum(ctx, n), benchRollingMin(ctx, n), benchRollingMax(ctx, n), benchRollingMean(ctx, n))
	}
	for _, n := range []int{16 * 1024, 256 * 1024} {
		runs = append(runs, benchWhenThen(ctx, n), benchOverSum(ctx, n), benchOverCumSum(ctx, n))
	}

	out := map[string]any{
		"engine":  "golars",
		"version": runtime.Version(),
		"runs":    runs,
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
