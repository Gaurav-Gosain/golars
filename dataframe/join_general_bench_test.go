package dataframe_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func benchInts(n int, seed uint64, bound int64) []int64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int64N(bound)
	}
	return out
}

func benchFrame(b *testing.B, cols map[string][]int64, order ...string) *dataframe.DataFrame {
	b.Helper()
	ss := make([]*series.Series, len(order))
	for i, name := range order {
		ss[i], _ = series.FromInt64(name, cols[name], nil)
	}
	df, err := dataframe.New(ss...)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func runJoinBench(b *testing.B, left, right *dataframe.DataFrame, on []string, how dataframe.JoinType) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		out, err := left.Join(ctx, right, on, how)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

const joinBenchRows = 1 << 20

// These mirror the FullJoin, SemiJoin, AntiJoin and InnerJoin2Key
// workloads of cmd/bench/joins_extra.go.

func BenchmarkJoinSemi(b *testing.B) {
	n := joinBenchRows
	left := benchFrame(b, map[string][]int64{"id": benchInts(n, 44, int64(2*n)), "lv": benchInts(n, 42, 1<<20)}, "id", "lv")
	defer left.Release()
	right := benchFrame(b, map[string][]int64{"id": benchInts(n, 45, int64(n))}, "id")
	defer right.Release()
	runJoinBench(b, left, right, []string{"id"}, dataframe.SemiJoin)
}

func BenchmarkJoinAnti(b *testing.B) {
	n := joinBenchRows
	left := benchFrame(b, map[string][]int64{"id": benchInts(n, 44, int64(2*n)), "lv": benchInts(n, 42, 1<<20)}, "id", "lv")
	defer left.Release()
	right := benchFrame(b, map[string][]int64{"id": benchInts(n, 45, int64(n))}, "id")
	defer right.Release()
	runJoinBench(b, left, right, []string{"id"}, dataframe.AntiJoin)
}

func BenchmarkJoinFull(b *testing.B) {
	n := joinBenchRows
	lk := make([]int64, n)
	rk := make([]int64, n)
	for i := range n {
		lk[i] = int64(i)
		rk[i] = int64((i*7919)%n + n/2)
	}
	left := benchFrame(b, map[string][]int64{"id": lk, "lv": benchInts(n, 42, 1<<20)}, "id", "lv")
	defer left.Release()
	right := benchFrame(b, map[string][]int64{"id": rk, "rv": benchInts(n, 43, 1<<20)}, "id", "rv")
	defer right.Release()
	runJoinBench(b, left, right, []string{"id"}, dataframe.FullJoin)
}

func BenchmarkJoinInner2Key(b *testing.B) {
	n := joinBenchRows
	la, lb, ra, rb := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
	for i := range n {
		p := (i * 7919) % n
		la[i], lb[i] = int64(i%1024), int64(i/1024)
		ra[i], rb[i] = int64(p%1024), int64(p/1024)
	}
	left := benchFrame(b, map[string][]int64{"a": la, "b": lb, "lv": benchInts(n, 42, 1<<20)}, "a", "b", "lv")
	defer left.Release()
	right := benchFrame(b, map[string][]int64{"a": ra, "b": rb, "rv": benchInts(n, 43, 1<<20)}, "a", "b", "rv")
	defer right.Release()
	runJoinBench(b, left, right, []string{"a", "b"}, dataframe.InnerJoin)
}
