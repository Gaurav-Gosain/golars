package dataframe_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Mirrors the InnerJoinStr, InnerJoin and LeftJoin workloads in
// cmd/bench so kernel work can be measured with go test -bench.

func joinBenchInts(n int, seed uint64) []int64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int64N(1 << 20)
	}
	return out
}

func joinBenchFrame(b *testing.B, key *series.Series, valName string, seed uint64) *dataframe.DataFrame {
	b.Helper()
	v, _ := series.FromInt64(valName, joinBenchInts(key.Len(), seed), nil)
	df, err := dataframe.New(key, v)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func BenchmarkJoinStrInner(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 18, 1 << 20} {
		lk := make([]string, n)
		rk := make([]string, n)
		for i := range n {
			lk[i] = fmt.Sprintf("k%07d", i)
			rk[i] = fmt.Sprintf("k%07d", (i*7919)%n)
		}
		ls, _ := series.FromString("k", lk, nil)
		rs, _ := series.FromString("k", rk, nil)
		left := joinBenchFrame(b, ls, "lv", 42)
		right := joinBenchFrame(b, rs, "rv", 43)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
		left.Release()
		right.Release()
	}
}

func BenchmarkJoinIntInnerLeft(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1 << 14, 1 << 18} {
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(i)
		}
		r := rand.New(rand.NewPCG(42, 100))
		pool := make([]int64, 2*n)
		for i := range pool {
			pool[i] = int64(i)
		}
		r.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

		lk, _ := series.FromInt64("id", ids, nil)
		rk, _ := series.FromInt64("id", ids, nil)
		rk2, _ := series.FromInt64("id", pool[:n], nil)
		left := joinBenchFrame(b, lk, "lv", 42)
		right := joinBenchFrame(b, rk, "rv", 43)
		rightSub := joinBenchFrame(b, rk2, "rv", 43)
		b.Run(fmt.Sprintf("inner/rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
		b.Run(fmt.Sprintf("left/rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := left.Join(ctx, rightSub, []string{"id"}, dataframe.LeftJoin)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
		left.Release()
		right.Release()
		rightSub.Release()
	}
}
