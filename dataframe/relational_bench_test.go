package dataframe_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// The benchmarks in this file mirror the relational workloads of
// cmd/bench (same shapes and sizes) so allocation counts and bytes can
// be tracked with -benchmem.

func relInts(n int, seed uint64, bound int64) []int64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int64N(bound)
	}
	return out
}

func relFloats(n int, seed uint64) []float64 {
	r := rand.New(rand.NewPCG(seed, seed+1))
	out := make([]float64, n)
	for i := range out {
		out[i] = r.Float64()
	}
	return out
}

func relKeys(n, card int) []string {
	out := make([]string, n)
	for i := range out {
		h := (uint64(i) * 2654435761) & 0xFFFFFFFF
		out[i] = fmt.Sprintf("key_%07d", h%uint64(card))
	}
	return out
}

func relWords(n int) []string {
	words := [8]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	out := make([]string, n)
	for i := range out {
		h := (uint64(i) * 2654435761) & 0xFFFFFF
		out[i] = fmt.Sprintf("%s_%06x", words[i%8], h)
	}
	return out
}

func relDF(b *testing.B, cols ...*series.Series) *dataframe.DataFrame {
	b.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		b.Fatal(err)
	}
	return df
}

func relI64(name string, v []int64) *series.Series {
	s, _ := series.FromInt64(name, v, nil)
	return s
}

func relF64(name string, v []float64) *series.Series {
	s, _ := series.FromFloat64(name, v, nil)
	return s
}

func relStr(name string, v []string) *series.Series {
	s, _ := series.FromString(name, v, nil)
	return s
}

// relRun runs fn as a sub-benchmark. Besides wall time it reports the
// process CPU time per operation (cpu-ns/op), which stays comparable
// when other processes compete for cores.
func relRun(b *testing.B, name string, fn func() (*dataframe.DataFrame, error)) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		done := reportCPU(b)
		for b.Loop() {
			out, err := fn()
			if err != nil {
				b.Fatal(err)
			}
			out.Release()
		}
		done()
	})
}

const (
	rel16K  = 16 * 1024
	rel256K = 256 * 1024
	rel1M   = 1024 * 1024
)

func BenchmarkRelGroupBy(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{rel16K, rel256K} {
		for _, g := range []int{8, 1024} {
			df := relDF(b, relI64("k", relInts(n, 42, int64(g))), relI64("v", relInts(n, 44, 1<<20)))
			aggs := []expr.Expr{expr.Col("v").Sum().Alias("s")}
			relRun(b, fmt.Sprintf("Sum/g=%d/n=%d", g, n), func() (*dataframe.DataFrame, error) {
				return df.GroupBy("k").Agg(ctx, aggs)
			})
			df.Release()
		}
		df := relDF(b, relI64("k", relInts(n, 42, 64)), relI64("v", relInts(n, 44, 1<<20)), relF64("w", relFloats(n, 44)))
		mean := []expr.Expr{expr.Col("w").Mean().Alias("s")}
		relRun(b, fmt.Sprintf("Mean/g=64/n=%d", n), func() (*dataframe.DataFrame, error) {
			return df.GroupBy("k").Agg(ctx, mean)
		})
		multi := []expr.Expr{
			expr.Col("v").Sum().Alias("s"), expr.Col("v").Mean().Alias("m"),
			expr.Col("v").Min().Alias("lo"), expr.Col("v").Max().Alias("hi"),
		}
		relRun(b, fmt.Sprintf("MultiAgg/g=64/n=%d", n), func() (*dataframe.DataFrame, error) {
			return df.GroupBy("k").Agg(ctx, multi)
		})
		df.Release()

		regions := []string{"n", "s", "e", "w", "ne", "nw", "se", "sw"}
		rs := make([]string, n)
		ys := make([]int64, n)
		for i := range rs {
			rs[i] = regions[i%len(regions)]
			ys[i] = 2020 + int64(i%5)
		}
		mk := relDF(b, relStr("region", rs), relI64("year", ys), relI64("v", relInts(n, 44, 1<<20)))
		sum := []expr.Expr{expr.Col("v").Sum().Alias("s")}
		relRun(b, fmt.Sprintf("MultiKey/n=%d", n), func() (*dataframe.DataFrame, error) {
			return mk.GroupBy("region", "year").Agg(ctx, sum)
		})
		mk.Release()
	}
	for _, g := range []int{16, 100000} {
		df := relDF(b, relStr("k", relKeys(rel1M, g)), relI64("v", relInts(rel1M, 44, 1<<20)))
		sum := []expr.Expr{expr.Col("v").Sum().Alias("s")}
		relRun(b, fmt.Sprintf("StrKey/g=%d/n=%d", g, rel1M), func() (*dataframe.DataFrame, error) {
			return df.GroupBy("k").Agg(ctx, sum)
		})
		df.Release()
	}
	df := relDF(b, relI64("k", relInts(rel1M, 41, 1024)), relI64("v", relInts(rel1M, 44, 1<<20)), relF64("w", relFloats(rel1M, 45)))
	many := []expr.Expr{
		expr.Col("v").Sum().Alias("v_sum"), expr.Col("v").Mean().Alias("v_mean"),
		expr.Col("v").Min().Alias("v_min"), expr.Col("v").Max().Alias("v_max"),
		expr.Col("v").Count().Alias("v_count"), expr.Col("w").First().Alias("w_first"),
		expr.Col("w").Last().Alias("w_last"), expr.Col("w").Mean().Alias("w_mean"),
		expr.Col("w").Min().Alias("w_min"), expr.Col("w").Max().Alias("w_max"),
	}
	relRun(b, fmt.Sprintf("ManyAggs/g=1024/n=%d", rel1M), func() (*dataframe.DataFrame, error) {
		return df.GroupBy("k").Agg(ctx, many)
	})
	df.Release()
}

func BenchmarkRelJoin(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{rel16K, rel256K} {
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(i)
		}
		left := relDF(b, relI64("id", ids), relI64("lv", relInts(n, 42, 1<<20)))
		right := relDF(b, relI64("id", ids), relI64("rv", relInts(n, 43, 1<<20)))
		relRun(b, fmt.Sprintf("Inner/n=%d", n), func() (*dataframe.DataFrame, error) {
			return left.Join(ctx, right, []string{"id"}, dataframe.InnerJoin)
		})
		right.Release()

		r := rand.New(rand.NewPCG(42, 100))
		pool := make([]int64, 2*n)
		for i := range pool {
			pool[i] = int64(i)
		}
		r.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
		right = relDF(b, relI64("id", pool[:n]), relI64("rv", relInts(n, 43, 1<<20)))
		relRun(b, fmt.Sprintf("Left/n=%d", n), func() (*dataframe.DataFrame, error) {
			return left.Join(ctx, right, []string{"id"}, dataframe.LeftJoin)
		})
		right.Release()
		left.Release()
	}
	n := rel1M
	lk := make([]string, n)
	rk := make([]string, n)
	for i := range n {
		lk[i] = fmt.Sprintf("k%07d", i)
		rk[i] = fmt.Sprintf("k%07d", (i*7919)%n)
	}
	left := relDF(b, relStr("k", lk), relI64("lv", relInts(n, 42, 1<<20)))
	right := relDF(b, relStr("k", rk), relI64("rv", relInts(n, 43, 1<<20)))
	relRun(b, fmt.Sprintf("InnerStr/n=%d", n), func() (*dataframe.DataFrame, error) {
		return left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin)
	})
	left.Release()
	right.Release()
}

func relSeq(b *testing.B, n int, names []string, fs []func(int) int64) *dataframe.DataFrame {
	cols := make([]*series.Series, len(names))
	for c := range names {
		v := make([]int64, n)
		for i := range v {
			v[i] = fs[c](i)
		}
		cols[c] = relI64(names[c], v)
	}
	return relDF(b, cols...)
}

func BenchmarkRelJoinAsofWhere(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{rel16K, rel256K} {
		m := n / 4
		for _, by := range []bool{false, true} {
			ln := []string{"t", "lv"}
			lf := []func(int) int64{func(i int) int64 { return int64(i) * 3 }, func(i int) int64 { return int64(i) }}
			rn := []string{"t", "rv"}
			rf := []func(int) int64{func(j int) int64 { return int64(j)*11 + 1 }, func(j int) int64 { return int64(j) }}
			opts := dataframe.AsofOptions{On: "t"}
			name := "Asof"
			if by {
				ln = append(ln, "g")
				lf = append(lf, func(i int) int64 { return int64(i % 64) })
				rn = append(rn, "g")
				rf = append(rf, func(j int) int64 { return int64(j % 64) })
				opts.By = []string{"g"}
				name = "AsofBy"
			}
			left := relSeq(b, n, ln, lf)
			right := relSeq(b, m, rn, rf)
			relRun(b, fmt.Sprintf("%s/n=%d", name, n), func() (*dataframe.DataFrame, error) {
				return left.JoinAsof(ctx, right, opts)
			})
			left.Release()
			right.Release()
		}
		wm := n / 64
		left := relSeq(b, n, []string{"a", "lv"}, []func(int) int64{
			func(i int) int64 { return int64(i) * 7919 % int64(n) },
			func(i int) int64 { return int64(i) },
		})
		right := relSeq(b, wm, []string{"lo", "hi", "rv"}, []func(int) int64{
			func(j int) int64 { return int64(j) * 64 },
			func(j int) int64 { return int64(j)*64 + 96 },
			func(j int) int64 { return int64(j) },
		})
		preds := []expr.Expr{expr.Col("a").Ge(expr.Col("lo")), expr.Col("a").Lt(expr.Col("hi"))}
		relRun(b, fmt.Sprintf("Where/n=%d", n), func() (*dataframe.DataFrame, error) {
			return left.JoinWhere(ctx, right, preds)
		})
		left.Release()
		right.Release()
	}
}

func BenchmarkRelSortUnique(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{rel16K, rel256K} {
		df := relDF(b, relI64("a", relInts(n, 42, 1<<16)), relI64("b", relInts(n, 43, 1<<16)))
		relRun(b, fmt.Sprintf("SortTwoKeys/n=%d", n), func() (*dataframe.DataFrame, error) {
			return df.SortBy(ctx, []string{"a", "b"}, []compute.SortOptions{{}, {}})
		})
		df.Release()
		u := relDF(b, relI64("a", relInts(n, 42, int64(n/4))))
		relRun(b, fmt.Sprintf("UniqueInt64/n=%d", n), func() (*dataframe.DataFrame, error) {
			return u.Unique(ctx)
		})
		u.Release()
	}
	s := relDF(b, relStr("s", relWords(rel1M)))
	relRun(b, fmt.Sprintf("SortStr/n=%d", rel1M), func() (*dataframe.DataFrame, error) {
		return s.Sort(ctx, "s", false)
	})
	s.Release()
	u := relDF(b, relStr("k", relKeys(rel1M, rel1M/4)))
	relRun(b, fmt.Sprintf("UniqueStr/n=%d", rel1M), func() (*dataframe.DataFrame, error) {
		return u.Unique(ctx)
	})
	u.Release()
	for _, n := range []int{rel16K, rel256K, rel1M} {
		vals := make([]int64, n)
		valid := make([]bool, n)
		r := rand.New(rand.NewPCG(7, 8))
		for i := range vals {
			vals[i] = int64(i)
			valid[i] = r.Float64() >= 0.3
		}
		c, _ := series.FromInt64("x", vals, valid)
		df := relDF(b, c)
		relRun(b, fmt.Sprintf("DropNulls/n=%d", n), func() (*dataframe.DataFrame, error) {
			return df.DropNulls(ctx)
		})
		df.Release()
	}
}
