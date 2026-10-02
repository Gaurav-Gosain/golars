package main

// Workloads beyond the original numeric kernel suite: strings, CSV and
// Parquet IO, string-keyed group-bys and joins, wide aggregations and a
// lazy query pipeline. Each has a twin with the same name and size in
// bench/polars-compare/bench.py and bench/polars-rust/src/extra.rs.
//
// Data shapes shared by all three harnesses:
//
//	benchWord(i)   = words8[i%8] + "_" + hex6((i*2654435761) mod 2^24)
//	benchKey(i, c) = "key_" + pad7(((i*2654435761) mod 2^32) mod c)
//
// Random numeric columns come from each engine's own RNG with the same
// bounds, so values differ but distributions match.
//
// Throughput uses a nominal bytes-in figure: 8 per numeric cell and 16
// per string cell.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

const extraRows = 1024 * 1024

var words8 = [8]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}

func benchWords(n int) []string {
	out := make([]string, n)
	for i := range out {
		h := (uint64(i) * 2654435761) & 0xFFFFFF
		out[i] = fmt.Sprintf("%s_%06x", words8[i%8], h)
	}
	return out
}

func benchKeys(n, card int) []string {
	out := make([]string, n)
	for i := range out {
		h := (uint64(i) * 2654435761) & 0xFFFFFFFF
		out[i] = fmt.Sprintf("key_%07d", h%uint64(card))
	}
	return out
}

func mbpsOf(bytes int, ns int64) float64 { return float64(bytes) / float64(ns) * 1000.0 }

func mustDF(cols ...*series.Series) *dataframe.DataFrame {
	df, err := dataframe.New(cols...)
	if err != nil {
		panic(err)
	}
	return df
}

func mustI64(name string, v []int64) *series.Series {
	s, err := series.FromInt64(name, v, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func mustF64(name string, v []float64) *series.Series {
	s, err := series.FromFloat64(name, v, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func mustStr(name string, v []string) *series.Series {
	s, err := series.FromString(name, v, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func addExtraWorkloads(ctx context.Context, add func(string, func() result)) {
	n := extraRows
	add("CSVReadNumeric", func() result { return benchCSVReadNumeric(ctx, n) })
	add("CSVReadMixed", func() result { return benchCSVReadMixed(ctx, n) })
	add("ParquetRead", func() result { return benchParquetRead(ctx, n) })
	add("ParquetWrite", func() result { return benchParquetWrite(ctx, n) })
	add("GroupByStrKey(groups=16)", func() result { return benchGroupByStrKey(ctx, n, 16) })
	add("GroupByStrKey(groups=100000)", func() result { return benchGroupByStrKey(ctx, n, 100000) })
	add("StrContains", func() result { return benchStrContains(n) })
	add("StrToUppercase", func() result { return benchStrToUppercase(n) })
	add("StrLenBytes", func() result { return benchStrLenBytes(n) })
	add("StrLenChars", func() result { return benchStrLenChars(n) })
	add("InnerJoinStr", func() result { return benchInnerJoinStr(ctx, n) })
	add("GroupByManyAggs(groups=1024)", func() result { return benchGroupByManyAggs(ctx, n) })
	add("SortStr", func() result { return benchSortStr(ctx, n) })
	add("UniqueStr", func() result { return benchUniqueStr(ctx, n) })
	add("LazyPipeline", func() result { return benchLazyPipeline(ctx, n) })
}

// benchTempDir returns a fresh temp directory and a cleanup func.
func benchTempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "golars-bench-*")
	if err != nil {
		panic(err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }
}

func numericFrame(n int) *dataframe.DataFrame {
	return mustDF(
		mustI64("a", randInt64s(n, 42, 1_000_000)),
		mustI64("b", randInt64s(n, 43, 1_000_000_000)),
		mustF64("c", randFloat64s(n, 44)),
		mustF64("d", randFloat64s(n, 45)),
	)
}

func mixedFrame(n int) *dataframe.DataFrame {
	return mustDF(
		mustI64("a", randInt64s(n, 42, 1_000_000)),
		mustF64("b", randFloat64s(n, 44)),
		mustStr("s", benchWords(n)),
		mustStr("k", benchKeys(n, 1000)),
	)
}

func benchCSVRead(ctx context.Context, name string, df *dataframe.DataFrame, bytesIn int) result {
	dir, cleanup := benchTempDir()
	defer cleanup()
	path := filepath.Join(dir, "in.csv")
	if err := iocsv.WriteFile(ctx, path, df); err != nil {
		panic(err)
	}
	rows := df.Height()
	df.Release()
	fn := func() {
		out, err := iocsv.ReadFile(ctx, path)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: name, Rows: rows, MedianNs: t, ThroughputMBps: mbpsOf(bytesIn, t)}
}

func benchCSVReadNumeric(ctx context.Context, n int) result {
	return benchCSVRead(ctx, "CSVReadNumeric", numericFrame(n), n*32)
}

func benchCSVReadMixed(ctx context.Context, n int) result {
	return benchCSVRead(ctx, "CSVReadMixed", mixedFrame(n), n*48)
}

func parquetFrame(n int) *dataframe.DataFrame {
	return mustDF(
		mustI64("a", randInt64s(n, 42, 1_000_000)),
		mustI64("b", randInt64s(n, 43, 1_000_000_000)),
		mustF64("c", randFloat64s(n, 44)),
		mustStr("s", benchKeys(n, 1000)),
	)
}

func benchParquetRead(ctx context.Context, n int) result {
	dir, cleanup := benchTempDir()
	defer cleanup()
	path := filepath.Join(dir, "in.parquet")
	df := parquetFrame(n)
	if err := ioparquet.WriteFile(ctx, path, df); err != nil {
		panic(err)
	}
	df.Release()
	fn := func() {
		out, err := ioparquet.ReadFile(ctx, path)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "ParquetRead", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*40, t)}
}

func benchParquetWrite(ctx context.Context, n int) result {
	dir, cleanup := benchTempDir()
	defer cleanup()
	path := filepath.Join(dir, "out.parquet")
	df := parquetFrame(n)
	defer df.Release()
	fn := func() {
		if err := ioparquet.WriteFile(ctx, path, df); err != nil {
			panic(err)
		}
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "ParquetWrite", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*40, t)}
}

func benchGroupByStrKey(ctx context.Context, n, groups int) result {
	df := mustDF(
		mustStr("k", benchKeys(n, groups)),
		mustI64("v", randInt64s(n, 44, 1<<20)),
	)
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
	return result{Name: fmt.Sprintf("GroupByStrKey(groups=%d)", groups), Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*24, t)}
}

func benchStrUnary(name string, n int, op func(*series.Series) (*series.Series, error)) result {
	s := mustStr("s", benchWords(n))
	defer s.Release()
	fn := func() {
		out, err := op(s)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: name, Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*16, t)}
}

func benchStrContains(n int) result {
	return benchStrUnary("StrContains", n, func(s *series.Series) (*series.Series, error) {
		return s.Str().Contains("ta")
	})
}

func benchStrToUppercase(n int) result {
	return benchStrUnary("StrToUppercase", n, func(s *series.Series) (*series.Series, error) {
		return s.Str().ToUppercase()
	})
}

func benchStrLenBytes(n int) result {
	return benchStrUnary("StrLenBytes", n, func(s *series.Series) (*series.Series, error) {
		return s.Str().LenBytes()
	})
}

func benchStrLenChars(n int) result {
	return benchStrUnary("StrLenChars", n, func(s *series.Series) (*series.Series, error) {
		return s.Str().LenChars()
	})
}

func benchInnerJoinStr(ctx context.Context, n int) result {
	lk := make([]string, n)
	rk := make([]string, n)
	for i := range n {
		lk[i] = fmt.Sprintf("k%07d", i)
		rk[i] = fmt.Sprintf("k%07d", (i*7919)%n)
	}
	left := mustDF(mustStr("k", lk), mustI64("lv", randInt64s(n, 42, 1<<20)))
	defer left.Release()
	right := mustDF(mustStr("k", rk), mustI64("rv", randInt64s(n, 43, 1<<20)))
	defer right.Release()
	fn := func() {
		out, err := left.Join(ctx, right, []string{"k"}, dataframe.InnerJoin)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "InnerJoinStr", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*24, t)}
}

func benchGroupByManyAggs(ctx context.Context, n int) result {
	df := mustDF(
		mustI64("k", randInt64s(n, 41, 1024)),
		mustI64("v", randInt64s(n, 44, 1<<20)),
		mustF64("w", randFloat64s(n, 45)),
	)
	defer df.Release()
	aggs := []expr.Expr{
		expr.Col("v").Sum().Alias("v_sum"),
		expr.Col("v").Mean().Alias("v_mean"),
		expr.Col("v").Min().Alias("v_min"),
		expr.Col("v").Max().Alias("v_max"),
		expr.Col("v").Count().Alias("v_count"),
		expr.Col("w").First().Alias("w_first"),
		expr.Col("w").Last().Alias("w_last"),
		expr.Col("w").Mean().Alias("w_mean"),
		expr.Col("w").Min().Alias("w_min"),
		expr.Col("w").Max().Alias("w_max"),
	}
	fn := func() {
		out, err := df.GroupBy("k").Agg(ctx, aggs)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "GroupByManyAggs(groups=1024)", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*24, t)}
}

func benchSortStr(ctx context.Context, n int) result {
	df := mustDF(mustStr("s", benchWords(n)))
	defer df.Release()
	fn := func() {
		out, err := df.Sort(ctx, "s", false)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "SortStr", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*16, t)}
}

func benchUniqueStr(ctx context.Context, n int) result {
	df := mustDF(mustStr("k", benchKeys(n, n/4)))
	defer df.Release()
	fn := func() {
		out, err := df.Unique(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "UniqueStr", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*16, t)}
}

// benchLazyPipeline: filter + with_columns + group_by + sort through the
// lazy planner, the shape of a typical analytic query.
func benchLazyPipeline(ctx context.Context, n int) result {
	df := mustDF(
		mustStr("s", benchKeys(n, 16)),
		mustI64("a", randInt64s(n, 42, 1_000_000)),
		mustF64("b", randFloat64s(n, 44)),
	)
	defer df.Release()
	fn := func() {
		out, err := lazy.FromDataFrame(df).
			Filter(expr.Col("a").Gt(expr.Lit(int64(500_000)))).
			WithColumns(
				expr.Col("a").Mul(expr.Lit(int64(3))).Alias("a3"),
				expr.Col("b").Mul(expr.Lit(2.0)).Alias("b2"),
			).
			GroupBy("s").
			Agg(
				expr.Col("a3").Sum().Alias("sa"),
				expr.Col("b2").Mean().Alias("mb"),
				expr.Col("a").Count().Alias("n"),
			).
			Sort("sa", true).
			Collect(ctx)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "LazyPipeline", Rows: n, MedianNs: t, ThroughputMBps: mbpsOf(n*32, t)}
}
