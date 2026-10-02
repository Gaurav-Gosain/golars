package main

import (
	"context"
	"strconv"

	"github.com/Gaurav-Gosain/golars/series"
)

// String-namespace workloads. The input matches bench.py's
// str_bench_values: "k{i%997}-v{i%13}-x{i}".

func strBenchValues(rows int) ([]string, int) {
	out := make([]string, rows)
	total := 0
	for i := range out {
		out[i] = "k" + strconv.Itoa(i%997) + "-v" + strconv.Itoa(i%13) + "-x" + strconv.Itoa(i)
		total += len(out[i])
	}
	return out, total
}

func benchStrContainsShort(ctx context.Context, rows int) result {
	vals, bytes := strBenchValues(rows)
	s, _ := series.FromString("s", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.Str().Contains("v7")
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "StrContainsShort", Rows: rows, MedianNs: t, ThroughputMBps: float64(bytes) / float64(t) * 1000.0}
}

func benchStrSplit(ctx context.Context, rows int) result {
	vals, bytes := strBenchValues(rows)
	s, _ := series.FromString("s", vals, nil)
	defer s.Release()
	fn := func() {
		out, err := s.Str().Split("-", false)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	_ = ctx
	return result{Name: "StrSplit", Rows: rows, MedianNs: t, ThroughputMBps: float64(bytes) / float64(t) * 1000.0}
}

// stringBenches runs the string workloads at 1M rows.
func stringBenches(ctx context.Context) []result {
	const n = 1024 * 1024
	return []result{benchStrContainsShort(ctx, n), benchStrSplit(ctx, n)}
}
