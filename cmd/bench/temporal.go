package main

import (
	"context"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Temporal workloads. bench.py builds the same inputs with the same
// arithmetic so both engines see identical data.

// temporalTimestamps spreads rows over ten years of microsecond
// timestamps without randomness so Python and Go agree exactly.
func temporalTimestamps(rows int) []int64 {
	const base = int64(1_500_000_000_000_000)
	const span = int64(315_360_000_000_000)
	out := make([]int64, rows)
	for i := range out {
		out[i] = base + (int64(i)*7_919_000_003)%span
	}
	return out
}

func temporalSeries(rows int) *series.Series {
	s, err := series.FromDatetime("t", temporalTimestamps(rows), nil, dtype.Microsecond, "")
	if err != nil {
		panic(err)
	}
	return s
}

func benchDtYear(_ context.Context, rows int) result {
	s := temporalSeries(rows)
	defer s.Release()
	fn := func() {
		out, err := s.Dt().Year()
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "DtYear", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchDtTruncate(_ context.Context, rows int) result {
	s := temporalSeries(rows)
	defer s.Release()
	fn := func() {
		out, err := s.Dt().Truncate("1h")
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "DtTruncate(1h)", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

func benchStrptime(_ context.Context, rows int) result {
	ts := temporalSeries(rows)
	strs, err := ts.Dt().Strftime("%Y-%m-%d %H:%M:%S")
	ts.Release()
	if err != nil {
		panic(err)
	}
	defer strs.Release()
	opts := series.DefaultStrptimeOptions()
	fn := func() {
		out, err := strs.Str().ToDatetime("%Y-%m-%d %H:%M:%S", "", "", opts)
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "Strptime", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*19) / float64(t) * 1000.0}
}

func benchStrftime(_ context.Context, rows int) result {
	s := temporalSeries(rows)
	defer s.Release()
	fn := func() {
		out, err := s.Dt().Strftime("%Y-%m-%d %H:%M:%S")
		if err != nil {
			panic(err)
		}
		out.Release()
	}
	t := timeNs(fn, 1, 5)
	return result{Name: "Strftime", Rows: rows, MedianNs: t, ThroughputMBps: float64(rows*8) / float64(t) * 1000.0}
}

// temporalRuns returns every temporal workload across the standard sizes.
func temporalRuns(ctx context.Context, sizes []int) []result {
	var runs []result
	for _, n := range sizes {
		runs = append(runs,
			benchDtYear(ctx, n),
			benchDtTruncate(ctx, n),
			benchStrptime(ctx, n),
			benchStrftime(ctx, n),
		)
	}
	return runs
}
