package series_test

import (
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

func argsortBenchSeries(n int) *series.Series {
	rng := rand.New(rand.NewPCG(42, 43))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = rng.Int64N(1 << 20)
	}
	s, _ := series.FromInt64("x", vals, nil)
	return s
}

// BenchmarkArgSortInt64 covers the comparison-sort path shared by
// Rank and large-k TopK.
func BenchmarkArgSortInt64(b *testing.B) {
	for _, n := range []int{16 * 1024, 256 * 1024} {
		s := argsortBenchSeries(n)
		defer s.Release()
		b.Run("", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				idx, err := s.ArgSort()
				if err != nil {
					b.Fatal(err)
				}
				_ = idx
			}
		})
	}
}

// BenchmarkRankInt64 exercises Rank end to end.
func BenchmarkRankInt64(b *testing.B) {
	s := argsortBenchSeries(256 * 1024)
	defer s.Release()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		out, err := s.Rank(series.RankAverage)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
