package series_test

import (
	"math/rand"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

func topkBenchSeries(n int) *series.Series {
	rng := rand.New(rand.NewSource(42))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = rng.Int63n(1 << 20)
	}
	s, _ := series.FromInt64("x", vals, nil)
	return s
}

// BenchmarkTopK exercises small-k selection over a large column.
func BenchmarkTopK(b *testing.B) {
	for _, k := range []int{10, 1000} {
		s := topkBenchSeries(256 * 1024)
		defer s.Release()
		b.Run("", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(256 * 1024 * 8)
			for i := 0; i < b.N; i++ {
				out, err := s.TopK(k)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
