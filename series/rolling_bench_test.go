package series_test

import (
	"math/rand"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

func rollingBenchSeries(n int) *series.Series {
	rng := rand.New(rand.NewSource(42))
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = rng.Int63n(1 << 20)
	}
	s, _ := series.FromInt64("x", vals, nil)
	return s
}

func BenchmarkRollingMinW32(b *testing.B) {
	for _, n := range []int{16 * 1024, 256 * 1024} {
		s := rollingBenchSeries(n)
		defer s.Release()
		b.Run("", func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for i := 0; i < b.N; i++ {
				out, err := s.RollingMin(series.RollingOptions{WindowSize: 32})
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func BenchmarkRollingMeanW32(b *testing.B) {
	for _, n := range []int{16 * 1024, 256 * 1024} {
		s := rollingBenchSeries(n)
		defer s.Release()
		b.Run("", func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for i := 0; i < b.N; i++ {
				out, err := s.RollingMean(series.RollingOptions{WindowSize: 32})
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

func BenchmarkRollingMaxW32(b *testing.B) {
	for _, n := range []int{16 * 1024, 256 * 1024} {
		s := rollingBenchSeries(n)
		defer s.Release()
		b.Run("", func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			for i := 0; i < b.N; i++ {
				out, err := s.RollingMax(series.RollingOptions{WindowSize: 32})
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}
