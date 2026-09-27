package series_test

import (
	"strconv"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

func strNSBenchSeries(b *testing.B, rows int) *series.Series {
	b.Helper()
	vals := make([]string, rows)
	for i := range vals {
		vals[i] = "k" + strconv.Itoa(i%997) + "-v" + strconv.Itoa(i%13) + "-x" + strconv.Itoa(i)
	}
	s, err := series.FromString("s", vals, nil)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkStrSplit1M(b *testing.B) {
	s := strNSBenchSeries(b, 1<<20)
	defer s.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Str().Split("-", false)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkStrContains1M(b *testing.B) {
	s := strNSBenchSeries(b, 1<<20)
	defer s.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Str().Contains("v7")
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkListSum1M(b *testing.B) {
	s := strNSBenchSeries(b, 1<<20)
	defer s.Release()
	parts, err := s.Str().Split("-", false)
	if err != nil {
		b.Fatal(err)
	}
	defer parts.Release()
	lens, err := parts.List().Len()
	if err != nil {
		b.Fatal(err)
	}
	defer lens.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := parts.List().NUnique()
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
