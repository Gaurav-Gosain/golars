package series_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

func benchTimestamps(b *testing.B, rows int) *series.Series {
	b.Helper()
	const base = int64(1_500_000_000_000_000)
	const span = int64(315_360_000_000_000)
	vals := make([]int64, rows)
	for i := range vals {
		vals[i] = base + (int64(i)*7_919_000_003)%span
	}
	s, err := series.FromDatetime("t", vals, nil, dtype.Microsecond, "")
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkDtYear1M(b *testing.B) {
	s := benchTimestamps(b, 1<<20)
	defer s.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Dt().Year()
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkDtTruncate1h1M(b *testing.B) {
	s := benchTimestamps(b, 1<<20)
	defer s.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Dt().Truncate("1h")
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkStrftime1M(b *testing.B) {
	s := benchTimestamps(b, 1<<20)
	defer s.Release()
	b.ResetTimer()
	for b.Loop() {
		out, err := s.Dt().Strftime("%Y-%m-%d %H:%M:%S")
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkStrptime1M(b *testing.B) {
	s := benchTimestamps(b, 1<<20)
	strs, err := s.Dt().Strftime("%Y-%m-%d %H:%M:%S")
	s.Release()
	if err != nil {
		b.Fatal(err)
	}
	defer strs.Release()
	opts := series.DefaultStrptimeOptions()
	b.ResetTimer()
	for b.Loop() {
		out, err := strs.Str().ToDatetime("%Y-%m-%d %H:%M:%S", "", "", opts)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
