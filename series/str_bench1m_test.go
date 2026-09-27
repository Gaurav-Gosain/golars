package series_test

import (
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/series"
)

// Mirrors the string workloads in cmd/bench/extra.go (1M short ASCII
// strings shaped like "delta_1a2b3c").
func benchWords1M() *series.Series {
	words := [8]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	n := 1 << 20
	out := make([]string, n)
	for i := range out {
		h := (uint64(i) * 2654435761) & 0xFFFFFF
		out[i] = fmt.Sprintf("%s_%06x", words[i%8], h)
	}
	s, _ := series.FromString("s", out, nil)
	return s
}

func benchStrOp(b *testing.B, op func(*series.Series) (*series.Series, error)) {
	s := benchWords1M()
	defer s.Release()
	b.ReportAllocs()
	for b.Loop() {
		out, err := op(s)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkStr1MToUppercase(b *testing.B) {
	benchStrOp(b, func(s *series.Series) (*series.Series, error) { return s.Str().ToUppercase() })
}

func BenchmarkStr1MLenBytes(b *testing.B) {
	benchStrOp(b, func(s *series.Series) (*series.Series, error) { return s.Str().LenBytes() })
}

func BenchmarkStr1MLenChars(b *testing.B) {
	benchStrOp(b, func(s *series.Series) (*series.Series, error) { return s.Str().LenChars() })
}

func BenchmarkStr1MContains(b *testing.B) {
	benchStrOp(b, func(s *series.Series) (*series.Series, error) { return s.Str().Contains("ta") })
}
