package compute_test

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestSortFloat64SkipsConstantDigits covers inputs where some radix
// byte positions are constant across all keys (skipped passes) with
// both parities of remaining passes, plus fully constant input, on the
// serial (1K to 64K) and parallel paths.
func TestSortFloat64SkipsConstantDigits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(5, 6))
	gens := map[string]func() float64{
		"unit":       r.Float64,
		"narrow":     func() float64 { return 1.5 + float64(r.IntN(256))/1024 },
		"one-byte":   func() float64 { return math.Float64frombits(0x3FF0000000000000 | uint64(r.IntN(256))) },
		"two-bytes":  func() float64 { return math.Float64frombits(0x3FF0000000000000 | uint64(r.IntN(65536))) },
		"three-byte": func() float64 { return math.Float64frombits(0x3FF0000000000000 | uint64(r.IntN(1<<24))) },
		"constant":   func() float64 { return 42.25 },
		"integers":   func() float64 { return float64(r.IntN(1000)) },
	}
	for name, gen := range gens {
		for _, n := range []int{1024, 5000, 70_000} {
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = gen()
			}
			s, _ := series.FromFloat64("x", vals, nil)
			out, err := compute.Sort(ctx, s, compute.SortOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := slices.Clone(vals)
			slices.Sort(want)
			got := out.Chunk(0).(*array.Float64).Float64Values()
			if !slices.Equal(got, want) {
				t.Fatalf("%s n=%d: sort mismatch", name, n)
			}
			out.Release()
			s.Release()
		}
	}
}
