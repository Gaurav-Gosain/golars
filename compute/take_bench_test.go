package compute_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// BenchmarkTakeInt64 mirrors the Take workload in cmd/bench: gather a
// random half-permutation of the rows.
func BenchmarkTakeInt64(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{16 * 1024, 256 * 1024} {
		r := rand.New(rand.NewPCG(42, 43))
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = r.Int64N(1 << 20)
		}
		s, _ := series.FromInt64("a", vals, nil)
		perm := r.Perm(n)[:n/2]
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n) * 8)
			b.ReportAllocs()
			for b.Loop() {
				out, err := compute.Take(ctx, s, perm)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
		s.Release()
	}
}
