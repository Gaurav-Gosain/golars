package pool

import (
	"context"
	"fmt"
	"testing"

	"golang.org/x/sync/errgroup"
)

// errgroupParallelFor is the previous ParallelFor, kept here only as a
// benchmark baseline for the spawn and wait overhead.
func errgroupParallelFor(ctx context.Context, n, parallelism int, fn func(ctx context.Context, start, end int) error) error {
	parallelism = min(parallelism, n)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)
	chunk := (n + parallelism - 1) / parallelism
	for start := 0; start < n; start += chunk {
		s, e := start, min(start+chunk, n)
		g.Go(func() error { return fn(gctx, s, e) })
	}
	return g.Wait()
}

// BenchmarkParallelForAdd times a memory-bound int64 add split across
// workers, the shape of most kernels that call ParallelFor.
func BenchmarkParallelForAdd(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{64 * 1024, 256 * 1024} {
		a := make([]int64, n)
		c := make([]int64, n)
		out := make([]int64, n)
		body := func(_ context.Context, s, e int) error {
			for i := s; i < e; i++ {
				out[i] = a[i] + c[i]
			}
			return nil
		}
		b.Run(fmt.Sprintf("errgroup/n=%d", n), func(b *testing.B) {
			for b.Loop() {
				_ = errgroupParallelFor(ctx, n, DefaultParallelism(), body)
			}
		})
		b.Run(fmt.Sprintf("current/n=%d", n), func(b *testing.B) {
			for b.Loop() {
				_ = ParallelFor(ctx, n, DefaultParallelism(), body)
			}
		})
	}
}
