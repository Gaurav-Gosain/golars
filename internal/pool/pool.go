// Package pool provides goroutine-pool primitives used by compute kernels
// and the in-memory executor.
//
// The package offers two levels:
//
//  1. ParallelFor: a high-level helper that partitions an index range into
//     chunks and runs a worker function on each chunk concurrently, capped by
//     a configurable parallelism bound. It returns on the first error.
//
//  2. Group: a context-aware errgroup wrapper with an explicit parallelism
//     bound. Use it directly for finer-grained control.
//
// Both are cancellation-aware: when the supplied context is cancelled, in
// flight workers observe the cancellation on their next ctx check, and new
// work is rejected.
package pool

import (
	"context"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
)

// DefaultParallelism is the parallelism used when no explicit bound is given.
// It reflects the current value of GOMAXPROCS at call time.
func DefaultParallelism() int { return runtime.GOMAXPROCS(0) }

// ParallelFor runs fn over the half-open index range [0, n) using up to
// parallelism goroutines. The range is split into approximately equal chunks;
// fn is called once per chunk with (ctx, start, end), with chunk 0 on the
// calling goroutine. A chunk that has not started when ctx is cancelled is
// skipped. ParallelFor returns the error of the lowest-indexed chunk that
// failed, or nil.
//
// If parallelism <= 0 it is replaced by DefaultParallelism.
// If n <= 0 the function returns nil without calling fn.
func ParallelFor(ctx context.Context, n, parallelism int, fn func(ctx context.Context, start, end int) error) error {
	if n <= 0 {
		return nil
	}
	if parallelism <= 0 {
		parallelism = DefaultParallelism()
	}
	parallelism = min(parallelism, n)

	if parallelism == 1 {
		return fn(ctx, 0, n)
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	// One goroutine per chunk beyond the first; the caller runs chunk 0
	// itself instead of parking in Wait. Kernels call this on every
	// batch at mid sizes (64K to 1M rows) where the spawn and wake-up
	// cost is a visible share of the run, so this path avoids errgroup's
	// derived context, semaphore channel and extra goroutine. Chunks run
	// to completion once started; the reported error is the one from
	// the lowest-indexed failing chunk, so it does not depend on
	// scheduling.
	chunk := (n + parallelism - 1) / parallelism
	nChunks := (n + chunk - 1) / chunk
	errs := make([]error, nChunks)
	var wg sync.WaitGroup
	for c := 1; c < nChunks; c++ {
		s := c * chunk
		e := min(s+chunk, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				errs[c] = err
				return
			}
			errs[c] = fn(ctx, s, e)
		}()
	}
	errs[0] = fn(ctx, 0, min(chunk, n))
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// MapChunks runs fn once per element of a slice of chunks concurrently. The
// output has the same length; fn must be safe to call from multiple
// goroutines. The i'th output slot receives the i'th fn result regardless of
// completion order.
func MapChunks[In, Out any](ctx context.Context, chunks []In, parallelism int, fn func(ctx context.Context, i int, in In) (Out, error)) ([]Out, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	if parallelism <= 0 {
		parallelism = DefaultParallelism()
	}
	if parallelism > len(chunks) {
		parallelism = len(chunks)
	}

	out := make([]Out, len(chunks))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)

	for i, c := range chunks {
		idx, in := i, c
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			o, err := fn(gctx, idx, in)
			if err != nil {
				return err
			}
			out[idx] = o
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// Group is a thin wrapper over errgroup.Group with an explicit parallelism
// limit. It exists so callers can construct their own named pool sites
// without re-deriving the context contract.
type Group struct {
	g   *errgroup.Group
	ctx context.Context
}

// NewGroup returns a Group that caps concurrent goroutines at parallelism.
// If parallelism <= 0 it is replaced by DefaultParallelism.
func NewGroup(ctx context.Context, parallelism int) *Group {
	if parallelism <= 0 {
		parallelism = DefaultParallelism()
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)
	return &Group{g: g, ctx: gctx}
}

// Context returns the group's derived context. It is cancelled on the first
// returned error or when Wait is called.
func (p *Group) Context() context.Context { return p.ctx }

// Go schedules fn. fn is called with the group's context.
func (p *Group) Go(fn func(ctx context.Context) error) {
	p.g.Go(func() error { return fn(p.ctx) })
}

// Wait blocks until all scheduled funcs return, then returns the first
// non-nil error if any.
func (p *Group) Wait() error { return p.g.Wait() }
