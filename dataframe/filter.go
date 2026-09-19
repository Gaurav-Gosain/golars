package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// FilterOption configures DataFrame.Filter.
type FilterOption func(*filterConfig)

type filterConfig struct {
	alloc memory.Allocator
}

// WithFilterAllocator overrides the allocator used while building the filtered
// output.
func WithFilterAllocator(alloc memory.Allocator) FilterOption {
	return func(c *filterConfig) { c.alloc = alloc }
}

func resolveFilter(opts []FilterOption) filterConfig {
	c := filterConfig{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Filter returns a DataFrame containing only rows where mask[i] is true. The
// mask must be a boolean Series of the same length as the DataFrame. Null
// mask entries are treated as false.
//
// Wide frames filter per-column in parallel; each output column is an
// independent Series owning its own buffers. Narrow frames stay serial,
// and compute.Filter parallelizes rows internally above its cutoff.
func (df *DataFrame) Filter(ctx context.Context, mask *series.Series, opts ...FilterOption) (*DataFrame, error) {
	if mask.Len() != df.height {
		return nil, fmt.Errorf("%w: mask=%d df=%d", compute.ErrLengthMismatch, mask.Len(), df.height)
	}
	if !mask.DType().IsBool() {
		return nil, compute.ErrMaskNotBool
	}
	cfg := resolveFilter(opts)

	if df.Width() == 0 {
		// No columns; just return an empty frame with same schema. Height of
		// result equals the number of true entries in the mask.
		maskArr := mask.Chunk(0)
		count := 0
		for i := 0; i < mask.Len(); i++ {
			if maskArr.IsValid(i) {
				if b, ok := maskArr.(interface{ Value(int) bool }); ok && b.Value(i) {
					count++
				}
			}
		}
		return &DataFrame{sch: df.sch, height: count}, nil
	}

	cols := make([]*series.Series, df.Width())
	if df.Width() >= parallelFilterMinCols {
		// Fan out independent per-column filters. Each worker writes
		// its own slot; on error every completed slot is released.
		g := pool.NewGroup(ctx, 0)
		for i, c := range df.cols {
			g.Go(func(gctx context.Context) error {
				out, err := compute.Filter(gctx, c, mask, compute.WithAllocator(cfg.alloc))
				if err != nil {
					return fmt.Errorf("column %q: %w", c.Name(), err)
				}
				cols[i] = out
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			for _, r := range cols {
				if r != nil {
					r.Release()
				}
			}
			return nil, err
		}
	} else {
		for i, c := range df.cols {
			out, err := compute.Filter(ctx, c, mask, compute.WithAllocator(cfg.alloc))
			if err != nil {
				for _, r := range cols[:i] {
					if r != nil {
						r.Release()
					}
				}
				return nil, fmt.Errorf("column %q: %w", c.Name(), err)
			}
			cols[i] = out
		}
	}

	newHeight := 0
	if len(cols) > 0 {
		newHeight = cols[0].Len()
	}
	return &DataFrame{sch: df.sch, cols: cols, height: newHeight}, nil
}

// parallelFilterMinCols is the minimum frame width that pays for
// goroutine fan-out in Filter. Narrow frames stay serial.
const parallelFilterMinCols = 8
