package lazy

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// executeTail evaluates the upstream and keeps the last N rows.
func executeTail(ctx context.Context, cfg execConfig, t TailNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, t.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	n := min(max(t.N, 0), input.Height())
	out := input.Tail(n)
	return out, nil
}

// executeReverse evaluates the upstream and reverses the row order.
func executeReverse(ctx context.Context, cfg execConfig, r ReverseNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, r.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Reverse(ctx)
}

// newProjectedFrame assembles the output of a select. Like polars, a
// length-1 result (col("a").sum()) broadcasts to the height of the
// other columns. It consumes cols, also on error.
func newProjectedFrame(cols []*series.Series, alloc memory.Allocator) (*dataframe.DataFrame, error) {
	release := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	height := 1
	for _, c := range cols {
		if c.Len() != 1 {
			height = c.Len()
			break
		}
	}
	if height != 1 {
		for i, c := range cols {
			if c.Len() != 1 {
				continue
			}
			b, err := c.Broadcast(height, series.WithAllocator(alloc))
			if err != nil {
				release()
				return nil, err
			}
			c.Release()
			cols[i] = b
		}
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		release()
		return nil, err
	}
	return df, nil
}
