package lazy

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
)

// Morsel execution over batched sources.
//
// A plan fragment made of row-local operators (elementwise Filter,
// WithColumns and Projection, Rename, Drop) over a source that can be
// read in independent batches (parquet row groups) runs one batch at a
// time: every worker reads a batch, runs the fragment on it with the
// ordinary executor, and keeps only the result. Results are stitched
// back in batch order, so the output equals the eager result row for
// row. The full source is never materialised, which removes the copy
// the eager scan makes to make columns contiguous and bounds the
// working set to the batches in flight plus the (usually much smaller)
// filtered output.
//
// An Aggregate, or a Projection of aggregations, directly above such a
// fragment goes further: each batch is reduced to partial aggregates
// and only those are combined (see morsel_agg.go).

// errMorselNotApplicable reports that a fragment cannot run as morsels
// (for example the source has a single batch); the caller falls back to
// the eager path.
var errMorselNotApplicable = errors.New("lazy: not a morsel fragment")

// morselFragment returns the batched source at the bottom of n when
// every node from n down to it is row-local. ops counts the operators
// above the source.
func morselFragment(n Node) (src SourceFunc, ops int, ok bool) {
	for {
		switch node := n.(type) {
		case SourceFunc:
			return node, ops, node.OpenBatches != nil
		case Filter:
			if !expr.IsElementwise(node.Predicate) {
				return SourceFunc{}, 0, false
			}
			n = node.Input
		case WithColumns:
			if !allElementwise(node.Exprs) {
				return SourceFunc{}, 0, false
			}
			n = node.Input
		case Projection:
			// A projection of literals only has one row in polars, not
			// one per batch.
			if !allElementwise(node.Exprs) || !everyExprReadsAColumn(node.Exprs) {
				return SourceFunc{}, 0, false
			}
			n = node.Input
		case Rename:
			n = node.Input
		case Drop:
			n = node.Input
		default:
			return SourceFunc{}, 0, false
		}
		ops++
	}
}

func everyExprReadsAColumn(exprs []expr.Expr) bool {
	for _, e := range exprs {
		if len(expr.Columns(e)) == 0 && !referencesAllColumns(e) {
			return false
		}
	}
	return true
}

// withLeaf rebuilds the row-local chain n with its source replaced by
// leaf.
func withLeaf(n Node, leaf Node) Node {
	if _, ok := n.(SourceFunc); ok {
		return leaf
	}
	return n.WithChildren([]Node{withLeaf(n.Children()[0], leaf)})
}

// runMorsels runs fragment (a row-local chain over src) on every batch
// of src and hands each result to reduce, returning the reduced results
// in batch order. reduce may return its input. Workers are bounded by
// GOMAXPROCS; each holds one batch at a time.
func runMorsels(ctx context.Context, cfg execConfig, fragment Node, src SourceFunc,
	reduce func(context.Context, *dataframe.DataFrame) (*dataframe.DataFrame, error),
) ([]*dataframe.DataFrame, error) {
	bs, err := src.OpenBatches(ctx, src.Projection)
	if err != nil {
		return nil, err
	}
	defer bs.Close()
	nb := bs.NumBatches()
	if nb < 2 {
		return nil, errMorselNotApplicable
	}
	out := make([]*dataframe.DataFrame, nb)
	errs := make([]error, nb)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(nb, runtime.GOMAXPROCS(0)) {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= nb || ctx.Err() != nil {
					return
				}
				out[i], errs[i] = runOneMorsel(ctx, cfg, fragment, bs, i, reduce)
				if errs[i] != nil {
					cancel()
					return
				}
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		releaseFrames(out)
		return nil, err
	}
	return out, nil
}

func runOneMorsel(ctx context.Context, cfg execConfig, fragment Node, bs BatchSource, i int,
	reduce func(context.Context, *dataframe.DataFrame) (*dataframe.DataFrame, error),
) (*dataframe.DataFrame, error) {
	batch, err := bs.ReadBatch(ctx, i)
	if err != nil {
		return nil, err
	}
	batch, err = rechunkFrame(ctx, batch)
	if err != nil {
		return nil, err
	}
	res, err := executeNode(ctx, cfg, withLeaf(fragment, DataFrameScan{Source: batch, Length: -1}))
	batch.Release()
	if err != nil || reduce == nil {
		return res, err
	}
	red, err := reduce(ctx, res)
	if red != res {
		res.Release()
	}
	return red, err
}

func releaseFrames(fs []*dataframe.DataFrame) {
	for _, f := range fs {
		if f != nil {
			f.Release()
		}
	}
}

// concatMorsels stitches batch results in order into one frame,
// consuming parts. With contiguous set every column is copied into one
// chunk, which kernels that read a column several times want; without
// it the columns keep one chunk per batch and no copy is made.
func concatMorsels(ctx context.Context, parts []*dataframe.DataFrame, contiguous bool) (*dataframe.DataFrame, error) {
	defer releaseFrames(parts)
	all, err := dataframe.Concat(parts...)
	if err != nil || !contiguous {
		return all, err
	}
	return rechunkFrame(ctx, all)
}

// tryMorselChain runs n as morsels when it is a row-local fragment with
// at least one operator over a batched source. ok is false when the
// eager path should run instead.
func tryMorselChain(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, bool, error) {
	return morselChain(ctx, cfg, n, true)
}

// executeJoinInput executes one input of a join. A morsel fragment keeps
// its per-batch chunks: the join reads each column once (the gather
// concatenates it then), so making the whole input contiguous first
// would only add a full copy to the peak.
func executeJoinInput(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, error) {
	if cfg.profiler == nil && cfg.tracer == nil {
		if out, ok, err := morselChain(ctx, cfg, n, false); ok {
			return out, err
		}
	}
	return executeNode(ctx, cfg, n)
}

func morselChain(ctx context.Context, cfg execConfig, n Node, contiguous bool) (*dataframe.DataFrame, bool, error) {
	src, ops, ok := morselFragment(n)
	if !ok || ops == 0 {
		return nil, false, nil
	}
	parts, err := runMorsels(ctx, cfg, n, src, nil)
	if errors.Is(err, errMorselNotApplicable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	out, err := concatMorsels(ctx, parts, contiguous)
	return out, true, err
}
