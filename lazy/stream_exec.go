package lazy

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/stream"
)

// errStreamNotApplicable marks plans that cannot run through the morsel
// engine. The eager executor is used as a fallback.
var errStreamNotApplicable = errors.New("lazy: plan is not streaming-friendly")

// WithStreaming routes streaming-friendly plan prefixes through the
// morsel-driven executor. Blocker nodes (Sort, Aggregate, Join) either fall
// back to eager or evaluate their input with streaming and then proceed
// eagerly. If nothing in the plan is streaming-friendly, this option has no
// effect.
func WithStreaming() ExecOption {
	return func(c *execConfig) { c.streaming = true }
}

// WithStreamingMorselRows tunes the morsel row count when streaming is
// enabled. A small value gives tight back-pressure; a large value amortizes
// per-morsel overhead. Defaults to stream.DefaultMorselRows.
func WithStreamingMorselRows(n int) ExecOption {
	return func(c *execConfig) {
		if n > 0 {
			c.morselRows = n
		}
	}
}

// WithStreamingWorkers sets the number of worker goroutines inside each
// streaming stage. Values <= 1 use the serial stage. Output order is
// preserved regardless of worker count. Unset (the default) uses
// min(GOMAXPROCS, 8).
func WithStreamingWorkers(n int) ExecOption {
	return func(c *execConfig) {
		if n > 0 {
			c.workers = n
		}
	}
}

// executeMaybeStreaming runs plan end-to-end, preferring streaming when the
// exec config requests it and the plan is streaming-friendly.
func executeMaybeStreaming(ctx context.Context, cfg execConfig, plan Node) (*dataframe.DataFrame, error) {
	if !cfg.streaming {
		return executeNode(ctx, cfg, plan)
	}
	df, err := executeStreaming(ctx, cfg, plan)
	if err == nil {
		return df, nil
	}
	if !errors.Is(err, errStreamNotApplicable) {
		return nil, err
	}
	// Fallback: run the child streaming-friendly prefix and the rest eagerly.
	return executeHybrid(ctx, cfg, plan)
}

// executeStreaming compiles the whole plan to a stream.Pipeline. Returns
// errStreamNotApplicable if any node in the chain is a blocker.
func executeStreaming(ctx context.Context, cfg execConfig, plan Node) (*dataframe.DataFrame, error) {
	streamCfg := buildStreamConfig(cfg)
	if h, ok := scanHeight(plan); ok && h <= streamCfg.MorselRows {
		// Single morsel: the pipeline (channels, goroutines, concat)
		// costs more than it parallelises. Same kernels, same result.
		return executeNode(ctx, cfg, plan)
	}
	src, stages, ok := compilePipeline(plan, streamCfg, cfg.workers)
	if !ok {
		return nil, errStreamNotApplicable
	}
	pipeline := stream.New(streamCfg, src, stages, stream.CollectSink(streamCfg))
	return pipeline.Run(ctx)
}

// executeHybrid walks the plan top-down. For each blocker (Sort, Aggregate,
// Join), it runs the child via streaming (if possible) to produce an
// intermediate DataFrame, then evaluates the blocker eagerly. This lets a
// filter+project prefix enjoy streaming back-pressure while a terminal sort
// still uses the eager sort kernel.
func executeHybrid(ctx context.Context, cfg execConfig, plan Node) (*dataframe.DataFrame, error) {
	switch n := plan.(type) {
	case Sort:
		input, err := executeMaybeStreaming(ctx, cfg, n.Input)
		if err != nil {
			return nil, err
		}
		defer input.Release()
		return input.SortBy(ctx, n.Keys, n.Options, dataframe.WithSortAllocator(cfg.alloc))
	case Aggregate:
		input, err := executeMaybeStreaming(ctx, cfg, n.Input)
		if err != nil {
			return nil, err
		}
		defer input.Release()
		return input.GroupBy(n.Keys...).Agg(ctx, n.Aggs, dataframe.WithGroupByAllocator(cfg.alloc))
	case Join:
		left, err := executeMaybeStreaming(ctx, cfg, n.Left)
		if err != nil {
			return nil, err
		}
		defer left.Release()
		right, err := executeMaybeStreaming(ctx, cfg, n.Right)
		if err != nil {
			return nil, err
		}
		defer right.Release()
		return left.Join(ctx, right, n.On, n.How, dataframe.WithJoinAllocator(cfg.alloc))
	}
	// Any other node that we could not compile as streaming: fall back fully.
	return executeNode(ctx, cfg, plan)
}

// compilePipeline walks the plan leaf-first and returns a source + stages if
// every node is streamable. A non-nil ok=false means a blocker was hit.
// workers > 1 selects parallel stage variants where available.
func compilePipeline(plan Node, cfg stream.Config, workers int) (stream.Source, []stream.Stage, bool) {
	switch n := plan.(type) {
	case DataFrameScan:
		source := stream.DataFrameSource(n.Source, cfg)
		var stages []stream.Stage
		if len(n.Projection) > 0 {
			exprs := make([]expr.Expr, len(n.Projection))
			for i, c := range n.Projection {
				exprs[i] = expr.Col(c)
			}
			stages = append(stages, projectStage(cfg, exprs, workers))
		}
		if n.Predicate != nil {
			stages = append(stages, filterStage(cfg, *n.Predicate, workers))
		}
		if n.Length >= 0 {
			stages = append(stages, stream.SliceStage(cfg, n.Offset, n.Length))
		}
		return source, stages, true
	case Projection:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, projectStage(cfg, n.Exprs, workers))
		return src, stages, true
	case WithColumns:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, withColumnsStage(cfg, n.Exprs, workers))
		return src, stages, true
	case Filter:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, filterStage(cfg, n.Predicate, workers))
		return src, stages, true
	case Rename:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, stream.RenameStage(cfg, n.Old, n.New))
		return src, stages, true
	case Drop:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, stream.DropStage(cfg, n.Columns))
		return src, stages, true
	case SliceNode:
		src, stages, ok := compilePipeline(n.Input, cfg, workers)
		if !ok {
			return nil, nil, false
		}
		stages = append(stages, stream.SliceStage(cfg, n.Offset, n.Length))
		return src, stages, true
	}
	return nil, nil, false
}

// filterStage returns the parallel variant when workers > 1, else the
// serial path. Slice/Rename/Drop stages have no parallel variant because
// they either must be serial (Slice) or are trivially cheap (Rename/Drop).
func filterStage(cfg stream.Config, pred expr.Expr, workers int) stream.Stage {
	if workers > 1 {
		return stream.ParallelFilterStage(cfg, pred, workers)
	}
	return stream.FilterStage(cfg, pred)
}

func projectStage(cfg stream.Config, exprs []expr.Expr, workers int) stream.Stage {
	if workers > 1 {
		return stream.ParallelProjectStage(cfg, exprs, workers)
	}
	return stream.ProjectStage(cfg, exprs)
}

func withColumnsStage(cfg stream.Config, exprs []expr.Expr, workers int) stream.Stage {
	if workers > 1 {
		return stream.ParallelWithColumnsStage(cfg, exprs, workers)
	}
	return stream.WithColumnsStage(cfg, exprs)
}

// scanHeight returns the source frame height when the plan bottoms
// out at a single DataFrameScan, which every streaming-friendly plan
// does (compilePipeline rejects anything else).
func scanHeight(plan Node) (int, bool) {
	for {
		switch n := plan.(type) {
		case DataFrameScan:
			if n.Source == nil {
				return 0, false
			}
			return n.Source.Height(), true
		case Projection:
			plan = n.Input
		case WithColumns:
			plan = n.Input
		case Filter:
			plan = n.Input
		case Rename:
			plan = n.Input
		case Drop:
			plan = n.Input
		case SliceNode:
			plan = n.Input
		default:
			return 0, false
		}
	}
}

func buildStreamConfig(cfg execConfig) stream.Config {
	streamCfg := stream.DefaultConfig()
	streamCfg.Allocator = cfg.alloc
	if cfg.morselRows > 0 {
		streamCfg.MorselRows = cfg.morselRows
	}
	return streamCfg
}

var _ = memory.DefaultAllocator
