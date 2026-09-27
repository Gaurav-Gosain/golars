package lazy

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// ExecOption configures plan execution.
type ExecOption func(*execConfig)

type execConfig struct {
	alloc      memory.Allocator
	streaming  bool
	morselRows int
	workers    int
	profiler   *Profiler
	tracer     Tracer
}

func resolveExec(opts []ExecOption) execConfig {
	c := execConfig{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	if c.workers <= 0 {
		// Parallel streaming stages by default: inter-morsel work is
		// independent and ordered output is preserved. Explicit
		// WithStreamingWorkers(1) still selects the serial stages.
		// Capped like the other fan-outs so in-flight morsels stay
		// memory-bounded.
		c.workers = min(runtime.GOMAXPROCS(0), 8)
	}
	return c
}

// WithExecAllocator overrides the allocator used during execution.
func WithExecAllocator(alloc memory.Allocator) ExecOption {
	return func(c *execConfig) { c.alloc = alloc }
}

// Execute runs the (optimized) plan against its in-memory source and returns
// the resulting DataFrame. Callers are responsible for releasing the result.
func Execute(ctx context.Context, plan Node, opts ...ExecOption) (*dataframe.DataFrame, error) {
	cfg := resolveExec(opts)
	return executeNode(ctx, cfg, plan)
}

func executeNode(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, error) {
	if cfg.tracer != nil {
		var span Span
		ctx, span = tracerSpan(ctx, cfg.tracer, fmt.Sprintf("%T", n))
		defer span.End()
	}
	if cfg.profiler != nil {
		return executeNodeProfiled(ctx, cfg, n)
	}
	return executeNodeRaw(ctx, cfg, n)
}

// executeNodeProfiled wraps each node's execution in a span record.
// Kept separate so the non-profiled path stays hot and allocation-free.
func executeNodeProfiled(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, error) {
	name := fmt.Sprintf("%T", n)
	detail := n.String()
	start := time.Now()
	df, err := executeNodeRaw(ctx, cfg, n)
	rows := 0
	if df != nil {
		rows = df.Height()
	}
	cfg.profiler.record(ProfileSpan{
		Name:      name,
		Detail:    detail,
		Duration:  time.Since(start),
		Rows:      rows,
		StartedAt: start,
	})
	return df, err
}

func executeNodeRaw(ctx context.Context, cfg execConfig, n Node) (*dataframe.DataFrame, error) {
	switch n.(type) {
	case Filter, WithColumns, Projection, Rename, Drop:
		if out, ok, err := tryMorselChain(ctx, cfg, n); ok {
			return out, err
		}
	}
	switch node := n.(type) {
	case DataFrameScan:
		return executeScan(ctx, cfg, node)
	case SourceFunc:
		return node.load(ctx)
	case Projection:
		return executeProjection(ctx, cfg, node)
	case WithColumns:
		return executeWithColumns(ctx, cfg, node)
	case Filter:
		return executeFilter(ctx, cfg, node)
	case Sort:
		return executeSort(ctx, cfg, node)
	case SliceNode:
		return executeSlice(ctx, cfg, node)
	case TailNode:
		return executeTail(ctx, cfg, node)
	case ReverseNode:
		return executeReverse(ctx, cfg, node)
	case UniqueNode:
		return executeUnique(ctx, cfg, node)
	case FillNullNode:
		return executeFillNull(ctx, cfg, node)
	case DropNullsNode:
		return executeDropNulls(ctx, cfg, node)
	case CastNode:
		return executeCast(ctx, cfg, node)
	case CacheNode:
		return executeCache(ctx, cfg, node)
	case HorizontalNode:
		return executeHorizontal(ctx, cfg, node)
	case FillNanNode:
		return executeFillNan(ctx, cfg, node)
	case ForwardFillNode:
		return executeForwardFill(ctx, cfg, node)
	case BackwardFillNode:
		return executeBackwardFill(ctx, cfg, node)
	case WithRowIndexNode:
		return executeWithRowIndex(ctx, cfg, node)
	case Rename:
		return executeRename(ctx, cfg, node)
	case Drop:
		return executeDrop(ctx, cfg, node)
	case Aggregate:
		return executeAggregate(ctx, cfg, node)
	case Join:
		return executeJoin(ctx, cfg, node)
	case FrameOpNode:
		return executeFrameOp(ctx, cfg, node)
	case BinaryFrameOpNode:
		return executeBinaryFrameOp(ctx, cfg, node)
	}
	if out, ok, err := executeTemporalGroup(ctx, cfg, n); ok {
		return out, err
	}
	return nil, fmt.Errorf("lazy: cannot execute node %T", n)
}

func executeAggregate(ctx context.Context, cfg execConfig, a Aggregate) (*dataframe.DataFrame, error) {
	if len(a.Keys) > 0 {
		if out, ok, err := tryMorselAggregate(ctx, cfg, a.Input, a.Keys, a.Aggs); ok {
			return out, err
		}
	}
	input, err := executeNode(ctx, cfg, a.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.GroupBy(a.Keys...).Agg(ctx, a.Aggs, dataframe.WithGroupByAllocator(cfg.alloc))
}

func executeJoin(ctx context.Context, cfg execConfig, j Join) (*dataframe.DataFrame, error) {
	left, err := executeNode(ctx, cfg, j.Left)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := executeNode(ctx, cfg, j.Right)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	return left.Join(ctx, right, j.On, j.How, dataframe.WithJoinAllocator(cfg.alloc))
}

func executeScan(ctx context.Context, cfg execConfig, s DataFrameScan) (*dataframe.DataFrame, error) {
	src := s.Source
	// Apply pushed-down predicate against the full source first so
	// any column referenced in the predicate is still available. The
	// post-filter frame is then projected down in the next step.
	var projected *dataframe.DataFrame
	if s.Predicate != nil {
		mask, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.alloc}, *s.Predicate, src)
		if err != nil {
			return nil, err
		}
		filtered, err := src.Filter(ctx, mask, dataframe.WithFilterAllocator(cfg.alloc))
		mask.Release()
		if err != nil {
			return nil, err
		}
		projected = filtered
	} else {
		projected = src.Clone()
	}

	// Apply pushed-down projection.
	if len(s.Projection) > 0 {
		p, err := projected.Select(s.Projection...)
		projected.Release()
		if err != nil {
			return nil, err
		}
		projected = p
	}

	// Apply pushed-down slice.
	if s.Length >= 0 {
		off, length := series.ClampSlice(s.Offset, s.Length, projected.Height())
		out, err := projected.Slice(off, length)
		projected.Release()
		if err != nil {
			return nil, err
		}
		projected = out
	}
	return projected, nil
}

func executeProjection(ctx context.Context, cfg execConfig, p Projection) (*dataframe.DataFrame, error) {
	if out, ok, err := tryMorselAggregate(ctx, cfg, p.Input, nil, p.Exprs); ok {
		return out, err
	}
	input, err := executeNode(ctx, cfg, p.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	cols, err := evalOutputs(ctx, cfg, p.Exprs, input, "projection")
	if err != nil {
		return nil, err
	}
	return newProjectedFrame(cols, cfg.alloc)
}

// Parallel expression evaluation cutoffs. Every expression in a
// select/with_columns reads the same input frame, so they are
// independent and can run concurrently, as polars does. Fan-out pays
// once the frame is tall enough that one expression costs well over the
// goroutine handoff (a few microseconds), or the list is wide enough
// that the serial sum does. Kernels also split rows internally at large
// heights; the Go scheduler absorbs the overlap.
const (
	parallelEvalMinExprs = 2
	parallelEvalMinRows  = 8 * 1024
	parallelSelectWide   = 8
)

func parallelEvalWorthIt(nexprs, height int) bool {
	if nexprs < parallelEvalMinExprs {
		return false
	}
	return height >= parallelEvalMinRows || nexprs >= parallelSelectWide
}

// evalOutputs evaluates every expression against input and renames each
// result to its output name. On error every produced series is released
// and the error of the lowest failing index is returned, so the error
// does not depend on scheduling. what prefixes the error message when
// non-empty.
func evalOutputs(ctx context.Context, cfg execConfig, exprs []expr.Expr, input *dataframe.DataFrame, what string) ([]*series.Series, error) {
	cols := make([]*series.Series, len(exprs))
	one := func(ctx context.Context, src *dataframe.DataFrame, i int) error {
		e := exprs[i]
		s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.alloc}, e, src)
		if err != nil {
			if what != "" {
				return fmt.Errorf("%s %s: %w", what, e, err)
			}
			return err
		}
		if name := expr.OutputName(e); s.Name() != name {
			renamed := s.Rename(name)
			s.Release()
			s = renamed
		}
		cols[i] = s
		return nil
	}

	if !parallelEvalWorthIt(len(exprs), input.Height()) {
		for i := range exprs {
			if err := one(ctx, input, i); err != nil {
				releaseSeries(cols[:i])
				return nil, err
			}
		}
		return cols, nil
	}

	// series.Chunk(0) consolidates a multi-chunk Series in place, which
	// is not safe to do concurrently on one shared Series. When the input
	// has such a column, each goroutine works on its own shallow clone
	// (fresh Series wrappers over the same buffers).
	shared := true
	for _, c := range input.Columns() {
		if c.NumChunks() > 1 {
			shared = false
			break
		}
	}
	errs := make([]error, len(exprs))
	g := pool.NewGroup(ctx, 0)
	for i := range exprs {
		g.Go(func(gctx context.Context) error {
			src := input
			if !shared {
				src = input.Clone()
				defer src.Release()
			}
			// Record instead of returning so one failure does not cancel
			// siblings mid-flight; the lowest index wins below.
			errs[i] = one(gctx, src, i)
			return nil
		})
	}
	_ = g.Wait()
	for _, err := range errs {
		if err != nil {
			releaseSeries(cols)
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		releaseSeries(cols)
		return nil, err
	}
	return cols, nil
}

func releaseSeries(cols []*series.Series) {
	for _, c := range cols {
		if c != nil {
			c.Release()
		}
	}
}

// executeWithColumns evaluates every expression against the input frame
// (not against columns added earlier in the same call), matching polars
// with_columns and the schema WithColumns.Schema reports. Outputs are
// then applied in order, so a later expression with the same output
// name replaces an earlier one.
func executeWithColumns(ctx context.Context, cfg execConfig, w WithColumns) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, w.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	cols, err := evalOutputs(ctx, cfg, w.Exprs, input, "")
	if err != nil {
		return nil, err
	}
	out := input.Clone()
	for i, s := range cols {
		updated, err := out.WithColumn(s)
		out.Release()
		if err != nil {
			releaseSeries(cols[i:])
			return nil, err
		}
		out = updated
	}
	return out, nil
}

func executeFilter(ctx context.Context, cfg execConfig, f Filter) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, f.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	mask, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.alloc}, f.Predicate, input)
	if err != nil {
		return nil, err
	}
	defer mask.Release()

	if !mask.DType().IsBool() {
		return nil, fmt.Errorf("%w: filter predicate must be bool, got %s",
			compute.ErrMaskNotBool, mask.DType())
	}
	return input.Filter(ctx, mask, dataframe.WithFilterAllocator(cfg.alloc))
}

func executeSort(ctx context.Context, cfg execConfig, s Sort) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, s.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.SortBy(ctx, s.Keys, s.Options, dataframe.WithSortAllocator(cfg.alloc))
}

func executeSlice(ctx context.Context, cfg execConfig, s SliceNode) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, s.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	off, length := series.ClampSlice(s.Offset, s.Length, input.Height())
	return input.Slice(off, length)
}

func executeRename(ctx context.Context, cfg execConfig, r Rename) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, r.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Rename(r.Old, r.New)
}

func executeDrop(ctx context.Context, cfg execConfig, d Drop) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, d.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	return input.Drop(d.Columns...), nil
}
