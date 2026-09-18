package lazy

import (
	"context"
	"fmt"
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
	switch node := n.(type) {
	case DataFrameScan:
		return executeScan(ctx, cfg, node)
	case SourceFunc:
		return node.Load(ctx)
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
	}
	return nil, fmt.Errorf("lazy: cannot execute node %T", n)
}

func executeAggregate(ctx context.Context, cfg execConfig, a Aggregate) (*dataframe.DataFrame, error) {
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
		out, err := projected.Slice(s.Offset, min(s.Length, projected.Height()-s.Offset))
		projected.Release()
		if err != nil {
			return nil, err
		}
		projected = out
	}
	return projected, nil
}

func executeProjection(ctx context.Context, cfg execConfig, p Projection) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, p.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	cols := make([]*series.Series, len(p.Exprs))
	if len(p.Exprs) >= parallelSelectMinExprs {
		// Fan out independent column evaluations. Each worker writes
		// its own slot; on error every completed slot is released.
		// WithColumns stays serial: later exprs observe earlier ones.
		g := pool.NewGroup(ctx, 0)
		for i, e := range p.Exprs {
			g.Go(func(gctx context.Context) error {
				s, err := eval.Eval(gctx, eval.EvalContext{Alloc: cfg.alloc}, e, input)
				if err != nil {
					return fmt.Errorf("projection %s: %w", e, err)
				}
				name := expr.OutputName(e)
				if s.Name() != name {
					renamed := s.Rename(name)
					s.Release()
					s = renamed
				}
				cols[i] = s
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			for _, c := range cols {
				if c != nil {
					c.Release()
				}
			}
			return nil, err
		}
		return dataframe.New(cols...)
	}
	for i, e := range p.Exprs {
		s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.alloc}, e, input)
		if err != nil {
			for _, c := range cols[:i] {
				if c != nil {
					c.Release()
				}
			}
			return nil, fmt.Errorf("projection %s: %w", e, err)
		}
		name := expr.OutputName(e)
		if s.Name() != name {
			renamed := s.Rename(name)
			s.Release()
			s = renamed
		}
		cols[i] = s
	}
	return dataframe.New(cols...)
}

// parallelSelectMinExprs is the minimum projection width that pays for
// goroutine fan-out. Below this the errgroup setup exceeds the win;
// kernels also parallelize rows internally at large heights, so width
// is the gating dimension.
const parallelSelectMinExprs = 8

func executeWithColumns(ctx context.Context, cfg execConfig, w WithColumns) (*dataframe.DataFrame, error) {
	input, err := executeNode(ctx, cfg, w.Input)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	out := input.Clone()
	for _, e := range w.Exprs {
		s, err := eval.Eval(ctx, eval.EvalContext{Alloc: cfg.alloc}, e, out)
		if err != nil {
			out.Release()
			return nil, err
		}
		name := expr.OutputName(e)
		if s.Name() != name {
			r := s.Rename(name)
			s.Release()
			s = r
		}
		updated, err := out.WithColumn(s)
		out.Release()
		if err != nil {
			s.Release()
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
	length := s.Length
	if s.Offset+length > input.Height() {
		length = input.Height() - s.Offset
	}
	if s.Offset < 0 || length < 0 {
		return nil, fmt.Errorf("%w: offset=%d length=%d", dataframe.ErrSliceOutOfBounds, s.Offset, length)
	}
	return input.Slice(s.Offset, length)
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
