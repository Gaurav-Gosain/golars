package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// GroupBy is a pending group-by operation that has not been materialized.
// Call Agg to produce a result DataFrame.
type GroupBy struct {
	df   *DataFrame
	keys []string
}

// GroupByOption configures Agg.
type GroupByOption func(*groupByConfig)

type groupByConfig struct {
	alloc memory.Allocator
}

func resolveGroupBy(opts []GroupByOption) groupByConfig {
	c := groupByConfig{alloc: memory.DefaultAllocator}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithGroupByAllocator overrides the allocator used by Agg.
func WithGroupByAllocator(alloc memory.Allocator) GroupByOption {
	return func(c *groupByConfig) { c.alloc = alloc }
}

// GroupBy returns a group-by builder keyed on the given columns.
func (df *DataFrame) GroupBy(keys ...string) *GroupBy {
	return &GroupBy{df: df, keys: keys}
}

// Agg materializes the group-by. Each aggregation expression must have the
// shape col(name).agg() or col(name).agg().alias(output_name), where agg is
// one of Sum, Mean, Min, Max, Count, NullCount, First, Last.
//
// To aggregate a computed expression, precompute it with WithColumns before
// grouping.
//
// Agg uses a sort-based algorithm: the input is sorted by the key columns,
// contiguous runs of equal keys form groups, and each aggregation is applied
// per group.
func (g *GroupBy) Agg(ctx context.Context, aggs []expr.Expr, opts ...GroupByOption) (*DataFrame, error) {
	if len(g.keys) == 0 {
		return nil, fmt.Errorf("dataframe.GroupBy: at least one key required")
	}
	// Empty aggs is valid: it emits the distinct key rows (polars-
	// compatible semantics, used by df.Unique). parseAggs returns an
	// empty slice so the downstream kernels handle it.
	specs, err := parseAggs(aggs)
	if err != nil {
		return nil, err
	}
	cfg := resolveGroupBy(opts)

	// Fast path: single-key hash groupby. O(n) vs the sort-based O(n log n);
	// measured ~80x faster on the GroupBySum benchmark. Falls through to the
	// sort path for unsupported dtype combinations.
	if len(g.keys) == 1 {
		if out, ok, err := hashAggSingleKey(ctx, g.df, g.keys[0], specs, cfg.alloc); err != nil {
			return nil, err
		} else if ok {
			return out, nil
		}
	}

	// Multi-key hash groupby: same O(n) assignment over encoded key
	// tuples, groups reordered to match the sort path. Falls through
	// for unsupported key dtypes.
	if len(g.keys) > 1 {
		if out, ok, err := hashAggMultiKey(ctx, g.df, g.keys, specs, cfg.alloc); err != nil {
			return nil, err
		} else if ok {
			return out, nil
		}
	}

	sortOpts := make([]compute.SortOptions, len(g.keys))
	sorted, err := g.df.SortBy(ctx, g.keys, sortOpts, WithSortAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	defer sorted.Release()

	boundaries, err := groupBoundaries(sorted, g.keys)
	if err != nil {
		return nil, err
	}

	keyCols := make([]*series.Series, len(g.keys))
	for i, k := range g.keys {
		src, err := sorted.Column(k)
		if err != nil {
			for _, kc := range keyCols[:i] {
				if kc != nil {
					kc.Release()
				}
			}
			return nil, err
		}
		col, err := compute.Take(ctx, src, boundaries, compute.WithAllocator(cfg.alloc))
		if err != nil {
			for _, kc := range keyCols[:i] {
				if kc != nil {
					kc.Release()
				}
			}
			return nil, err
		}
		keyCols[i] = col
	}

	aggCols := make([]*series.Series, len(specs))
	for i, sp := range specs {
		out, err := runAggForGroups(ctx, cfg.alloc, sorted, sp, boundaries)
		if err != nil {
			for _, kc := range keyCols {
				kc.Release()
			}
			for _, ac := range aggCols[:i] {
				if ac != nil {
					ac.Release()
				}
			}
			return nil, fmt.Errorf("agg %s: %w", sp.original, err)
		}
		aggCols[i] = out
	}

	out := append([]*series.Series{}, keyCols...)
	out = append(out, aggCols...)
	return New(out...)
}

// aggSpec is the normalized form of an aggregation expression accepted by
// GroupBy.Agg.
type aggSpec struct {
	colName    string
	op         expr.AggOp
	outputName string
	original   expr.Expr
}

// parseAggs validates and normalizes each aggregation into an aggSpec. It
// accepts expressions of the form Col(name).AggOp() or the same wrapped in a
// single Alias. More complex shapes are rejected with a descriptive error.
func parseAggs(in []expr.Expr) ([]aggSpec, error) {
	out := make([]aggSpec, len(in))
	for i, e := range in {
		node := e.Node()
		outputName := expr.OutputName(e)
		if alias, ok := node.(expr.AliasNode); ok {
			node = alias.Inner.Node()
		}
		agg, ok := node.(expr.AggNode)
		if !ok {
			return nil, fmt.Errorf("GroupBy.Agg: expression %s must be an aggregation", e)
		}
		col, ok := agg.Inner.Node().(expr.ColNode)
		if !ok {
			return nil, fmt.Errorf("GroupBy.Agg: aggregation input for %s must be a bare column (use WithColumns to precompute)", e)
		}
		out[i] = aggSpec{
			colName:    col.Name,
			op:         agg.Op,
			outputName: outputName,
			original:   e,
		}
	}
	return out, nil
}

// runAggForGroups applies sp's aggregation across every group defined by
// boundaries on sorted, producing a column with one row per group.
func runAggForGroups(ctx context.Context, alloc memory.Allocator, sorted *DataFrame, sp aggSpec, boundaries []int) (*series.Series, error) {
	col, err := sorted.Column(sp.colName)
	if err != nil {
		return nil, err
	}
	// Same shape as the source so we can dispatch over dtypes once.
	switch sp.op {
	case expr.AggCount:
		return countGroups(boundaries, sorted.Height(), col, sp.outputName, alloc)
	case expr.AggNullCount:
		return nullCountGroups(boundaries, sorted.Height(), col, sp.outputName, alloc)
	}

	switch col.DType().ID() {
	case arrow.INT32, arrow.INT64, arrow.UINT32, arrow.UINT64:
		return aggIntegerGroups(ctx, alloc, sorted, col, sp, boundaries)
	case arrow.FLOAT32, arrow.FLOAT64:
		return aggFloatGroups(ctx, alloc, sorted, col, sp, boundaries)
	case arrow.STRING, arrow.BOOL:
		return aggPassthroughGroups(ctx, alloc, sorted, col, sp, boundaries)
	}
	return nil, fmt.Errorf("GroupBy: unsupported dtype %s for %s", col.DType(), sp.op)
}

func countGroups(boundaries []int, total int, col *series.Series, name string, alloc memory.Allocator) (*series.Series, error) {
	arr := col.Chunk(0)
	out := make([]int64, len(boundaries))
	if arr.NullN() == 0 {
		// No nulls: every row counts, no per-row scan.
		for i, start := range boundaries {
			out[i] = int64(groupEnd(boundaries, i, total) - start)
		}
		return series.FromInt64(name, out, nil, series.WithAllocator(alloc))
	}
	for i, start := range boundaries {
		end := groupEnd(boundaries, i, total)
		var n int64
		for j := start; j < end; j++ {
			if arr.IsValid(j) {
				n++
			}
		}
		out[i] = n
	}
	return series.FromInt64(name, out, nil, series.WithAllocator(alloc))
}

func nullCountGroups(boundaries []int, total int, col *series.Series, name string, alloc memory.Allocator) (*series.Series, error) {
	arr := col.Chunk(0)
	out := make([]int64, len(boundaries))
	if arr.NullN() == 0 {
		// No nulls: every group contributes zero.
		return series.FromInt64(name, out, nil, series.WithAllocator(alloc))
	}
	for i, start := range boundaries {
		end := groupEnd(boundaries, i, total)
		var n int64
		for j := start; j < end; j++ {
			if arr.IsNull(j) {
				n++
			}
		}
		out[i] = n
	}
	return series.FromInt64(name, out, nil, series.WithAllocator(alloc))
}

// aggIntegerGroups applies an integer-valued aggregation per group. Output
// dtype is i64 for sum; f64 for mean; source dtype for min, max, first, last.
func aggIntegerGroups(ctx context.Context, alloc memory.Allocator, sorted *DataFrame, col *series.Series, sp aggSpec, boundaries []int) (*series.Series, error) {
	total := sorted.Height()
	opts := []compute.Option{compute.WithAllocator(alloc)}

	switch sp.op {
	case expr.AggSum:
		out := make([]int64, len(boundaries))
		for i, start := range boundaries {
			end := groupEnd(boundaries, i, total)
			sub, err := col.Slice(start, end-start)
			if err != nil {
				return nil, err
			}
			v, err := compute.SumInt64(ctx, sub, opts...)
			sub.Release()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return series.FromInt64(sp.outputName, out, nil, series.WithAllocator(alloc))

	case expr.AggMean:
		out := make([]float64, len(boundaries))
		valid := make([]bool, len(boundaries))
		for i, start := range boundaries {
			end := groupEnd(boundaries, i, total)
			sub, err := col.Slice(start, end-start)
			if err != nil {
				return nil, err
			}
			v, ok, err := compute.MeanFloat64(ctx, sub, opts...)
			sub.Release()
			if err != nil {
				return nil, err
			}
			if ok {
				out[i] = v
				valid[i] = true
			}
		}
		anyNull := false
		for _, v := range valid {
			if !v {
				anyNull = true
				break
			}
		}
		if !anyNull {
			valid = nil
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(alloc))

	case expr.AggMin, expr.AggMax:
		out := make([]int64, len(boundaries))
		valid := make([]bool, len(boundaries))
		for i, start := range boundaries {
			end := groupEnd(boundaries, i, total)
			sub, err := col.Slice(start, end-start)
			if err != nil {
				return nil, err
			}
			var v int64
			var ok bool
			if sp.op == expr.AggMin {
				v, ok, err = compute.MinInt64(ctx, sub, opts...)
			} else {
				v, ok, err = compute.MaxInt64(ctx, sub, opts...)
			}
			sub.Release()
			if err != nil {
				return nil, err
			}
			if ok {
				out[i] = v
				valid[i] = true
			}
		}
		anyNull := false
		for _, v := range valid {
			if !v {
				anyNull = true
				break
			}
		}
		if !anyNull {
			valid = nil
		}
		return series.FromInt64(sp.outputName, out, valid, series.WithAllocator(alloc))

	case expr.AggFirst, expr.AggLast:
		return firstLastGroups(col, sp, boundaries, total, alloc)
	}
	return nil, fmt.Errorf("GroupBy: op %s not implemented for integer", sp.op)
}

func aggFloatGroups(ctx context.Context, alloc memory.Allocator, sorted *DataFrame, col *series.Series, sp aggSpec, boundaries []int) (*series.Series, error) {
	total := sorted.Height()
	opts := []compute.Option{compute.WithAllocator(alloc)}

	switch sp.op {
	case expr.AggSum, expr.AggMean, expr.AggMin, expr.AggMax:
		out := make([]float64, len(boundaries))
		valid := make([]bool, len(boundaries))
		for i, start := range boundaries {
			end := groupEnd(boundaries, i, total)
			sub, err := col.Slice(start, end-start)
			if err != nil {
				return nil, err
			}
			var v float64
			ok := true
			switch sp.op {
			case expr.AggSum:
				v, err = compute.SumFloat64(ctx, sub, opts...)
			case expr.AggMean:
				v, ok, err = compute.MeanFloat64(ctx, sub, opts...)
			case expr.AggMin:
				v, ok, err = compute.MinFloat64(ctx, sub, opts...)
			case expr.AggMax:
				v, ok, err = compute.MaxFloat64(ctx, sub, opts...)
			}
			sub.Release()
			if err != nil {
				return nil, err
			}
			if ok {
				out[i] = v
				valid[i] = true
			}
		}
		allValid := true
		for _, v := range valid {
			if !v {
				allValid = false
				break
			}
		}
		if allValid {
			valid = nil
		}
		return series.FromFloat64(sp.outputName, out, valid, series.WithAllocator(alloc))

	case expr.AggFirst, expr.AggLast:
		return firstLastGroups(col, sp, boundaries, total, alloc)
	}
	return nil, fmt.Errorf("GroupBy: op %s not implemented for float", sp.op)
}

// aggPassthroughGroups handles string and bool columns: only First, Last
// make sense semantically (ordering is not total here).
func aggPassthroughGroups(ctx context.Context, alloc memory.Allocator, sorted *DataFrame, col *series.Series, sp aggSpec, boundaries []int) (*series.Series, error) {
	_ = ctx
	total := sorted.Height()
	switch sp.op {
	case expr.AggFirst, expr.AggLast:
		return firstLastGroups(col, sp, boundaries, total, alloc)
	}
	return nil, fmt.Errorf("GroupBy: op %s not supported for %s", sp.op, col.DType())
}

// firstLastGroups gathers the first or last row of each group via Take.
func firstLastGroups(col *series.Series, sp aggSpec, boundaries []int, total int, alloc memory.Allocator) (*series.Series, error) {
	indices := make([]int, len(boundaries))
	for i, start := range boundaries {
		end := groupEnd(boundaries, i, total)
		if sp.op == expr.AggFirst {
			indices[i] = start
		} else {
			indices[i] = end - 1
		}
	}
	return compute.Take(context.Background(), col, indices, compute.WithAllocator(alloc))
}

func groupEnd(boundaries []int, i, total int) int {
	if i+1 < len(boundaries) {
		return boundaries[i+1]
	}
	return total
}

// groupBoundaries walks the sorted frame and returns the start index of each
// contiguous run of equal keys.
func groupBoundaries(sorted *DataFrame, keys []string) ([]int, error) {
	n := sorted.Height()
	if n == 0 {
		return nil, nil
	}

	keyArrs := make([]arrow.Array, len(keys))
	for i, k := range keys {
		col, err := sorted.Column(k)
		if err != nil {
			return nil, err
		}
		chunks := col.Chunks()
		if len(chunks) != 1 {
			return nil, fmt.Errorf("dataframe.GroupBy: expected single chunk after sort for key %q, got %d", k, len(chunks))
		}
		keyArrs[i] = chunks[0]
	}

	boundaries := make([]int, 0, 16)
	boundaries = append(boundaries, 0)
	for i := 1; i < n; i++ {
		if !rowKeysEqual(keyArrs, i-1, i) {
			boundaries = append(boundaries, i)
		}
	}
	return boundaries, nil
}

func rowKeysEqual(cols []arrow.Array, a, b int) bool {
	for _, c := range cols {
		aNull, bNull := c.IsNull(a), c.IsNull(b)
		if aNull != bNull {
			return false
		}
		if aNull && bNull {
			continue
		}
		if !arrowValuesEqual(c, a, b) {
			return false
		}
	}
	return true
}

func arrowValuesEqual(arr arrow.Array, a, b int) bool {
	switch x := arr.(type) {
	case *array.Int8:
		return x.Value(a) == x.Value(b)
	case *array.Int16:
		return x.Value(a) == x.Value(b)
	case *array.Int32:
		return x.Value(a) == x.Value(b)
	case *array.Int64:
		return x.Value(a) == x.Value(b)
	case *array.Uint8:
		return x.Value(a) == x.Value(b)
	case *array.Uint16:
		return x.Value(a) == x.Value(b)
	case *array.Uint32:
		return x.Value(a) == x.Value(b)
	case *array.Uint64:
		return x.Value(a) == x.Value(b)
	case *array.Float32:
		va, vb := x.Value(a), x.Value(b)
		if va != va && vb != vb {
			return true
		}
		return va == vb
	case *array.Float64:
		va, vb := x.Value(a), x.Value(b)
		if va != va && vb != vb {
			return true
		}
		return va == vb
	case *array.Boolean:
		return x.Value(a) == x.Value(b)
	case *array.String:
		return x.Value(a) == x.Value(b)
	case *array.Binary:
		av, bv := x.Value(a), x.Value(b)
		if len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
		return true
	}
	return false
}
