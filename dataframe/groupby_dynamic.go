package dataframe

import (
	"context"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

// DynamicGroupOptions configures GroupByDynamic. Empty strings select
// the polars defaults noted on each field.
type DynamicGroupOptions struct {
	// Every is the interval between window starts ("1h", "1d", "1mo",
	// "3i" for integer index columns). Required.
	Every string
	// Period is the window length. Defaults to Every.
	Period string
	// Offset shifts the window starts. Defaults to "0ns".
	Offset string
	// Closed is "left" (default), "right", "both" or "none".
	Closed string
	// Label is "left" (default), "right" or "datapoint".
	Label string
	// IncludeBoundaries adds _lower_boundary and _upper_boundary.
	IncludeBoundaries bool
	// GroupBy partitions the frame before windowing.
	GroupBy []string
	// StartBy is "window" (default), "datapoint" or a weekday name.
	StartBy string
}

// RollingGroupOptions configures Rolling.
type RollingGroupOptions struct {
	// Period is the window length ("2h", "3d", "5i"). Required.
	Period string
	// Offset shifts the window; defaults to -Period so each window ends
	// at its row.
	Offset string
	// Closed is "right" (default), "left", "both" or "none".
	Closed string
	// GroupBy partitions the frame before windowing.
	GroupBy []string
}

// DynamicGroupBy is a pending time-window group-by. Call Agg.
type DynamicGroupBy struct {
	df    *DataFrame
	index string
	opts  DynamicGroupOptions
}

// RollingGroupBy is a pending rolling (one window per row) group-by.
type RollingGroupBy struct {
	df    *DataFrame
	index string
	opts  RollingGroupOptions
}

// GroupByDynamic groups rows into time windows of the index column,
// mirroring polars' DataFrame.group_by_dynamic. The index column must
// be sorted ascending (within each GroupBy partition).
func (df *DataFrame) GroupByDynamic(index string, o DynamicGroupOptions) *DynamicGroupBy {
	return &DynamicGroupBy{df: df, index: index, opts: o}
}

// Rolling creates one window per row ending at that row's index value
// (polars' DataFrame.rolling). The index column must be sorted.
func (df *DataFrame) Rolling(index string, o RollingGroupOptions) *RollingGroupBy {
	return &RollingGroupBy{df: df, index: index, opts: o}
}

// windowAgg is one parsed aggregation of a windowed group-by.
type windowAgg struct {
	col, op, out string
	opts         series.RangeAggOptions
}

// parseWindowAggs accepts col(x).<agg>() shapes (sum, mean, min, max,
// count, null_count, first, last, median, std, var, quantile,
// n_unique), optionally aliased.
func parseWindowAggs(aggs []expr.Expr) ([]windowAgg, error) {
	out := make([]windowAgg, 0, len(aggs))
	for _, e := range aggs {
		name := expr.OutputName(e)
		node := e.Node()
		if a, ok := node.(expr.AliasNode); ok {
			node = a.Inner.Node()
		}
		wa := windowAgg{out: name, opts: series.RangeAggOptions{Ddof: 1}}
		switch n := node.(type) {
		case expr.AggNode:
			c, ok := n.Inner.Node().(expr.ColNode)
			if !ok {
				return nil, fmt.Errorf("group_by_dynamic: aggregation input for %s must be a bare column", e)
			}
			wa.col, wa.op = c.Name, n.Op.String()
		case expr.FunctionNode:
			if len(n.Args) != 1 {
				return nil, fmt.Errorf("group_by_dynamic: unsupported aggregation %s", e)
			}
			c, ok := n.Args[0].Node().(expr.ColNode)
			if !ok {
				return nil, fmt.Errorf("group_by_dynamic: aggregation input for %s must be a bare column", e)
			}
			wa.col = c.Name
			switch n.Name {
			case "median", "std", "var", "n_unique":
				wa.op = n.Name
			case "quantile":
				wa.op = "quantile"
				wa.opts.Interpolation = "nearest"
				if len(n.Params) > 0 {
					wa.opts.Quantile, _ = n.Params[0].(float64)
				}
			default:
				return nil, fmt.Errorf("group_by_dynamic: unsupported aggregation %s", e)
			}
		default:
			return nil, fmt.Errorf("group_by_dynamic: expression %s must be an aggregation", e)
		}
		out = append(out, wa)
	}
	return out, nil
}

// partitioned is the frame sorted by the group keys (stable) plus the
// row ranges of each key partition.
type partitioned struct {
	df     *DataFrame
	bounds [][2]int // [start, end) per partition
}

func partitionByKeys(ctx context.Context, df *DataFrame, keys []string, mem memory.Allocator) (*partitioned, error) {
	n := df.Height()
	if len(keys) == 0 {
		return &partitioned{df: df.Clone(), bounds: [][2]int{{0, n}}}, nil
	}
	cols := make([]*series.Series, len(keys))
	sopts := make([]compute.SortOptions, len(keys))
	for i, k := range keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		cols[i] = c
		sopts[i] = compute.SortOptions{Nulls: compute.NullsFirst}
	}
	perm, err := compute.SortIndicesMulti(ctx, cols, sopts, compute.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	sorted, err := takeFrame(ctx, df, perm, mem)
	if err != nil {
		return nil, err
	}
	keyArrs := make([]*series.Series, len(keys))
	for i, k := range keys {
		keyArrs[i], _ = sorted.Column(k)
	}
	var bounds [][2]int
	start := 0
	for i := 1; i <= n; i++ {
		if i == n || !sameKeys(keyArrs, i-1, i) {
			bounds = append(bounds, [2]int{start, i})
			start = i
		}
	}
	return &partitioned{df: sorted, bounds: bounds}, nil
}

func sameKeys(cols []*series.Series, a, b int) bool {
	for _, c := range cols {
		arr := c.Chunk(0)
		if arr.IsNull(a) != arr.IsNull(b) {
			return false
		}
		if arr.IsNull(a) {
			continue
		}
		if !arrowValuesEqual(arr, a, b) {
			return false
		}
	}
	return true
}

func takeFrame(ctx context.Context, df *DataFrame, idx []int, mem memory.Allocator) (*DataFrame, error) {
	cols := make([]*series.Series, 0, df.Width())
	for _, c := range df.Columns() {
		t, err := compute.Take(ctx, c, idx, compute.WithAllocator(mem))
		if err != nil {
			for _, x := range cols {
				x.Release()
			}
			return nil, err
		}
		cols = append(cols, t)
	}
	return New(cols...)
}

// windowSetup parses the shared duration arguments and the timeline.
func windowTimeline(df *DataFrame, index string) (series.Timeline, *series.Series, error) {
	idx, err := df.Column(index)
	if err != nil {
		return series.Timeline{}, nil, err
	}
	tl, err := series.TimelineOf(idx)
	if err != nil {
		return series.Timeline{}, nil, err
	}
	return tl, idx, nil
}

func parseDur(s, def string) (temporal.Duration, error) {
	if s == "" {
		s = def
	}
	return temporal.ParseDuration(s)
}

// Agg runs the aggregations per window and returns the result frame:
// group keys, optional boundaries, the index label, then aggregations.
func (g *DynamicGroupBy) Agg(ctx context.Context, aggs []expr.Expr, opts ...GroupByOption) (*DataFrame, error) {
	cfg := resolveGroupBy(opts)
	mem := cfg.alloc
	o := g.opts
	if o.Every == "" {
		return nil, fmt.Errorf("group_by_dynamic: `every` is required")
	}
	every, err := temporal.ParseDuration(o.Every)
	if err != nil {
		return nil, err
	}
	if every.Negative {
		return nil, fmt.Errorf("'every' argument must be positive")
	}
	period := every
	if o.Period != "" {
		if period, err = temporal.ParseDuration(o.Period); err != nil {
			return nil, err
		}
	}
	offset, err := parseDur(o.Offset, "0ns")
	if err != nil {
		return nil, err
	}
	closed, err := temporal.ParseClosed(orDefault(o.Closed, "left"))
	if err != nil {
		return nil, err
	}
	startBy, err := temporal.ParseStartBy(o.StartBy)
	if err != nil {
		return nil, err
	}
	label := orDefault(o.Label, "left")
	if label != "left" && label != "right" && label != "datapoint" {
		return nil, fmt.Errorf("invalid label %q: expected 'left', 'right' or 'datapoint'", label)
	}
	specs, err := parseWindowAggs(aggs)
	if err != nil {
		return nil, err
	}
	part, err := partitionByKeys(ctx, g.df, o.GroupBy, mem)
	if err != nil {
		return nil, err
	}
	defer part.df.Release()
	tl, idxCol, err := windowTimeline(part.df, g.index)
	if err != nil {
		return nil, err
	}
	for _, d := range []struct {
		d    temporal.Duration
		name string
	}{{every, "every"}, {period, "period"}, {offset, "offset"}} {
		if err := tl.CheckDuration(d.d, d.name); err != nil {
			return nil, err
		}
	}
	unit := tl.Unit
	z, err := temporal.NewZone(tl.TZ)
	if err != nil {
		return nil, err
	}
	w := temporal.Window{Every: every, Period: period, Offset: offset}
	var starts, lens []int
	var lower, upper []int64
	var groupFirst []int // row of the partition start, for key columns
	for _, b := range part.bounds {
		times := tl.Values[b[0]:b[1]]
		if len(times) == 0 {
			continue
		}
		if !slices.IsSorted(times) {
			return nil, fmt.Errorf("argument in operation 'group_by_dynamic' is not sorted, please sort the 'expr/series/column' first")
		}
		wg, err := temporal.GroupByWindows(w, times, closed, unit, z, startBy)
		if err != nil {
			return nil, err
		}
		for i := range wg.Starts {
			starts = append(starts, wg.Starts[i]+b[0])
			lens = append(lens, wg.Lens[i])
			groupFirst = append(groupFirst, b[0])
		}
		lower = append(lower, wg.Lower...)
		upper = append(upper, wg.Upper...)
	}
	var out []*series.Series
	release := func() {
		for _, s := range out {
			s.Release()
		}
	}
	for _, k := range o.GroupBy {
		c, err := part.df.Column(k)
		if err != nil {
			release()
			return nil, err
		}
		kc, err := compute.Take(ctx, c, groupFirst, compute.WithAllocator(mem))
		if err != nil {
			release()
			return nil, err
		}
		out = append(out, kc)
	}
	boundDT := dtype.Datetime(unit, tl.TZ)
	if tl.IsInt {
		boundDT = dtype.Int64()
	}
	if o.IncludeBoundaries {
		lb, err := physicalSeries("_lower_boundary", lower, boundDT, mem)
		if err != nil {
			release()
			return nil, err
		}
		out = append(out, lb)
		ub, err := physicalSeries("_upper_boundary", upper, boundDT, mem)
		if err != nil {
			release()
			return nil, err
		}
		out = append(out, ub)
	}
	var labelCol *series.Series
	switch label {
	case "left":
		labelCol, err = labelSeries(g.index, lower, idxCol.DType(), unit, mem)
	case "right":
		labelCol, err = labelSeries(g.index, upper, idxCol.DType(), unit, mem)
	default:
		labelCol, err = compute.Take(ctx, idxCol, starts, compute.WithAllocator(mem))
	}
	if err != nil {
		release()
		return nil, err
	}
	out = append(out, labelCol)
	for _, sp := range specs {
		c, err := part.df.Column(sp.col)
		if err != nil {
			release()
			return nil, err
		}
		a, err := series.AggRanges(c, sp.op, starts, lens, sp.opts, series.WithAllocator(mem))
		if err != nil {
			release()
			return nil, fmt.Errorf("agg %s: %w", sp.out, err)
		}
		if a.Name() != sp.out {
			r := a.Rename(sp.out)
			a.Release()
			a = r
		}
		out = append(out, a)
	}
	res, err := New(out...)
	if err != nil {
		release()
		return nil, err
	}
	return res, nil
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// physicalSeries wraps int64 physical values as dt.
func physicalSeries(name string, vals []int64, dt dtype.DType, mem memory.Allocator) (*series.Series, error) {
	if dt.IsDatetime() {
		u, _ := dt.TimeUnit()
		return series.FromDatetime(name, vals, nil, u, dt.TimeZone(), series.WithAllocator(mem))
	}
	return series.FromInt64(name, vals, nil, series.WithAllocator(mem))
}

// labelSeries converts window bounds (timeline ticks) back to the dtype
// of the index column.
func labelSeries(name string, vals []int64, idxDT dtype.DType, unit dtype.TimeUnit, mem memory.Allocator) (*series.Series, error) {
	switch {
	case idxDT.IsDate():
		per := temporal.UnitsPerDay(unit)
		days := make([]int32, len(vals))
		for i, v := range vals {
			days[i] = int32(temporal.FloorDiv(v, per))
		}
		return series.FromDate(name, days, nil, series.WithAllocator(mem))
	case idxDT.IsDatetime():
		return series.FromDatetime(name, vals, nil, unit, idxDT.TimeZone(), series.WithAllocator(mem))
	}
	s, err := series.FromInt64(name, vals, nil, series.WithAllocator(mem))
	if err != nil || idxDT.Equal(dtype.Int64()) {
		return s, err
	}
	defer s.Release()
	return compute.Cast(context.Background(), s, idxDT, compute.WithAllocator(mem))
}

// Agg runs the aggregations over each row's window and returns the
// group keys, the index column and one column per aggregation.
func (g *RollingGroupBy) Agg(ctx context.Context, aggs []expr.Expr, opts ...GroupByOption) (*DataFrame, error) {
	cfg := resolveGroupBy(opts)
	mem := cfg.alloc
	o := g.opts
	if o.Period == "" {
		return nil, fmt.Errorf("rolling: `period` is required")
	}
	period, err := temporal.ParseDuration(o.Period)
	if err != nil {
		return nil, err
	}
	if period.IsZero() || period.Negative {
		return nil, fmt.Errorf("rolling window period should be strictly positive")
	}
	offset := period.Neg()
	if o.Offset != "" {
		if offset, err = temporal.ParseDuration(o.Offset); err != nil {
			return nil, err
		}
	}
	closed, err := temporal.ParseClosed(orDefault(o.Closed, "right"))
	if err != nil {
		return nil, err
	}
	specs, err := parseWindowAggs(aggs)
	if err != nil {
		return nil, err
	}
	part, err := partitionByKeys(ctx, g.df, o.GroupBy, mem)
	if err != nil {
		return nil, err
	}
	defer part.df.Release()
	tl, idxCol, err := windowTimeline(part.df, g.index)
	if err != nil {
		return nil, err
	}
	if err := tl.CheckDuration(period, "period"); err != nil {
		return nil, err
	}
	if err := tl.CheckDuration(offset, "offset"); err != nil {
		return nil, err
	}
	z, err := temporal.NewZone(tl.TZ)
	if err != nil {
		return nil, err
	}
	n := part.df.Height()
	starts := make([]int, n)
	lens := make([]int, n)
	for _, b := range part.bounds {
		times := tl.Values[b[0]:b[1]]
		if !slices.IsSorted(times) {
			return nil, fmt.Errorf("argument in operation 'rolling' is not sorted, please sort the 'expr/series/column' first")
		}
		if err := temporal.GroupByValues(period, offset, times, closed, tl.Unit, z, starts[b[0]:b[1]], lens[b[0]:b[1]]); err != nil {
			return nil, err
		}
		for i := b[0]; i < b[1]; i++ {
			starts[i] += b[0]
		}
	}
	var out []*series.Series
	release := func() {
		for _, s := range out {
			s.Release()
		}
	}
	for _, k := range o.GroupBy {
		c, err := part.df.Column(k)
		if err != nil {
			release()
			return nil, err
		}
		out = append(out, c.Clone())
	}
	out = append(out, idxCol.Clone())
	for _, sp := range specs {
		c, err := part.df.Column(sp.col)
		if err != nil {
			release()
			return nil, err
		}
		a, err := series.AggRanges(c, sp.op, starts, lens, sp.opts, series.WithAllocator(mem))
		if err != nil {
			release()
			return nil, fmt.Errorf("agg %s: %w", sp.out, err)
		}
		if a.Name() != sp.out {
			r := a.Rename(sp.out)
			a.Release()
			a = r
		}
		out = append(out, a)
	}
	res, err := New(out...)
	if err != nil {
		release()
		return nil, err
	}
	return res, nil
}
