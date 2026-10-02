package eval

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func init() {
	dataframe.GenericAgg = func(ctx context.Context, df *dataframe.DataFrame, keys []string, aggs []expr.Expr, alloc memory.Allocator) (*dataframe.DataFrame, error) {
		return GroupByAgg(ctx, EvalContext{Alloc: alloc}, df, keys, aggs)
	}
}

// groupLayout describes a grouping of a frame's rows: perm lists the
// rows grouped together (group g owns perm[offsets[g]:offsets[g+1]]),
// and groups are ordered by ascending key.
type groupLayout struct {
	perm    []int
	offsets []int
	first   []int // first row (in input order) of each group
}

// computeGroupLayout groups df's rows by the key columns. Groups are
// sorted by key (nulls last) so the output order matches the other
// group-by paths.
func computeGroupLayout(ctx context.Context, ec EvalContext, keyCols []*series.Series, height int) (groupLayout, error) {
	gids, numGroups := groupIDsFor(keyCols, height)
	first := make([]int, numGroups)
	for i := range first {
		first[i] = -1
	}
	for r, g := range gids {
		if first[g] < 0 {
			first[g] = r
		}
	}
	// Order groups by key value.
	order := make([]int, numGroups)
	for i := range order {
		order[i] = i
	}
	if numGroups > 1 {
		reps := make([]*series.Series, len(keyCols))
		for i, kc := range keyCols {
			r, err := kc.Gather(first, seriesAlloc(ec))
			if err != nil {
				releaseAll(reps)
				return groupLayout{}, err
			}
			reps[i] = r
		}
		sortOpts := make([]compute.SortOptions, len(reps))
		sorted, err := compute.SortIndicesMulti(ctx, reps, sortOpts, kernelOpts(ec)...)
		releaseAll(reps)
		if err == nil {
			order = sorted
		}
		// Unsortable key dtypes keep first-seen order.
	}
	rank := make([]int, numGroups)
	for pos, g := range order {
		rank[g] = pos
	}
	counts := make([]int, numGroups+1)
	for _, g := range gids {
		counts[rank[g]+1]++
	}
	for i := 1; i <= numGroups; i++ {
		counts[i] += counts[i-1]
	}
	offsets := append([]int(nil), counts...)
	perm := make([]int, height)
	for r, g := range gids {
		pos := rank[g]
		perm[counts[pos]] = r
		counts[pos]++
	}
	firstSorted := make([]int, numGroups)
	for pos, g := range order {
		firstSorted[pos] = first[g]
	}
	return groupLayout{perm: perm, offsets: offsets, first: firstSorted}, nil
}

// groupIDsFor assigns first-seen group ids to every row.
func groupIDsFor(keyCols []*series.Series, height int) ([]int, int) {
	multi := false
	for _, kc := range keyCols {
		if kc.NumChunks() > 1 {
			multi = true
		}
	}
	if multi {
		// The fast paths index the first chunk by row, so multi-chunk
		// keys are made contiguous first. Rechunk can leave several
		// chunks (dictionaries that do not concatenate); those go
		// through the generic chunk-aware path.
		one := make([]*series.Series, len(keyCols))
		for i, k := range keyCols {
			one[i] = k.Rechunk()
		}
		defer releaseAll(one)
		keyCols = one
		for _, kc := range keyCols {
			if kc.NumChunks() > 1 {
				return genericGroupIDs(keyCols, height)
			}
		}
	}
	if len(keyCols) == 1 && keyCols[0].NumChunks() == 1 {
		if ids, n, ok := singleKeyIDs(keyCols[0].Chunk(0), height); ok {
			return ids, n
		}
	}
	if dataframe.SupportedKeyTuple(keyCols) {
		return assignOverGroupsBinary(keyCols, height)
	}
	return genericGroupIDs(keyCols, height)
}

// genericGroupIDs keys rows by their rendered values. It reads every
// chunk of the key columns.
func genericGroupIDs(keyCols []*series.Series, height int) ([]int, int) {
	gids := make([]int, height)
	table := make(map[string]int)
	type cursor struct {
		chunks  []arrow.Array
		ci, off int
	}
	curs := make([]cursor, len(keyCols))
	for i, kc := range keyCols {
		curs[i] = cursor{chunks: kc.Chunks()}
	}
	var buf []byte
	for r := range height {
		buf = buf[:0]
		for k := range curs {
			cu := &curs[k]
			for cu.ci < len(cu.chunks) && r-cu.off >= cu.chunks[cu.ci].Len() {
				cu.off += cu.chunks[cu.ci].Len()
				cu.ci++
			}
			if cu.ci >= len(cu.chunks) {
				buf = append(buf, 0, 0x1f)
				continue
			}
			c, i := cu.chunks[cu.ci], r-cu.off
			if c.IsNull(i) {
				buf = append(buf, 0)
			} else {
				buf = append(buf, 1)
				if raw, ok := physicalKeyBytes(c, i); ok {
					buf = append(buf, raw...)
				} else {
					buf = appendFallbackKey(buf, series.ValueAt(c, i))
				}
			}
			buf = append(buf, 0x1f)
		}
		if id, ok := table[string(buf)]; ok {
			gids[r] = id
		} else {
			id := len(table)
			table[string(buf)] = id
			gids[r] = id
		}
	}
	return gids, len(table)
}

// physicalKeyBytes returns the raw value bytes of row r for integer
// and temporal arrays. Going through series.ValueAt instead converts
// durations and times to time.Duration, which overflows for large
// microsecond values and made distinct keys collide (found by
// internal/difftest). Floats keep the rendered path so -0.0 and NaN
// fold together.
func physicalKeyBytes(a arrow.Array, r int) ([]byte, bool) {
	switch a.DataType().ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME32, arrow.TIME64:
	default:
		return nil, false
	}
	w := a.DataType().(arrow.FixedWidthDataType).BitWidth() / 8
	d := a.Data()
	start := (d.Offset() + r) * w
	return d.Buffers()[1].Bytes()[start : start+w], true
}

// appendFallbackKey renders one non-null key value for the generic
// grouping map. -0.0 folds into 0.0 (polars groups them together; NaNs
// already print alike), and the value is length prefixed so separator
// bytes inside a string cannot make two different tuples collide.
func appendFallbackKey(buf []byte, v any) []byte {
	if f, ok := v.(float64); ok && f == 0 {
		v = 0.0
	}
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	buf = fmt.Appendf(buf, "%v", v)
	binary.LittleEndian.PutUint32(buf[start:], uint32(len(buf)-start-4))
	return buf
}

// GroupByAgg evaluates arbitrary expressions per group. Each expression
// sees a sub-frame holding just the group's rows, so filter, sort_by,
// head, rank and friends behave as in polars' aggregation context.
// Expressions that reduce to a scalar produce one value per group;
// others produce a list column. Output rows are sorted by key.
func GroupByAgg(ctx context.Context, ec EvalContext, df *dataframe.DataFrame, keys []string, aggs []expr.Expr) (*dataframe.DataFrame, error) {
	if ec.Alloc == nil {
		ec.Alloc = memory.DefaultAllocator
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("eval: group_by requires at least one key")
	}
	height := df.Height()
	keyCols := make([]*series.Series, len(keys))
	for i, k := range keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		keyCols[i] = c
	}
	layout, err := computeGroupLayout(ctx, ec, keyCols, height)
	if err != nil {
		return nil, err
	}
	numGroups := len(layout.first)

	// Materialise only the columns the aggregations read, permuted so each
	// group is a contiguous zero-copy slice.
	names := df.Schema().Names()
	complete := true
	var needed []string
	seen := map[string]bool{}
	for _, e := range aggs {
		cols, ok := expr.ReferencedColumns(e)
		if !ok {
			complete = false
			break
		}
		for _, c := range cols {
			if !seen[c] {
				seen[c] = true
				needed = append(needed, c)
			}
		}
	}
	if complete {
		names = needed
	}
	permCols := make([]*series.Series, 0, len(names))
	for _, name := range names {
		c, err := df.Column(name)
		if err != nil {
			releaseAll(permCols)
			return nil, err
		}
		g, err := c.Gather(layout.perm, seriesAlloc(ec))
		if err != nil {
			releaseAll(permCols)
			return nil, err
		}
		permCols = append(permCols, g)
	}
	var sorted *dataframe.DataFrame
	if len(permCols) > 0 {
		sorted, err = dataframe.New(permCols...)
		if err != nil {
			releaseAll(permCols)
			return nil, err
		}
	} else {
		// No column references (e.g. pl.len()): a zero-width frame still
		// needs the right height, so carry the first key.
		kc, err := keyCols[0].Gather(layout.perm, seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		sorted, err = dataframe.New(kc)
		if err != nil {
			kc.Release()
			return nil, err
		}
	}
	defer sorted.Release()

	outCols := make([]*series.Series, 0, len(keys)+len(aggs))
	fail := func(err error) (*dataframe.DataFrame, error) {
		releaseAll(outCols)
		return nil, err
	}
	for _, kc := range keyCols {
		g, err := kc.Gather(layout.first, seriesAlloc(ec))
		if err != nil {
			return fail(err)
		}
		outCols = append(outCols, g)
	}
	for _, e := range aggs {
		col, err := aggOverGroups(ctx, ec, e, sorted, layout.offsets, numGroups)
		if err != nil {
			return fail(fmt.Errorf("agg %s: %w", e, err))
		}
		outCols = append(outCols, col)
	}
	out, err := dataframe.New(outCols...)
	if err != nil {
		return fail(err)
	}
	return out, nil
}

// aggOverGroups evaluates e once per group slice and assembles either a
// scalar column or a list column.
func aggOverGroups(ctx context.Context, ec EvalContext, e expr.Expr, sorted *dataframe.DataFrame, offsets []int, numGroups int) (*series.Series, error) {
	name := expr.OutputName(e)
	if expr.IsLiteralOnly(e) {
		// A literal-only expression (lit(1), lit("a").shift(1)) has
		// the same unit-length value in every group, and polars gives
		// it one scalar per group rather than a list.
		res, err := evalNode(ctx, ec, e, sorted)
		if err != nil {
			return nil, err
		}
		if res.Len() == 1 {
			defer res.Release()
			return res.Broadcast(numGroups, seriesAlloc(ec))
		}
		res.Release()
	}
	scalar := expr.ReturnsScalar(e)
	parts := make([]*series.Series, 0, numGroups)
	defer func() { releaseAll(parts) }()
	for g := range numGroups {
		sub, err := sorted.Slice(offsets[g], offsets[g+1]-offsets[g])
		if err != nil {
			return nil, err
		}
		gec := ec
		gec.inGroup = true
		res, err := evalNode(ctx, gec, e, sub)
		sub.Release()
		if err != nil {
			return nil, err
		}
		if scalar && res.Len() != 1 {
			if res.Len() == 0 {
				nullRes, err := series.FullNull(name, res.Chunked().DataType(), 1, seriesAlloc(ec))
				res.Release()
				if err != nil {
					return nil, err
				}
				res = nullRes
			} else {
				n := res.Len()
				res.Release()
				return nil, fmt.Errorf("expected one value per group, got %d", n)
			}
		}
		parts = append(parts, res)
	}
	if numGroups == 0 {
		// Evaluate on the empty frame to learn the output dtype.
		res, err := evalNode(ctx, ec, e, sorted)
		if err != nil {
			return nil, err
		}
		defer res.Release()
		if scalar {
			return series.FullNull(name, res.Chunked().DataType(), 0, seriesAlloc(ec))
		}
		empty, err := series.FullNull(name, res.Chunked().DataType(), 0, seriesAlloc(ec))
		if err != nil {
			return nil, err
		}
		defer empty.Release()
		return series.ListFromInt32Offsets(name, empty, []int32{0}, nil, seriesAlloc(ec))
	}
	values, err := concatHarmonized(ctx, ec, name, parts)
	if err != nil {
		return nil, err
	}
	if scalar {
		return values, nil
	}
	defer values.Release()
	offs := make([]int32, numGroups+1)
	for i, p := range parts {
		offs[i+1] = offs[i] + int32(p.Len())
	}
	return series.ListFromInt32Offsets(name, values, offs, nil, seriesAlloc(ec))
}

// concatHarmonized concatenates per-group results, casting parts whose
// dtype differs from the first non-null dtype.
func concatHarmonized(ctx context.Context, ec EvalContext, name string, parts []*series.Series) (*series.Series, error) {
	var target arrow.DataType
	for _, p := range parts {
		dt := p.Chunked().DataType()
		if dt.ID() != arrow.NULL {
			target = dt
			break
		}
	}
	if target == nil {
		return series.ConcatSeries(name, parts, seriesAlloc(ec))
	}
	fixed := make([]*series.Series, len(parts))
	var owned []*series.Series
	defer func() { releaseAll(owned) }()
	for i, p := range parts {
		dt := p.Chunked().DataType()
		if arrow.TypeEqual(dt, target) {
			fixed[i] = p
			continue
		}
		var c *series.Series
		var err error
		if dt.ID() == arrow.NULL {
			c, err = series.FullNull(p.Name(), target, p.Len(), seriesAlloc(ec))
		} else {
			c, err = castTo(ctx, ec, p, target)
		}
		if err != nil {
			return nil, err
		}
		owned = append(owned, c)
		fixed[i] = c
	}
	return series.ConcatSeries(name, fixed, seriesAlloc(ec))
}

// castTo casts s to an arrow dtype through compute.Cast.
func castTo(ctx context.Context, ec EvalContext, s *series.Series, dt arrow.DataType) (*series.Series, error) {
	return compute.Cast(ctx, s, dtype.FromArrow(dt), kernelOpts(ec)...)
}
