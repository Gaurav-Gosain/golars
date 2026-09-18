package eval

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// evalOver implements `expr.over(keys...)`: evaluate the inner
// expression per group defined by keys and broadcast the result back
// to the original row positions.
//
// Shape rules mirror polars:
//   - scalar inner (length 1)   -> broadcast across every row of the group.
//   - length-height inner       -> gather the subset for the group.
//
// Non-matching shapes return an error.
func evalOver(ctx context.Context, ec EvalContext, n expr.OverNode, df *dataframe.DataFrame) (*series.Series, error) {
	if len(n.Keys) == 0 {
		// Without keys, over() degenerates to the inner expression.
		return evalNode(ctx, ec, n.Inner, df)
	}
	// Fast path: when the inner is a plain scalar aggregation
	// (sum/mean/min/max/count on a single column), avoid the
	// per-group materialise-and-eval loop entirely. We groupby
	// the keys, run the agg once, then hash-join the result back.
	if fast, err := tryScalarAggOver(ctx, ec, n, df); fast != nil || err != nil {
		return fast, err
	}
	// Fast path: cumulative sum over groups. Rows are counting-sorted
	// by group once, the value column is gathered once, and a single
	// segmented scan with per-group resets replaces the per-group
	// Take-and-eval loop.
	if fast, ok, err := tryCumSumOver(ctx, ec, n, df); ok || err != nil {
		return fast, err
	}
	height := df.Height()
	// Build per-row group key and a per-group row list.
	keyCols := make([]*series.Series, len(n.Keys))
	for i, k := range n.Keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, err
		}
		keyCols[i] = c
	}
	// Assign a group id per row, then bucket rows into exact-size lists
	// (no append-growth waste). Binary tuple keys avoid the per-row
	// decimal formatting and fresh string of overKey.
	var groupIDs []int
	var numGroups int
	if dataframe.SupportedKeyTuple(keyCols) {
		groupIDs, numGroups = assignOverGroupsBinary(keyCols, height)
	} else {
		groupIDs, numGroups = assignOverGroupsString(keyCols, height)
	}
	counts := make([]int, numGroups)
	for _, gid := range groupIDs {
		counts[gid]++
	}
	groupRows := make([][]int, numGroups)
	for gi, c := range counts {
		groupRows[gi] = make([]int, 0, c)
	}
	for r, gid := range groupIDs {
		groupRows[gid] = append(groupRows[gid], r)
	}
	// Only gather columns the inner reads; unknown shapes keep the full
	// frame. Sub-frames keep every row (Take preserves length), so row-
	// sensitive inners are unaffected.
	gatherNames := df.Schema().Names()
	if needed, ok := expr.ReferencedColumns(n.Inner); ok && len(needed) > 0 {
		gatherNames = needed
	}
	var out overOut
	out.height = height
	for _, rows := range groupRows {
		sub, err := gatherGroupFrameCols(ctx, df, rows, gatherNames)
		if err != nil {
			return nil, err
		}
		res, err := evalNode(ctx, ec, n.Inner, sub)
		sub.Release()
		if err != nil {
			return nil, err
		}
		resLen := res.Len()
		switch resLen {
		case 1:
			out.broadcast(res.Chunk(0), rows)
		default:
			if resLen != len(rows) {
				res.Release()
				return nil, fmt.Errorf("eval: over() inner produced len %d for %d rows", resLen, len(rows))
			}
			out.scatter(res.Chunk(0), rows)
		}
		res.Release()
	}
	return out.materialize(n.String(), seriesAllocOpt(ec))
}

// tryCumSumOver recognises pl.col("v").cum_sum().over(keys...): one
// counting sort by group, one gather of the value column, one
// segmented scan. Returns ok=false for any other shape (or key/value
// dtypes outside the binary encoding) so the generic path handles it.
func tryCumSumOver(ctx context.Context, ec EvalContext, n expr.OverNode, df *dataframe.DataFrame) (*series.Series, bool, error) {
	fn, ok := n.Inner.Node().(expr.FunctionNode)
	if !ok || fn.Name != "cum_sum" || len(fn.Args) != 1 {
		return nil, false, nil
	}
	col, ok := fn.Args[0].Node().(expr.ColNode)
	if !ok {
		return nil, false, nil
	}
	valCol, err := df.Column(col.Name)
	if err != nil {
		return nil, true, err
	}
	if valCol.NumChunks() != 1 {
		return nil, false, nil
	}
	// Streaming fused path: single int64 key without nulls. One pass
	// keeps a running total per group and writes each row in input
	// order: no permutation, no gather, no scatter-back.
	if len(n.Keys) == 1 {
		if fast, ok, err := tryCumSumOverSingleInt64(ctx, ec, n, df, col.Name); ok || err != nil {
			return fast, ok, err
		}
	}
	height := df.Height()
	keyCols := make([]*series.Series, len(n.Keys))
	for i, k := range n.Keys {
		c, err := df.Column(k)
		if err != nil {
			return nil, true, err
		}
		keyCols[i] = c
	}
	if !dataframe.SupportedKeyTuple(keyCols) {
		return nil, false, nil
	}
	groupIDs, numGroups := assignOverGroupsBinary(keyCols, height)
	// Counting sort rows by group: offsets, then a single fill pass.
	counts := make([]int, numGroups)
	for _, gid := range groupIDs {
		counts[gid]++
	}
	offsets := make([]int, numGroups+1)
	for g, c := range counts {
		offsets[g+1] = offsets[g] + c
	}
	perm := make([]int, height)
	next := append([]int(nil), offsets[:numGroups]...)
	for r, gid := range groupIDs {
		perm[next[gid]] = r
		next[gid]++
	}
	// Gather values once in group order (fresh offset-0 array), then a
	// fused segmented scan scatters results to original positions.
	gathered, err := compute.Take(ctx, valCol, perm, compute.WithAllocator(ec.Alloc))
	if err != nil {
		return nil, true, err
	}
	defer gathered.Release()
	gArr := gathered.Chunk(0)
	outName := n.String()
	switch a := gArr.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		hasNulls := a.NullN() > 0
		res, err := series.BuildFloat64DirectFused(outName, height, ec.Alloc, func(out []float64, validBits []byte) int {
			nulls := 0
			for g := range numGroups {
				var acc float64
				for i := offsets[g]; i < offsets[g+1]; i++ {
					if hasNulls && !a.IsValid(i) {
						nulls++
						continue
					}
					acc += float64(vals[i])
					out[perm[i]] = acc
					validBits[perm[i]>>3] |= 1 << uint(perm[i]&7)
				}
			}
			return nulls
		})
		return res, true, err
	case *array.Float64:
		vals := a.Float64Values()
		hasNulls := a.NullN() > 0
		res, err := series.BuildFloat64DirectFused(outName, height, ec.Alloc, func(out []float64, validBits []byte) int {
			nulls := 0
			for g := range numGroups {
				var acc float64
				for i := offsets[g]; i < offsets[g+1]; i++ {
					if hasNulls && !a.IsValid(i) {
						nulls++
						continue
					}
					acc += vals[i]
					out[perm[i]] = acc
					validBits[perm[i]>>3] |= 1 << uint(perm[i]&7)
				}
			}
			return nulls
		})
		return res, true, err
	case *array.Int32:
		vals := a.Int32Values()
		hasNulls := a.NullN() > 0
		res, err := series.BuildFloat64DirectFused(outName, height, ec.Alloc, func(out []float64, validBits []byte) int {
			nulls := 0
			for g := range numGroups {
				var acc float64
				for i := offsets[g]; i < offsets[g+1]; i++ {
					if hasNulls && !a.IsValid(i) {
						nulls++
						continue
					}
					acc += float64(vals[i])
					out[perm[i]] = acc
					validBits[perm[i]>>3] |= 1 << uint(perm[i]&7)
				}
			}
			return nulls
		})
		return res, true, err
	}
	return nil, false, nil
}

// assignOverGroupsBinary maps rows to first-seen group ids with binary
// tuple keys. Map reads of string(buf) allocate nothing; only new
// groups copy into the table.
func assignOverGroupsBinary(keyCols []*series.Series, height int) ([]int, int) {
	groupIDs := make([]int, height)
	table := make(map[string]int)
	var buf []byte
	for r := range height {
		buf, _ = dataframe.AppendKeyTuple(buf[:0], keyCols, r)
		if id, ok := table[string(buf)]; ok {
			groupIDs[r] = id
		} else {
			id := len(table)
			table[string(buf)] = id
			groupIDs[r] = id
		}
	}
	return groupIDs, len(table)
}

// assignOverGroupsString is the fallback for key dtypes outside the
// binary encoding.
func assignOverGroupsString(keyCols []*series.Series, height int) ([]int, int) {
	groupIDs := make([]int, height)
	table := make(map[string]int)
	for r := range height {
		key := overKey(keyCols, r)
		if id, ok := table[key]; ok {
			groupIDs[r] = id
		} else {
			id := len(table)
			table[key] = id
			groupIDs[r] = id
		}
	}
	return groupIDs, len(table)
}

// overKey formats a row's group tuple into a stable string.
func overKey(cols []*series.Series, row int) string {
	buf := make([]byte, 0, 32)
	for i, c := range cols {
		if i > 0 {
			buf = append(buf, 0x1f)
		}
		buf = appendCell(buf, c.Chunk(0), row)
	}
	return string(buf)
}

func appendCell(buf []byte, chunk any, i int) []byte {
	switch a := chunk.(type) {
	case *array.Int64:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendInt(buf, a.Value(i), 10)
	case *array.Int32:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendInt(buf, int64(a.Value(i)), 10)
	case *array.Float64:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return strconv.AppendFloat(buf, a.Value(i), 'g', -1, 64)
	case *array.String:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		return append(buf, a.Value(i)...)
	case *array.Boolean:
		if !a.IsValid(i) {
			return append(buf, 0xff, 'n')
		}
		if a.Value(i) {
			return append(buf, '1')
		}
		return append(buf, '0')
	}
	return buf
}

// gatherGroupFrameCols builds a sub-DataFrame containing only rows in
// the given indices for the named columns, one column at a time. Uses
// compute.Take per column.
func gatherGroupFrameCols(ctx context.Context, df *dataframe.DataFrame, rows []int, names []string) (*dataframe.DataFrame, error) {
	cols := make([]*series.Series, 0, len(names))
	for _, name := range names {
		c, err := df.Column(name)
		if err != nil {
			for _, p := range cols {
				p.Release()
			}
			return nil, err
		}
		taken, err := compute.Take(ctx, c, rows)
		if err != nil {
			for _, p := range cols {
				p.Release()
			}
			return nil, err
		}
		cols = append(cols, taken)
	}
	return dataframe.New(cols...)
}

// overOut accumulates per-group results into typed output buffers. The
// buffers are allocated on first touch and the dtype pins then
// (first-wins, values of any later different dtype are dropped),
// exactly mirroring the old dominant-dtype scan.
type overOut struct {
	height int
	dtype  string // "", "float", "string", "bool"
	fVals  []float64
	sVals  []string
	bVals  []bool
	valid  []bool
	filled int // valid values written; height when dense
}

func (o *overOut) ensure(dtype string) {
	if o.dtype != "" {
		return
	}
	o.dtype = dtype
	o.valid = make([]bool, o.height)
	switch dtype {
	case "float":
		o.fVals = make([]float64, o.height)
	case "string":
		o.sVals = make([]string, o.height)
	case "bool":
		o.bVals = make([]bool, o.height)
	}
}

func (o *overOut) broadcast(chunk any, rows []int) {
	for _, r := range rows {
		o.write(chunk, 0, r)
	}
}

func (o *overOut) scatter(chunk any, rows []int) {
	for i, r := range rows {
		o.write(chunk, i, r)
	}
}

func (o *overOut) write(chunk any, src int, dst int) {
	switch a := chunk.(type) {
	case *array.Float64:
		o.ensure("float")
		if o.dtype != "float" || !a.IsValid(src) {
			return
		}
		o.fVals[dst] = a.Value(src)
		o.valid[dst] = true
		o.filled++
	case *array.Float32:
		o.ensure("float")
		if o.dtype != "float" || !a.IsValid(src) {
			return
		}
		o.fVals[dst] = float64(a.Value(src))
		o.valid[dst] = true
		o.filled++
	case *array.Int64:
		o.ensure("float")
		if o.dtype != "float" || !a.IsValid(src) {
			return
		}
		o.fVals[dst] = float64(a.Value(src))
		o.valid[dst] = true
		o.filled++
	case *array.Int32:
		o.ensure("float")
		if o.dtype != "float" || !a.IsValid(src) {
			return
		}
		o.fVals[dst] = float64(a.Value(src))
		o.valid[dst] = true
		o.filled++
	case *array.Boolean:
		o.ensure("bool")
		if o.dtype != "bool" || !a.IsValid(src) {
			return
		}
		o.bVals[dst] = a.Value(src)
		o.valid[dst] = true
		o.filled++
	case *array.String:
		o.ensure("string")
		if o.dtype != "string" || !a.IsValid(src) {
			return
		}
		o.sVals[dst] = a.Value(src)
		o.valid[dst] = true
		o.filled++
	}
}

func (o *overOut) materialize(name string, opt series.Option) (*series.Series, error) {
	// Dense output skips the validity bitmap, mirroring compactValid in
	// the groupby kernels.
	var valid []bool
	if o.filled != o.height {
		valid = o.valid
	}
	switch o.dtype {
	case "float":
		return series.FromFloat64(name, o.fVals, valid, opt)
	case "string":
		return series.FromString(name, o.sVals, valid, opt)
	case "bool":
		return series.FromBool(name, o.bVals, valid, opt)
	}
	// All-null fallback (empty input).
	return series.FromFloat64(name, make([]float64, o.height), make([]bool, o.height), opt)
}

// seriesAllocOpt returns the allocator option matching ec.
func seriesAllocOpt(ec EvalContext) series.Option {
	return seriesAlloc(ec)
}
