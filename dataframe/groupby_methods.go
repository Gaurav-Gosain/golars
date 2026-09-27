package dataframe

import (
	"context"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// MaintainOrder returns a copy of g whose results list groups in the
// order their keys first appear in the input, like polars'
// group_by(..., maintain_order=True). Without it the group order is
// unspecified. The flag applies to Agg and to every convenience method.
func (g *GroupBy) MaintainOrder() *GroupBy {
	c := *g
	c.maintainOrder = true
	return &c
}

// Len returns one row per group with the key columns and a u32 column
// holding the group size. An empty name means "len", like polars'
// GroupBy.len(name=None).
func (g *GroupBy) Len(ctx context.Context, name string, opts ...GroupByOption) (*DataFrame, error) {
	if name == "" {
		name = "len"
	}
	return g.sizes(ctx, name, opts)
}

// Count returns the group sizes in a u32 column named "count". It is the
// deprecated polars GroupBy.count, which counts rows including nulls.
func (g *GroupBy) Count(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.sizes(ctx, "count", opts)
}

func (g *GroupBy) sizes(ctx context.Context, name string, opts []GroupByOption) (*DataFrame, error) {
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	if slices.Contains(g.keys, name) {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateColumn, name)
	}
	vals, _ := newOut[uint32](p.gi.n)
	for gid := range p.gi.n {
		vals[gid] = uint32(p.gi.size(gid))
	}
	s, err := buildOut(name, arrow.PrimitiveTypes.Uint32, vals, nil, p.mem)
	if err != nil {
		return nil, err
	}
	return p.finish([]*series.Series{s})
}

// First returns the first row of every group, nulls included.
func (g *GroupBy) First(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.pick(ctx, true, opts)
}

// Last returns the last row of every group, nulls included.
func (g *GroupBy) Last(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.pick(ctx, false, opts)
}

func (g *GroupBy) pick(ctx context.Context, first bool, opts []GroupByOption) (*DataFrame, error) {
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	idx := p.gi.firstRows()
	if !first {
		idx = p.gi.lastRows()
	}
	return p.mapColumns(func(col *series.Series) (*series.Series, error) {
		return takeOptional(ctx, col, idx, p.mem)
	})
}

// Sum returns per-group sums with polars' dtype rules: i8, i16, u8 and
// u16 widen to i64, bool counts true values as u32, durations stay
// durations, and date, datetime and time columns become all null. A
// group without valid values sums to zero. String and binary columns are
// rejected like in polars.
func (g *GroupBy) Sum(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, gbSum)
}

// Mean returns per-group means. Numeric and bool columns give f64
// (f32 stays f32), date becomes datetime[us], datetime and duration
// keep their dtype, and other columns become all null of their dtype.
func (g *GroupBy) Mean(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, gbMean)
}

// Median returns per-group medians (linear interpolation) with the
// dtype rules of Mean. NaN sorts after every other value.
func (g *GroupBy) Median(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, func(p *gbPlan, col *series.Series) (*series.Series, error) {
		return gbQuantile(p, col, 0.5, QuantileLinear, true)
	})
}

// Quantile returns the per-group q-quantile. An empty method selects
// QuantileNearest, polars' default. Bool columns give an all-null bool
// column, as in polars.
func (g *GroupBy) Quantile(ctx context.Context, q float64, method QuantileMethod, opts ...GroupByOption) (*DataFrame, error) {
	if q < 0 || q > 1 || q != q {
		return nil, errQuantileRange(q)
	}
	method = method.orDefault()
	if err := method.validate(); err != nil {
		return nil, err
	}
	return g.reduceColumns(ctx, opts, func(p *gbPlan, col *series.Series) (*series.Series, error) {
		return gbQuantile(p, col, q, method, false)
	})
}

// Min returns per-group minima. Float NaNs are skipped unless every
// valid value of the group is NaN.
func (g *GroupBy) Min(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, func(p *gbPlan, col *series.Series) (*series.Series, error) {
		return gbMinMax(p, col, false)
	})
}

// Max returns per-group maxima. Float NaNs are skipped unless every
// valid value of the group is NaN.
func (g *GroupBy) Max(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, func(p *gbPlan, col *series.Series) (*series.Series, error) {
		return gbMinMax(p, col, true)
	})
}

// NUnique returns the number of distinct values per group as u32. Null
// counts as one value; all NaNs count as one value and -0.0 equals 0.0.
func (g *GroupBy) NUnique(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	return g.reduceColumns(ctx, opts, gbNUnique)
}

// All aggregates every non-key column into a list per group, keeping
// the rows in input order (polars' GroupBy.all / agg_list).
func (g *GroupBy) All(ctx context.Context, opts ...GroupByOption) (*DataFrame, error) {
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	offs := make([]int32, p.gi.n+1)
	for i, o := range p.gi.offsets {
		offs[i] = int32(o)
	}
	return p.mapColumns(func(col *series.Series) (*series.Series, error) {
		child, err := takeOptional(ctx, col, p.gi.rows, p.mem)
		if err != nil {
			return nil, err
		}
		defer child.Release()
		childArr, err := child.Consolidated()
		if err != nil {
			return nil, err
		}
		defer childArr.Release()
		offBuf := memory.NewResizableBuffer(p.mem)
		offBuf.Resize(len(offs) * 4)
		copy(arrow.Int32Traits.CastFromBytes(offBuf.Bytes()), offs)
		dt := arrow.ListOf(childArr.DataType())
		data := array.NewData(dt, p.gi.n, []*memory.Buffer{nil, offBuf}, []arrow.ArrayData{childArr.Data()}, 0, 0)
		offBuf.Release()
		arr := array.MakeFromData(data)
		data.Release()
		return series.New(col.Name(), arr)
	})
}

// Head returns the first n rows of every group, key columns first.
// Following polars, n == 0 yields one row per group whose non-key
// values are null.
func (g *GroupBy) Head(ctx context.Context, n int, opts ...GroupByOption) (*DataFrame, error) {
	return g.slice(ctx, n, true, opts)
}

// Tail returns the last n rows of every group, key columns first. See
// Head for the n == 0 case.
func (g *GroupBy) Tail(ctx context.Context, n int, opts ...GroupByOption) (*DataFrame, error) {
	return g.slice(ctx, n, false, opts)
}

func (g *GroupBy) slice(ctx context.Context, n int, head bool, opts []GroupByOption) (*DataFrame, error) {
	if n < 0 {
		return nil, fmt.Errorf("dataframe.GroupBy: head/tail n must be non-negative, got %d", n)
	}
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	var keyIdx, valIdx []int
	if n == 0 {
		keyIdx = p.gi.firstRows()
		valIdx = make([]int, len(keyIdx))
		for i := range valIdx {
			valIdx[i] = -1
		}
	} else {
		for gid := range p.gi.n {
			rows := p.gi.groupRows(gid)
			if len(rows) > n {
				if head {
					rows = rows[:n]
				} else {
					rows = rows[len(rows)-n:]
				}
			}
			keyIdx = append(keyIdx, rows...)
		}
		valIdx = keyIdx
	}
	out := make([]*series.Series, 0, g.df.Width())
	fail := func(err error) (*DataFrame, error) {
		for _, s := range out {
			s.Release()
		}
		return nil, err
	}
	for _, k := range g.keys {
		col, _ := g.df.Column(k)
		s, err := takeOptional(ctx, col, keyIdx, p.mem)
		if err != nil {
			return fail(err)
		}
		out = append(out, s)
	}
	for _, col := range p.vals {
		s, err := takeOptional(ctx, col, valIdx, p.mem)
		if err != nil {
			return fail(err)
		}
		out = append(out, s)
	}
	df, err := New(out...)
	if err != nil {
		return fail(err)
	}
	return df, nil
}

// MapGroups calls fn once per group with a frame holding every column of
// that group's rows (keys included, input column order) and concatenates
// the results in group order. Every result must share one schema. The
// frames passed to fn are released after fn returns; fn must not keep
// them, and the frames it returns are owned by MapGroups.
func (g *GroupBy) MapGroups(ctx context.Context, fn func(*DataFrame) (*DataFrame, error), opts ...GroupByOption) (*DataFrame, error) {
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	if p.gi.n == 0 {
		empty := g.df.Clear()
		defer empty.Release()
		return fn(empty)
	}
	parts := make([]*DataFrame, 0, p.gi.n)
	releaseParts := func() {
		for _, d := range parts {
			d.Release()
		}
	}
	for gid := range p.gi.n {
		if err := ctx.Err(); err != nil {
			releaseParts()
			return nil, err
		}
		sub, err := g.df.gatherOptional(ctx, p.gi.groupRows(gid), p.mem)
		if err != nil {
			releaseParts()
			return nil, err
		}
		res, err := fn(sub)
		sub.Release()
		if err != nil {
			releaseParts()
			return nil, err
		}
		parts = append(parts, res)
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	// Concat borrows its inputs, so the parts are released either way.
	out, err := Concat(parts...)
	releaseParts()
	return out, err
}

func (df *DataFrame) gatherOptional(ctx context.Context, idx []int, mem memory.Allocator) (*DataFrame, error) {
	cols := make([]*series.Series, 0, df.Width())
	for _, c := range df.cols {
		s, err := takeOptional(ctx, c, idx, mem)
		if err != nil {
			for _, p := range cols {
				p.Release()
			}
			return nil, err
		}
		cols = append(cols, s)
	}
	return New(cols...)
}

// aggMaintainOrder runs Agg and then reorders the groups to first
// appearance. It aggregates the minimum row number of every group next
// to the requested aggregations, sorts by it and drops it.
func (g *GroupBy) aggMaintainOrder(ctx context.Context, aggs []expr.Expr, opts ...GroupByOption) (*DataFrame, error) {
	cfg := resolveGroupBy(opts)
	name := "__golars_first_row"
	for g.df.Contains(name) {
		name += "_"
	}
	rowIdx, err := series.BuildInt64Direct(name, g.df.Height(), cfg.alloc, func(out []int64) {
		for i := range out {
			out[i] = int64(i)
		}
	})
	if err != nil {
		return nil, err
	}
	withIdx, err := g.df.WithColumn(rowIdx)
	if err != nil {
		rowIdx.Release()
		return nil, err
	}
	defer withIdx.Release()
	extended := append(slices.Clone(aggs), expr.Col(name).Min().Alias(name))
	plain := &GroupBy{df: withIdx, keys: g.keys}
	res, err := plain.Agg(ctx, extended, opts...)
	if err != nil {
		return nil, err
	}
	defer res.Release()
	sorted, err := res.Sort(ctx, name, false, WithSortAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	defer sorted.Release()
	return sorted.Drop(name), nil
}

// gbPlan holds the shared state of one convenience-method call.
type gbPlan struct {
	ctx  context.Context
	g    *GroupBy
	gi   *groupIndex
	vals []*series.Series
	keys []*series.Series
	mem  memory.Allocator
}

func (g *GroupBy) prepare(ctx context.Context, opts []GroupByOption) (*gbPlan, error) {
	if len(g.keys) == 0 {
		return nil, fmt.Errorf("dataframe.GroupBy: at least one key required")
	}
	mem := compute.PoolingMem(resolveGroupBy(opts).alloc)
	keyArrs := make([]arrow.Array, len(g.keys))
	defer func() {
		for _, a := range keyArrs {
			if a != nil {
				a.Release()
			}
		}
	}()
	keySet := make(map[string]struct{}, len(g.keys))
	for i, k := range g.keys {
		col, err := g.df.Column(k)
		if err != nil {
			return nil, err
		}
		keySet[k] = struct{}{}
		arr, err := col.Consolidated()
		if err != nil {
			return nil, err
		}
		keyArrs[i] = arr
	}
	gi, err := buildGroupIndex(keyArrs, g.df.Height(), mem)
	if err != nil {
		return nil, err
	}
	p := &gbPlan{ctx: ctx, g: g, gi: gi, mem: mem}
	for _, c := range g.df.cols {
		if _, isKey := keySet[c.Name()]; !isKey {
			p.vals = append(p.vals, c)
		}
	}
	first := gi.firstRows()
	for _, k := range g.keys {
		col, _ := g.df.Column(k)
		s, err := takeOptional(ctx, col, first, mem)
		if err != nil {
			p.release()
			return nil, err
		}
		p.keys = append(p.keys, s)
	}
	return p, nil
}

func (p *gbPlan) release() {
	for _, k := range p.keys {
		k.Release()
	}
	p.keys = nil
}

// finish combines the key columns with outs. It consumes outs.
func (p *gbPlan) finish(outs []*series.Series) (*DataFrame, error) {
	cols := make([]*series.Series, 0, len(p.keys)+len(outs))
	for _, k := range p.keys {
		cols = append(cols, k.Clone())
	}
	cols = append(cols, outs...)
	df, err := New(cols...)
	if err != nil {
		for _, c := range cols {
			c.Release()
		}
		return nil, err
	}
	return df, nil
}

func (p *gbPlan) mapColumns(fn func(*series.Series) (*series.Series, error)) (*DataFrame, error) {
	outs := make([]*series.Series, 0, len(p.vals))
	for _, col := range p.vals {
		s, err := fn(col)
		if err != nil {
			for _, o := range outs {
				o.Release()
			}
			return nil, err
		}
		outs = append(outs, s)
	}
	return p.finish(outs)
}

func (g *GroupBy) reduceColumns(ctx context.Context, opts []GroupByOption, fn func(*gbPlan, *series.Series) (*series.Series, error)) (*DataFrame, error) {
	p, err := g.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer p.release()
	return p.mapColumns(func(col *series.Series) (*series.Series, error) {
		return fn(p, col)
	})
}

// parallelGroupThreshold is the input height above which per-group
// kernels fan out across groups.
const parallelGroupThreshold = 64 * 1024

// forGroups runs fn over group ranges, in parallel for large inputs. fn
// may allocate scratch space once per call; each call owns its range.
func (p *gbPlan) forGroups(fn func(start, end int)) error {
	if len(p.gi.rows) < parallelGroupThreshold || p.gi.n < 2 {
		fn(0, p.gi.n)
		return nil
	}
	return pool.ParallelFor(p.ctx, p.gi.n, 0, func(_ context.Context, start, end int) error {
		fn(start, end)
		return nil
	})
}

// validity is an inlinable null check over an arrow validity bitmap.
type validity struct {
	bits []byte
	off  int
	all  bool
}

func validityOf(arr arrow.Array) validity {
	if arr.NullN() == 0 {
		return validity{all: true}
	}
	return validity{bits: arr.NullBitmapBytes(), off: arr.Data().Offset()}
}

func (v validity) ok(i int) bool { return v.all || bitutil.BitIsSet(v.bits, v.off+i) }

// rawValues views the value buffer of a fixed-width array as []T.
func rawValues[T any](arr arrow.Array) []T {
	d := arr.Data()
	if arr.Len() == 0 || len(d.Buffers()) < 2 || d.Buffers()[1] == nil {
		return nil
	}
	b := d.Buffers()[1].Bytes()
	var zero T
	sz := int(unsafe.Sizeof(zero))
	all := unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(b))), len(b)/sz)
	return all[d.Offset() : d.Offset()+arr.Len()]
}

func newOut[T any](n int) ([]T, []bool) {
	return make([]T, n), make([]bool, n)
}

// buildOut copies vals into a new arrow array of type dt. valid may be
// nil when every value is valid.
func buildOut[T any](name string, dt arrow.DataType, vals []T, valid []bool, mem memory.Allocator) (*series.Series, error) {
	n := len(vals)
	var zero T
	sz := int(unsafe.Sizeof(zero))
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * sz)
	if n > 0 {
		dst := unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(buf.Bytes()))), n)
		copy(dst, vals)
	}
	var vbuf *memory.Buffer
	nulls := 0
	if valid != nil {
		for _, ok := range valid {
			if !ok {
				nulls++
			}
		}
		if nulls > 0 {
			vbuf = memory.NewResizableBuffer(mem)
			vbuf.Resize(int(bitutil.BytesForBits(int64(n))))
			bits := vbuf.Bytes()
			clear(bits)
			for i, ok := range valid {
				if ok {
					bitutil.SetBit(bits, i)
				}
			}
		}
	}
	data := array.NewData(dt, n, []*memory.Buffer{vbuf, buf}, nil, nulls, 0)
	buf.Release()
	if vbuf != nil {
		vbuf.Release()
	}
	arr := array.MakeFromData(data)
	data.Release()
	return series.New(name, arr)
}

func nullOut(name string, dt arrow.DataType, n int, mem memory.Allocator) (*series.Series, error) {
	return series.New(name, array.MakeArrayOfNull(mem, dt, n))
}

func errUnsupportedGroupOp(op string, dt arrow.DataType) error {
	return fmt.Errorf("dataframe.GroupBy: `%s` operation not supported for dtype `%s`", op, reprDType(dt))
}

func reprDType(dt arrow.DataType) string { return dtype.FromArrow(dt).String() }

var usTimestamp = &arrow.TimestampType{Unit: arrow.Microsecond}

// --- sum ---------------------------------------------------------------

func gbSum(p *gbPlan, col *series.Series) (*series.Series, error) {
	arr, err := col.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	name := col.Name()
	switch a := arr.(type) {
	case *array.Int8:
		return sumSigned(p, name, a, rawValues[int8](a), arrow.PrimitiveTypes.Int64, func(s int64) int64 { return s })
	case *array.Int16:
		return sumSigned(p, name, a, rawValues[int16](a), arrow.PrimitiveTypes.Int64, func(s int64) int64 { return s })
	case *array.Int32:
		return sumSigned(p, name, a, rawValues[int32](a), arrow.PrimitiveTypes.Int32, func(s int64) int32 { return int32(s) })
	case *array.Int64:
		return sumSigned(p, name, a, rawValues[int64](a), arrow.PrimitiveTypes.Int64, func(s int64) int64 { return s })
	case *array.Duration:
		return sumSigned(p, name, a, rawValues[int64](a), a.DataType(), func(s int64) int64 { return s })
	case *array.Uint8:
		return sumUnsigned(p, name, a, rawValues[uint8](a), arrow.PrimitiveTypes.Int64, func(s uint64) int64 { return int64(s) })
	case *array.Uint16:
		return sumUnsigned(p, name, a, rawValues[uint16](a), arrow.PrimitiveTypes.Int64, func(s uint64) int64 { return int64(s) })
	case *array.Uint32:
		return sumUnsigned(p, name, a, rawValues[uint32](a), arrow.PrimitiveTypes.Uint32, func(s uint64) uint32 { return uint32(s) })
	case *array.Uint64:
		return sumUnsigned(p, name, a, rawValues[uint64](a), arrow.PrimitiveTypes.Uint64, func(s uint64) uint64 { return s })
	case *array.Float32:
		return sumFloat(p, name, a, rawValues[float32](a))
	case *array.Float64:
		return sumFloat(p, name, a, rawValues[float64](a))
	case *array.Boolean:
		v := validityOf(a)
		out, _ := newOut[uint32](p.gi.n)
		err := p.forGroups(func(start, end int) {
			for gid := start; gid < end; gid++ {
				var c uint32
				for _, r := range p.gi.groupRows(gid) {
					if v.ok(r) && a.Value(r) {
						c++
					}
				}
				out[gid] = c
			}
		})
		if err != nil {
			return nil, err
		}
		return buildOut(name, arrow.PrimitiveTypes.Uint32, out, nil, p.mem)
	case *array.Null:
		return nullOut(name, arrow.Null, p.gi.n, p.mem)
	case *array.Date32, *array.Date64, *array.Timestamp, *array.Time32, *array.Time64:
		return nullOut(name, a.DataType(), p.gi.n, p.mem)
	}
	return nil, errUnsupportedGroupOp("sum", arr.DataType())
}

type signedInt interface {
	~int8 | ~int16 | ~int32 | ~int64
}
type unsignedInt interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func sumSigned[T signedInt, O any](p *gbPlan, name string, arr arrow.Array, vals []T, dt arrow.DataType, conv func(int64) O) (*series.Series, error) {
	v := validityOf(arr)
	out, _ := newOut[O](p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			var s int64
			for _, r := range p.gi.groupRows(gid) {
				if v.ok(r) {
					s += int64(vals[r])
				}
			}
			out[gid] = conv(s)
		}
	})
	if err != nil {
		return nil, err
	}
	return buildOut(name, dt, out, nil, p.mem)
}

func sumUnsigned[T unsignedInt, O any](p *gbPlan, name string, arr arrow.Array, vals []T, dt arrow.DataType, conv func(uint64) O) (*series.Series, error) {
	v := validityOf(arr)
	out, _ := newOut[O](p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			var s uint64
			for _, r := range p.gi.groupRows(gid) {
				if v.ok(r) {
					s += uint64(vals[r])
				}
			}
			out[gid] = conv(s)
		}
	})
	if err != nil {
		return nil, err
	}
	return buildOut(name, dt, out, nil, p.mem)
}

func sumFloat[T float32 | float64](p *gbPlan, name string, arr arrow.Array, vals []T) (*series.Series, error) {
	v := validityOf(arr)
	out, _ := newOut[T](p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			var s T
			for _, r := range p.gi.groupRows(gid) {
				if v.ok(r) {
					s += vals[r]
				}
			}
			out[gid] = s
		}
	})
	if err != nil {
		return nil, err
	}
	return buildOut(name, arr.DataType(), out, nil, p.mem)
}

// --- float views -------------------------------------------------------

// floatView exposes a column as float64 values for mean and quantile
// kernels. scale converts dates to microseconds so their results can be
// emitted as datetime[us].
type floatView struct {
	at    func(i int) float64
	valid validity
	// outKind: 0 f64, 1 f32, 2 int64-backed temporal of outType.
	outKind int
	outType arrow.DataType
}

// floatViewOf returns a float view of arr, or ok=false when the dtype has
// no numeric interpretation.
func floatViewOf(arr arrow.Array) (floatView, bool) {
	fv := floatView{valid: validityOf(arr), outType: arrow.PrimitiveTypes.Float64}
	switch a := arr.(type) {
	case *array.Int8:
		vals := rawValues[int8](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Int16:
		vals := rawValues[int16](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Int32:
		vals := rawValues[int32](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Int64:
		vals := rawValues[int64](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Uint8:
		vals := rawValues[uint8](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Uint16:
		vals := rawValues[uint16](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Uint32:
		vals := rawValues[uint32](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Uint64:
		vals := rawValues[uint64](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
	case *array.Float64:
		vals := rawValues[float64](a)
		fv.at = func(i int) float64 { return vals[i] }
	case *array.Float32:
		vals := rawValues[float32](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
		fv.outKind = 1
		fv.outType = arrow.PrimitiveTypes.Float32
	case *array.Boolean:
		fv.at = func(i int) float64 {
			if a.Value(i) {
				return 1
			}
			return 0
		}
	case *array.Date32:
		vals := rawValues[int32](a)
		fv.at = func(i int) float64 { return float64(vals[i]) * 86_400_000_000 }
		fv.outKind = 2
		fv.outType = usTimestamp
	case *array.Timestamp, *array.Duration, *array.Time64:
		vals := rawValues[int64](a)
		fv.at = func(i int) float64 { return float64(vals[i]) }
		fv.outKind = 2
		fv.outType = a.DataType()
	default:
		return fv, false
	}
	return fv, true
}

// emit builds the output column for per-group float results.
func (fv floatView) emit(name string, vals []float64, valid []bool, mem memory.Allocator) (*series.Series, error) {
	switch fv.outKind {
	case 1:
		out := make([]float32, len(vals))
		for i, v := range vals {
			out[i] = float32(v)
		}
		return buildOut(name, fv.outType, out, valid, mem)
	case 2:
		out := make([]int64, len(vals))
		for i, v := range vals {
			if valid[i] {
				out[i] = int64(v)
			}
		}
		return buildOut(name, fv.outType, out, valid, mem)
	}
	return buildOut(name, fv.outType, vals, valid, mem)
}

// --- mean --------------------------------------------------------------

func gbMean(p *gbPlan, col *series.Series) (*series.Series, error) {
	arr, err := col.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	fv, ok := floatViewOf(arr)
	if !ok {
		return nullOut(col.Name(), arr.DataType(), p.gi.n, p.mem)
	}
	out, valid := newOut[float64](p.gi.n)
	err = p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			var s float64
			c := 0
			for _, r := range p.gi.groupRows(gid) {
				if fv.valid.ok(r) {
					s += fv.at(r)
					c++
				}
			}
			if c > 0 {
				out[gid] = s / float64(c)
				valid[gid] = true
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return fv.emit(col.Name(), out, valid, p.mem)
}

// --- quantile ----------------------------------------------------------

func gbQuantile(p *gbPlan, col *series.Series, q float64, method QuantileMethod, median bool) (*series.Series, error) {
	arr, err := col.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if arr.DataType().ID() == arrow.BOOL && !median {
		return nullOut(col.Name(), arr.DataType(), p.gi.n, p.mem)
	}
	fv, ok := floatViewOf(arr)
	if !ok {
		return nullOut(col.Name(), arr.DataType(), p.gi.n, p.mem)
	}
	out, valid := newOut[float64](p.gi.n)
	err = p.forGroups(func(start, end int) {
		var scratch []float64
		for gid := start; gid < end; gid++ {
			scratch = scratch[:0]
			for _, r := range p.gi.groupRows(gid) {
				if fv.valid.ok(r) {
					scratch = append(scratch, fv.at(r))
				}
			}
			if v, ok := quantileSelect(scratch, q, method); ok {
				out[gid] = v
				valid[gid] = true
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return fv.emit(col.Name(), out, valid, p.mem)
}

// --- min / max ---------------------------------------------------------

func gbMinMax(p *gbPlan, col *series.Series, isMax bool) (*series.Series, error) {
	arr, err := col.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	name := col.Name()
	dt := arr.DataType()
	switch a := arr.(type) {
	case *array.Int8:
		return minMaxOrdered(p, name, a, rawValues[int8](a), isMax)
	case *array.Int16:
		return minMaxOrdered(p, name, a, rawValues[int16](a), isMax)
	case *array.Int32, *array.Date32, *array.Time32:
		return minMaxOrdered(p, name, a, rawValues[int32](a), isMax)
	case *array.Int64, *array.Timestamp, *array.Duration, *array.Time64, *array.Date64:
		return minMaxOrdered(p, name, a, rawValues[int64](a), isMax)
	case *array.Uint8:
		return minMaxOrdered(p, name, a, rawValues[uint8](a), isMax)
	case *array.Uint16:
		return minMaxOrdered(p, name, a, rawValues[uint16](a), isMax)
	case *array.Uint32:
		return minMaxOrdered(p, name, a, rawValues[uint32](a), isMax)
	case *array.Uint64:
		return minMaxOrdered(p, name, a, rawValues[uint64](a), isMax)
	case *array.Float32:
		return minMaxFloat(p, name, a, rawValues[float32](a), isMax)
	case *array.Float64:
		return minMaxFloat(p, name, a, rawValues[float64](a), isMax)
	case *array.Boolean:
		v := validityOf(a)
		out, valid := newOut[bool](p.gi.n)
		err := p.forGroups(func(start, end int) {
			for gid := start; gid < end; gid++ {
				seen, acc := false, !isMax
				for _, r := range p.gi.groupRows(gid) {
					if !v.ok(r) {
						continue
					}
					seen = true
					if isMax {
						acc = acc || a.Value(r)
					} else {
						acc = acc && a.Value(r)
					}
				}
				out[gid], valid[gid] = acc, seen
			}
		})
		if err != nil {
			return nil, err
		}
		s, err := series.FromBool(name, out, valid, series.WithAllocator(p.mem))
		return s, err
	case *array.String:
		return minMaxBytes(p, name, a, a.Value, isMax)
	case *array.LargeString:
		return minMaxBytes(p, name, a, a.Value, isMax)
	case *array.Binary:
		return minMaxBytes(p, name, a, func(i int) string { return string(a.Value(i)) }, isMax)
	case *array.Null:
		return nullOut(name, dt, p.gi.n, p.mem)
	}
	op := "min"
	if isMax {
		op = "max"
	}
	return nil, errUnsupportedGroupOp(op, dt)
}

type ordered interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func minMaxOrdered[T ordered](p *gbPlan, name string, arr arrow.Array, vals []T, isMax bool) (*series.Series, error) {
	v := validityOf(arr)
	out, valid := newOut[T](p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			seen := false
			var acc T
			for _, r := range p.gi.groupRows(gid) {
				if !v.ok(r) {
					continue
				}
				x := vals[r]
				if !seen || (isMax && x > acc) || (!isMax && x < acc) {
					acc = x
					seen = true
				}
			}
			out[gid], valid[gid] = acc, seen
		}
	})
	if err != nil {
		return nil, err
	}
	return buildOut(name, arr.DataType(), out, valid, p.mem)
}

func minMaxFloat[T float32 | float64](p *gbPlan, name string, arr arrow.Array, vals []T, isMax bool) (*series.Series, error) {
	v := validityOf(arr)
	out, valid := newOut[T](p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			seen, seenNum := false, false
			var acc T
			for _, r := range p.gi.groupRows(gid) {
				if !v.ok(r) {
					continue
				}
				seen = true
				x := vals[r]
				if x != x {
					continue
				}
				if !seenNum || (isMax && x > acc) || (!isMax && x < acc) {
					acc = x
					seenNum = true
				}
			}
			if seen && !seenNum {
				acc = T(math.NaN())
			}
			out[gid], valid[gid] = acc, seen
		}
	})
	if err != nil {
		return nil, err
	}
	return buildOut(name, arr.DataType(), out, valid, p.mem)
}

func minMaxBytes(p *gbPlan, name string, arr arrow.Array, value func(int) string, isMax bool) (*series.Series, error) {
	v := validityOf(arr)
	best := make([]int, p.gi.n)
	err := p.forGroups(func(start, end int) {
		for gid := start; gid < end; gid++ {
			bi := -1
			for _, r := range p.gi.groupRows(gid) {
				if !v.ok(r) {
					continue
				}
				if bi < 0 {
					bi = r
					continue
				}
				x, b := value(r), value(bi)
				if (isMax && x > b) || (!isMax && x < b) {
					bi = r
				}
			}
			best[gid] = bi
		}
	})
	if err != nil {
		return nil, err
	}
	arr.Retain()
	s, err := series.New(name, arr)
	if err != nil {
		arr.Release()
		return nil, err
	}
	defer s.Release()
	return takeOptional(p.ctx, s, best, p.mem)
}

// --- n_unique ----------------------------------------------------------

func gbNUnique(p *gbPlan, col *series.Series) (*series.Series, error) {
	arr, err := col.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	name := col.Name()
	out, _ := newOut[uint32](p.gi.n)
	v := validityOf(arr)
	switch a := arr.(type) {
	case *array.Null:
		for gid := range p.gi.n {
			out[gid] = 1
		}
	case *array.String:
		err = nUniqueStrings(p, out, v, a.Value)
	case *array.LargeString:
		err = nUniqueStrings(p, out, v, a.Value)
	case *array.Binary:
		err = nUniqueStrings(p, out, v, func(i int) string {
			b := a.Value(i)
			return unsafe.String(unsafe.SliceData(b), len(b))
		})
	case *array.Boolean:
		err = p.forGroups(func(start, end int) {
			for gid := start; gid < end; gid++ {
				var seen [3]bool
				for _, r := range p.gi.groupRows(gid) {
					switch {
					case !v.ok(r):
						seen[2] = true
					case a.Value(r):
						seen[1] = true
					default:
						seen[0] = true
					}
				}
				var c uint32
				for _, s := range seen {
					if s {
						c++
					}
				}
				out[gid] = c
			}
		})
	default:
		canon, ok := canonicalInt64Keys(arr)
		if !ok {
			return nil, errUnsupportedGroupOp("n_unique", arr.DataType())
		}
		err = p.forGroups(func(start, end int) {
			var scratch []int64
			for gid := start; gid < end; gid++ {
				scratch = scratch[:0]
				hasNull := false
				for _, r := range p.gi.groupRows(gid) {
					if v.ok(r) {
						scratch = append(scratch, canon[r])
					} else {
						hasNull = true
					}
				}
				slices.Sort(scratch)
				c := uint32(len(slices.Compact(scratch)))
				if hasNull {
					c++
				}
				out[gid] = c
			}
		})
	}
	if err != nil {
		return nil, err
	}
	return buildOut(name, arrow.PrimitiveTypes.Uint32, out, nil, p.mem)
}

func nUniqueStrings(p *gbPlan, out []uint32, v validity, value func(int) string) error {
	return p.forGroups(func(start, end int) {
		var scratch []string
		for gid := start; gid < end; gid++ {
			scratch = scratch[:0]
			hasNull := false
			for _, r := range p.gi.groupRows(gid) {
				if v.ok(r) {
					scratch = append(scratch, value(r))
				} else {
					hasNull = true
				}
			}
			slices.Sort(scratch)
			c := uint32(len(slices.Compact(scratch)))
			if hasNull {
				c++
			}
			out[gid] = c
		}
	})
}
