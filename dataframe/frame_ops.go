package dataframe

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// withFallbackTake gathers from the virtual concatenation of base and
// extra: index i < base.Len() reads base, i >= base.Len() reads
// extra[i-base.Len()], and a negative index yields null. It is the
// building block for fills, padding and merges that mix two sources of
// the same dtype.
func withFallbackTake(name string, base, extra arrow.Array, idx []int, mem memory.Allocator) (*series.Series, error) {
	src := base
	if extra != nil && extra.Len() > 0 {
		cat, err := array.Concatenate([]arrow.Array{base, extra}, mem)
		if err != nil {
			return nil, err
		}
		defer cat.Release()
		src = cat
	}
	out, err := takeArrayOptional(src, idx, mem)
	if err != nil {
		return nil, err
	}
	return seriesFromArray(name, out)
}

// Shift moves every column by n rows (negative n shifts up). Vacated
// slots take fillValue, or null when fillValue is nil. An integer column
// shifted with a float fill value becomes f64, as in polars. Mirrors
// polars' DataFrame.shift(n, fill_value=...).
func (df *DataFrame) Shift(ctx context.Context, n int, fillValue any) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		s, err := shiftSeries(ctx, c, n, fillValue)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[i] = s
	}
	return New(out...)
}

func shiftSeries(ctx context.Context, c *series.Series, n int, fill any) (*series.Series, error) {
	src := c.Clone()
	// A closure so the release sees src after the reassignment below.
	defer func() { src.Release() }()
	if _, isFloat := fill.(float64); isFloat && c.DType().IsInteger() {
		casted, err := compute.Cast(ctx, c, dtype.Float64())
		if err != nil {
			return nil, err
		}
		src.Release()
		src = casted
	}
	h := src.Len()
	arr, err := src.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	fillArr, err := constantArray(arr.DataType(), fillForDType(fill, arr.DataType()), 1, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("dataframe.Shift: column %q: %w", c.Name(), err)
	}
	defer fillArr.Release()
	idx := make([]int, h)
	for i := range h {
		j := i - n
		if j < 0 || j >= h {
			idx[i] = h
		} else {
			idx[i] = j
		}
	}
	return withFallbackTake(c.Name(), arr, fillArr, idx, memory.DefaultAllocator)
}

// fillForDType adapts a Go fill value to a column dtype the way polars
// casts a literal: numbers become strings for string columns.
func fillForDType(v any, dt arrow.DataType) any {
	if v == nil {
		return nil
	}
	if dt.ID() == arrow.STRING || dt.ID() == arrow.LARGE_STRING {
		switch x := v.(type) {
		case string:
			return x
		case float64:
			return pyFloat(x)
		case float32:
			return pyFloat(float64(x))
		default:
			return fmt.Sprint(v)
		}
	}
	return v
}

// GatherEvery keeps every n-th row starting at offset. Mirrors polars'
// DataFrame.gather_every.
func (df *DataFrame) GatherEvery(ctx context.Context, n, offset int) (*DataFrame, error) {
	if n <= 0 {
		return nil, fmt.Errorf("dataframe.GatherEvery: n must be positive, got %d", n)
	}
	if offset < 0 {
		return nil, fmt.Errorf("dataframe.GatherEvery: offset must be non-negative, got %d", offset)
	}
	var idx []int
	for i := offset; i < df.height; i += n {
		idx = append(idx, i)
	}
	return df.gatherAll(ctx, idx)
}

// gatherAll takes idx (negative means null) from every column.
func (df *DataFrame) gatherAll(ctx context.Context, idx []int) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		s, err := takeOptional(ctx, c, idx, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[i] = s
	}
	if len(out) == 0 {
		return &DataFrame{sch: df.sch, height: len(idx)}, nil
	}
	return New(out...)
}

// FillNan replaces NaN in every float column with value, which may be a
// number or nil for null. Other columns pass through. Mirrors polars'
// DataFrame.fill_nan.
func (df *DataFrame) FillNan(value any) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		if !c.DType().IsFloating() {
			out[i] = c.Clone()
			continue
		}
		arr, err := c.Consolidated()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		fill, err := constantArray(arr.DataType(), value, 1, memory.DefaultAllocator)
		if err != nil {
			arr.Release()
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.FillNan: %w", err)
		}
		h := arr.Len()
		idx := make([]int, h)
		isNaN := floatNaNMask(arr)
		for r := range h {
			if isNaN(r) {
				idx[r] = h
			} else {
				idx[r] = r
			}
		}
		s, err := withFallbackTake(c.Name(), arr, fill, idx, memory.DefaultAllocator)
		arr.Release()
		fill.Release()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[i] = s
	}
	return New(out...)
}

// floatNaNMask returns a predicate reporting whether row r is a valid NaN.
func floatNaNMask(a arrow.Array) func(int) bool {
	switch a.DataType().ID() {
	case arrow.FLOAT32:
		v := rawValues[float32](a)
		return func(r int) bool { return a.IsValid(r) && v[r] != v[r] }
	case arrow.FLOAT64:
		v := rawValues[float64](a)
		return func(r int) bool { return a.IsValid(r) && v[r] != v[r] }
	}
	return func(int) bool { return false }
}

// DropNans removes rows holding NaN in any float column of subset (every
// column when subset is empty). Nulls are kept. Mirrors polars'
// DataFrame.drop_nans.
func (df *DataFrame) DropNans(ctx context.Context, subset ...string) (*DataFrame, error) {
	cols, err := df.subsetColumns(subset)
	if err != nil {
		return nil, err
	}
	var preds []func(int) bool
	var arrs []arrow.Array
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	for _, c := range cols {
		if !c.DType().IsFloating() {
			continue
		}
		a, err := c.Consolidated()
		if err != nil {
			return nil, err
		}
		arrs = append(arrs, a)
		preds = append(preds, floatNaNMask(a))
	}
	if len(preds) == 0 {
		return df.Clone(), nil
	}
	idx := make([]int, 0, df.height)
	for r := range df.height {
		keep := true
		for _, p := range preds {
			if p(r) {
				keep = false
				break
			}
		}
		if keep {
			idx = append(idx, r)
		}
	}
	if len(idx) == df.height {
		return df.Clone(), nil
	}
	return df.gatherAll(ctx, idx)
}

// FillNullStrategy names a polars fill_null strategy.
type FillNullStrategy string

// Fill strategies accepted by FillNullWithStrategy.
const (
	FillForward  FillNullStrategy = "forward"
	FillBackward FillNullStrategy = "backward"
	FillMin      FillNullStrategy = "min"
	FillMax      FillNullStrategy = "max"
	FillMean     FillNullStrategy = "mean"
	FillZero     FillNullStrategy = "zero"
	FillOne      FillNullStrategy = "one"
)

// FillNullWithStrategy fills nulls in every column using strategy.
// limit bounds consecutive fills for forward and backward (0 means no
// limit). Mirrors polars' DataFrame.fill_null(strategy=..., limit=...),
// including its errors for strategies a dtype does not support (mean of
// a string or boolean, one of a string).
func (df *DataFrame) FillNullWithStrategy(ctx context.Context, strategy FillNullStrategy, limit int) (*DataFrame, error) {
	out := make([]*series.Series, len(df.cols))
	for i, c := range df.cols {
		s, err := fillNullStrategySeries(ctx, c, strategy, limit)
		if err != nil {
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.FillNullWithStrategy: column %q: %w", c.Name(), err)
		}
		out[i] = s
	}
	return New(out...)
}

func fillNullStrategySeries(ctx context.Context, c *series.Series, strategy FillNullStrategy, limit int) (*series.Series, error) {
	arr, err := c.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	h := arr.Len()
	dt := arr.DataType()
	if arr.NullN() == 0 {
		return c.Clone(), nil
	}
	switch strategy {
	case FillForward, FillBackward:
		idx := make([]int, h)
		run := 0
		if strategy == FillForward {
			last := -1
			for r := range h {
				if arr.IsValid(r) {
					last, run = r, 0
					idx[r] = r
					continue
				}
				run++
				if last >= 0 && (limit <= 0 || run <= limit) {
					idx[r] = last
				} else {
					idx[r] = -1
				}
			}
		} else {
			next := -1
			for r := h - 1; r >= 0; r-- {
				if arr.IsValid(r) {
					next, run = r, 0
					idx[r] = r
					continue
				}
				run++
				if next >= 0 && (limit <= 0 || run <= limit) {
					idx[r] = next
				} else {
					idx[r] = -1
				}
			}
		}
		return withFallbackTake(c.Name(), arr, nil, idx, memory.DefaultAllocator)
	}

	var fill arrow.Array
	switch strategy {
	case FillMin, FillMax:
		op := reduceMin
		if strategy == FillMax {
			op = reduceMax
		}
		s, err := reduceColumn(c, op, reduceArgs{}, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
		defer s.Release()
		fill = firstChunk(s)
		fill.Retain()
	case FillMean:
		if !(dt.ID() == arrow.FLOAT32 || dt.ID() == arrow.FLOAT64 || isIntegerID(dt.ID())) {
			return nil, fmt.Errorf("`mean` operation not supported for dtype `%s`", dtype.FromArrow(dt))
		}
		s, err := reduceColumn(c, reduceMean, reduceArgs{}, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
		defer s.Release()
		var mean any
		if s.NullCount() == 0 {
			switch m := firstChunk(s).(type) {
			case *array.Float64:
				mean = m.Value(0)
				if isIntegerID(dt.ID()) {
					mean = int64(m.Value(0))
				}
			case *array.Float32:
				mean = m.Value(0)
			}
		}
		fill, err = constantArray(dt, mean, 1, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
	case FillZero, FillOne:
		v, err := zeroOneValue(dt, strategy == FillOne)
		if err != nil {
			return nil, err
		}
		fill, err = constantArray(dt, v, 1, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown fill strategy %q", string(strategy))
	}
	defer fill.Release()
	idx := make([]int, h)
	for r := range h {
		if arr.IsValid(r) {
			idx[r] = r
		} else {
			idx[r] = h
		}
	}
	_ = ctx
	return withFallbackTake(c.Name(), arr, fill, idx, memory.DefaultAllocator)
}

func isIntegerID(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

func zeroOneValue(dt arrow.DataType, one bool) (any, error) {
	id := dt.ID()
	switch {
	case isIntegerID(id), id == arrow.DATE32, id == arrow.TIMESTAMP, id == arrow.DURATION,
		id == arrow.TIME32, id == arrow.TIME64:
		if one {
			return 1, nil
		}
		return 0, nil
	case id == arrow.FLOAT32 || id == arrow.FLOAT64:
		if one {
			return 1.0, nil
		}
		return 0.0, nil
	case id == arrow.BOOL:
		return one, nil
	case id == arrow.STRING || id == arrow.LARGE_STRING:
		if one {
			return nil, fmt.Errorf("fill-null strategy One is not supported for dtype `str`")
		}
		return "", nil
	}
	return nil, fmt.Errorf("fill-null strategy is not supported for dtype `%s`", dtype.FromArrow(dt))
}

// ToDummiesOptions mirrors polars' DataFrame.to_dummies keyword
// arguments. An empty Separator means "_".
type ToDummiesOptions struct {
	Columns   []string
	Separator string
	DropFirst bool
	DropNulls bool
}

// ToDummies one-hot encodes the selected columns (all when Columns is
// empty) into u8 indicator columns named <column><sep><value>. Each
// source column's indicators replace it in place, ordered by name like
// polars (lexicographic byte order, so "a_10" sorts before "a_9" and
// "a_null" sorts after digits). Unselected columns pass through.
// Mirrors polars' DataFrame.to_dummies.
func (df *DataFrame) ToDummies(ctx context.Context, opts ToDummiesOptions) (*DataFrame, error) {
	sep := opts.Separator
	if sep == "" {
		sep = "_"
	}
	selected := make(map[string]bool, len(opts.Columns))
	for _, n := range opts.Columns {
		if !df.sch.Contains(n) {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, n)
		}
		selected[n] = true
	}
	var out []*series.Series
	for _, c := range df.cols {
		if err := ctx.Err(); err != nil {
			releaseAll(out)
			return nil, err
		}
		if len(selected) > 0 && !selected[c.Name()] {
			out = append(out, c.Clone())
			continue
		}
		dummies, err := dummiesFor(c, sep, opts.DropFirst, opts.DropNulls)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, dummies...)
	}
	if len(out) == 0 {
		return New()
	}
	return New(out...)
}

func dummiesFor(c *series.Series, sep string, dropFirst, dropNulls bool) ([]*series.Series, error) {
	h := c.Len()
	ids, first := rowGroupIDs([]*series.Series{c}, h)
	arr := firstChunk(c)
	type dummy struct {
		name string
		id   int
		null bool
	}
	ds := make([]dummy, len(first))
	for id, r := range first {
		label := "null"
		isNull := arr.IsNull(r)
		if !isNull {
			label = dummyLabel(arr, r)
		}
		ds[id] = dummy{name: c.Name() + sep + label, id: id, null: isNull}
	}
	if dropFirst {
		// polars drops the first non-null value in order of appearance.
		for i, d := range ds {
			if !d.null {
				ds = slices.Delete(ds, i, i+1)
				break
			}
		}
	}
	slices.SortFunc(ds, func(a, b dummy) int { return strings.Compare(a.name, b.name) })
	if dropNulls {
		ds = slices.DeleteFunc(ds, func(d dummy) bool { return d.null })
	}
	out := make([]*series.Series, 0, len(ds))
	for _, d := range ds {
		vals := make([]uint8, h)
		for r, id := range ids {
			if id == d.id {
				vals[r] = 1
			}
		}
		b := array.NewUint8Builder(memory.DefaultAllocator)
		b.AppendValues(vals, nil)
		a := b.NewArray()
		b.Release()
		s, err := seriesFromArray(d.name, a)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// dummyLabel renders a value the way polars casts it to a string.
func dummyLabel(a arrow.Array, r int) string {
	switch v := a.(type) {
	case *array.Float32:
		return pyFloat(float64(v.Value(r)))
	case *array.Float64:
		return pyFloat(v.Value(r))
	case *array.Boolean:
		return strconv.FormatBool(v.Value(r))
	case *array.Date32:
		return time.Unix(int64(v.Value(r))*86400, 0).UTC().Format("2006-01-02")
	}
	val := cellValue(a, r)
	switch x := val.(type) {
	case string:
		// The label becomes a column name that outlives a: copy it out
		// of the arrow buffer, which may be recycled once a is released.
		return strings.Clone(x)
	case time.Time:
		return x.Format("2006-01-02 15:04:05.999999999")
	}
	return fmt.Sprint(val)
}

// pyFloat formats v like Python's repr for floats.
func pyFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(v, 'g', -1, 64)
	if !strings.ContainsAny(s, ".e") {
		s += ".0"
	}
	return s
}

// UnstackDirection selects how Unstack lays values out.
type UnstackDirection string

// Unstack directions.
const (
	UnstackVertical   UnstackDirection = "vertical"
	UnstackHorizontal UnstackDirection = "horizontal"
)

// UnstackOptions mirrors polars' DataFrame.unstack keyword arguments.
type UnstackOptions struct {
	Step int
	// How defaults to UnstackVertical.
	How UnstackDirection
	// Columns restricts the unstacked columns; empty means all. Only the
	// unstacked columns appear in the output.
	Columns []string
	// FillValues pads the last partial block instead of null. Either one
	// value for every column or one value per unstacked column.
	FillValues []any
}

// Unstack reshapes each selected column into ceil(height/step) columns
// named <column>_<i> without aggregation. Vertical fills column by
// column in blocks of step rows; horizontal fills row by row with step
// columns. Mirrors polars' DataFrame.unstack.
func (df *DataFrame) Unstack(ctx context.Context, opts UnstackOptions) (*DataFrame, error) {
	step := opts.Step
	if step <= 0 {
		return nil, fmt.Errorf("dataframe.Unstack: step must be positive, got %d", step)
	}
	cols, err := df.subsetColumns(opts.Columns)
	if err != nil {
		return nil, err
	}
	if len(opts.FillValues) > 1 && len(opts.FillValues) != len(cols) {
		return nil, fmt.Errorf("dataframe.Unstack: got %d fill values for %d columns", len(opts.FillValues), len(cols))
	}
	h := df.height
	blocks := (h + step - 1) / step
	outH, outW := step, blocks
	if opts.How == UnstackHorizontal {
		outH, outW = blocks, step
	} else if opts.How != "" && opts.How != UnstackVertical {
		return nil, fmt.Errorf("dataframe.Unstack: unknown direction %q", string(opts.How))
	}
	var out []*series.Series
	for ci, c := range cols {
		if err := ctx.Err(); err != nil {
			releaseAll(out)
			return nil, err
		}
		arr, err := c.Consolidated()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		var fillVal any
		switch len(opts.FillValues) {
		case 0:
		case 1:
			fillVal = opts.FillValues[0]
		default:
			fillVal = opts.FillValues[ci]
		}
		fill, err := constantArray(arr.DataType(), fillForDType(fillVal, arr.DataType()), 1, memory.DefaultAllocator)
		if err != nil {
			arr.Release()
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.Unstack: column %q: %w", c.Name(), err)
		}
		for j := range outW {
			idx := make([]int, outH)
			for i := range outH {
				src := j*step + i
				if opts.How == UnstackHorizontal {
					src = i*step + j
				}
				if src >= h {
					src = h
				}
				idx[i] = src
			}
			s, err := withFallbackTake(fmt.Sprintf("%s_%d", c.Name(), j), arr, fill, idx, memory.DefaultAllocator)
			if err != nil {
				arr.Release()
				fill.Release()
				releaseAll(out)
				return nil, err
			}
			out = append(out, s)
		}
		arr.Release()
		fill.Release()
	}
	if len(out) == 0 {
		return New()
	}
	return New(out...)
}

// UpdateHow selects which rows DataFrame.Update keeps.
type UpdateHow string

// Update join strategies.
const (
	UpdateLeft  UpdateHow = "left"
	UpdateInner UpdateHow = "inner"
	UpdateFull  UpdateHow = "full"
)

// UpdateOptions mirrors polars' DataFrame.update keyword arguments. With
// no key columns the frames are aligned by row position.
type UpdateOptions struct {
	On           []string
	LeftOn       []string
	RightOn      []string
	How          UpdateHow
	IncludeNulls bool
}

// Update overwrites values of df with non-null values from other, matched
// on key columns (or by row position when no keys are given). Only
// columns present in both frames are updated; other columns of other are
// ignored. Updated columns take the numeric supertype of both sides. With
// IncludeNulls, nulls in other overwrite too. Row order: every left row
// in order (repeated once per match), then for UpdateFull the unmatched
// rows of other. Mirrors polars' DataFrame.update.
func (df *DataFrame) Update(ctx context.Context, other *DataFrame, opts UpdateOptions) (*DataFrame, error) {
	how := opts.How
	if how == "" {
		how = UpdateLeft
	}
	if how != UpdateLeft && how != UpdateInner && how != UpdateFull {
		return nil, fmt.Errorf("dataframe.Update: unknown how %q", string(how))
	}
	leftOn, rightOn := opts.LeftOn, opts.RightOn
	if len(opts.On) > 0 {
		leftOn, rightOn = opts.On, opts.On
	}
	if len(leftOn) != len(rightOn) {
		return nil, fmt.Errorf("dataframe.Update: left_on and right_on must have the same length")
	}

	var li, ri []int
	if len(leftOn) == 0 {
		// Positional alignment.
		n := df.height
		for r := range n {
			li = append(li, r)
			if r < other.height {
				ri = append(ri, r)
			} else {
				ri = append(ri, -1)
			}
		}
		if how == UpdateInner {
			li, ri = li[:min(n, other.height)], ri[:min(n, other.height)]
		}
		if how == UpdateFull {
			for r := n; r < other.height; r++ {
				li = append(li, -1)
				ri = append(ri, r)
			}
		}
	} else {
		lk, err := df.subsetColumns(leftOn)
		if err != nil {
			return nil, err
		}
		rk, err := other.subsetColumns(rightOn)
		if err != nil {
			return nil, err
		}
		for i := range lk {
			if !lk[i].DType().Equal(rk[i].DType()) {
				return nil, fmt.Errorf("dataframe.Update: key dtypes differ: %s vs %s", lk[i].DType(), rk[i].DType())
			}
		}
		li, ri = equiJoinPairs(lk, rk, df.height, other.height, how == UpdateFull, how == UpdateInner)
	}

	keySet := make(map[string]int, len(leftOn))
	for i, k := range leftOn {
		keySet[k] = i
	}
	nLeft := df.height
	out := make([]*series.Series, 0, len(df.cols))
	for _, c := range df.cols {
		name := c.Name()
		var otherCol *series.Series
		isKey := false
		if ki, ok := keySet[name]; ok {
			isKey = true
			otherCol, _ = other.Column(rightOn[ki])
		} else if other.sch.Contains(name) {
			otherCol, _ = other.Column(name)
		}
		if otherCol == nil {
			s, err := takeOptional(ctx, c, li, memory.DefaultAllocator)
			if err != nil {
				releaseAll(out)
				return nil, err
			}
			out = append(out, s)
			continue
		}
		lc, rc, err := toCommonDType(ctx, c, otherCol)
		if err != nil {
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.Update: column %q: %w", name, err)
		}
		la, _ := lc.Consolidated()
		ra, _ := rc.Consolidated()
		idx := make([]int, len(li))
		for j := range li {
			switch {
			case isKey:
				if li[j] >= 0 {
					idx[j] = li[j]
				} else {
					idx[j] = nLeft + ri[j]
				}
			case ri[j] >= 0 && (opts.IncludeNulls || ra.IsValid(ri[j])):
				idx[j] = nLeft + ri[j]
			default:
				idx[j] = li[j]
			}
		}
		s, err := withFallbackTake(name, la, ra, idx, memory.DefaultAllocator)
		la.Release()
		ra.Release()
		lc.Release()
		rc.Release()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}

// equiJoinPairs matches rows of left and right key columns. Left rows
// keep their order and repeat once per right match (right matches in
// right order). Unmatched left rows pair with -1 unless inner is set;
// with full, unmatched right rows are appended paired with -1. Nulls
// match nulls, as polars does in update.
func equiJoinPairs(lk, rk []*series.Series, nl, nr int, full, inner bool) ([]int, []int) {
	renc := newRowKeyEncoder(rk)
	lenc := newRowKeyEncoder(lk)
	heads := make(map[string]int, nr)
	next := make([]int, nr)
	tails := make(map[string]int, nr)
	buf := make([]byte, 0, 64)
	for r := range nr {
		buf = renc.append(buf[:0], r)
		next[r] = -1
		if t, ok := tails[string(buf)]; ok {
			next[t] = r
			tails[string(buf)] = r
			continue
		}
		k := string(buf)
		heads[k] = r
		tails[k] = r
	}
	matchedRight := make([]bool, nr)
	li := make([]int, 0, nl)
	ri := make([]int, 0, nl)
	for l := range nl {
		buf = lenc.append(buf[:0], l)
		h, ok := heads[string(buf)]
		if !ok {
			if !inner {
				li = append(li, l)
				ri = append(ri, -1)
			}
			continue
		}
		for r := h; r >= 0; r = next[r] {
			li = append(li, l)
			ri = append(ri, r)
			matchedRight[r] = true
		}
	}
	if full {
		for r, m := range matchedRight {
			if !m {
				li = append(li, -1)
				ri = append(ri, r)
			}
		}
	}
	return li, ri
}

// toCommonDType casts a and b to a shared dtype: equal dtypes pass
// through, mixed numerics widen (any float makes f64, otherwise i64), and
// anything else is an error. The caller releases both results.
func toCommonDType(ctx context.Context, a, b *series.Series) (*series.Series, *series.Series, error) {
	if a.DType().Equal(b.DType()) {
		return a.Clone(), b.Clone(), nil
	}
	var target dtype.DType
	switch {
	case a.DType().IsNull():
		target = b.DType()
	case b.DType().IsNull():
		target = a.DType()
	case a.DType().IsNumeric() && b.DType().IsNumeric():
		if a.DType().IsFloating() || b.DType().IsFloating() {
			target = dtype.Float64()
		} else {
			target = dtype.Int64()
		}
	default:
		return nil, nil, fmt.Errorf("incompatible dtypes %s and %s", a.DType(), b.DType())
	}
	ac, err := castSeries(ctx, a, target, false)
	if err != nil {
		return nil, nil, err
	}
	bc, err := castSeries(ctx, b, target, false)
	if err != nil {
		ac.Release()
		return nil, nil, err
	}
	return ac, bc, nil
}

// MergeSorted merges two frames that are each sorted ascending by key
// into one sorted frame. Nulls sort first and on ties rows of df come
// before rows of other. The inputs are not checked for sortedness, like
// polars. Both frames must have the same schema. Mirrors polars'
// DataFrame.merge_sorted.
func (df *DataFrame) MergeSorted(ctx context.Context, other *DataFrame, key string) (*DataFrame, error) {
	if !df.sch.Equal(other.sch) {
		return nil, fmt.Errorf("dataframe.MergeSorted: schemas differ: %s vs %s", df.sch, other.sch)
	}
	lk, err := df.Column(key)
	if err != nil {
		return nil, err
	}
	rk, _ := other.Column(key)
	la, err := lk.Consolidated()
	if err != nil {
		return nil, err
	}
	defer la.Release()
	ra, err := rk.Consolidated()
	if err != nil {
		return nil, err
	}
	defer ra.Release()
	less, err := crossLess(la, ra)
	if err != nil {
		return nil, fmt.Errorf("dataframe.MergeSorted: %w", err)
	}
	nl, nr := la.Len(), ra.Len()
	idx := make([]int, 0, nl+nr)
	i, j := 0, 0
	for i < nl && j < nr {
		// Take the right row only when it sorts strictly before the left.
		if less(j, i) {
			idx = append(idx, nl+j)
			j++
		} else {
			idx = append(idx, i)
			i++
		}
	}
	for ; i < nl; i++ {
		idx = append(idx, i)
	}
	for ; j < nr; j++ {
		idx = append(idx, nl+j)
	}
	out := make([]*series.Series, len(df.cols))
	for c := range df.cols {
		if err := ctx.Err(); err != nil {
			releaseAll(out)
			return nil, err
		}
		a, _ := df.cols[c].Consolidated()
		b, _ := other.cols[c].Consolidated()
		s, err := withFallbackTake(df.cols[c].Name(), a, b, idx, memory.DefaultAllocator)
		a.Release()
		b.Release()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out[c] = s
	}
	return New(out...)
}

// crossLess returns less(j, i) reporting whether right[j] sorts strictly
// before left[i], with nulls first.
func crossLess(left, right arrow.Array) (func(j, i int) bool, error) {
	wrap := func(cmpVals func(j, i int) int) func(j, i int) bool {
		return func(j, i int) bool {
			rn, ln := right.IsNull(j), left.IsNull(i)
			switch {
			case rn && ln:
				return false
			case rn:
				return true
			case ln:
				return false
			}
			return cmpVals(j, i) < 0
		}
	}
	switch left.DataType().ID() {
	case arrow.INT8:
		return wrap(cmpRaw(rawValues[int8](left), rawValues[int8](right))), nil
	case arrow.INT16:
		return wrap(cmpRaw(rawValues[int16](left), rawValues[int16](right))), nil
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		return wrap(cmpRaw(rawValues[int32](left), rawValues[int32](right))), nil
	case arrow.INT64, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME64, arrow.DATE64:
		return wrap(cmpRaw(rawValues[int64](left), rawValues[int64](right))), nil
	case arrow.UINT8:
		return wrap(cmpRaw(rawValues[uint8](left), rawValues[uint8](right))), nil
	case arrow.UINT16:
		return wrap(cmpRaw(rawValues[uint16](left), rawValues[uint16](right))), nil
	case arrow.UINT32:
		return wrap(cmpRaw(rawValues[uint32](left), rawValues[uint32](right))), nil
	case arrow.UINT64:
		return wrap(cmpRaw(rawValues[uint64](left), rawValues[uint64](right))), nil
	case arrow.FLOAT32:
		return wrap(cmpRaw(rawValues[float32](left), rawValues[float32](right))), nil
	case arrow.FLOAT64:
		return wrap(cmpRaw(rawValues[float64](left), rawValues[float64](right))), nil
	case arrow.BOOL:
		lb, rb := left.(*array.Boolean), right.(*array.Boolean)
		return wrap(func(j, i int) int {
			return cmp.Compare(b2i(rb.Value(j)), b2i(lb.Value(i)))
		}), nil
	case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		lv, rv := bytesAccessor(left), bytesAccessor(right)
		return wrap(func(j, i int) int { return strings.Compare(rv(j), lv(i)) }), nil
	}
	return nil, fmt.Errorf("unsupported key dtype %s", dtype.FromArrow(left.DataType()))
}

func cmpRaw[T numeric](left, right []T) func(j, i int) int {
	return func(j, i int) int { return cmp.Compare(right[j], left[i]) }
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// MapRows applies fn to every row (values as in IterRows) and builds a
// frame from the results. When fn returns a []any each element becomes
// a column named column_0, column_1, ...; any other value produces a
// single column named "map". Column dtypes are inferred from the values:
// Go integers become i64 (f64 if any row holds a float), floats f64,
// strings str, bools bool and time.Time datetime[us]. Mirrors polars'
// DataFrame.map_rows.
func (df *DataFrame) MapRows(ctx context.Context, fn func(row []any) (any, error)) (*DataFrame, error) {
	var results [][]any
	tuple := false
	for row := range df.IterRows() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v, err := fn(row)
		if err != nil {
			return nil, err
		}
		if t, ok := v.([]any); ok {
			if results == nil {
				tuple = true
			}
			results = append(results, t)
		} else {
			results = append(results, []any{v})
		}
	}
	width := 1
	if len(results) > 0 {
		width = len(results[0])
	}
	cols := make([]*series.Series, 0, width)
	for c := range width {
		name := "map"
		if tuple {
			name = fmt.Sprintf("column_%d", c)
		}
		vals := make([]any, len(results))
		for r, t := range results {
			if c < len(t) {
				vals[r] = t[c]
			}
		}
		s, err := seriesFromValues(name, vals)
		if err != nil {
			releaseAll(cols)
			return nil, fmt.Errorf("dataframe.MapRows: column %d: %w", c, err)
		}
		cols = append(cols, s)
	}
	return New(cols...)
}

// seriesFromValues infers a dtype from Go values and builds a Series.
func seriesFromValues(name string, vals []any) (*series.Series, error) {
	var dt arrow.DataType
	for _, v := range vals {
		var cand arrow.DataType
		switch v.(type) {
		case nil:
			continue
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			cand = arrow.PrimitiveTypes.Int64
		case float32, float64:
			cand = arrow.PrimitiveTypes.Float64
		case string:
			cand = arrow.BinaryTypes.String
		case bool:
			cand = arrow.FixedWidthTypes.Boolean
		case time.Time:
			cand = &arrow.TimestampType{Unit: arrow.Microsecond}
		case time.Duration:
			cand = &arrow.DurationType{Unit: arrow.Microsecond}
		default:
			return nil, fmt.Errorf("unsupported value type %T", v)
		}
		switch {
		case dt == nil:
			dt = cand
		case arrow.TypeEqual(dt, cand):
		case dt.ID() == arrow.INT64 && cand.ID() == arrow.FLOAT64:
			dt = cand
		case dt.ID() == arrow.FLOAT64 && cand.ID() == arrow.INT64:
		default:
			return nil, fmt.Errorf("mixed value types %s and %s", dt, cand)
		}
	}
	if dt == nil {
		dt = arrow.Null
	}
	if dt.ID() == arrow.NULL {
		arr := array.MakeArrayOfNull(memory.DefaultAllocator, dt, len(vals))
		return seriesFromArray(name, arr)
	}
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for _, v := range vals {
		if err := appendGoValue(b, v); err != nil {
			return nil, err
		}
	}
	arr := b.NewArray()
	return seriesFromArray(name, arr)
}
