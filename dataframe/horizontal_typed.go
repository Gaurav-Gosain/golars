package dataframe

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// horizontalTyped computes sum, min and max across columns with polars'
// output dtype: the common supertype of the inputs (equal dtypes are
// kept, mixed integers widen to i64, any float gives f64 unless every
// input is f32; sums of 8 and 16 bit integers widen to i64). With
// IgnoreNulls a row without values sums to 0 and has a null min or max.
// It returns ok=false for other operations so the float path handles
// them.
func horizontalTyped(op string, cols []*series.Series, strategy NullStrategy, n int) (*series.Series, bool, error) {
	if op != "sum" && op != "min" && op != "max" {
		return nil, false, nil
	}
	target := horizontalSupertype(cols, op == "sum")
	if target == nil {
		return nil, false, nil
	}
	arrs := make([]arrow.Array, len(cols))
	for i, c := range cols {
		arrs[i] = firstChunk(c)
	}
	isFloat := target.ID() == arrow.FLOAT32 || target.ID() == arrow.FLOAT64
	fvals := make([]float64, n)
	ivals := make([]int64, n)
	valid := make([]bool, n)
	seen := make([]bool, n)
	nulled := make([]bool, n)
	for _, a := range arrs {
		get := numericGetter(a)
		for r := range n {
			if a.IsNull(r) {
				nulled[r] = true
				continue
			}
			f, iv := get(r)
			switch {
			case !seen[r]:
				fvals[r], ivals[r] = f, iv
			case op == "sum":
				fvals[r] += f
				ivals[r] += iv
			case op == "min":
				if isFloat && f < fvals[r] || !isFloat && iv < ivals[r] {
					fvals[r], ivals[r] = f, iv
				}
			default:
				if isFloat && f > fvals[r] || !isFloat && iv > ivals[r] {
					fvals[r], ivals[r] = f, iv
				}
			}
			seen[r] = true
		}
	}
	for r := range n {
		switch {
		case strategy == PropagateNulls && nulled[r]:
			valid[r] = false
		case !seen[r]:
			valid[r] = op == "sum"
		default:
			valid[r] = true
		}
	}
	b := array.NewBuilder(memory.DefaultAllocator, target)
	defer b.Release()
	for r := range n {
		if !valid[r] {
			b.AppendNull()
			continue
		}
		var v any = ivals[r]
		if isFloat {
			v = fvals[r]
		}
		if err := appendWrapped(b, v); err != nil {
			return nil, true, err
		}
	}
	s, err := seriesFromArray(op, b.NewArray())
	return s, true, err
}

// appendWrapped appends v to an integer or float builder, truncating
// integers to the builder width (sums wrap like polars).
func appendWrapped(b array.Builder, v any) error {
	if iv, ok := v.(int64); ok {
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(int8(iv))
			return nil
		case *array.Int16Builder:
			bb.Append(int16(iv))
			return nil
		case *array.Int32Builder:
			bb.Append(int32(iv))
			return nil
		case *array.Uint8Builder:
			bb.Append(uint8(iv))
			return nil
		case *array.Uint16Builder:
			bb.Append(uint16(iv))
			return nil
		case *array.Uint32Builder:
			bb.Append(uint32(iv))
			return nil
		case *array.Uint64Builder:
			bb.Append(uint64(iv))
			return nil
		}
	}
	return appendGoValue(b, v)
}

// numericGetter returns row r of a numeric array as float64 and int64.
func numericGetter(a arrow.Array) func(int) (float64, int64) {
	switch a.DataType().ID() {
	case arrow.INT8:
		v := rawValues[int8](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.INT16:
		v := rawValues[int16](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.INT32:
		v := rawValues[int32](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.INT64:
		v := rawValues[int64](a)
		return func(r int) (float64, int64) { return float64(v[r]), v[r] }
	case arrow.UINT8:
		v := rawValues[uint8](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.UINT16:
		v := rawValues[uint16](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.UINT32:
		v := rawValues[uint32](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.UINT64:
		v := rawValues[uint64](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.FLOAT32:
		v := rawValues[float32](a)
		return func(r int) (float64, int64) { return float64(v[r]), int64(v[r]) }
	case arrow.FLOAT64:
		v := rawValues[float64](a)
		return func(r int) (float64, int64) { return v[r], int64(v[r]) }
	}
	return func(int) (float64, int64) { return 0, 0 }
}

// horizontalSupertype returns the output dtype for a horizontal sum
// (widenSmall) or min/max over cols, or nil when a column is not numeric
// or there are no columns.
func horizontalSupertype(cols []*series.Series, widenSmall bool) arrow.DataType {
	if len(cols) == 0 {
		return nil
	}
	first := firstChunk(cols[0]).DataType()
	same, anyFloat, allF32 := true, false, true
	for _, c := range cols {
		dt := c.DType()
		if !dt.IsNumeric() {
			return nil
		}
		if !arrow.TypeEqual(dt.Arrow(), first) {
			same = false
		}
		if dt.IsFloating() {
			anyFloat = true
		}
		if dt.ID() != arrow.FLOAT32 {
			allF32 = false
		}
	}
	switch {
	case anyFloat && allF32:
		return arrow.PrimitiveTypes.Float32
	case anyFloat:
		return arrow.PrimitiveTypes.Float64
	case same:
		if widenSmall {
			switch first.ID() {
			case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16:
				return arrow.PrimitiveTypes.Int64
			}
		}
		return first
	}
	return arrow.PrimitiveTypes.Int64
}
