package dataframe

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// fillNullValue fills the nulls of c with value when the value's kind
// matches the column (see DataFrame.FillNull) and otherwise returns a
// clone. The fast Series.FillNull kernels are used when they apply.
func fillNullValue(c *series.Series, value any) (*series.Series, error) {
	dt := c.DType()
	_, isFloatVal := value.(float64)
	if _, ok := value.(float32); ok {
		isFloatVal = true
	}
	_, _, _, isIntVal := goInteger(value)
	if _, isBool := value.(bool); isBool {
		isIntVal = false
	}
	var matches bool
	switch {
	case value == nil:
		matches = false
	case dt.IsNumeric():
		matches = isFloatVal || isIntVal
	case dt.IsBool():
		_, matches = value.(bool)
	case dt.IsString():
		_, matches = value.(string)
	}
	if !matches || c.NullCount() == 0 {
		if matches && isFloatVal && dt.IsInteger() {
			return castToF64(c)
		}
		return c.Clone(), nil
	}
	src := c.Clone()
	// A closure so the release sees src after the reassignment below.
	defer func() { src.Release() }()
	if isFloatVal && dt.IsInteger() {
		casted, err := castToF64(c)
		if err != nil {
			return nil, err
		}
		src.Release()
		src = casted
	}
	if filled, err := src.FillNull(value); err == nil {
		return filled, nil
	}
	arr, err := src.Consolidated()
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	fill, err := constantArray(arr.DataType(), value, 1, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	defer fill.Release()
	h := arr.Len()
	idx := make([]int, h)
	for r := range h {
		if arr.IsValid(r) {
			idx[r] = r
		} else {
			idx[r] = h
		}
	}
	return withFallbackTake(c.Name(), arr, fill, idx, memory.DefaultAllocator)
}

// castToF64 widens an integer column to f64.
func castToF64(c *series.Series) (*series.Series, error) {
	if c.DType().ID() == arrow.FLOAT64 {
		return c.Clone(), nil
	}
	return castSeries(context.Background(), c, dtype.Float64(), false)
}
