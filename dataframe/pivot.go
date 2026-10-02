package dataframe

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Transpose returns a DataFrame that is the transpose of df. All
// columns must share a numeric dtype; the output has one column per
// row of the input. `headerCol` names the first output column that
// carries the original column names; `colPrefix` is used to name the
// resulting columns ("0", "1", ... when colPrefix is empty).
//
// Mirrors polars' DataFrame.transpose(include_header=False,
// header_name=headerCol, column_names=None) for numeric input.
func (df *DataFrame) Transpose(_ context.Context, headerCol, colPrefix string) (*DataFrame, error) {
	if df.Width() == 0 {
		return New()
	}
	if headerCol == "" {
		headerCol = "column"
	}
	// Verify all input columns are numeric or boolean; otherwise polars
	// falls back to Object dtype which we don't support yet.
	for _, c := range df.cols {
		if !c.DType().IsNumeric() && !c.DType().IsBool() {
			return nil, fmt.Errorf("dataframe.Transpose: column %q dtype %s not supported (need numeric/bool)",
				c.Name(), c.DType())
		}
	}
	height := df.Height()
	width := df.Width()
	// Output: one header column + `height` data columns. Each data
	// column has `width` rows (one per input column).
	headerVals := make([]string, width)
	for i, c := range df.cols {
		headerVals[i] = c.Name()
	}
	header, err := series.FromString(headerCol, headerVals, nil)
	if err != nil {
		return nil, err
	}
	out := []*series.Series{header}
	for row := range height {
		vals := make([]float64, width)
		valid := make([]bool, width)
		for ci, c := range df.cols {
			if v, ok := floatCell(c.Chunk(0), row); ok {
				vals[ci] = v
				valid[ci] = true
			}
		}
		name := strconv.Itoa(row)
		if colPrefix != "" {
			name = colPrefix + name
		}
		s, err := series.FromFloat64(name, vals, valid)
		if err != nil {
			for _, prev := range out {
				prev.Release()
			}
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}

// Unpivot reshapes df from wide to long form. idVars stay as row
// identifiers (repeated once per value column); every value column
// contributes one block of rows to a "variable" column holding its name
// and a "value" column holding its cells. valueVars defaults to every
// non-id column. The value column takes the common dtype of the value
// columns: equal dtypes are kept, integers widen to i64, any float makes
// f64 and any string makes str. Mirrors polars' DataFrame.unpivot.
func (df *DataFrame) Unpivot(ctx context.Context, idVars []string, valueVars []string) (*DataFrame, error) {
	if len(valueVars) == 0 {
		seen := make(map[string]struct{}, len(idVars))
		for _, v := range idVars {
			seen[v] = struct{}{}
		}
		for _, c := range df.cols {
			if _, ok := seen[c.Name()]; !ok {
				valueVars = append(valueVars, c.Name())
			}
		}
	}
	// polars resolves the output schema from the whole input schema
	// plus "variable" and "value", so an input column with either name
	// is a DuplicateError even when it is not kept.
	for _, n := range []string{"variable", "value"} {
		if _, ok := df.sch.Index(n); ok {
			return nil, fmt.Errorf("dataframe.Unpivot: duplicate column name %q", n)
		}
	}
	seenID := map[string]bool{}
	for _, n := range idVars {
		if seenID[n] {
			return nil, fmt.Errorf("dataframe.Unpivot: duplicate column name %q", n)
		}
		seenID[n] = true
	}
	idCols, err := df.subsetColumns(idVars)
	if err != nil {
		return nil, err
	}
	if len(idVars) == 0 {
		idCols = nil
	}
	valCols := make([]*series.Series, len(valueVars))
	for i, v := range valueVars {
		c, err := df.Column(v)
		if err != nil {
			return nil, err
		}
		valCols[i] = c
	}
	h := df.height
	total := h * len(valueVars)
	rep := make([]int, total)
	for i := range rep {
		rep[i] = i % max(h, 1)
	}
	out := make([]*series.Series, 0, len(idCols)+2)
	for _, c := range idCols {
		s, err := takeOptional(ctx, c, rep, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	names := make([]string, total)
	for i, v := range valueVars {
		for r := range h {
			names[i*h+r] = v
		}
	}
	varCol, err := series.FromString("variable", names, nil)
	if err != nil {
		releaseAll(out)
		return nil, err
	}
	out = append(out, varCol)

	target, err := unpivotDType(valCols)
	if err != nil {
		releaseAll(out)
		return nil, err
	}
	var parts []arrow.Array
	defer func() {
		for _, a := range parts {
			a.Release()
		}
	}()
	for _, c := range valCols {
		casted, err := castSeries(ctx, c, target, false)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		a, err := casted.Consolidated()
		casted.Release()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		parts = append(parts, a)
	}
	var values arrow.Array
	if len(parts) == 0 {
		values = array.MakeArrayOfNull(memory.DefaultAllocator, target.Arrow(), 0)
	} else {
		values, err = array.Concatenate(parts, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
	}
	valSeries, err := seriesFromArray("value", values)
	if err != nil {
		releaseAll(out)
		return nil, err
	}
	out = append(out, valSeries)
	return New(out...)
}

// unpivotDType is the common dtype of the value columns of an unpivot.
func unpivotDType(cols []*series.Series) (dtype.DType, error) {
	if len(cols) == 0 {
		return dtype.Null(), nil
	}
	dt := cols[0].DType()
	same, anyStr, anyFloat, allNum := true, false, false, true
	for _, c := range cols {
		d := c.DType()
		if !d.Equal(dt) {
			same = false
		}
		switch {
		case d.IsString():
			anyStr = true
		case d.IsFloating():
			anyFloat = true
		case d.IsInteger(), d.IsNull():
		default:
			allNum = false
		}
	}
	switch {
	case same:
		return dt, nil
	case anyStr:
		return dtype.String(), nil
	case allNum && anyFloat:
		return dtype.Float64(), nil
	case allNum:
		return dtype.Int64(), nil
	}
	return dtype.DType{}, fmt.Errorf("dataframe.Unpivot: value columns have incompatible dtypes")
}

func floatCell(chunk any, i int) (float64, bool) {
	switch a := chunk.(type) {
	case *array.Float64:
		if !a.IsValid(i) {
			return 0, false
		}
		return a.Value(i), true
	case *array.Float32:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Int64:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Int32:
		if !a.IsValid(i) {
			return 0, false
		}
		return float64(a.Value(i)), true
	case *array.Boolean:
		if !a.IsValid(i) {
			return 0, false
		}
		if a.Value(i) {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}
