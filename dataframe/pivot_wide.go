package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// PivotAgg names the reduction polars' DataFrame.pivot applies when
// several rows map to the same (index, on) cell. The empty value means
// no aggregation: every cell must hold at most one value.
type PivotAgg string

// Pivot aggregations. PivotCount is polars' deprecated alias of PivotLen.
const (
	PivotFirst  PivotAgg = "first"
	PivotLast   PivotAgg = "last"
	PivotSum    PivotAgg = "sum"
	PivotMean   PivotAgg = "mean"
	PivotMedian PivotAgg = "median"
	PivotMin    PivotAgg = "min"
	PivotMax    PivotAgg = "max"
	PivotCount  PivotAgg = "count"
	PivotLen    PivotAgg = "len"
)

// Pivot reshapes a long-form DataFrame into wide form. index names the
// row identifier columns, on the column whose distinct values become new
// columns and values the column that fills them. Rows and new columns
// appear in order of first appearance. agg reduces cells that receive
// several values; cells without values are null, except for sum, count
// and len which report 0. Output dtypes follow the aggregation (sum keeps
// the value dtype, mean is f64, len is u32).
//
// Mirrors polars' DataFrame.pivot(on=, index=, values=,
// aggregate_function=).
func (df *DataFrame) Pivot(ctx context.Context, index []string, on string, values string, agg PivotAgg) (*DataFrame, error) {
	if len(index) == 0 {
		return nil, fmt.Errorf("dataframe.Pivot: at least one index column required")
	}
	keys := append(append([]string(nil), index...), on)
	names := append(append([]string(nil), keys...), values)
	src, err := df.Select(names...)
	if err != nil {
		return nil, err
	}
	defer src.Release()
	gb := src.GroupBy(keys...).MaintainOrder()

	if agg == "" {
		counts, err := gb.Len(ctx, "__pivot_len")
		if err != nil {
			return nil, err
		}
		lenCol, _ := counts.Column("__pivot_len")
		maxLen := uint32(0)
		for _, v := range rawValues[uint32](firstChunk(lenCol)) {
			maxLen = max(maxLen, v)
		}
		counts.Release()
		if maxLen > 1 {
			return nil, fmt.Errorf("dataframe.Pivot: found multiple elements in the same group, please specify an aggregation function")
		}
		agg = PivotFirst
	}

	var long *DataFrame
	switch agg {
	case PivotFirst:
		long, err = gb.First(ctx)
	case PivotLast:
		long, err = gb.Last(ctx)
	case PivotSum:
		long, err = gb.Sum(ctx)
	case PivotMean:
		long, err = gb.Mean(ctx)
	case PivotMedian:
		long, err = gb.Median(ctx)
	case PivotMin:
		long, err = gb.Min(ctx)
	case PivotMax:
		long, err = gb.Max(ctx)
	case PivotCount, PivotLen:
		long, err = gb.Len(ctx, values)
	default:
		return nil, fmt.Errorf("dataframe.Pivot: unknown aggregation %q", string(agg))
	}
	if err != nil {
		return nil, err
	}
	defer long.Release()

	idxCols, err := long.subsetColumns(index)
	if err != nil {
		return nil, err
	}
	onCol, _ := long.Column(on)
	valCol, _ := long.Column(values)
	n := long.Height()
	rowIDs, rowFirst := rowGroupIDs(idxCols, n)
	colIDs, colFirst := rowGroupIDs([]*series.Series{onCol}, n)

	out := make([]*series.Series, 0, len(index)+len(colFirst))
	for _, c := range idxCols {
		s, err := takeOptional(ctx, c, rowFirst, memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}

	valArr, err := valCol.Consolidated()
	if err != nil {
		releaseAll(out)
		return nil, err
	}
	defer valArr.Release()
	var fill arrow.Array
	if agg == PivotSum || agg == PivotCount || agg == PivotLen {
		v, err := zeroOneValue(valArr.DataType(), false)
		if err == nil {
			fill, err = constantArray(valArr.DataType(), v, 1, memory.DefaultAllocator)
			if err != nil {
				releaseAll(out)
				return nil, err
			}
			defer fill.Release()
		}
	}
	// colFirst indexes the whole column, so labels come from a
	// contiguous copy (a multi-chunk column would read the wrong row
	// or past the first chunk).
	onArr, err := onCol.Consolidated()
	if err != nil {
		releaseAll(out)
		return nil, err
	}
	defer onArr.Release()
	cells := make([][]int, len(colFirst))
	for c := range cells {
		cells[c] = make([]int, len(rowFirst))
		for r := range cells[c] {
			cells[c][r] = -1
			if fill != nil {
				cells[c][r] = n
			}
		}
	}
	for i := range n {
		cells[colIDs[i]][rowIDs[i]] = i
	}
	for c, first := range colFirst {
		label := "null"
		if onArr.IsValid(first) {
			label = dummyLabel(onArr, first)
		}
		s, err := withFallbackTake(label, valArr, fill, cells[c], memory.DefaultAllocator)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, s)
	}
	return New(out...)
}
