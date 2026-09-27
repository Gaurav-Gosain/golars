package dataframe

import (
	"context"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// Describe returns summary statistics for every numeric column: count,
// null_count, mean, std, min, 25%, 50%, 75%, max. Non-numeric columns are
// described with count and null_count only (other fields are null).
//
// Mirrors polars' DataFrame.describe(). The output is a new DataFrame with
// a "statistic" column naming the row.
func (df *DataFrame) Describe(ctx context.Context) (*DataFrame, error) {
	rowNames := []string{"count", "null_count", "mean", "std", "min", "25%", "50%", "75%", "max"}
	statCol, err := series.FromString("statistic", rowNames, nil)
	if err != nil {
		return nil, err
	}

	cols := []*series.Series{statCol}
	for _, src := range df.cols {
		col, err := describeColumn(ctx, src, len(rowNames))
		if err != nil {
			for _, c := range cols {
				c.Release()
			}
			return nil, err
		}
		cols = append(cols, col)
	}
	return New(cols...)
}

func describeColumn(ctx context.Context, s *series.Series, nRows int) (*series.Series, error) {
	name := s.Name()
	nonNull := int64(s.Len() - s.NullCount())
	nullCount := int64(s.NullCount())

	if s.DType().IsNumeric() {
		vals, ok := collectFloat64(s)
		if !ok {
			// Shouldn't happen for IsNumeric but guard anyway.
			return fillStatsString(name, nRows, nonNull, nullCount), nil
		}
		mean, std, minV, q25, q50, q75, maxV := quantileStats(vals)
		out := make([]float64, nRows)
		valid := make([]bool, nRows)
		out[0], valid[0] = float64(nonNull), true
		out[1], valid[1] = float64(nullCount), true
		if len(vals) > 0 {
			out[2], valid[2] = mean, true
			if len(vals) > 1 {
				out[3], valid[3] = std, true
			}
			out[4], valid[4] = minV, true
			out[5], valid[5] = q25, true
			out[6], valid[6] = q50, true
			out[7], valid[7] = q75, true
			out[8], valid[8] = maxV, true
		}
		_ = ctx
		return series.FromFloat64(name, out, compactValidBools(valid))
	}
	return fillStatsString(name, nRows, nonNull, nullCount), nil
}

// fillStatsString builds a string series where only the first two rows
// (count, null_count) carry values; everything else is null. Used for
// non-numeric columns.
func fillStatsString(name string, nRows int, nonNull, nullCount int64) *series.Series {
	vals := make([]string, nRows)
	valid := make([]bool, nRows)
	vals[0] = formatInt(nonNull)
	valid[0] = true
	vals[1] = formatInt(nullCount)
	valid[1] = true
	s, _ := series.FromString(name, vals, valid)
	return s
}

func formatInt(v int64) string {
	// Fast enough for stat rows: no allocator tuning needed.
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// collectFloat64 materialises a numeric Series' non-null values as
// float64. Returns ok=false for non-numeric inputs.
func collectFloat64(s *series.Series) ([]float64, bool) {
	chunk := s.Chunk(0)
	n := chunk.Len()
	out := make([]float64, 0, n)
	switch a := chunk.(type) {
	case *array.Int8:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Int16:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Int32:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Int64:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Uint8:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Uint16:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Uint32:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Uint64:
		for i := range n {
			if !a.IsNull(i) {
				out = append(out, float64(a.Value(i)))
			}
		}
	case *array.Float32:
		for i := range n {
			if !a.IsNull(i) {
				v := float64(a.Value(i))
				if !math.IsNaN(v) {
					out = append(out, v)
				}
			}
		}
	case *array.Float64:
		for i := range n {
			if !a.IsNull(i) {
				v := a.Value(i)
				if !math.IsNaN(v) {
					out = append(out, v)
				}
			}
		}
	default:
		return nil, false
	}
	return out, true
}

// quantileStats returns (mean, std, min, q25, q50, q75, max) over the
// non-null samples. Quantiles use the nearest sorted sample, as polars'
// describe does. std is the sample std dev (N-1).
func quantileStats(vals []float64) (mean, std, minV, q25, q50, q75, maxV float64) {
	n := len(vals)
	if n == 0 {
		return 0, 0, math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean = sum / float64(n)

	if n > 1 {
		sq := 0.0
		for _, v := range vals {
			d := v - mean
			sq += d * d
		}
		std = math.Sqrt(sq / float64(n-1))
	}

	// Sort a copy for quantiles. For full parity with polars we could use
	// compute.SortIndices, but a plain sort here keeps the method simple
	// and avoids round-tripping through Series wrappers.
	sorted := make([]float64, n)
	copy(sorted, vals)
	sort.Float64s(sorted)
	minV = sorted[0]
	maxV = sorted[n-1]
	q25 = quantileNearest(sorted, 0.25)
	q50 = quantileNearest(sorted, 0.50)
	q75 = quantileNearest(sorted, 0.75)
	return
}

// quantileNearest is polars' "nearest" quantile, which describe uses:
// the sorted value at round(p*(n-1)), ties away from zero.
func quantileNearest(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	return sorted[int(math.Round(p*float64(len(sorted)-1)))]
}

func compactValidBools(valid []bool) []bool {
	for _, v := range valid {
		if !v {
			return valid
		}
	}
	return nil
}
