package dataframe_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestGroupByGenericKeyRegression covers the generic key encoding used
// for key tuples the hash paths do not handle (here anything with a
// date column). It used to put -0.0 and 0.0 in different groups and let
// separator bytes inside strings merge different tuples. polars 1.39:
//
//	pl.DataFrame({"f": [0.0, -0.0], "d": [None, None]}, schema={"f": pl.Float32, "d": pl.Date})
//	  .group_by("f", "d").agg(pl.len())  -> 1 row, len 2
//	pl.DataFrame({"a": ["a\x1f\x01b", "a"], "b": ["c", "b\x1f\x01c"], "d": [None, None]}, ...)
//	  .group_by("a", "b", "d").agg(pl.len())  -> 2 rows
func TestGroupByGenericKeyRegression(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	build := func(cols ...*series.Series) *dataframe.DataFrame {
		df, err := dataframe.New(cols...)
		if err != nil {
			t.Fatal(err)
		}
		return df
	}
	col := func(name string, dt arrow.DataType, vals ...any) *series.Series {
		s, err := series.FromValues(name, dt, vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	cases := []struct {
		df   *dataframe.DataFrame
		keys []string
		want int
	}{
		{build(
			col("f", arrow.PrimitiveTypes.Float32, 0.0, math.Copysign(0, -1)),
			col("d", arrow.FixedWidthTypes.Date32, nil, nil)), []string{"f", "d"}, 1},
		{build(
			col("f", arrow.PrimitiveTypes.Float64, math.Copysign(0, -1), 0.0, math.NaN(), math.NaN()),
			col("d", arrow.FixedWidthTypes.Date32, int64(3), int64(3), nil, nil)), []string{"f", "d"}, 2},
		{build(
			col("a", arrow.BinaryTypes.String, "a\x1f\x01b", "a"),
			col("b", arrow.BinaryTypes.String, "c", "b\x1f\x01c"),
			col("d", arrow.FixedWidthTypes.Date32, nil, nil)), []string{"a", "b", "d"}, 2},
	}
	for i, c := range cases {
		out, err := c.df.GroupBy(c.keys...).Agg(ctx, []expr.Expr{expr.Len()}, dataframe.WithGroupByAllocator(mem))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if out.Height() != c.want {
			t.Errorf("case %d: %d groups, want %d", i, out.Height(), c.want)
		}
		out.Release()
		c.df.Release()
	}
}
