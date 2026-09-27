package dataframe_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Distinct integer keys far apart must form distinct groups. polars:
// group_by on [-1, 2305843009213693951] gives two groups of length 1.
// Found by internal/difftest (duration key).
func TestGroupByFarApartIntKeys(t *testing.T) {
	for _, keys := range [][]int64{
		{-1, 2305843009213693951},
		{-1, 1<<63 - 1},
		{0, 1 << 62, -(1 << 62)},
	} {
		s, err := series.FromInt64("k", keys, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, dt := range []dtype.DType{dtype.Int64(), dtype.Duration(dtype.Microsecond)} {
			ks := s
			if !dt.Equal(dtype.Int64()) {
				ks, err = compute.Cast(context.Background(), s, dt)
				if err != nil {
					t.Fatal(err)
				}
			}
			df, err := dataframe.New(ks.Clone())
			if err != nil {
				t.Fatal(err)
			}
			out, err := df.GroupBy("k").Agg(context.Background(), []expr.Expr{expr.Len().Alias("n")})
			if err != nil {
				t.Fatal(err)
			}
			if out.Height() != len(keys) {
				t.Errorf("%s keys %v: %d groups, want %d", dt, keys, out.Height(), len(keys))
			}
			out.Release()
			df.Release()
		}
		s.Release()
	}
}
