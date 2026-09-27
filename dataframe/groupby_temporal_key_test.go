package dataframe_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Multi-key group-bys with a temporal key used to put every row in its
// own group: the hash path did not accept dates and the sort path's key
// equality returned false for any temporal array.
func TestGroupByMultiKeyTemporal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		key  func(mem series.Option) *series.Series
		dt   dtype.DType
	}{
		{"date", func(o series.Option) *series.Series {
			s, _ := series.FromDate("d", []int32{10, 11, 10, 11, 10, 12}, []bool{true, true, true, true, true, false}, o)
			return s
		}, dtype.Date()},
		{"datetime", func(o series.Option) *series.Series {
			s, _ := series.FromDatetime("d", []int64{10, 11, 10, 11, 10, 12}, []bool{true, true, true, true, true, false}, dtype.Microsecond, "", o)
			return s
		}, dtype.Datetime(dtype.Microsecond, "")},
		{"duration", func(o series.Option) *series.Series {
			s, _ := series.FromDuration("d", []int64{10, 11, 10, 11, 10, 12}, []bool{true, true, true, true, true, false}, dtype.Microsecond, o)
			return s
		}, dtype.Duration(dtype.Microsecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			o := series.WithAllocator(mem)
			k, _ := series.FromInt64("k", []int64{1, 1, 1, 1, 2, 2}, nil, o)
			v, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5, 6}, nil, o)
			df, err := dataframe.New(k, tc.key(o), v)
			if err != nil {
				t.Fatal(err)
			}
			defer df.Release()
			out, err := df.GroupBy("k", "d").Agg(ctx,
				[]expr.Expr{expr.Col("v").Sum().Alias("s")},
				dataframe.WithGroupByAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if out.Height() != 4 {
				t.Fatalf("groups = %d, want 4\n%s", out.Height(), out)
			}
			d, _ := out.Column("d")
			if !d.DType().Equal(tc.dt) {
				t.Fatalf("key dtype = %s, want %s", d.DType(), tc.dt)
			}
			sorted, err := out.SortBy(ctx, []string{"k", "d"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer sorted.Release()
			s, _ := sorted.Column("s")
			want := []int64{4, 6, 6, 5} // nulls sort first
			for i, w := range want {
				if got, _ := s.Get(i); got != w {
					t.Fatalf("sum[%d] = %v, want %d\n%s", i, got, w, sorted)
				}
			}
		})
	}
}
