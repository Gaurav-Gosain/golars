package dataframe_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Float keys in a multi-key group-by: -0.0 groups with 0.0, NaN with
// NaN, null apart (polars 1.39 gives the same groups).
func TestGroupByMultiKeyFloat(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	nan := math.NaN()
	k, _ := series.FromInt64("k", []int64{1, 1, 1, 1, 1, 1, 2}, nil, o)
	f, _ := series.FromFloat64("f", []float64{0, math.Copysign(0, -1), nan, nan, 0, -1.5, -1.5},
		[]bool{true, true, true, true, false, true, true}, o)
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5, 6, 7}, nil, o)
	df, _ := dataframe.New(k, f, v)
	defer df.Release()
	ctx := context.Background()
	out, err := df.GroupBy("k", "f").Agg(ctx, []expr.Expr{expr.Col("v").Sum().Alias("s")}, dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	sorted, err := out.SortBy(ctx, []string{"k", "f"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Release()
	want := []string{"1 <nil> 5", "1 -1.5 6", "1 0 3", "1 NaN 7", "2 -1.5 7"}
	if sorted.Height() != len(want) {
		t.Fatalf("groups = %d, want %d\n%s", sorted.Height(), len(want), sorted)
	}
	for i, w := range want {
		kv, _ := sorted.ColumnAt(0).Get(i)
		fv, _ := sorted.ColumnAt(1).Get(i)
		sv, _ := sorted.ColumnAt(2).Get(i)
		if got := fmt.Sprint(kv, " ", fv, " ", sv); got != w {
			t.Fatalf("row %d = %q, want %q\n%s", i, got, w, sorted)
		}
	}
	if fv, _ := sorted.ColumnAt(1).Get(2); math.Signbit(fv.(float64)) {
		t.Fatal("0.0 group key came out as -0.0")
	}
}

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
