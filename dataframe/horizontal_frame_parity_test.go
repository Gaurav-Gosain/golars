package dataframe_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Expected values come from polars 1.39.3 DataFrame.*_horizontal and
// DataFrame.fill_null on the same frame.
func TestParityFrameHorizontalAndFillValue(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := fxFrame(t,
		fxCol(t, mem, "a", fxI64, 1, nil, 3, nil),
		fxCol(t, mem, "b", fxF64, 4.0, 5.0, nil, nil),
		fxCol(t, mem, "c", fxI32, 1, 2, 3, nil),
	)
	defer df.Release()
	ints, _ := df.Select("a", "c")
	defer ints.Release()
	i32, _ := df.Select("c")
	defer i32.Release()

	cases := []struct {
		name string
		fn   func() (*series.Series, error)
		want string
	}{
		{"sum", func() (*series.Series, error) { return df.SumHorizontal(ctx, dataframe.IgnoreNulls) },
			"sum:f64\n6.0\n7.0\n6.0\n0.0"},
		{"sum propagate", func() (*series.Series, error) { return df.SumHorizontal(ctx, dataframe.PropagateNulls) },
			"sum:f64\n6.0\nnull\nnull\nnull"},
		{"mean", func() (*series.Series, error) { return df.MeanHorizontal(ctx, dataframe.IgnoreNulls) },
			"mean:f64\n2.0\n3.5\n3.0\nnull"},
		{"mean propagate", func() (*series.Series, error) { return df.MeanHorizontal(ctx, dataframe.PropagateNulls) },
			"mean:f64\n2.0\nnull\nnull\nnull"},
		{"min", func() (*series.Series, error) { return df.MinHorizontal(ctx, dataframe.IgnoreNulls) },
			"min:f64\n1.0\n2.0\n3.0\nnull"},
		{"max", func() (*series.Series, error) { return df.MaxHorizontal(ctx, dataframe.IgnoreNulls) },
			"max:f64\n4.0\n5.0\n3.0\nnull"},
		{"sum ints", func() (*series.Series, error) { return ints.SumHorizontal(ctx, dataframe.IgnoreNulls) },
			"sum:i64\n2\n2\n6\n0"},
		{"max ints", func() (*series.Series, error) { return ints.MaxHorizontal(ctx, dataframe.IgnoreNulls) },
			"max:i64\n1\n2\n3\nnull"},
		{"min i32", func() (*series.Series, error) { return i32.MinHorizontal(ctx, dataframe.IgnoreNulls) },
			"min:i32\n1\n2\n3\nnull"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := c.fn()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Release()
			framerepr.AssertSeries(t, s, c.want)
		})
	}

	withStr := fxFrame(t,
		fxCol(t, mem, "a", fxI64, 1, nil, 3, nil),
		fxCol(t, mem, "b", fxF64, 4.0, 5.0, nil, nil),
		fxCol(t, mem, "c", fxI32, 1, 2, 3, nil),
		fxCol(t, mem, "s", fxStr, "x", "y", "z", "w"),
	)
	defer withStr.Release()
	fills := []struct {
		name string
		v    any
		want string
	}{
		{"fill 2.5", 2.5, "a:f64|b:f64|c:f64|s:str\n1.0|4.0|1.0|\"x\"\n2.5|5.0|2.0|\"y\"\n3.0|2.5|3.0|\"z\"\n2.5|2.5|2.5|\"w\""},
		{"fill 0", 0, "a:i64|b:f64|c:i32|s:str\n1|4.0|1|\"x\"\n0|5.0|2|\"y\"\n3|0.0|3|\"z\"\n0|0.0|0|\"w\""},
	}
	for _, f := range fills {
		t.Run(f.name, func(t *testing.T) {
			out, err := withStr.FillNull(f.v)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			framerepr.AssertFrame(t, out, f.want)
		})
	}
}
