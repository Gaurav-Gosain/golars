package golars_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/expr"
)

func TestSelectWithoutFrame(t *testing.T) {
	ctx := context.Background()
	// polars: pl.select(pl.lit(1), pl.int_range(0, 3).alias("r"))
	out, err := golars.Select(ctx, golars.Lit(1), expr.IntRange(0, 3, 1).Alias("r"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if h, w := out.Shape(); h != 3 || w != 2 {
		t.Fatalf("shape = %d x %d", h, w)
	}
	lit := out.ColumnAt(0).ToList()
	for _, v := range lit {
		if v != int64(1) {
			t.Fatalf("literal column = %v", lit)
		}
	}
	rep, err := golars.Select(ctx, golars.Repeat("z", 2).Alias("y"), golars.LinearSpace(0, 1, 2, "").Alias("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Release()
	if got := rep.ColumnAt(1).ToList(); len(got) != 2 || got[1] != 1.0 {
		t.Fatalf("linear_space = %v", got)
	}
}

func TestToFrameAndDescribeSeries(t *testing.T) {
	s, err := golars.FromFloat64("x", []float64{1, 0, 3.5}, []bool{true, false, true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	df, err := golars.ToFrame(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	if df.Width() != 1 || df.Height() != 3 {
		t.Fatalf("to_frame shape %dx%d", df.Height(), df.Width())
	}
	desc, err := golars.DescribeSeries(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer desc.Release()
	if desc.Height() == 0 {
		t.Fatal("describe returned no rows")
	}
}
