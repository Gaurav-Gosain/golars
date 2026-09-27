package dataframe_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestGroupByMultiChunkMatchesSingleChunk checks that a frame whose
// columns span several chunks (as produced by parallel readers or
// concat) groups exactly like the same data in one chunk, for single
// and multi-key group-bys.
func TestGroupByMultiChunkMatchesSingleChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mk := func(start, n int) *dataframe.DataFrame {
		regs := make([]string, n)
		years := make([]int64, n)
		vals := make([]int64, n)
		for i := range n {
			j := start + i
			regs[i] = fmt.Sprintf("r%d", j%7)
			years[i] = int64(2000 + j%3)
			vals[i] = int64(j)
		}
		r, _ := series.FromString("r", regs, nil)
		y, _ := series.FromInt64("y", years, nil)
		v, _ := series.FromInt64("v", vals, nil)
		df, _ := dataframe.New(r, y, v)
		return df
	}
	parts := []*dataframe.DataFrame{mk(0, 500), mk(500, 700), mk(1200, 300)}
	chunked, err := dataframe.Concat(parts...)
	if err != nil {
		t.Fatal(err)
	}
	defer chunked.Release()
	for _, p := range parts {
		p.Release()
	}
	whole := mk(0, 1500)
	defer whole.Release()

	col, _ := chunked.Column("r")
	if col.NumChunks() < 2 {
		t.Skip("Concat produced a single chunk; nothing to test")
	}
	aggs := []expr.Expr{expr.Col("v").Sum().Alias("s"), expr.Col("v").Max().Alias("m")}
	for _, keys := range [][]string{{"r"}, {"r", "y"}} {
		got, err := chunked.GroupBy(keys...).Agg(ctx, aggs)
		if err != nil {
			t.Fatal(err)
		}
		want, err := whole.GroupBy(keys...).Agg(ctx, aggs)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equals(want) {
			t.Fatalf("keys %v: multi-chunk result differs:\n%v\nwant\n%v", keys, got, want)
		}
		got.Release()
		want.Release()
	}
}
