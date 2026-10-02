package dataframe_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestSumHorizontalFusedMatchesSequential checks the fused fast path
// against a strict left-to-right sum for every column count the fusion
// special-cases, on both sides of the parallel cutoff. Float inputs use
// magnitudes where association order changes the result, so a reorder
// would show up as a mismatch.
func TestSumHorizontalFusedMatchesSequential(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, n := range []int{0, 1, 7, 1000, 200_000} {
		for k := 1; k <= 6; k++ {
			ints := make([]*series.Series, k)
			floats := make([]*series.Series, k)
			iv := make([][]int64, k)
			fv := make([][]float64, k)
			for c := range k {
				iv[c] = make([]int64, n)
				fv[c] = make([]float64, n)
				for i := range n {
					iv[c][i] = int64((i*31+c*17)%1000) - 500
					fv[c][i] = math.Pow(10, float64((i+c)%17-8)) * float64(1+(i*7+c)%13)
				}
				name := string(rune('a' + c))
				ints[c], _ = series.FromInt64(name, iv[c], nil)
				floats[c], _ = series.FromFloat64(name, fv[c], nil)
			}
			idf, _ := dataframe.New(ints...)
			fdf, _ := dataframe.New(floats...)

			gotI, err := idf.SumHorizontal(ctx, dataframe.IgnoreNulls)
			if err != nil {
				t.Fatal(err)
			}
			gotF, err := fdf.SumHorizontal(ctx, dataframe.IgnoreNulls)
			if err != nil {
				t.Fatal(err)
			}
			if gotI.Len() != n || gotF.Len() != n {
				t.Fatalf("n=%d k=%d: lengths %d %d", n, k, gotI.Len(), gotF.Len())
			}
			if n > 0 {
				ia := gotI.Chunk(0).(*array.Int64).Int64Values()
				fa := gotF.Chunk(0).(*array.Float64).Float64Values()
				for i := range n {
					var ws int64
					var wf float64
					for c := range k {
						ws += iv[c][i]
						wf += fv[c][i]
					}
					if ia[i] != ws {
						t.Fatalf("n=%d k=%d row %d: int sum %d, want %d", n, k, i, ia[i], ws)
					}
					if math.Float64bits(fa[i]) != math.Float64bits(wf) {
						t.Fatalf("n=%d k=%d row %d: float sum %v, want %v", n, k, i, fa[i], wf)
					}
				}
			}
			gotI.Release()
			gotF.Release()
			idf.Release()
			fdf.Release()
		}
	}
}
