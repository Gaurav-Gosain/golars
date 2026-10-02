package series_test

import (
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestDirectionalFillNoLimitAgainstBrute(t *testing.T) {
	r := rand.New(rand.NewPCG(41, 42))
	for _, n := range []int{0, 1, 7, 8, 9, 17, 64, 100, 1000} {
		for _, density := range []float64{0, 0.3, 0.9, 1} {
			for _, off := range []int{0, 5} {
				mem := testutil.NewCheckedAllocator(t)
				raw := make([]int64, n+off)
				valid := make([]bool, n+off)
				for i := range raw {
					raw[i] = r.Int64()
					valid[i] = r.Float64() >= density
				}
				full, _ := series.FromInt64("x", raw, valid, series.WithAllocator(mem))
				s, _ := full.Slice(off, n)
				vals, ok := raw[off:], valid[off:]
				for _, forward := range []bool{true, false} {
					var out *series.Series
					var err error
					if forward {
						out, err = s.ForwardFill(0, series.WithAllocator(mem))
					} else {
						out, err = s.BackwardFill(0, series.WithAllocator(mem))
					}
					if err != nil {
						t.Fatal(err)
					}
					got := out.Chunk(0).(*array.Int64)
					for i := range n {
						want, wok := int64(0), false
						if forward {
							for j := i; j >= 0; j-- {
								if ok[j] {
									want, wok = vals[j], true
									break
								}
							}
						} else {
							for j := i; j < n; j++ {
								if ok[j] {
									want, wok = vals[j], true
									break
								}
							}
						}
						if got.IsValid(i) != wok || (wok && got.Value(i) != want) {
							t.Fatalf("n=%d d=%v off=%d fwd=%v row %d: got %d,%v want %d,%v",
								n, density, off, forward, i, got.Value(i), got.IsValid(i), want, wok)
						}
					}
					out.Release()
				}
				s.Release()
				full.Release()
			}
		}
	}
}
