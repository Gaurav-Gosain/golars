package dataframe

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestQuantileSelectMatchesSort checks the selection-based quantile
// against sorting plus quantileSorted on random inputs with NaNs and
// duplicates, for every method.
func TestQuantileSelectMatchesSort(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	methods := []QuantileMethod{QuantileNearest, QuantileHigher, QuantileLower,
		QuantileMidpoint, QuantileLinear, QuantileEquiprobable}
	for iter := range 3000 {
		n := 1 + r.IntN(60)
		if iter%50 == 0 {
			n = 500 + r.IntN(500)
		}
		vals := make([]float64, n)
		for i := range vals {
			switch r.IntN(10) {
			case 0:
				vals[i] = math.NaN()
			case 1:
				vals[i] = 3
			default:
				vals[i] = float64(r.IntN(40)) - 20
			}
		}
		sorted := slices.Clone(vals)
		sortFloatsNaNLast(sorted)
		for _, m := range methods {
			q := r.Float64()
			if iter%7 == 0 {
				q = float64(r.IntN(5)) / 4
			}
			want, _ := quantileSorted(sorted, q, m)
			got, _ := quantileSelect(slices.Clone(vals), q, m)
			if !(got == want || (math.IsNaN(got) && math.IsNaN(want))) {
				t.Fatalf("n=%d q=%v method=%s: got %v want %v", n, q, m, got, want)
			}
		}
	}
}

func TestSelectKth(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for range 500 {
		n := 1 + r.IntN(300)
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = float64(r.IntN(50))
		}
		sorted := slices.Clone(vals)
		slices.Sort(sorted)
		k := r.IntN(n)
		if got := selectKth(vals, k); got != sorted[k] {
			t.Fatalf("k=%d got %v want %v", k, got, sorted[k])
		}
	}
}
