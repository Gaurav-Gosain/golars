package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/series"
)

// FuzzRollingMinMax cross-checks the deque drivers against a scalar
// brute force on adversarial small inputs (ties, nulls, NaN, edges).
func FuzzRollingMinMax(f *testing.F) {
	f.Add([]byte{3, 1, 4, 1, 5, 9, 2, 6})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data) > 48 {
			t.Skip()
		}
		n := len(data)
		vals := make([]float64, n)
		valid := make([]bool, n)
		for i, b := range data {
			switch b % 9 {
			case 0:
				vals[i] = math.NaN()
			case 1:
				vals[i] = math.Inf(1)
			case 2:
				vals[i] = math.Inf(-1)
			default:
				vals[i] = float64(int64(b) % 5)
			}
			valid[i] = b%4 != 0
		}
		s, err := series.FromFloat64("x", vals, valid)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Release()

		// Oracle: a NaN surfaces exactly while it is the oldest
		// valid entry (matching the deque); otherwise NaNs are
		// skipped and the min/max runs over the rest.
		oracle := func(i, w int, isMin bool) (float64, bool) {
			lo := max(0, i-w+1)
			oldest := -1
			count := 0
			for j := lo; j <= i; j++ {
				if !valid[j] {
					continue
				}
				if oldest < 0 {
					oldest = j
				}
				count++
			}
			if count == 0 {
				return 0, false
			}
			if math.IsNaN(vals[oldest]) {
				return math.NaN(), true
			}
			best := vals[oldest]
			for j := oldest + 1; j <= i; j++ {
				if !valid[j] || math.IsNaN(vals[j]) {
					continue
				}
				if isMin && vals[j] < best {
					best = vals[j]
				}
				if !isMin && vals[j] > best {
					best = vals[j]
				}
			}
			return best, true
		}
		for _, w := range []int{1, 2, 3, 7} {
			if w > n {
				continue
			}
			for _, mp := range []int{1, w} {
				for _, isMin := range []bool{true, false} {
					var got *series.Series
					var err error
					if isMin {
						got, err = s.RollingMin(series.RollingOptions{WindowSize: w, MinPeriods: mp})
					} else {
						got, err = s.RollingMax(series.RollingOptions{WindowSize: w, MinPeriods: mp})
					}
					if err != nil {
						t.Fatal(err)
					}
					arr := got.Chunk(0).(*array.Float64)
					for i := range n {
						lo := max(0, i-w+1)
						count := 0
						for j := lo; j <= i; j++ {
							if valid[j] {
								count++
							}
						}
						effMP := mp
						if effMP <= 0 {
							effMP = w
						}
						wantOK := count >= effMP
						if arr.IsValid(i) != wantOK {
							got.Release()
							t.Fatalf("w=%d mp=%d min=%v idx %d valid=%v want %v (data %v valid %v)",
								w, mp, isMin, i, arr.IsValid(i), wantOK, vals, valid)
						}
						if !wantOK {
							continue
						}
						want, _ := oracle(i, w, isMin)
						gotV := arr.Value(i)
						if math.IsNaN(want) {
							if !math.IsNaN(gotV) {
								got.Release()
								t.Fatalf("w=%d mp=%d min=%v idx %d: got %v want NaN", w, mp, isMin, i, gotV)
							}
						} else if gotV != want {
							got.Release()
							t.Fatalf("w=%d mp=%d min=%v idx %d: got %v want %v", w, mp, isMin, i, gotV, want)
						}
					}
					got.Release()
				}
			}
		}
	})
}
