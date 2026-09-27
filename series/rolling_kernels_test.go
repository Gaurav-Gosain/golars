package series_test

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// rollingBrute evaluates a fixed-window aggregate the slow way for a
// column without nulls. ok is false for rows short of minPeriods.
func rollingBrute(vals []int64, i, w, mp int, kind string) (float64, bool) {
	lo := max(0, i-w+1)
	if i-lo+1 < mp {
		return 0, false
	}
	switch kind {
	case "min", "max":
		best := vals[lo]
		for _, v := range vals[lo : i+1] {
			if kind == "min" {
				best = min(best, v)
			} else {
				best = max(best, v)
			}
		}
		return float64(best), true
	}
	var sum float64
	for _, v := range vals[lo : i+1] {
		sum += float64(v)
	}
	if kind == "mean" {
		return sum / float64(i-lo+1), true
	}
	return sum, true
}

func TestRollingNoNullKernelsAgainstBrute(t *testing.T) {
	r := rand.New(rand.NewPCG(31, 32))
	type rollFn func(*series.Series, series.RollingOptions, ...series.Option) (*series.Series, error)
	kinds := map[string]rollFn{
		"sum":  (*series.Series).RollingSum,
		"mean": (*series.Series).RollingMean,
		"min":  (*series.Series).RollingMin,
		"max":  (*series.Series).RollingMax,
	}
	for _, n := range []int{0, 1, 2, 5, 31, 32, 33, 64, 100, 1000} {
		for _, w := range []int{1, 2, 3, 7, 32, 150} {
			for _, mp := range []int{0, 1, w / 2, w} {
				for _, off := range []int{0, 3} {
					mem := testutil.NewCheckedAllocator(t)
					raw := make([]int64, n+off)
					for i := range raw {
						// Small magnitudes keep float sums exact; the
						// extremes check the int64 compare paths.
						switch r.IntN(20) {
						case 0:
							raw[i] = math.MaxInt64
						case 1:
							raw[i] = math.MinInt64
						default:
							raw[i] = r.Int64N(2001) - 1000
						}
					}
					full, _ := series.FromInt64("x", raw, nil, series.WithAllocator(mem))
					s, err := full.Slice(off, n)
					if err != nil {
						t.Fatal(err)
					}
					vals := raw[off:]
					for kind, fn := range kinds {
						if (kind == "sum" || kind == "mean") && hasExtreme(vals) {
							continue
						}
						out, err := fn(s, series.RollingOptions{WindowSize: w, MinPeriods: mp}, series.WithAllocator(mem))
						if err != nil {
							t.Fatal(err)
						}
						got := out.Chunk(0).(*array.Float64)
						effMP := mp
						if effMP <= 0 {
							effMP = w
						}
						for i := range n {
							want, ok := rollingBrute(vals, i, w, min(effMP, w), kind)
							if got.IsValid(i) != ok {
								t.Fatalf("%s n=%d w=%d mp=%d off=%d row %d: valid=%v want %v", kind, n, w, mp, off, i, got.IsValid(i), ok)
							}
							if ok && got.Value(i) != want {
								t.Fatalf("%s n=%d w=%d mp=%d off=%d row %d: got %v want %v", kind, n, w, mp, off, i, got.Value(i), want)
							}
						}
						if want := countShort(n, w, effMP); got.NullN() != want {
							t.Fatalf("%s n=%d w=%d mp=%d: null count %d want %d", kind, n, w, mp, got.NullN(), want)
						}
						out.Release()
					}
					s.Release()
					full.Release()
				}
			}
		}
	}
}

func hasExtreme(vals []int64) bool {
	for _, v := range vals {
		if v == math.MaxInt64 || v == math.MinInt64 {
			return true
		}
	}
	return false
}

func countShort(n, w, mp int) int { return min(min(mp, w)-1, n) }

func TestRollingMinMaxInt32NoNull(t *testing.T) {
	vals := []int32{5, -3, 8, 8, 1, math.MaxInt32, math.MinInt32, 0, 2}
	s, _ := series.FromInt32("x", vals, nil)
	defer s.Release()
	for _, w := range []int{1, 2, 3, 4, 9, 20} {
		mn, _ := s.RollingMin(series.RollingOptions{WindowSize: w, MinPeriods: 1})
		mx, _ := s.RollingMax(series.RollingOptions{WindowSize: w, MinPeriods: 1})
		a, b := mn.Chunk(0).(*array.Float64), mx.Chunk(0).(*array.Float64)
		for i := range vals {
			lo, hi := vals[max(0, i-w+1)], vals[max(0, i-w+1)]
			for _, v := range vals[max(0, i-w+1) : i+1] {
				lo, hi = min(lo, v), max(hi, v)
			}
			if a.Value(i) != float64(lo) || b.Value(i) != float64(hi) {
				t.Fatalf("w=%d row %d: got %v %v want %d %d", w, i, a.Value(i), b.Value(i), lo, hi)
			}
		}
		mn.Release()
		mx.Release()
	}
}
