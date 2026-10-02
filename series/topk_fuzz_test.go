package series

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// FuzzHeapMatchesSort drives heapTopKInt64/Float64 against the argsort
// path on adversarial small inputs (ties, extremes, NaN, nulls).
func FuzzHeapMatchesSort(f *testing.F) {
	f.Add([]byte{5, 5, 3, 5, 0, 1, 9, 9, 2, 6, 5, 3, 5})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 64 {
			t.Skip()
		}
		n := len(data)
		ivals := make([]int64, n)
		ivalid := make([]bool, n)
		fvals := make([]float64, n)
		fvalid := make([]bool, n)
		for i, b := range data {
			ivals[i] = int64(b) % 7
			ivalid[i] = b%5 != 0
			switch b % 13 {
			case 0:
				fvals[i] = math.NaN()
			case 1:
				fvals[i] = math.Inf(1)
			case 2:
				fvals[i] = math.Copysign(0, -1)
			default:
				fvals[i] = float64(int64(b)%7) / 2
			}
			fvalid[i] = b%5 != 0
		}
		is, err := FromInt64("x", ivals, ivalid)
		if err != nil {
			t.Fatal(err)
		}
		defer is.Release()
		fs, err := FromFloat64("x", fvals, fvalid)
		if err != nil {
			t.Fatal(err)
		}
		defer fs.Release()
		ia := is.Chunk(0).(*array.Int64)
		fa := fs.Chunk(0).(*array.Float64)
		inn := n - is.NullCount()
		fnn := n - fs.NullCount()
		check := func(k int, desc bool, got, want []int) {
			t.Helper()
			if len(got) != len(want) {
				t.Fatalf("k=%d desc=%v len %d vs %d", k, desc, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("k=%d desc=%v idx %d: %d vs %d", k, desc, i, got[i], want[i])
				}
			}
		}
		for _, k := range []int{1, 2, 3, 5, 8} {
			for _, desc := range []bool{true, false} {
				if k*heapTopKDivisor < inn {
					got := heapTopKInt64(ia, n, k, desc)
					want, err := topKIndicesSort(is, k, inn, desc)
					if err != nil {
						t.Fatal(err)
					}
					check(k, desc, got, want)
				}
				if k*heapTopKDivisor < fnn {
					got := heapTopKFloat64(fa, n, k, desc)
					want, err := topKIndicesSort(fs, k, fnn, desc)
					if err != nil {
						t.Fatal(err)
					}
					check(k, desc, got, want)
				}
			}
		}
	})
}
