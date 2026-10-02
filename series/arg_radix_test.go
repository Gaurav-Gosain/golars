package series

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestArgRadixMatchesComparison verifies the radix indices against an
// independent comparison sort across ties, nulls, negatives, signed
// zeros and NaNs.
func TestArgRadixMatchesComparison(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	const n = 2000
	ivals := make([]int64, n)
	ivalid := make([]bool, n)
	fvals := make([]float64, n)
	fvalid := make([]bool, n)
	for i := range ivals {
		switch rng.Intn(10) {
		case 0:
			ivals[i] = 0
			fvals[i] = 0
		case 1:
			ivals[i] = -rng.Int63n(100)
			fvals[i] = -rng.Float64() * 100
		case 2:
			ivals[i] = math.MaxInt64
			fvals[i] = math.Inf(1)
		case 3:
			ivals[i] = math.MinInt64
			fvals[i] = math.Inf(-1)
		case 4:
			ivals[i] = int64(rng.Intn(5))
			fvals[i] = math.NaN()
		default:
			ivals[i] = int64(rng.Intn(50))
			fvals[i] = rng.NormFloat64()
		}
		ivalid[i] = rng.Intn(8) != 0
		fvalid[i] = rng.Intn(8) != 0
	}
	// Sprinkle signed zeros in float (must tie with +0).
	for i := 0; i < n; i += 97 {
		fvals[i] = math.Copysign(0, -1)
		fvalid[i] = true
	}
	is, _ := FromInt64("x", ivals, ivalid)
	defer is.Release()
	fs, _ := FromFloat64("x", fvals, fvalid)
	defer fs.Release()

	refInt := func() []int {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		ia := is.Chunk(0).(*array.Int64)
		slices.SortStableFunc(idx, func(a, b int) int {
			va, vb := ia.IsValid(a), ia.IsValid(b)
			switch {
			case !va && !vb:
				return 0
			case !va:
				return 1
			case !vb:
				return -1
			}
			return cmpInt(ivals[a], ivals[b])
		})
		return idx
	}
	refFloat := func() []int {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		fa := fs.Chunk(0).(*array.Float64)
		slices.SortStableFunc(idx, func(a, b int) int {
			va, vb := fa.IsValid(a), fa.IsValid(b)
			switch {
			case !va && !vb:
				return 0
			case !va:
				return 1
			case !vb:
				return -1
			}
			x, y := fvals[a], fvals[b]
			xn, yn := math.IsNaN(x), math.IsNaN(y)
			switch {
			case xn && yn:
				return 0
			case xn:
				return 1
			case yn:
				return -1
			}
			return cmpFloat(x, y)
		})
		return idx
	}

	ia := is.Chunk(0).(*array.Int64)
	gotI := argSortInt64Asc(ia, ia.Int64Values(), n)
	if !slices.Equal(gotI, refInt()) {
		t.Fatal("int64 radix order differs from comparison order")
	}
	fa := fs.Chunk(0).(*array.Float64)
	gotF := argSortFloat64Asc(fa, fa.Float64Values(), n)
	if !slices.Equal(gotF, refFloat()) {
		t.Fatal("float radix order differs from comparison order")
	}

	// Public entry points agree too.
	pubI, err := is.ArgSort()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pubI, refInt()) {
		t.Fatal("ArgSort int64 differs")
	}
	pubF, err := fs.ArgSort()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pubF, refFloat()) {
		t.Fatal("ArgSort float differs")
	}
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
