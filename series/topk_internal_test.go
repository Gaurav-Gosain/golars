package series

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Tie order is stable (input order) for both directions, matching
// polars top_k and DataFrame.TopK. The heap fast path must reproduce
// these exact index sequences.
func TestTopKIndicesTies(t *testing.T) {
	s, _ := FromInt64("x",
		[]int64{5, 5, 3, 5, 0, 1},
		[]bool{true, true, true, true, false, true})
	defer s.Release()

	for k, want := range map[int][]int{
		0: {},
		1: {0},
		2: {0, 1},
		3: {0, 1, 3},
		4: {0, 1, 3, 2},
		5: {0, 1, 3, 2, 5},
		9: {0, 1, 3, 2, 5},
	} {
		got, err := topKIndices(s, k, true)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("topk k=%d = %v, want %v", k, got, want)
		}
	}
	for k, want := range map[int][]int{
		0: {},
		1: {5},
		2: {5, 2},
		3: {5, 2, 0},
		5: {5, 2, 0, 1, 3},
		9: {5, 2, 0, 1, 3},
	} {
		got, err := topKIndices(s, k, false)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("bottomk k=%d = %v, want %v", k, got, want)
		}
	}
}

// TestHeapMatchesSort drives the heap primitives directly against the
// argsort path on tie-heavy data with nulls (and NaNs for float).
func TestHeapMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const n = 1000
	ivals := make([]int64, n)
	ivalid := make([]bool, n)
	fvals := make([]float64, n)
	fvalid := make([]bool, n)
	for i := range ivals {
		ivals[i] = int64(rng.Intn(20))
		ivalid[i] = rng.Intn(8) != 0
		fvals[i] = float64(rng.Intn(20))
		if rng.Intn(16) == 0 {
			fvals[i] = math.NaN()
		}
		fvalid[i] = rng.Intn(8) != 0
	}
	is, _ := FromInt64("x", ivals, ivalid)
	defer is.Release()
	fs, _ := FromFloat64("x", fvals, fvalid)
	defer fs.Release()
	ia := is.Chunk(0).(*array.Int64)
	fa := fs.Chunk(0).(*array.Float64)
	inn := n - is.NullCount()
	fnn := n - fs.NullCount()
	for _, k := range []int{1, 2, 3, 7, 50, 60, 100} {
		if k*heapTopKDivisor >= inn {
			t.Fatalf("k=%d does not take the heap path", k)
		}
		for _, desc := range []bool{true, false} {
			got := heapTopKInt64(ia, n, k, desc)
			want, err := topKIndicesSort(is, k, inn, desc)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("int64 k=%d desc=%v:\n got %v\nwant %v", k, desc, got[:min(8, len(got))], want[:min(8, len(want))])
			}
			gotF := heapTopKFloat64(fa, n, k, desc)
			wantF, err := topKIndicesSort(fs, k, fnn, desc)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(gotF, wantF) {
				t.Fatalf("float k=%d desc=%v:\n got %v\nwant %v", k, desc, gotF[:min(8, len(gotF))], wantF[:min(8, len(wantF))])
			}
		}
	}
}
