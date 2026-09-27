package dataframe

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// uniqueRef returns the distinct values of vals (nil = null) in
// first-seen order.
func uniqueRef(vals []*string) []*string {
	seen := map[string]bool{}
	sawNull := false
	var out []*string
	for _, v := range vals {
		if v == nil {
			if !sawNull {
				sawNull = true
				out = append(out, nil)
			}
			continue
		}
		if !seen[*v] {
			seen[*v] = true
			out = append(out, v)
		}
	}
	return out
}

func strColumn(t *testing.T, s *series.Series) []*string {
	t.Helper()
	arr, err := s.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	sa := arr.(*array.String)
	out := make([]*string, sa.Len())
	for i := range out {
		if sa.IsValid(i) {
			v := sa.Value(i)
			out[i] = &v
		}
	}
	return out
}

func sameStrs(a, b []*string) bool {
	return slices.EqualFunc(a, b, func(x, y *string) bool {
		if x == nil || y == nil {
			return x == y
		}
		return *x == *y
	})
}

// genStrs builds n values over card distinct keys, mixing the short
// (<= 7 bytes), mid (8 to 15) and long (> 15) length classes.
func genStrs(n, card int, nullEvery int, seed uint64) ([]string, []bool) {
	r := rand.New(rand.NewPCG(seed, seed+1))
	vals := make([]string, n)
	valid := make([]bool, n)
	for i := range vals {
		k := r.IntN(card)
		switch k % 3 {
		case 0:
			vals[i] = fmt.Sprintf("s%d", k)
		case 1:
			vals[i] = fmt.Sprintf("mid_%08d", k)
		default:
			vals[i] = fmt.Sprintf("a_long_key_value_%d", k)
		}
		valid[i] = nullEvery == 0 || i%nullEvery != 3
	}
	return vals, valid
}

func ptrs(vals []string, valid []bool) []*string {
	out := make([]*string, len(vals))
	for i := range vals {
		if valid == nil || valid[i] {
			out[i] = &vals[i]
		}
	}
	return out
}

func TestUniqueStringMatchesReference(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		n, card, nullEvery int
	}{
		{0, 1, 0},
		{1, 1, 0},
		{100, 7, 0},
		{100, 7, 5},
		{strPartThreshold - 1, 40_000, 0},
		{strPartThreshold, 40_000, 0},
		{200_000, 150_000, 0},
		{200_000, 150_000, 11},
		{200_000, 100, 0},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("n=%d/card=%d/nulls=%d", c.n, c.card, c.nullEvery), func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			vals, valid := genStrs(c.n, c.card, c.nullEvery, 9)
			if c.nullEvery == 0 {
				valid = nil
			}
			s, err := series.FromString("k", vals, valid, series.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			df, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			defer df.Release()
			out, err := df.Unique(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			got := strColumn(t, out.cols[0])
			want := uniqueRef(ptrs(vals, valid))
			if !sameStrs(got, want) {
				t.Fatalf("got %d values, want %d", len(got), len(want))
			}
		})
	}
}

func TestUniqueStringSlicedAndMultiChunk(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	vals, _ := genStrs(150_000, 120_000, 0, 3)
	s, _ := series.FromString("k", vals, nil, series.WithAllocator(mem))
	full, _ := New(s)
	defer full.Release()

	sliced, _ := full.Slice(1001, 140_000)
	defer sliced.Release()
	out, err := sliced.Unique(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strColumn(t, out.cols[0]), uniqueRef(ptrs(vals[1001:141_001], nil)); !sameStrs(got, want) {
		t.Fatalf("sliced: got %d values, want %d", len(got), len(want))
	}
	out.Release()

	a, _ := full.Slice(0, 70_000)
	b, _ := full.Slice(70_000, 80_000)
	multi, err := Concat(a, b)
	a.Release()
	b.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Release()
	out, err = multi.Unique(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got, want := strColumn(t, out.cols[0]), uniqueRef(ptrs(vals, nil)); !sameStrs(got, want) {
		t.Fatalf("multi-chunk: got %d values, want %d", len(got), len(want))
	}
}

func TestEncodeStringCodesPartitionedMatchesSerial(t *testing.T) {
	for _, nullEvery := range []int{0, 7} {
		vals, valid := genStrs(300_000, 90_000, nullEvery, 5)
		if nullEvery == 0 {
			valid = nil
		}
		s, _ := series.FromString("k", vals, valid)
		arr := s.Chunk(0).(*array.String)
		got := encodeStringCodesPartitioned(arr)
		e := newStrEncoder(arr, 64)
		want := make([]int32, arr.Len())
		e.encodeRange(0, arr.Len(), want)
		if !slices.Equal(got.firstRows, e.firstRows) {
			t.Fatalf("nulls=%d: first rows differ", nullEvery)
		}
		if !slices.Equal(got.codes, want) {
			t.Fatalf("nulls=%d: codes differ", nullEvery)
		}
		if rows := distinctStringRows(arr); !slices.Equal(rows, e.firstRows) {
			t.Fatalf("nulls=%d: distinct rows differ", nullEvery)
		}
		int32Scratch.put(got.codes)
		s.Release()
	}
}

func TestUniqueInt64MultiChunk(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	a, _ := series.FromInt64("x", []int64{3, 1, 3, 2}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("x", []int64{5, 1, 4, 5}, nil, series.WithAllocator(mem))
	fa, _ := New(a)
	fb, _ := New(b)
	df, err := Concat(fa, fb)
	fa.Release()
	fb.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := df.Unique(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	got := out.cols[0].Chunk(0).(*array.Int64).Int64Values()
	if want := []int64{3, 1, 2, 5, 4}; !slices.Equal(got, want) {
		t.Fatalf("chunks=%d got %v want %v", df.cols[0].NumChunks(), got, want)
	}
}

func TestEstimateDistinct(t *testing.T) {
	for _, card := range []int{1, 1000, 100_000, 262_144, 500_000} {
		n := max(card*2, 1000)
		sk := make([]uint64, strSketchWords)
		r := rand.New(rand.NewPCG(1, uint64(card)))
		keys := make([]uint64, card)
		for i := range keys {
			keys[i] = r.Uint64()
		}
		for i := range n {
			h := keys[i%card]
			b := (h * 0xD6E8FEB86659FD93) >> (64 - strSketchBits)
			sk[b>>6] |= 1 << (b & 63)
		}
		got := estimateDistinct([][]uint64{sk}, n)
		if lo, hi := card*9/10, card*11/10+2; got < lo || got > hi {
			t.Fatalf("card=%d: estimate %d outside [%d, %d]", card, got, lo, hi)
		}
	}
}
