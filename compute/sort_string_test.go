package compute

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// refStringSort is the comparator definition the radix path must match:
// stable, byte order, nulls grouped at one end in input order.
func refStringSort(vals []string, valid []bool, so SortOptions) []int {
	idx := make([]int, len(vals))
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		av, bv := valid == nil || valid[a], valid == nil || valid[b]
		switch {
		case !av && !bv:
			return 0
		case !av:
			if so.Nulls == NullsFirst {
				return -1
			}
			return 1
		case !bv:
			if so.Nulls == NullsFirst {
				return 1
			}
			return -1
		}
		c := strings.Compare(vals[a], vals[b])
		if so.Descending {
			c = -c
		}
		return c
	})
	return idx
}

func randSortStrings(r *rand.Rand, n int) []string {
	prefixes := []string{"", "a", "ab", "charlie_", "charlie_x", "same_prefix_long_", "\x00", "ab\x00", "\xff\xfe", "日本語"}
	out := make([]string, n)
	for i := range out {
		var b strings.Builder
		b.WriteString(prefixes[r.IntN(len(prefixes))])
		for range r.IntN(20) {
			// Small alphabet forces many equal prefixes and exact ties.
			b.WriteByte("ab\x00\xffz"[r.IntN(5)])
		}
		out[i] = b.String()
	}
	return out
}

func TestSortIndicesStringMatchesComparator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{0, 1, 2, 5, 47, 48, 49, 200, 5000, 70000} {
		for _, nullFrac := range []float64{0, 0.2, 1} {
			vals := randSortStrings(r, n)
			var valid []bool
			if nullFrac > 0 {
				valid = make([]bool, n)
				for i := range valid {
					valid[i] = r.Float64() >= nullFrac
				}
			}
			s, err := series.FromString("s", vals, valid)
			if err != nil {
				t.Fatal(err)
			}
			for _, so := range []SortOptions{{}, {Descending: true}, {Nulls: NullsFirst}, {Descending: true, Nulls: NullsFirst}} {
				got, err := SortIndices(ctx, s, so)
				if err != nil {
					t.Fatal(err)
				}
				want := refStringSort(vals, valid, so)
				if !slices.Equal(got, want) {
					t.Fatalf("n=%d nulls=%v opts=%+v: permutation differs from comparator sort", n, nullFrac, so)
				}
				multi, err := SortIndicesMulti(ctx, []*series.Series{s}, []SortOptions{so})
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(multi, want) {
					t.Fatalf("n=%d SortIndicesMulti differs", n)
				}
			}
			s.Release()
		}
	}
}

func TestSortIndicesStringSlicedAndChunked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(3, 4))
	vals := randSortStrings(r, 3000)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues(vals, nil)
	full := b.NewStringArray()
	b.Release()
	defer full.Release()

	// A sliced view has a non-zero data offset and non-zero first offset.
	sl := array.NewSlice(full, 700, 2500)
	s1, err := series.New("s", sl)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Release()
	got, err := SortIndices(ctx, s1, SortOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := refStringSort(vals[700:2500], nil, SortOptions{}); !slices.Equal(got, want) {
		t.Fatal("sliced input: permutation differs")
	}

	// Multi-chunk series.
	c1 := array.NewSlice(full, 0, 1000)
	c2 := array.NewSlice(full, 1000, 3000)
	s2, err := series.New("s", []arrow.Array{c1, c2}...)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Release()
	got, err = SortIndices(ctx, s2, SortOptions{Descending: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := refStringSort(vals, nil, SortOptions{Descending: true}); !slices.Equal(got, want) {
		t.Fatal("chunked input: permutation differs")
	}
}

func BenchmarkSortIndicesString1M(b *testing.B) {
	words := [8]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	n := 1 << 20
	vals := make([]string, n)
	for i := range vals {
		h := (uint64(i) * 2654435761) & 0xFFFFFF
		vals[i] = words[i%8] + "_" + hex6(h)
	}
	s, _ := series.FromString("s", vals, nil)
	defer s.Release()
	ctx := context.Background()
	for b.Loop() {
		if _, err := SortIndices(ctx, s, SortOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func hex6(h uint64) string {
	const digits = "0123456789abcdef"
	var buf [6]byte
	for i := 5; i >= 0; i-- {
		buf[i] = digits[h&0xf]
		h >>= 4
	}
	return string(buf[:])
}

// TestSortIndicesStringParallelShapes covers both parallel top-level
// strategies: leading bytes spread over many partitions, and one
// dominant leading byte that falls back to a single radix plus
// parallel run refinement.
func TestSortIndicesStringParallelShapes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(5, 6))
	n := 3 * strSortParallelMin
	spread := make([]string, n)
	skewed := make([]string, n)
	for i := range n {
		tail := randSortStrings(r, 1)[0]
		spread[i] = string(rune('A'+r.IntN(26))) + tail
		skewed[i] = "key_" + tail
	}
	for name, vals := range map[string][]string{"spread": spread, "skewed": skewed} {
		valid := make([]bool, n)
		for i := range valid {
			valid[i] = r.IntN(10) != 0
		}
		for _, v := range [][]bool{nil, valid} {
			s, err := series.FromString("s", vals, v)
			if err != nil {
				t.Fatal(err)
			}
			for _, so := range []SortOptions{{}, {Descending: true, Nulls: NullsFirst}} {
				got, err := SortIndices(ctx, s, so)
				if err != nil {
					t.Fatal(err)
				}
				if want := refStringSort(vals, v, so); !slices.Equal(got, want) {
					t.Fatalf("%s nulls=%v opts=%+v: permutation differs", name, v != nil, so)
				}
			}
			s.Release()
		}
	}
}
