package dataframe

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// refJoinString is the straightforward map-based join the hash table
// replaced: left order, then ascending right order per left row.
func refJoinString(ls, rs *array.String, how JoinType) ([]int, []int) {
	idx := map[string][]int{}
	for j := range rs.Len() {
		if rs.IsValid(j) {
			k := rs.Value(j)
			idx[k] = append(idx[k], j)
		}
	}
	lOut, rOut := []int{}, []int{}
	for i := range ls.Len() {
		var m []int
		if ls.IsValid(i) {
			m = idx[ls.Value(i)]
		}
		if len(m) == 0 {
			if how == LeftJoin {
				lOut = append(lOut, i)
				rOut = append(rOut, -1)
			}
			continue
		}
		for _, j := range m {
			lOut = append(lOut, i)
			rOut = append(rOut, j)
		}
	}
	return lOut, rOut
}

func randStrKeys(r *rand.Rand, mem memory.Allocator, n, card int, nullRate float64, prefix string) *array.String {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for range n {
		if r.Float64() < nullRate {
			b.AppendNull()
			continue
		}
		// Mix short and long keys so both strhash paths are hit.
		k := r.IntN(card)
		if k%5 == 0 {
			b.Append(fmt.Sprintf("%s-a-much-longer-key-over-sixteen-bytes-%d", prefix, k))
		} else {
			b.Append(fmt.Sprintf("%s%d", prefix, k))
		}
	}
	return b.NewStringArray()
}

func TestHashJoinStringMatchesReference(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(7, 9))
	type tc struct {
		nl, nr, card int
		nullRate     float64
	}
	cases := []tc{
		{0, 0, 1, 0},
		{0, 10, 5, 0},
		{10, 0, 5, 0},
		{1, 1, 1, 0},
		{100, 80, 30, 0},           // duplicates on both sides
		{100, 80, 1000, 0.2},       // sparse matches, nulls
		{5000, 5000, 5000, 0},      // mostly unique
		{40000, 36000, 50000, 0},   // parallel path, unique-ish
		{40000, 36000, 3000, 0.05}, // parallel path, heavy duplicates
		{70000, 70000, 1 << 30, 0}, // parallel path, almost no matches
		{33000, 1, 2, 0.5},         // tiny right side
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("l=%d,r=%d,card=%d,null=%v", c.nl, c.nr, c.card, c.nullRate), func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			ls := randStrKeys(r, mem, c.nl, c.card, c.nullRate, "k")
			defer ls.Release()
			rs := randStrKeys(r, mem, c.nr, c.card, c.nullRate, "k")
			defer rs.Release()
			for _, how := range []JoinType{InnerJoin, LeftJoin} {
				gl, gr, err := hashJoinString(ls, rs, how)
				if err != nil {
					t.Fatal(err)
				}
				wl, wr := refJoinString(ls, rs, how)
				if !slices.Equal(gl, wl) || !slices.Equal(gr, wr) {
					t.Fatalf("%v: got %d pairs, want %d; first diff at %d", how, len(gl), len(wl), firstDiff(gl, gr, wl, wr))
				}
			}
		})
	}
}

// TestPackShortKeyInjective: for one length, packed values collide only
// when the strings are equal.
func TestPackShortKeyInjective(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(5, 6))
	for n := 0; n <= 8; n++ {
		seen := map[uint64]string{}
		for range 5000 {
			b := make([]byte, n)
			for i := range b {
				b[i] = byte(r.IntN(4)) // small alphabet forces near misses
			}
			s := string(b)
			v, ok := packShortKey(s)
			if !ok {
				t.Fatalf("len %d not packed", n)
			}
			if prev, dup := seen[v]; dup && prev != s {
				t.Fatalf("len %d: %q and %q pack to %x", n, prev, s, v)
			}
			seen[v] = s
		}
	}
	if _, ok := packShortKey("123456789"); ok {
		t.Fatal("9-byte key packed")
	}
}

// TestHashJoinStringSlicedInput covers keys whose arrays carry an offset.
func TestHashJoinStringSlicedInput(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	r := rand.New(rand.NewPCG(3, 4))
	full := randStrKeys(r, mem, 50000, 20000, 0.1, "k")
	defer full.Release()
	ls := array.NewSlice(full, 1234, 45000).(*array.String)
	defer ls.Release()
	rs := array.NewSlice(full, 7, 38000).(*array.String)
	defer rs.Release()
	for _, how := range []JoinType{InnerJoin, LeftJoin} {
		gl, gr, _ := hashJoinString(ls, rs, how)
		wl, wr := refJoinString(ls, rs, how)
		if !slices.Equal(gl, wl) || !slices.Equal(gr, wr) {
			t.Fatalf("%v: mismatch at %d", how, firstDiff(gl, gr, wl, wr))
		}
	}
}

func firstDiff(gl, gr, wl, wr []int) int {
	for i := range min(len(gl), len(wl)) {
		if gl[i] != wl[i] || gr[i] != wr[i] {
			return i
		}
	}
	return min(len(gl), len(wl))
}

// TestJoinMultiChunkKeys: keys split over several chunks must join on
// every chunk, not only the first.
func TestJoinMultiChunkKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	mk := func(name string, lo, hi int, str bool) *DataFrame {
		n := hi - lo
		ids := make([]int64, n)
		keys := make([]string, n)
		for i := range n {
			ids[i] = int64(lo + i)
			keys[i] = fmt.Sprintf("s%d", lo+i)
		}
		var k *series.Series
		if str {
			k, _ = series.FromString("k", keys, nil, series.WithAllocator(mem))
		} else {
			k, _ = series.FromInt64("k", ids, nil, series.WithAllocator(mem))
		}
		v, _ := series.FromInt64(name, ids, nil, series.WithAllocator(mem))
		df, err := New(k, v)
		if err != nil {
			t.Fatal(err)
		}
		return df
	}
	for _, str := range []bool{false, true} {
		concat := func(parts ...*DataFrame) *DataFrame {
			out, err := Concat(parts...)
			for _, p := range parts {
				p.Release()
			}
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		left := concat(mk("lv", 0, 50, str), mk("lv", 50, 100, str), mk("lv", 100, 150, str))
		right := concat(mk("rv", 120, 140, str), mk("rv", 10, 30, str))
		if left.ColumnAt(0).NumChunks() < 2 || right.ColumnAt(1).NumChunks() < 2 {
			t.Fatalf("test frames are not multi-chunk: %d, %d", left.ColumnAt(0).NumChunks(), right.ColumnAt(1).NumChunks())
		}
		for _, how := range []JoinType{InnerJoin, LeftJoin} {
			out, err := left.Join(ctx, right, []string{"k"}, how, WithJoinAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			want := 40
			if how == LeftJoin {
				want = 150
			}
			rv, _ := out.Column("rv")
			lv, _ := out.Column("lv")
			if out.Height() != want || (how == LeftJoin && rv.NullCount() != 110) {
				t.Errorf("str=%v %v: height=%d nulls=%d, want %d", str, how, out.Height(), rv.NullCount(), want)
			}
			// Every matched row pairs equal ids, so rv must equal lv.
			rvArr, _ := rv.Consolidated()
			lvArr, _ := lv.Consolidated()
			rvs, lvs := rvArr.(*array.Int64), lvArr.(*array.Int64)
			for i := range out.Height() {
				if rvs.IsValid(i) && rvs.Value(i) != lvs.Value(i) {
					t.Fatalf("str=%v %v row %d: rv=%d lv=%d", str, how, i, rvs.Value(i), lvs.Value(i))
				}
			}
			rvArr.Release()
			lvArr.Release()
			out.Release()
		}
		left.Release()
		right.Release()
	}
}
