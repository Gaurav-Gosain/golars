package dataframe_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// refPair is one output row of a join as (left row, right row), with
// -1 for a missing side.
type refPair struct{ l, r int }

// propKey is a random key tuple; null components are nil.
type propKey struct {
	a *int64
	b *string
}

func (k propKey) hasNull(multi bool) bool { return k.a == nil || (multi && k.b == nil) }

func (k propKey) id(multi bool) string {
	s := "n"
	if k.a != nil {
		s = fmt.Sprint(*k.a)
	}
	if multi {
		if k.b == nil {
			s += "|n"
		} else {
			s += "|s" + *k.b
		}
	}
	return s
}

func randKeys(r *rand.Rand, n, card int) []propKey {
	out := make([]propKey, n)
	for i := range out {
		if r.IntN(10) > 0 {
			v := int64(r.IntN(card))
			out[i].a = &v
		}
		if r.IntN(10) > 0 {
			s := fmt.Sprintf("s%d", r.IntN(3))
			out[i].b = &s
		}
	}
	return out
}

// referenceJoin is a direct implementation of the polars semantics.
func referenceJoin(lk, rk []propKey, multi bool, how dataframe.JoinType, order dataframe.JoinOrder, nullsEqual bool) []refPair {
	matches := func(probe, build []propKey) [][]int {
		idx := map[string][]int{}
		for j, k := range build {
			if !nullsEqual && k.hasNull(multi) {
				continue
			}
			idx[k.id(multi)] = append(idx[k.id(multi)], j)
		}
		out := make([][]int, len(probe))
		for i, k := range probe {
			if !nullsEqual && k.hasNull(multi) {
				continue
			}
			out[i] = idx[k.id(multi)]
		}
		return out
	}
	rightMajor := order == dataframe.JoinOrderRight || order == dataframe.JoinOrderRightLeft
	if how == dataframe.RightJoin {
		rightMajor = !(order == dataframe.JoinOrderLeft || order == dataframe.JoinOrderLeftRight)
	}
	var out []refPair
	switch how {
	case dataframe.SemiJoin, dataframe.AntiJoin:
		m := matches(lk, rk)
		for i := range lk {
			if (len(m[i]) > 0) == (how == dataframe.SemiJoin) {
				out = append(out, refPair{i, -1})
			}
		}
		return out
	}
	probe, build := lk, rk
	if rightMajor {
		probe, build = rk, lk
	}
	keepProbe := how == dataframe.FullJoin || (how == dataframe.LeftJoin && !rightMajor) || (how == dataframe.RightJoin && rightMajor)
	keepBuild := how == dataframe.FullJoin || (how == dataframe.LeftJoin && rightMajor) || (how == dataframe.RightJoin && !rightMajor)
	m := matches(probe, build)
	hit := make([]bool, len(build))
	for i := range probe {
		for _, j := range m[i] {
			out = append(out, refPair{i, j})
			hit[j] = true
		}
		if len(m[i]) == 0 && keepProbe {
			out = append(out, refPair{i, -1})
		}
	}
	if keepBuild {
		for j := range build {
			if !hit[j] {
				out = append(out, refPair{-1, j})
			}
		}
	}
	if rightMajor {
		for i := range out {
			out[i].l, out[i].r = out[i].r, out[i].l
		}
	}
	return out
}

func propFrame(t *testing.T, keys []propKey, idxName string, chunked bool, alloc memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	n := len(keys)
	a := make([]int64, n)
	av := make([]bool, n)
	b := make([]string, n)
	bv := make([]bool, n)
	idx := make([]int64, n)
	for i, k := range keys {
		if k.a != nil {
			a[i], av[i] = *k.a, true
		}
		if k.b != nil {
			b[i], bv[i] = *k.b, true
		}
		idx[i] = int64(i)
	}
	sa, _ := series.FromInt64("a", a, av, series.WithAllocator(alloc))
	sb, _ := series.FromString("b", b, bv, series.WithAllocator(alloc))
	si, _ := series.FromInt64(idxName, idx, nil, series.WithAllocator(alloc))
	cols := []*series.Series{sa, sb, si}
	if chunked && n >= 2 {
		for i, s := range cols {
			arr, _ := s.Consolidated()
			p1 := array.NewSlice(arr, 0, int64(n/3))
			p2 := array.NewSlice(arr, int64(n/3), int64(n))
			arr.Release()
			name := s.Name()
			s.Release()
			cols[i], _ = series.New(name, p1, p2)
		}
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func pairsOf(t *testing.T, df *dataframe.DataFrame, semi bool) []refPair {
	t.Helper()
	col := func(name string) []int {
		s, err := df.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := s.Consolidated()
		defer a.Release()
		v := a.(*array.Int64)
		out := make([]int, v.Len())
		for i := range out {
			out[i] = -1
			if v.IsValid(i) {
				out[i] = int(v.Value(i))
			}
		}
		return out
	}
	li := col("li")
	out := make([]refPair, len(li))
	var ri []int
	if !semi {
		ri = col("ri")
	}
	for i := range out {
		out[i] = refPair{li[i], -1}
		if !semi {
			out[i].r = ri[i]
		}
	}
	return out
}

func sortRefPairs(p []refPair) {
	slices.SortFunc(p, func(x, y refPair) int {
		if c := cmp.Compare(x.l, y.l); c != 0 {
			return c
		}
		return cmp.Compare(x.r, y.r)
	})
}

// TestJoinAgainstReference checks every join type, key count and order
// against referenceJoin on random inputs, including sizes past the
// parallel probe cutoff and multi-chunk inputs. Unordered joins (the
// default) compare as multisets.
func TestJoinAgainstReference(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	sizes := [][2]int{{0, 0}, {0, 5}, {5, 0}, {1, 1}, {37, 50}, {1000, 800}, {70000, 3000}, {2000, 70000}, {150000, 150000}}
	hows := []dataframe.JoinType{dataframe.InnerJoin, dataframe.LeftJoin, dataframe.RightJoin, dataframe.FullJoin, dataframe.SemiJoin, dataframe.AntiJoin}
	orders := []dataframe.JoinOrder{dataframe.JoinOrderNone, dataframe.JoinOrderLeftRight, dataframe.JoinOrderRightLeft}
	for _, sz := range sizes {
		for _, card := range []int{3, 1 << 20, 1 << 40} {
			lk := randKeys(r, sz[0], card)
			rk := randKeys(r, sz[1], card)
			for _, multi := range []bool{false, true} {
				for _, chunked := range []bool{false, true} {
					if chunked && sz[0] > 5000 {
						continue
					}
					for _, how := range hows {
						for _, order := range orders {
							for _, ne := range []bool{false, true} {
								name := fmt.Sprintf("%dx%d_card%d_multi%v_chunk%v_%s_%s_ne%v", sz[0], sz[1], card, multi, chunked, how, order, ne)
								// About a tenth of the keys are null, so equal
								// nulls (or a tiny key range) on two big sides
								// would produce a huge output.
								if (card == 3 || ne) && sz[0]*sz[1] > 1<<22 {
									continue
								}
								t.Run(name, func(t *testing.T) {
									alloc := testutil.NewCheckedAllocator(t)
									left := propFrame(t, lk, "li", chunked, alloc)
									defer left.Release()
									right := propFrame(t, rk, "ri", chunked, alloc)
									defer right.Release()
									on := []string{"a"}
									if multi {
										on = []string{"a", "b"}
									}
									out, err := left.Join(context.Background(), right, on, how,
										dataframe.WithJoinAllocator(alloc), dataframe.WithJoinMaintainOrder(order),
										dataframe.WithJoinNullsEqual(ne))
									if err != nil {
										t.Fatal(err)
									}
									defer out.Release()
									got := pairsOf(t, out, how == dataframe.SemiJoin || how == dataframe.AntiJoin)
									want := referenceJoin(lk, rk, multi, how, order, ne)
									if order == dataframe.JoinOrderNone {
										sortRefPairs(got)
										sortRefPairs(want)
									}
									if !slices.Equal(got, want) {
										t.Fatalf("pairs differ: got %d rows, want %d rows", len(got), len(want))
									}
								})
							}
						}
					}
				}
			}
		}
	}
}
