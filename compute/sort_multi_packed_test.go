package compute_test

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// TestSortIndicesMultiIntegerKeys checks multi-key integer sorts
// (including the packed-word path) against a stable comparison sort.
func TestSortIndicesMultiIntegerKeys(t *testing.T) {
	ctx := context.Background()
	type tc struct {
		n      int
		bound  int64
		desc   [2]bool
		i32    bool
		sliced bool
		wide   bool
	}
	var cases []tc
	for _, n := range []int{2, 3, 100, 64 * 1024, 64*1024 + 1, 256 * 1024} {
		for _, d := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
			cases = append(cases, tc{n: n, bound: 1 << 16, desc: d})
		}
		cases = append(cases, tc{n: n, bound: 7, i32: true})
		cases = append(cases, tc{n: n, bound: 1 << 16, sliced: true})
		cases = append(cases, tc{n: n, bound: 1 << 16, wide: true})
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%+v", c), func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			r := rand.New(rand.NewPCG(uint64(c.n), 7))
			total := c.n
			if c.sliced {
				total += 13
			}
			a := make([]int64, total)
			b := make([]int64, total)
			for i := range a {
				a[i] = r.Int64N(c.bound) - c.bound/2
				b[i] = r.Int64N(c.bound) - c.bound/2
			}
			if c.wide {
				a[0], a[1%total] = math.MinInt64, math.MaxInt64
			}
			var sa, sb *series.Series
			if c.i32 {
				a32 := make([]int32, total)
				b32 := make([]int32, total)
				for i := range a {
					a32[i], b32[i] = int32(a[i]), int32(b[i])
				}
				sa, _ = series.FromInt32("a", a32, nil, series.WithAllocator(mem))
				sb, _ = series.FromInt32("b", b32, nil, series.WithAllocator(mem))
			} else {
				sa, _ = series.FromInt64("a", a, nil, series.WithAllocator(mem))
				sb, _ = series.FromInt64("b", b, nil, series.WithAllocator(mem))
			}
			defer sa.Release()
			defer sb.Release()
			if c.sliced {
				a, b = a[5:5+c.n], b[5:5+c.n]
				xa, _ := sa.Slice(5, c.n)
				xb, _ := sb.Slice(5, c.n)
				defer xa.Release()
				defer xb.Release()
				sa, sb = xa, xb
			}
			opts := []compute.SortOptions{{Descending: c.desc[0]}, {Descending: c.desc[1]}}
			got, err := compute.SortIndicesMulti(ctx, []*series.Series{sa, sb}, opts, compute.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			want := make([]int, c.n)
			for i := range want {
				want[i] = i
			}
			dir := func(d bool, x int) int {
				if d {
					return -x
				}
				return x
			}
			slices.SortStableFunc(want, func(i, j int) int {
				if x := dir(c.desc[0], cmp.Compare(a[i], a[j])); x != 0 {
					return x
				}
				return dir(c.desc[1], cmp.Compare(b[i], b[j]))
			})
			if !slices.Equal(got, want) {
				t.Fatal("order differs from stable reference")
			}
		})
	}
}
