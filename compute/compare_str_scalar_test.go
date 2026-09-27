package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// String column vs string literal compares in place. Checked against
// plain Go comparisons for every operator, with nulls, empty strings,
// prefixes of the literal, a sliced input and sizes around the byte
// boundary and the parallel cutoff.
func TestCompareStringScalar(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	words := []string{"AIR", "", "AI", "AIR REG", "air", "MAIL", "AIR", "B"}
	type opFn func(context.Context, *series.Series, any, ...compute.Option) (*series.Series, error)
	ops := []struct {
		name string
		fn   opFn
		ref  func(a, b string) bool
	}{
		{"eq", compute.EqLit, func(a, b string) bool { return a == b }},
		{"ne", compute.NeLit, func(a, b string) bool { return a != b }},
		{"lt", compute.LtLit, func(a, b string) bool { return a < b }},
		{"le", compute.LeLit, func(a, b string) bool { return a <= b }},
		{"gt", compute.GtLit, func(a, b string) bool { return a > b }},
		{"ge", compute.GeLit, func(a, b string) bool { return a >= b }},
	}
	for _, n := range []int{0, 1, 8, 9, 23, 70000} {
		vals := make([]string, n)
		valid := make([]bool, n)
		for i := range vals {
			vals[i] = words[i%len(words)]
			valid[i] = i%7 != 3
		}
		col, err := series.FromString("s", vals, valid, o)
		if err != nil {
			t.Fatal(err)
		}
		inputs := []*series.Series{col}
		off := 0
		if n > 2 {
			sl, _ := col.Slice(1, n-2)
			inputs = append(inputs, sl)
			off = 1
		}
		for k, in := range inputs {
			for _, op := range ops {
				t.Run(fmt.Sprintf("n=%d/slice=%d/%s", n, k, op.name), func(t *testing.T) {
					got, err := op.fn(ctx, in, "AIR", compute.WithAllocator(mem))
					if err != nil {
						t.Fatal(err)
					}
					defer got.Release()
					base := 0
					if k == 1 {
						base = off
					}
					for i := range in.Len() {
						v, _ := got.Get(i)
						if !valid[base+i] {
							if v != nil {
								t.Fatalf("row %d: want null, got %v", i, v)
							}
							continue
						}
						if want := op.ref(vals[base+i], "AIR"); v != want {
							t.Fatalf("row %d (%q): got %v want %v", i, vals[base+i], v, want)
						}
					}
				})
			}
		}
		for _, in := range inputs {
			in.Release()
		}
	}
}

func BenchmarkStrEqLit(b *testing.B) {
	ctx := context.Background()
	modes := []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
	vals := make([]string, 1<<20)
	for i := range vals {
		vals[i] = modes[i*7919%len(modes)]
	}
	col, _ := series.FromString("s", vals, nil)
	defer col.Release()
	for b.Loop() {
		out, err := compute.EqLit(ctx, col, "AIR")
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
