package compute_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars 1.39: Categorical sorts by category string, Enum by category
// order, nulls first.
func TestSortCategoricalAndEnum(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	cases := []struct {
		name  string
		vals  []string
		valid []bool
		to    dtype.DType
		asc   string
		desc  string
	}{
		{"categorical", []string{"b", "a", "", "c", "a"}, []bool{true, true, false, true, true}, dtype.Categorical(),
			"[None, 'a', 'a', 'b', 'c']", "[None, 'c', 'b', 'a', 'a']"},
		{"enum", []string{"a", "c", "", "b"}, []bool{true, true, false, true}, dtype.Enum([]string{"c", "a", "b"}),
			"[None, 'c', 'a', 'b']", "[None, 'b', 'a', 'c']"},
	}
	for _, c := range cases {
		s, _ := series.FromString("s", c.vals, c.valid, series.WithAllocator(mem))
		cat, err := compute.Cast(ctx, s, c.to, compute.WithAllocator(mem))
		s.Release()
		if err != nil {
			t.Fatal(err)
		}
		for _, desc := range []bool{false, true} {
			out, err := compute.Sort(ctx, cat, compute.SortOptions{Descending: desc}, compute.WithAllocator(mem))
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			str, _ := compute.Cast(ctx, out, dtype.String(), compute.WithAllocator(mem))
			want := c.asc
			if desc {
				want = c.desc
			}
			if got := testutil.PyRepr(str.Chunk(0)); got != want {
				t.Errorf("%s desc=%v: %s, want %s", c.name, desc, got, want)
			}
			str.Release()
			out.Release()
		}
		cat.Release()
	}
}
