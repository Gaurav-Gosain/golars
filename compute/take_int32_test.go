package compute_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestTakeInt32String(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	for _, n := range []int{0, 5, 70_000} {
		vals := make([]string, n)
		valid := make([]bool, n)
		for i := range vals {
			vals[i] = fmt.Sprintf("v%d", i)
			valid[i] = i%9 != 4
		}
		s, _ := series.FromString("s", vals, valid, series.WithAllocator(mem))
		idx := make([]int32, 0, n)
		for i := n - 1; i >= 0; i -= 2 {
			idx = append(idx, int32(i))
		}
		out, err := compute.TakeInt32(ctx, s, idx, compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if out.Len() != len(idx) {
			t.Fatalf("len %d want %d", out.Len(), len(idx))
		}
		arr := out.Chunk(0)
		for j, i := range idx {
			if arr.IsValid(j) != valid[i] {
				t.Fatalf("n=%d row %d: validity mismatch", n, j)
			}
			if valid[i] && out.Chunk(0).ValueStr(j) != vals[i] {
				t.Fatalf("n=%d row %d: got %q want %q", n, j, arr.ValueStr(j), vals[i])
			}
		}
		out.Release()

		// Out-of-range indices must error, also on the parallel path
		// where the check runs on worker goroutines.
		if n > 0 {
			bad := append([]int32(nil), idx...)
			bad[len(bad)-1] = int32(n)
			if _, err := compute.TakeInt32(ctx, s, bad, compute.WithAllocator(mem)); err == nil {
				t.Fatalf("n=%d: expected out-of-range error", n)
			}
			bad[len(bad)-1] = -1
			if _, err := compute.TakeInt32(ctx, s, bad, compute.WithAllocator(mem)); err == nil {
				t.Fatalf("n=%d: expected error for negative index", n)
			}
		}
		s.Release()
	}
}

func TestTakeInt32Int64(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("a", []int64{10, 20, 30}, []bool{true, false, true}, series.WithAllocator(mem))
	defer s.Release()
	out, err := compute.TakeInt32(ctx, s, []int32{2, 1, 0, 2}, compute.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	want := []string{"30", "(null)", "10", "30"}
	for j, w := range want {
		if got := out.Chunk(0).ValueStr(j); got != w {
			t.Fatalf("row %d: got %q want %q", j, got, w)
		}
	}
	if _, err := compute.TakeInt32(ctx, s, []int32{3}, compute.WithAllocator(mem)); err == nil {
		t.Fatal("expected out-of-range error")
	}
}
