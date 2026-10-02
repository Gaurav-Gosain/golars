package compute_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestCastUint32ToFloat64(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	for _, valid := range [][]bool{nil, {true, false, true, true}} {
		s, _ := series.FromUint32("n", []uint32{0, 7, 1 << 31, 4294967295}, valid, series.WithAllocator(mem))
		out, err := compute.Cast(context.Background(), s, dtype.Float64(), compute.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		want := []any{0.0, 7.0, float64(1 << 31), 4294967295.0}
		if valid != nil {
			want[1] = nil
		}
		for i, w := range want {
			if got, _ := out.Get(i); got != w {
				t.Fatalf("row %d: got %v want %v", i, got, w)
			}
		}
		out.Release()
		s.Release()
	}
}
