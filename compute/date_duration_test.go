package compute_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// polars 1.39.3, date 1996-05-27 (day 9643) plus and minus durations.
func TestDatePlusDurationMatchesPolars(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	durs := []int64{-1, 1, -86400*1_000_000_000 - 1, -2305843009213693952, 1_000_000_000_000_000_000}
	cases := []struct {
		unit      dtype.TimeUnit
		plus, sub string
	}{
		{dtype.Nanosecond, "[9643, 9643, 9642, -17045, 21217]", "[9643, 9643, 9644, 36330, -1932]"},
		{dtype.Microsecond, "[9642, 9643, 8642, -26678355, 11583717]", "[9643, 9642, 10643, 26697640, -11564432]"},
		{dtype.Millisecond, "[9642, 9643, -990358, None, None]", "[9643, 9642, 1009643, None, None]"},
	}
	for _, c := range cases {
		d, _ := series.FromDate("d", []int32{9643, 9643, 9643, 9643, 9643}, nil, opt)
		du, _ := series.FromDuration("x", durs, nil, c.unit, opt)
		for _, sub := range []bool{false, true} {
			var out *series.Series
			var err error
			want := c.plus
			if sub {
				out, err = compute.Sub(context.Background(), d, du, compute.WithAllocator(mem))
				want = c.sub
			} else {
				out, err = compute.Add(context.Background(), d, du, compute.WithAllocator(mem))
			}
			if err != nil {
				t.Fatal(err)
			}
			phys, err := compute.Cast(context.Background(), out, dtype.Int32(), compute.WithAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			if got := testutil.PyRepr(phys.Chunk(0)); got != want {
				t.Errorf("%s sub=%v: got %s, want %s", c.unit, sub, got, want)
			}
			phys.Release()
			out.Release()
		}
		d.Release()
		du.Release()
	}
}
