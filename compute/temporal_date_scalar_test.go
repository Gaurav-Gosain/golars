package compute_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func dateLit(t testing.TB, day int32, o series.Option) *series.Series {
	t.Helper()
	s, err := series.FromDate("lit", []int32{day}, nil, o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The Date column vs Date scalar fast path must match the general
// temporal path, which it bypasses, on every operator, with nulls, on
// sliced input and around the 8-row byte boundary.
func TestDateCompareScalarFastPath(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	for _, n := range []int{0, 1, 7, 8, 9, 17, 64, 1000} {
		days := make([]int32, n)
		valid := make([]bool, n)
		for i := range days {
			days[i] = int32(i%13) - 3
			valid[i] = i%5 != 2
		}
		col, _ := series.FromDate("d", days, valid, o)
		lit := dateLit(t, 4, o)
		sliced := sliceOrSelf(col)
		for _, src := range []*series.Series{col, sliced} {
			ops := map[string]func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error){
				"eq": compute.Eq, "ne": compute.Ne, "lt": compute.Lt, "le": compute.Le, "gt": compute.Gt, "ge": compute.Ge,
			}
			for name, op := range ops {
				got, err := op(ctx, src, lit, compute.WithAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				if got.Len() != src.Len() {
					t.Fatalf("n=%d %s: len %d", n, name, got.Len())
				}
				for i := range src.Len() {
					gv, _ := got.Get(i)
					sv, _ := src.Get(i)
					if sv == nil {
						if gv != nil {
							t.Fatalf("n=%d %s row %d: want null, got %v", n, name, i, gv)
						}
						continue
					}
					x := int32(days[i+(n-src.Len())])
					want := map[string]bool{"eq": x == 4, "ne": x != 4, "lt": x < 4, "le": x <= 4, "gt": x > 4, "ge": x >= 4}[name]
					if gv != want {
						t.Fatalf("n=%d %s row %d (day %d): got %v want %v", n, name, i, x, gv, want)
					}
				}
				got.Release()
			}
		}
		if sliced != col {
			sliced.Release()
		}
		col.Release()
		lit.Release()
	}
}

// Date column vs Date column compares row by row on int32.
func TestDateComparePairFastPath(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	for _, n := range []int{0, 1, 7, 8, 9, 33, 1000} {
		xd, yd := make([]int32, n), make([]int32, n)
		xvld, yvld := make([]bool, n), make([]bool, n)
		for i := range n {
			xd[i], yd[i] = int32(i%5), int32(i%3)
			xvld[i], yvld[i] = i%6 != 1, i%4 != 2
		}
		for _, nullsOn := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
			xv, yv := []bool(nil), []bool(nil)
			if nullsOn[0] {
				xv = xvld
			}
			if nullsOn[1] {
				yv = yvld
			}
			x, _ := series.FromDate("x", xd, xv, o)
			y, _ := series.FromDate("y", yd, yv, o)
			xs, ys := sliceOrSelf(x), sliceOrSelf(y)
			off := n - xs.Len()
			ops := map[string]func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error){
				"eq": compute.Eq, "ne": compute.Ne, "lt": compute.Lt, "le": compute.Le, "gt": compute.Gt, "ge": compute.Ge,
			}
			for name, op := range ops {
				got, err := op(ctx, xs, ys, compute.WithAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				for i := range xs.Len() {
					r := i + off
					gv, _ := got.Get(i)
					if (xv != nil && !xv[r]) || (yv != nil && !yv[r]) {
						if gv != nil {
							t.Fatalf("n=%d %s row %d: want null, got %v", n, name, i, gv)
						}
						continue
					}
					a, b := xd[r], yd[r]
					want := map[string]bool{"eq": a == b, "ne": a != b, "lt": a < b, "le": a <= b, "gt": a > b, "ge": a >= b}[name]
					if gv != want {
						t.Fatalf("n=%d nulls=%v %s row %d (%d vs %d): got %v want %v", n, nullsOn, name, i, a, b, gv, want)
					}
				}
				got.Release()
			}
			if xs != x {
				xs.Release()
			}
			if ys != y {
				ys.Release()
			}
			x.Release()
			y.Release()
		}
	}
}

// sliceOrSelf drops the first row so the fast path sees an offset array.
func sliceOrSelf(s *series.Series) *series.Series {
	if s.Len() < 2 {
		return s
	}
	out, err := s.Slice(1, s.Len()-1)
	if err != nil {
		return s
	}
	return out
}

func BenchmarkDateLtPair(b *testing.B) {
	ctx := context.Background()
	n := 1 << 20
	xd, yd := make([]int32, n), make([]int32, n)
	for i := range xd {
		xd[i], yd[i] = int32(i*7%2500), int32(i*13%2500)
	}
	x, _ := series.FromDate("x", xd, nil)
	y, _ := series.FromDate("y", yd, nil)
	defer x.Release()
	defer y.Release()
	for b.Loop() {
		out, err := compute.Lt(ctx, x, y)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

func BenchmarkDateLtScalar(b *testing.B) {
	ctx := context.Background()
	n := 1 << 20
	days := make([]int32, n)
	for i := range days {
		days[i] = int32(i * 7 % 2500)
	}
	col, _ := series.FromDate("d", days, nil)
	defer col.Release()
	lit := dateLit(b, 1200, series.WithAllocator(nil))
	defer lit.Release()
	b.SetBytes(int64(n * 4))
	for b.Loop() {
		out, err := compute.Lt(ctx, col, lit)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
