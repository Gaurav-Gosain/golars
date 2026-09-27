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
