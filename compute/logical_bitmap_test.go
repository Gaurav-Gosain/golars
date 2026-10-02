package compute_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// boolCells flattens a boolean series into (value, valid) pairs using
// the per-element arrow accessors, independent of the bitmap kernels.
func boolCells(t *testing.T, s *series.Series) (vals, valid []bool) {
	t.Helper()
	for _, c := range s.Chunks() {
		b := c.(*array.Boolean)
		for i := range b.Len() {
			valid = append(valid, b.IsValid(i))
			vals = append(vals, b.IsValid(i) && b.Value(i))
		}
	}
	return vals, valid
}

func refKleene(isAnd, av, aok, bv, bok bool) (bool, bool) {
	if isAnd {
		if (aok && !av) || (bok && !bv) {
			return false, true
		}
		if !aok || !bok {
			return false, false
		}
		return true, true
	}
	if (aok && av) || (bok && bv) {
		return true, true
	}
	if !aok || !bok {
		return false, false
	}
	return false, true
}

// randBool builds a boolean series of n rows. nullRate < 0 means no
// validity bitmap at all. offset > 0 builds a longer series and slices
// it, so the kernels see a non-zero (often non-byte-aligned) offset.
// chunks > 1 splits the rows into that many chunks.
func randBool(t *testing.T, mem memory.Allocator, r *rand.Rand, n, offset, chunks int, nullRate float64) *series.Series {
	t.Helper()
	total := n + offset
	vals := make([]bool, total)
	var valid []bool
	if nullRate >= 0 {
		valid = make([]bool, total)
	}
	for i := range vals {
		vals[i] = r.IntN(2) == 1
		if valid != nil {
			valid[i] = r.Float64() >= nullRate
		}
	}
	if chunks <= 1 || n < chunks {
		s, err := series.FromBool("x", vals, valid, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if offset == 0 {
			return s
		}
		out, err := s.Slice(offset, n)
		s.Release()
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	parts := make([]*series.Series, chunks)
	step := n / chunks
	for i := range parts {
		lo, hi := offset+i*step, offset+(i+1)*step
		if i == chunks-1 {
			hi = total
		}
		var v []bool
		if valid != nil {
			v = valid[lo:hi]
		}
		p, err := series.FromBool("x", vals[lo:hi], v, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		parts[i] = p
	}
	arrs := make([]arrow.Array, 0, chunks)
	for _, p := range parts {
		c := p.Chunks()[0]
		c.Retain()
		arrs = append(arrs, c)
		p.Release()
	}
	out, err := series.New("x", arrs...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestKleeneBitmapMatchesReference(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	sizes := []int{0, 1, 7, 8, 9, 63, 64, 65, 127, 1000, 4099}
	type layout struct {
		offset, chunks int
		nullA, nullB   float64
	}
	layouts := []layout{
		{0, 1, -1, -1},
		{0, 1, 0.3, -1},
		{0, 1, -1, 0.3},
		{0, 1, 0.3, 0.5},
		{0, 1, 1, 0.2}, // all null on one side
		{3, 1, 0.3, 0.3},
		{8, 1, 0.3, -1},
		{13, 1, -1, -1},
		{0, 3, 0.25, 0.25},
		{5, 2, -1, 0.1},
	}
	for _, n := range sizes {
		for li, lay := range layouts {
			t.Run(fmt.Sprintf("n=%d/layout=%d", n, li), func(t *testing.T) {
				mem := testutil.NewCheckedAllocator(t)
				a := randBool(t, mem, r, n, lay.offset, lay.chunks, lay.nullA)
				defer a.Release()
				b := randBool(t, mem, r, n, (lay.offset*7)%11, 1, lay.nullB)
				defer b.Release()
				av, aok := boolCells(t, a)
				bv, bok := boolCells(t, b)

				for _, isAnd := range []bool{true, false} {
					var got *series.Series
					var err error
					if isAnd {
						got, err = compute.And(ctx, a, b, compute.WithAllocator(mem))
					} else {
						got, err = compute.Or(ctx, a, b, compute.WithAllocator(mem))
					}
					if err != nil {
						t.Fatal(err)
					}
					gv, gok := boolCells(t, got)
					nulls := 0
					for i := range n {
						wv, wok := refKleene(isAnd, av[i], aok[i], bv[i], bok[i])
						if gok[i] != wok || (wok && gv[i] != wv) {
							t.Fatalf("and=%v row %d: a=(%v,%v) b=(%v,%v) got (%v,%v) want (%v,%v)",
								isAnd, i, av[i], aok[i], bv[i], bok[i], gv[i], gok[i], wv, wok)
						}
						if !wok {
							nulls++
						}
					}
					if got.NullCount() != nulls {
						t.Fatalf("and=%v NullCount = %d, want %d", isAnd, got.NullCount(), nulls)
					}
					got.Release()
				}

				for _, kind := range []string{"not", "isnull", "isnotnull"} {
					var got *series.Series
					var err error
					switch kind {
					case "not":
						got, err = compute.Not(ctx, a, compute.WithAllocator(mem))
					case "isnull":
						got, err = compute.IsNull(ctx, a, compute.WithAllocator(mem))
					default:
						got, err = compute.IsNotNull(ctx, a, compute.WithAllocator(mem))
					}
					if err != nil {
						t.Fatal(err)
					}
					gv, gok := boolCells(t, got)
					if len(gv) != n {
						t.Fatalf("%s len = %d, want %d", kind, len(gv), n)
					}
					for i := range n {
						var wv, wok bool
						switch kind {
						case "not":
							wv, wok = !av[i], aok[i]
						case "isnull":
							wv, wok = !aok[i], true
						default:
							wv, wok = aok[i], true
						}
						if gok[i] != wok || (wok && gv[i] != wv) {
							t.Fatalf("%s row %d: got (%v,%v) want (%v,%v)", kind, i, gv[i], gok[i], wv, wok)
						}
					}
					got.Release()
				}
			})
		}
	}
}
