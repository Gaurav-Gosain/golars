package compute

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// compareStringScalar compares a single-chunk String column with a
// string literal without materialising the literal as a column. The
// broadcast fallback built an n-row string array holding copies of the
// literal and then compared row pairs through a closure; filters like
// l_shipmode == 'AIR' (TPC-H Q12, Q19) spent most of their time there.
// Equality checks the length from the offsets before touching bytes.
// ok is false when the input shape does not match.
func compareStringScalar(ctx context.Context, a *series.Series, lit string, cfg config, op compareOp) (*series.Series, bool, error) {
	if a.NumChunks() != 1 {
		return nil, false, nil
	}
	arr, ok := a.Chunk(0).(*array.String)
	if !ok {
		return nil, false, nil
	}
	n := arr.Len()
	offs := arr.ValueOffsets()
	// ValueBytes starts at the first value of the (possibly sliced)
	// array while offsets stay absolute, so rebase by offs[0].
	data := arr.ValueBytes()
	base := int32(0)
	if n > 0 {
		base = offs[0]
	}
	mem := cfg.alloc
	valid := series.CopyValidityBitmap(arr, mem)
	nbytes := (n + 7) / 8
	out, err := series.BuildBoolDirectWithValidity(cfg.outName(a.Name()), n, mem, func(bits []byte) {
		fill := func(_ context.Context, s, e int) error {
			lo, hi := s*8, min(e*8, n)
			switch op {
			case opEq, opNe:
				want := byte(1)
				if op == opNe {
					want = 0
				}
				for i := lo; i < hi; i++ {
					b, c := offs[i]-base, offs[i+1]-base
					eq := int(c-b) == len(lit) && string(data[b:c]) == lit
					if b2u8(eq) == want {
						bits[i>>3] |= 1 << uint(i&7)
					}
				}
			default:
				pred := stringPredicate(op)
				for i := lo; i < hi; i++ {
					if pred(string(data[offs[i]-base:offs[i+1]-base]), lit) {
						bits[i>>3] |= 1 << uint(i&7)
					}
				}
			}
			return nil
		}
		// Workers own whole output bytes, so no two write the same byte.
		_ = pool.ParallelFor(ctx, nbytes, inferParallelism(cfg, n), fill)
	}, valid, arr.NullN())
	return out, true, err
}
