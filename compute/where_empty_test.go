package compute_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Where on zero rows used to dereference the nil value buffers of the
// empty inputs in the fused int64 and float64 kernels. Found by
// internal/difftest (when/then over an empty frame).
func TestWhereEmptyInputs(t *testing.T) {
	ctx := context.Background()
	for _, dt := range []dtype.DType{dtype.Int64(), dtype.Float64(), dtype.Int32()} {
		cond := series.Empty("c", dtype.Bool())
		a := series.Empty("a", dt)
		b := series.Empty("b", dt)
		out, err := compute.Where(ctx, cond, a, b)
		if err != nil {
			t.Fatalf("%s: %v", dt, err)
		}
		if out.Len() != 0 || !out.DType().Equal(dt) {
			t.Fatalf("%s: got len %d dtype %s", dt, out.Len(), out.DType())
		}
		out.Release()
		cond.Release()
		a.Release()
		b.Release()
	}
}
