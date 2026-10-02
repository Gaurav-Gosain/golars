package compute

import (
	"context"

	"github.com/Gaurav-Gosain/golars/internal/pool"
)

// runBinInt64 writes out[i] = a[i] op b[i] (add, sub or mul) with the
// serial, parallel and non-temporal strategy picked from the tuned
// cutoffs in tune_*.go.
func runBinInt64(ctx context.Context, out, a, b []int64, op arithOp, par int) {
	n := len(out)
	if n < elementwiseSerialCutoff {
		vecBinInt64(out, a, b, op)
		return
	}
	if n >= ntStoreCutoff && (op == opAdd || op == opMul) {
		// Two writers saturate the streaming-store path.
		_ = pool.ParallelFor(ctx, n, 2, func(_ context.Context, s, e int) error {
			if op == opAdd {
				simdAddInt64NT(out[s:e], a[s:e], b[s:e])
			} else {
				simdMulInt64NT(out[s:e], a[s:e], b[s:e])
			}
			return nil
		})
		return
	}
	_ = pool.ParallelFor(ctx, n, elementwiseWorkers(par), func(_ context.Context, s, e int) error {
		vecBinInt64(out[s:e], a[s:e], b[s:e], op)
		return nil
	})
}

// runBinFloat64 is the float64 counterpart, including division.
func runBinFloat64(ctx context.Context, out, a, b []float64, op arithOp, par int) {
	n := len(out)
	if n < elementwiseSerialCutoff {
		vecBinFloat64(out, a, b, op)
		return
	}
	if n >= ntStoreCutoff && op == opAdd {
		_ = pool.ParallelFor(ctx, n, addFloat64NTWorkers(par), func(_ context.Context, s, e int) error {
			simdAddFloat64NT(out[s:e], a[s:e], b[s:e])
			return nil
		})
		return
	}
	_ = pool.ParallelFor(ctx, n, elementwiseWorkers(par), func(_ context.Context, s, e int) error {
		vecBinFloat64(out[s:e], a[s:e], b[s:e], op)
		return nil
	})
}
