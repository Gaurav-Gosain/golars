package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// whereParallelThreshold is the element count above which Where fans
// the blend out across workers. Below it, the serial kernel wins
// because the parallel fan-out overhead dominates.
const whereParallelThreshold = 64 * 1024

// resolveWorkers converts an inferParallelism result (0 = "use default")
// into a concrete positive worker count suitable for use as both the
// iteration space and parallelism argument to pool.ParallelFor.
func resolveWorkers(par int) int {
	if par <= 0 {
		par = pool.DefaultParallelism()
	}
	if par < 1 {
		par = 1
	}
	return par
}

// blendInt64Parallel partitions the blend work across workers. Each
// partition is aligned to an 8-element boundary so the bitmap slicing
// stays byte-aligned (simdBlendInt64 reads whole bytes of condBits).
func blendInt64Parallel(ctx context.Context, condBits []byte, aVals, bVals, out []int64, par int) {
	n := len(out)
	par = resolveWorkers(par)
	if par < 2 || n < whereParallelThreshold {
		simdBlendInt64(condBits, aVals, bVals, out)
		return
	}
	chunk := (n + par - 1) / par
	chunk = (chunk + 7) &^ 7 // round up to multiple of 8
	if chunk <= 0 {
		simdBlendInt64(condBits, aVals, bVals, out)
		return
	}
	_ = pool.ParallelFor(ctx, par, par, func(_ context.Context, s, e int) error {
		for w := s; w < e; w++ {
			start := w * chunk
			if start >= n {
				return nil
			}
			end := min(start+chunk, n)
			simdBlendInt64(condBits[start>>3:], aVals[start:end], bVals[start:end], out[start:end])
		}
		return nil
	})
}

func blendFloat64Parallel(ctx context.Context, condBits []byte, aVals, bVals, out []float64, par int) {
	n := len(out)
	par = resolveWorkers(par)
	if par < 2 || n < whereParallelThreshold {
		simdBlendFloat64(condBits, aVals, bVals, out)
		return
	}
	chunk := (n + par - 1) / par
	chunk = (chunk + 7) &^ 7
	if chunk <= 0 {
		simdBlendFloat64(condBits, aVals, bVals, out)
		return
	}
	_ = pool.ParallelFor(ctx, par, par, func(_ context.Context, s, e int) error {
		for w := s; w < e; w++ {
			start := w * chunk
			if start >= n {
				return nil
			}
			end := min(start+chunk, n)
			simdBlendFloat64(condBits[start>>3:], aVals[start:end], bVals[start:end], out[start:end])
		}
		return nil
	})
}

// Where implements polars' when(cond).then(ifTrue).otherwise(ifFalse).
// For each row:
//   - if cond is null or false -> take ifFalse[row]
//   - if cond is true          -> take ifTrue[row]
//
// A null mask entry is treated as false (same as polars). The three
// inputs must share length. Dtypes must match between ifTrue and
// ifFalse; the output dtype matches them.
//
// Nulls in ifTrue/ifFalse propagate: if the picked side is null, the
// output position is null.
func Where(ctx context.Context, cond, ifTrue, ifFalse *series.Series, opts ...Option) (*series.Series, error) {
	if cond.Len() != ifTrue.Len() || cond.Len() != ifFalse.Len() {
		return nil, fmt.Errorf("compute.Where: length mismatch: cond=%d, then=%d, otherwise=%d",
			cond.Len(), ifTrue.Len(), ifFalse.Len())
	}
	if !ifTrue.DType().Equal(ifFalse.DType()) {
		return nil, fmt.Errorf("compute.Where: dtype mismatch: then=%s otherwise=%s",
			ifTrue.DType(), ifFalse.DType())
	}
	cfg := resolve(opts)
	condArr, err := extractChunk(cond, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer condArr.Release()
	condBool, ok := condArr.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("compute.Where: cond must be bool, got %s", cond.DType())
	}

	aArr, err := extractChunk(ifTrue, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	bArr, err := extractChunk(ifFalse, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bArr.Release()

	name := cfg.outName(ifTrue.Name())
	n := condBool.Len()
	pick := func(i int) bool { return condBool.IsValid(i) && condBool.Value(i) }

	if n == 0 {
		// Empty inputs may carry nil value buffers, which the fused
		// kernels would dereference.
		return series.Empty(name, ifTrue.DType()), nil
	}
	par := inferParallelism(cfg, n)
	switch a := aArr.(type) {
	case *array.Int64:
		b := bArr.(*array.Int64)
		return whereInt64Fused(ctx, name, condBool, a, b, n, cfg.alloc, par)
	case *array.Float64:
		b := bArr.(*array.Float64)
		return whereFloat64Fused(ctx, name, condBool, a, b, n, cfg.alloc, par)
	case *array.Int32:
		b := bArr.(*array.Int32)
		out := make([]int32, n)
		valid := make([]bool, n)
		for i := range n {
			if pick(i) {
				if a.IsValid(i) {
					out[i] = a.Value(i)
					valid[i] = true
				}
			} else {
				if b.IsValid(i) {
					out[i] = b.Value(i)
					valid[i] = true
				}
			}
		}
		return series.FromInt32(name, out, valid, series.WithAllocator(cfg.alloc))
	case *array.Float32:
		b := bArr.(*array.Float32)
		out := make([]float32, n)
		valid := make([]bool, n)
		for i := range n {
			if pick(i) {
				if a.IsValid(i) {
					out[i] = a.Value(i)
					valid[i] = true
				}
			} else {
				if b.IsValid(i) {
					out[i] = b.Value(i)
					valid[i] = true
				}
			}
		}
		return series.FromFloat32(name, out, valid, series.WithAllocator(cfg.alloc))
	case *array.Boolean:
		b := bArr.(*array.Boolean)
		out := make([]bool, n)
		valid := make([]bool, n)
		for i := range n {
			if pick(i) {
				if a.IsValid(i) {
					out[i] = a.Value(i)
					valid[i] = true
				}
			} else {
				if b.IsValid(i) {
					out[i] = b.Value(i)
					valid[i] = true
				}
			}
		}
		return series.FromBool(name, out, valid, series.WithAllocator(cfg.alloc))
	case *array.String:
		b := bArr.(*array.String)
		out := make([]string, n)
		valid := make([]bool, n)
		for i := range n {
			if pick(i) {
				if a.IsValid(i) {
					out[i] = a.Value(i)
					valid[i] = true
				}
			} else {
				if b.IsValid(i) {
					out[i] = b.Value(i)
					valid[i] = true
				}
			}
		}
		return series.FromString(name, out, valid, series.WithAllocator(cfg.alloc))
	}
	return whereGather(name, pick, ifTrue, ifFalse, n, cfg.alloc)
}

// whereGather is the any-dtype path of Where: it concatenates both
// branches and gathers row i from the first or second half.
func whereGather(name string, pick func(int) bool, ifTrue, ifFalse *series.Series, n int, alloc memory.Allocator) (*series.Series, error) {
	both, err := series.ConcatSeries(name, []*series.Series{ifTrue, ifFalse}, series.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("compute.Where: %w", err)
	}
	defer both.Release()
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
		if !pick(i) {
			idx[i] = n + i
		}
	}
	return both.Gather(idx, series.WithAllocator(alloc))
}

// whereInt64Fused is the direct-buffer specialisation: reads the
// packed validity bitmaps of cond / a / b, writes output + validity
// in a single pass. Roughly 10x faster than the scalar IsValid-per-
// row branch because it avoids per-row method dispatch.
func whereInt64Fused(
	ctx context.Context, name string, cond *array.Boolean, a, b *array.Int64, n int, alloc memory.Allocator, par int,
) (*series.Series, error) {
	aVals := a.Int64Values()
	bVals := b.Int64Values()
	condBits := cond.Data().Buffers()[1].Bytes()
	condNullBits, condHasNulls := bitmapOrNil(cond)
	aNullBits, aHasNulls := bitmapOrNil(a)
	bNullBits, bHasNulls := bitmapOrNil(b)
	condOff := cond.Data().Offset()
	aOff := a.Data().Offset()
	bOff := b.Data().Offset()

	// Fast path: no nulls anywhere. Skip the validity bitmap alloc
	// entirely: the output is fully valid so we can use the simpler
	// direct builder. Parallel blend kicks in once n passes the
	// whereParallelThreshold so the fan-out overhead is amortised.
	if !condHasNulls && !aHasNulls && !bHasNulls && condOff == 0 {
		return series.BuildInt64Direct(name, n, poolingMem(alloc), func(out []int64) {
			if simdAvailable && hasSIMDInt64() {
				blendInt64Parallel(ctx, condBits, aVals, bVals, out, par)
				return
			}
			// Branchless scalar fallback.
			for i := range n {
				bit := int64(condBits[i>>3]>>uint(i&7)) & 1
				mask := -bit
				out[i] = bVals[i] ^ ((aVals[i] ^ bVals[i]) & mask)
			}
		})
	}

	return series.BuildInt64DirectFused(name, n, poolingMem(alloc), func(out []int64, outBits []byte) int {
		nulls := 0
		for i := range n {
			// Treat null cond as false (polars semantics).
			condTrue := bitSet(condBits, condOff+i)
			if condHasNulls && !bitSet(condNullBits, condOff+i) {
				condTrue = false
			}
			var src int64
			var valid bool
			if condTrue {
				if !aHasNulls || bitSet(aNullBits, aOff+i) {
					src = aVals[i]
					valid = true
				}
			} else {
				if !bHasNulls || bitSet(bNullBits, bOff+i) {
					src = bVals[i]
					valid = true
				}
			}
			if valid {
				out[i] = src
				outBits[i>>3] |= 1 << (i & 7)
			} else {
				nulls++
			}
		}
		return nulls
	})
}

func whereFloat64Fused(
	ctx context.Context, name string, cond *array.Boolean, a, b *array.Float64, n int, alloc memory.Allocator, par int,
) (*series.Series, error) {
	aVals := a.Float64Values()
	bVals := b.Float64Values()
	condBits := cond.Data().Buffers()[1].Bytes()
	condNullBits, condHasNulls := bitmapOrNil(cond)
	aNullBits, aHasNulls := bitmapOrNil(a)
	bNullBits, bHasNulls := bitmapOrNil(b)
	condOff := cond.Data().Offset()
	aOff := a.Data().Offset()
	bOff := b.Data().Offset()

	if !condHasNulls && !aHasNulls && !bHasNulls && condOff == 0 {
		return series.BuildFloat64Direct(name, n, poolingMem(alloc), func(out []float64) {
			if simdAvailable && hasSIMDInt64() {
				blendFloat64Parallel(ctx, condBits, aVals, bVals, out, par)
				return
			}
			for i := range n {
				if condBits[i>>3]&(1<<(i&7)) != 0 {
					out[i] = aVals[i]
				} else {
					out[i] = bVals[i]
				}
			}
		})
	}

	return series.BuildFloat64DirectFused(name, n, poolingMem(alloc), func(out []float64, outBits []byte) int {
		nulls := 0
		for i := range n {
			condTrue := bitSet(condBits, condOff+i)
			if condHasNulls && !bitSet(condNullBits, condOff+i) {
				condTrue = false
			}
			var src float64
			var valid bool
			if condTrue {
				if !aHasNulls || bitSet(aNullBits, aOff+i) {
					src = aVals[i]
					valid = true
				}
			} else {
				if !bHasNulls || bitSet(bNullBits, bOff+i) {
					src = bVals[i]
					valid = true
				}
			}
			if valid {
				out[i] = src
				outBits[i>>3] |= 1 << (i & 7)
			} else {
				nulls++
			}
		}
		return nulls
	})
}

func bitmapOrNil(arr interface {
	NullN() int
	NullBitmapBytes() []byte
}) ([]byte, bool) {
	if arr.NullN() == 0 {
		return nil, false
	}
	return arr.NullBitmapBytes(), true
}

func bitSet(b []byte, i int) bool { return b[i>>3]&(1<<(i&7)) != 0 }

// keep arrow imported even when the switch uses only package types.
var _ = arrow.BOOL
