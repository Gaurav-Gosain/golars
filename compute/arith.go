package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/series"
)

// Numeric is the set of dtypes that participate in arithmetic kernels.
type Numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// int64BinaryBitmap runs op(a, b) elementwise and materialises the
// result straight into an arrow-backed int64 Series with a pre-built
// output validity bitmap (the AND of the two inputs' validities).
//
// Skips the []bool round-trip the old applyBinaryNumeric + FromInt64
// path took: that path built a []bool (O(n)), then FromInt64 packed
// it back into a bitmap (O(n) again). Building the bitmap directly
// from the Arrow buffers once and writing the output once halves the
// validity-side work.
//
// Writes to null positions are harmless - the bitmap hides them - so
// the hot loop stays branchless and auto-vectorisable. Only use for
// ops that don't trap on any inputs (add/sub/mul/float-div). Integer
// div by zero stays on the fallible path.
func int64BinaryBitmap(
	ctx context.Context,
	name string,
	aArr, bArr arrow.Array,
	aVals, bVals []int64,
	op func(int64, int64) int64,
	par int,
	mem memory.Allocator,
) (*series.Series, error) {
	n := aArr.Len()
	nullBuf, nulls := series.AndValidityBitmap(aArr, bArr, mem)
	return series.BuildInt64DirectWithValidity(name, n, mem, func(out []int64) {
		_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
			for i := s; i < e; i++ {
				out[i] = op(aVals[i], bVals[i])
			}
			return nil
		})
	}, nullBuf, nulls)
}

// float64BinaryBitmap is the float64 counterpart. float64 never traps,
// so every arithmetic op including div can go through it.
func float64BinaryBitmap(
	ctx context.Context,
	name string,
	aArr, bArr arrow.Array,
	aVals, bVals []float64,
	op func(float64, float64) float64,
	par int,
	mem memory.Allocator,
) (*series.Series, error) {
	n := aArr.Len()
	nullBuf, nulls := series.AndValidityBitmap(aArr, bArr, mem)
	return series.BuildFloat64DirectWithValidity(name, n, mem, func(out []float64) {
		_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
			for i := s; i < e; i++ {
				out[i] = op(aVals[i], bVals[i])
			}
			return nil
		})
	}, nullBuf, nulls)
}

// applyBinaryNumeric runs op(a, b) elementwise with null propagation. It
// returns the output slice plus the validity slice (nil if no nulls). The
// caller wraps these into a typed Series.
//
// Deprecated in favour of int64BinaryBitmap / float64BinaryBitmap for
// the hot paths. Kept for int32 / mixed-type ops that haven't been
// migrated yet.
func applyBinaryNumeric[T Numeric](
	ctx context.Context,
	aArr, bArr arrow.Array,
	aVals, bVals []T,
	op func(T, T) T,
	parallelism int,
) ([]T, []bool, error) {
	n := aArr.Len()
	out := make([]T, n)
	valid := buildValidityAnd(aArr, bArr)
	err := pool.ParallelFor(ctx, n, parallelism, func(ctx context.Context, s, e int) error {
		if valid == nil {
			for i := s; i < e; i++ {
				out[i] = op(aVals[i], bVals[i])
			}
			return nil
		}
		for i := s; i < e; i++ {
			if valid[i] {
				out[i] = op(aVals[i], bVals[i])
			}
		}
		return nil
	})
	return out, valid, err
}

// applyBinaryNumericFallible is like applyBinaryNumeric but op can signal a
// per-element null by returning ok=false. This is the path used by integer
// division with zero handling.
func applyBinaryNumericFallible[T Numeric](
	ctx context.Context,
	aArr, bArr arrow.Array,
	aVals, bVals []T,
	op func(T, T) (T, bool),
	parallelism int,
) ([]T, []bool, error) {
	n := aArr.Len()
	out := make([]T, n)
	pv := buildValidityAnd(aArr, bArr)
	valid := make([]bool, n)
	err := pool.ParallelFor(ctx, n, parallelism, func(ctx context.Context, s, e int) error {
		for i := s; i < e; i++ {
			if pv != nil && !pv[i] {
				continue
			}
			v, ok := op(aVals[i], bVals[i])
			if ok {
				out[i] = v
				valid[i] = true
			}
		}
		return nil
	})
	return out, valid, err
}

// Add returns a Series with elementwise a + b. a and b must have the same
// length and dtype. Nulls propagate: if a[i] or b[i] is null, result[i] is
// null.
func Add(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runArith(ctx, a, b, opts, "Add", opAdd)
}

// Sub returns a Series with elementwise a - b.
func Sub(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runArith(ctx, a, b, opts, "Sub", opSub)
}

// Mul returns a Series with elementwise a * b.
func Mul(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runArith(ctx, a, b, opts, "Mul", opMul)
}

// Div returns a Series with elementwise a / b. For integer dtypes, division
// by zero produces a null in the output. For floats, zero divisors follow
// IEEE 754 (inf, -inf, nan).
func Div(ctx context.Context, a, b *series.Series, opts ...Option) (*series.Series, error) {
	return runArith(ctx, a, b, opts, "Div", opDiv)
}

// AddLit / SubLit / MulLit / DivLit compute a ∘ lit elementwise
// without materialising lit as a full broadcast Series. Halves the
// memory traffic vs Add/Sub/Mul/Div for the `col OP scalar` case
// that dominates query patterns like `pl.col("x") * 2`.
func AddLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runArithLit(ctx, a, lit, opts, "AddLit", opAdd)
}

func SubLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runArithLit(ctx, a, lit, opts, "SubLit", opSub)
}

func MulLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runArithLit(ctx, a, lit, opts, "MulLit", opMul)
}

func DivLit(ctx context.Context, a *series.Series, lit any, opts ...Option) (*series.Series, error) {
	return runArithLit(ctx, a, lit, opts, "DivLit", opDiv)
}

func runArithLit(ctx context.Context, a *series.Series, lit any, opts []Option, kernel string, op arithOp) (*series.Series, error) {
	if out, ok, err := extraArithLit(ctx, a, lit, opts, op); ok {
		return out, err
	}
	cfg := resolve(opts)
	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	name := cfg.outName(a.Name())
	par := inferParallelism(cfg, a.Len())
	_ = par

	switch v := aArr.(type) {
	case *array.Int64:
		lv, ok := toInt64Scalar(lit)
		if !ok {
			break
		}
		return int64ArithLit(name, v, lv, op, poolingMem(cfg.alloc))
	case *array.Float64:
		lv, ok := toFloat64Scalar(lit)
		if !ok {
			break
		}
		return float64ArithLit(name, v, lv, op, poolingMem(cfg.alloc))
	}
	// Unsupported dtype/lit combo: fall back via broadcast.
	litSer, err := literalBroadcastSeries(a.Len(), lit, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer litSer.Release()
	return runArith(ctx, a, litSer, opts, kernel, op)
}

func int64ArithLit(name string, a *array.Int64, lit int64, op arithOp, mem memory.Allocator) (*series.Series, error) {
	src := a.Int64Values()
	n := len(src)
	if a.NullN() > 0 {
		// Nullable path: share the input's validity bitmap verbatim.
		// The op preserves nulls (a null + anything = null), so the
		// output's bitmap == the input's bitmap. No []bool round-trip,
		// no per-row IsValid loop.
		//
		// Integer division by a zero literal flips the result to
		// all-null to match polars int-div semantics; easier to fold
		// that into the builder by swapping the bitmap for a freshly-
		// allocated all-zero buffer.
		var nullBuf *memory.Buffer
		nullCount := a.NullN()
		if op == opDiv && lit == 0 {
			nullBuf = memory.NewResizableBuffer(mem)
			nullBuf.Resize(int(bitutil.BytesForBits(int64(n))))
			// bytes are zero-initialised by Resize: already "all null".
			nullCount = n
		} else {
			nullBuf = series.CopyValidityBitmap(a, mem)
		}
		return series.BuildInt64DirectWithValidity(name, n, mem, func(out []int64) {
			// Writes into null slots are harmless - the bitmap hides
			// them. Skipping the branch keeps the loop auto-vectorisable.
			switch op {
			case opAdd:
				for i := range n {
					out[i] = src[i] + lit
				}
			case opSub:
				for i := range n {
					out[i] = src[i] - lit
				}
			case opMul:
				for i := range n {
					out[i] = src[i] * lit
				}
			case opDiv:
				if lit == 0 {
					// Values here are hidden by the all-null bitmap.
					return
				}
				for i := range n {
					out[i] = src[i] / lit
				}
			}
		}, nullBuf, nullCount)
	}
	// No-nulls fast path.
	return series.BuildInt64Direct(name, n, mem, func(out []int64) {
		start := 0
		if simdAvailable && hasSIMDInt64() {
			switch op {
			case opAdd:
				start = simdAddLitInt64(src, lit, out)
			case opSub:
				start = simdSubLitInt64(src, lit, out)
			}
		}
		switch op {
		case opAdd:
			for i := start; i < n; i++ {
				out[i] = src[i] + lit
			}
		case opSub:
			for i := start; i < n; i++ {
				out[i] = src[i] - lit
			}
		case opMul:
			for i := range n {
				out[i] = src[i] * lit
			}
		case opDiv:
			if lit == 0 {
				// Handled above; should not reach here for no-null path.
				for i := range n {
					out[i] = 0
				}
				return
			}
			for i := range n {
				out[i] = src[i] / lit
			}
		}
	})
}

func float64ArithLit(name string, a *array.Float64, lit float64, op arithOp, mem memory.Allocator) (*series.Series, error) {
	src := a.Float64Values()
	n := len(src)
	if a.NullN() > 0 {
		// Pass the input's validity through unchanged - arithmetic
		// with a scalar literal preserves the null pattern. Saves
		// the O(n) IsValid loop + []bool round-trip.
		nullBuf := series.CopyValidityBitmap(a, mem)
		return series.BuildFloat64DirectWithValidity(name, n, mem, func(out []float64) {
			applyFloat64ArithLit(src, lit, out, op)
		}, nullBuf, a.NullN())
	}
	return series.BuildFloat64Direct(name, n, mem, func(out []float64) {
		applyFloat64ArithLit(src, lit, out, op)
	})
}

func applyFloat64ArithLit(src []float64, lit float64, out []float64, op arithOp) {
	n := len(src)
	start := 0
	if simdAvailable && hasSIMDInt64() {
		switch op {
		case opAdd:
			start = simdAddLitFloat64(src, lit, out)
		case opSub:
			start = simdSubLitFloat64(src, lit, out)
		case opMul:
			start = simdMulLitFloat64(src, lit, out)
		case opDiv:
			start = simdDivLitFloat64(src, lit, out)
		}
	}
	switch op {
	case opAdd:
		for i := start; i < n; i++ {
			out[i] = src[i] + lit
		}
	case opSub:
		for i := start; i < n; i++ {
			out[i] = src[i] - lit
		}
	case opMul:
		for i := start; i < n; i++ {
			out[i] = src[i] * lit
		}
	case opDiv:
		for i := start; i < n; i++ {
			out[i] = src[i] / lit
		}
	}
}

func toInt64Scalar(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	}
	return 0, false
}

func toFloat64Scalar(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

// literalBroadcastSeries builds a length-n Series filled with lit.
// Used only as a fallback when runArithLit can't take a direct path.
func literalBroadcastSeries(n int, lit any, mem memory.Allocator) (*series.Series, error) {
	switch v := lit.(type) {
	case int64:
		out := make([]int64, n)
		for i := range out {
			out[i] = v
		}
		return series.FromInt64("__lit", out, nil, series.WithAllocator(mem))
	case int:
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(v)
		}
		return series.FromInt64("__lit", out, nil, series.WithAllocator(mem))
	case float64:
		out := make([]float64, n)
		for i := range out {
			out[i] = v
		}
		return series.FromFloat64("__lit", out, nil, series.WithAllocator(mem))
	}
	return nil, fmt.Errorf("compute: literal type %T not supported in runArithLit fallback", lit)
}

type arithOp int

const (
	opAdd arithOp = iota
	opSub
	opMul
	opDiv
)

func runArith(ctx context.Context, a, b *series.Series, opts []Option, kernel string, op arithOp) (*series.Series, error) {
	if out, ok, err := temporalArith(ctx, a, b, opts, op); ok {
		return out, err
	}
	if err := checkBinary(a, b); err != nil {
		return nil, err
	}
	cfg := resolve(opts)

	aArr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer aArr.Release()
	bArr, err := extractChunk(b, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer bArr.Release()

	name := cfg.outName(a.Name())
	par := inferParallelism(cfg, a.Len())

	switch a.DType().ID() {
	case arrow.INT32:
		return dispatchArithInt32(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.INT64:
		return dispatchArithInt64(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.UINT32:
		return dispatchArithUint32(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.UINT64:
		return dispatchArithUint64(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.FLOAT32:
		return dispatchArithFloat32(ctx, name, aArr, bArr, op, cfg.alloc, par)
	case arrow.FLOAT64:
		return dispatchArithFloat64(ctx, name, aArr, bArr, op, cfg.alloc, par)
	}
	return nil, isUnsupported(kernel, a.DType())
}

func dispatchArithInt32(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := int32Values(aArr), int32Values(bArr)
	switch op {
	case opAdd:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y int32) int32 { return x + y }, par)
		if err != nil {
			return nil, err
		}
		return fromInt32Result(name, out, valid, mem)
	case opSub:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y int32) int32 { return x - y }, par)
		if err != nil {
			return nil, err
		}
		return fromInt32Result(name, out, valid, mem)
	case opMul:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y int32) int32 { return x * y }, par)
		if err != nil {
			return nil, err
		}
		return fromInt32Result(name, out, valid, mem)
	case opDiv:
		out, valid, err := applyBinaryNumericFallible(ctx, aArr, bArr, av, bv, func(x, y int32) (int32, bool) {
			if y == 0 {
				return 0, false
			}
			return x / y, true
		}, par)
		if err != nil {
			return nil, err
		}
		return fromInt32Result(name, out, valid, mem)
	}
	return nil, ErrUnsupportedDType
}

func dispatchArithInt64(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := int64Values(aArr), int64Values(bArr)
	n := aArr.Len()
	noNulls := aArr.NullN() == 0 && bArr.NullN() == 0
	switch op {
	case opAdd:
		if noNulls {
			// Direct buffer write. At 1M+ the output exceeds L2 and the
			// next consumer almost certainly hits DRAM anyway, so AVX2
			// non-temporal stores (VMOVNTDQ) win by skipping the write-
			// allocate. Below that the cached autovec store is faster
			// because the output lines may get re-read.
			//
			// poolingMem reuses the backing byte slice across back-to-back
			// calls: for benchmark loops and streaming pipelines this
			// saves the ~50μs mallocgc zero for a fresh 8 MB buffer.
			return series.BuildInt64Direct(name, n, poolingMem(mem), func(out []int64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] + bv[i]
					}
					return
				}
				if n >= 1024*1024 {
					_ = pool.ParallelFor(ctx, n, 2, func(_ context.Context, s, e int) error {
						simdAddInt64NT(out[s:e], av[s:e], bv[s:e])
						return nil
					})
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] + bv[i]
					}
					return nil
				})
			})
		}
		return int64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y int64) int64 { return x + y }, par, mem)
	case opSub:
		if noNulls {
			return series.BuildInt64Direct(name, n, poolingMem(mem), func(out []int64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] - bv[i]
					}
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] - bv[i]
					}
					return nil
				})
			})
		}
		return int64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y int64) int64 { return x - y }, par, mem)
	case opMul:
		if noNulls {
			return series.BuildInt64Direct(name, n, poolingMem(mem), func(out []int64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] * bv[i]
					}
					return
				}
				if n >= 1024*1024 {
					_ = pool.ParallelFor(ctx, n, 2, func(_ context.Context, s, e int) error {
						simdMulInt64NT(out[s:e], av[s:e], bv[s:e])
						return nil
					})
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] * bv[i]
					}
					return nil
				})
			})
		}
		return int64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y int64) int64 { return x * y }, par, mem)
	case opDiv:
		out, valid, err := applyBinaryNumericFallible(ctx, aArr, bArr, av, bv, func(x, y int64) (int64, bool) {
			if y == 0 {
				return 0, false
			}
			return x / y, true
		}, par)
		if err != nil {
			return nil, err
		}
		return fromInt64Result(name, out, valid, mem)
	}
	return nil, ErrUnsupportedDType
}

func dispatchArithUint32(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := uint32Values(aArr), uint32Values(bArr)
	builder := func(out []uint32, valid []bool) (*series.Series, error) {
		return fromUint32Result(name, out, valid, mem)
	}
	switch op {
	case opAdd:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint32) uint32 { return x + y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opSub:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint32) uint32 { return x - y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opMul:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint32) uint32 { return x * y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opDiv:
		out, valid, err := applyBinaryNumericFallible(ctx, aArr, bArr, av, bv, func(x, y uint32) (uint32, bool) {
			if y == 0 {
				return 0, false
			}
			return x / y, true
		}, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	}
	return nil, ErrUnsupportedDType
}

func dispatchArithUint64(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := uint64Values(aArr), uint64Values(bArr)
	builder := func(out []uint64, valid []bool) (*series.Series, error) {
		return fromUint64Result(name, out, valid, mem)
	}
	switch op {
	case opAdd:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint64) uint64 { return x + y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opSub:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint64) uint64 { return x - y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opMul:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y uint64) uint64 { return x * y }, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	case opDiv:
		out, valid, err := applyBinaryNumericFallible(ctx, aArr, bArr, av, bv, func(x, y uint64) (uint64, bool) {
			if y == 0 {
				return 0, false
			}
			return x / y, true
		}, par)
		if err != nil {
			return nil, err
		}
		return builder(out, valid)
	}
	return nil, ErrUnsupportedDType
}

func dispatchArithFloat32(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := float32Values(aArr), float32Values(bArr)
	switch op {
	case opAdd:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y float32) float32 { return x + y }, par)
		if err != nil {
			return nil, err
		}
		return fromFloat32Result(name, out, valid, mem)
	case opSub:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y float32) float32 { return x - y }, par)
		if err != nil {
			return nil, err
		}
		return fromFloat32Result(name, out, valid, mem)
	case opMul:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y float32) float32 { return x * y }, par)
		if err != nil {
			return nil, err
		}
		return fromFloat32Result(name, out, valid, mem)
	case opDiv:
		out, valid, err := applyBinaryNumeric(ctx, aArr, bArr, av, bv, func(x, y float32) float32 { return x / y }, par)
		if err != nil {
			return nil, err
		}
		return fromFloat32Result(name, out, valid, mem)
	}
	return nil, ErrUnsupportedDType
}

func dispatchArithFloat64(ctx context.Context, name string, aArr, bArr arrow.Array, op arithOp, mem memory.Allocator, par int) (*series.Series, error) {
	av, bv := float64Values(aArr), float64Values(bArr)
	n := aArr.Len()
	noNulls := aArr.NullN() == 0 && bArr.NullN() == 0
	switch op {
	case opAdd:
		if noNulls {
			return series.BuildFloat64Direct(name, n, poolingMem(mem), func(out []float64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] + bv[i]
					}
					return
				}
				if n >= 1024*1024 {
					// Worker count tuned per-arch: on amd64 the two-writer
					// saturation for VMOVNTPD is at ~2 cores; on arm64 the
					// M-series chips have more memory bandwidth headroom
					// and scale to 4+ writers before saturating. par is
					// runtime-inferred from GOMAXPROCS.
					_ = pool.ParallelFor(ctx, n, addFloat64NTWorkers(par), func(_ context.Context, s, e int) error {
						simdAddFloat64NT(out[s:e], av[s:e], bv[s:e])
						return nil
					})
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] + bv[i]
					}
					return nil
				})
			})
		}
		return float64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y float64) float64 { return x + y }, par, mem)
	case opSub:
		if noNulls {
			return series.BuildFloat64Direct(name, n, poolingMem(mem), func(out []float64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] - bv[i]
					}
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] - bv[i]
					}
					return nil
				})
			})
		}
		return float64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y float64) float64 { return x - y }, par, mem)
	case opMul:
		if noNulls {
			return series.BuildFloat64Direct(name, n, poolingMem(mem), func(out []float64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] * bv[i]
					}
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] * bv[i]
					}
					return nil
				})
			})
		}
		return float64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y float64) float64 { return x * y }, par, mem)
	case opDiv:
		if noNulls {
			return series.BuildFloat64Direct(name, n, poolingMem(mem), func(out []float64) {
				if n < 128*1024 {
					for i := range out {
						out[i] = av[i] / bv[i]
					}
					return
				}
				_ = pool.ParallelFor(ctx, n, par, func(_ context.Context, s, e int) error {
					for i := s; i < e; i++ {
						out[i] = av[i] / bv[i]
					}
					return nil
				})
			})
		}
		return float64BinaryBitmap(ctx, name, aArr, bArr, av, bv,
			func(x, y float64) float64 { return x / y }, par, mem)
	}
	return nil, ErrUnsupportedDType
}
