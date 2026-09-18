package compute

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// Cast returns a Series whose values are s converted to the target dtype.
//
// Semantics follow polars' strict=false default:
//   - Integer widening (e.g. i32 -> i64) never fails.
//   - Integer narrowing (e.g. i64 -> i32) produces null when the value does
//     not fit in the target range.
//   - Float-to-integer truncates; values outside the target range produce null.
//   - Integer-to-float is exact for 32-bit integers; f64 may lose precision
//     for large i64.
//   - Numeric-to-string uses strconv's default formatting.
//   - String-to-numeric uses strconv.Parse*; unparseable values produce null.
//   - Bool-to-integer maps false=0, true=1; bool-to-float likewise.
//
// Casting to the same dtype returns a clone of the input.
func Cast(ctx context.Context, s *series.Series, to dtype.DType, opts ...Option) (*series.Series, error) {
	cfg := resolve(opts)
	if s.DType().Equal(to) {
		return s.Clone(), nil
	}

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	name := cfg.outName(s.Name())

	// Dispatch on (fromID, toID) pair.
	switch to.ID() {
	case arrow.INT32:
		return castToInt32(name, arr, cfg)
	case arrow.INT64:
		return castToInt64(name, arr, cfg)
	case arrow.UINT32:
		return castToUint32(name, arr, cfg)
	case arrow.UINT64:
		return castToUint64(name, arr, cfg)
	case arrow.FLOAT32:
		return castToFloat32(name, arr, cfg)
	case arrow.FLOAT64:
		return castToFloat64(name, arr, cfg)
	case arrow.BOOL:
		return castToBool(name, arr, cfg)
	case arrow.STRING:
		return castToString(name, arr, cfg)
	}
	return nil, fmt.Errorf("%w: cast to %s", ErrUnsupportedDType, to)
}

func castToInt64(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]int64, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int32:
		raw := x.Int32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = int64(raw[i])
			valid[i] = true
		}
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = raw[i]
			valid[i] = true
		}
	case *array.Uint32:
		raw := x.Uint32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = int64(raw[i])
			valid[i] = true
		}
	case *array.Uint64:
		raw := x.Uint64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v > 1<<63-1 {
				continue // overflow -> null
			}
			out[i] = int64(v)
			valid[i] = true
		}
	case *array.Float32:
		raw := x.Float32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v != v || v < -9.2233720368547758e+18 || v > 9.2233720368547758e+18 {
				continue
			}
			out[i] = int64(v)
			valid[i] = true
		}
	case *array.Float64:
		raw := x.Float64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v != v || v < -9.2233720368547758e+18 || v > 9.2233720368547758e+18 {
				continue
			}
			out[i] = int64(v)
			valid[i] = true
		}
	case *array.Boolean:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			if x.Value(i) {
				out[i] = 1
			}
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseInt(x.Value(i), 10, 64)
			if err != nil {
				continue
			}
			out[i] = v
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> i64", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromInt64(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToInt32(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]int32, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int32:
		raw := x.Int32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = raw[i]
			valid[i] = true
		}
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v < -1<<31 || v > 1<<31-1 {
				continue
			}
			out[i] = int32(v)
			valid[i] = true
		}
	case *array.Float64:
		raw := x.Float64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v != v || v < -2147483648 || v > 2147483647 {
				continue
			}
			out[i] = int32(v)
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseInt(x.Value(i), 10, 32)
			if err != nil {
				continue
			}
			out[i] = int32(v)
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> i32", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromInt32(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToUint32(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]uint32, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v < 0 || v > 1<<32-1 {
				continue
			}
			out[i] = uint32(v)
			valid[i] = true
		}
	case *array.Uint32:
		raw := x.Uint32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = raw[i]
			valid[i] = true
		}
	case *array.Uint64:
		raw := x.Uint64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v > 1<<32-1 {
				continue
			}
			out[i] = uint32(v)
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseUint(x.Value(i), 10, 32)
			if err != nil {
				continue
			}
			out[i] = uint32(v)
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> u32", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromUint32(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToUint64(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]uint64, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			if v < 0 {
				continue
			}
			out[i] = uint64(v)
			valid[i] = true
		}
	case *array.Uint64:
		raw := x.Uint64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = raw[i]
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseUint(x.Value(i), 10, 64)
			if err != nil {
				continue
			}
			out[i] = v
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> u64", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromUint64(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToFloat64(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	// Fast path: no-null numeric inputs use direct-buffer write with
	// parallel chunks. Measured ~12x speedup on 256K int64->float64.
	if arr.NullN() == 0 {
		switch x := arr.(type) {
		case *array.Int64:
			raw := x.Int64Values()
			return series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
				castInt64ToFloat64(out, raw)
			})
		case *array.Int32:
			raw := x.Int32Values()
			return series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
				castInt32ToFloat64(out, raw)
			})
		case *array.Uint64:
			raw := x.Uint64Values()
			return series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
				castUint64ToFloat64(out, raw)
			})
		case *array.Float32:
			raw := x.Float32Values()
			return series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
				castFloat32ToFloat64(out, raw)
			})
		case *array.Float64:
			raw := x.Float64Values()
			return series.BuildFloat64Direct(name, n, poolingMem(cfg.alloc), func(out []float64) {
				copy(out, raw)
			})
		}
	}
	// Pure numeric casts preserve the input's null pattern - a valid
	// source always maps to a valid destination (no NaN/range
	// introduction in the widening direction). Share the input's
	// validity bitmap verbatim and run a branchless typed convert;
	// writes to null slots are hidden by the bitmap.
	switch x := arr.(type) {
	case *array.Int32:
		raw := x.Int32Values()
		nullBuf := series.CopyValidityBitmap(arr, cfg.alloc)
		return series.BuildFloat64DirectWithValidity(name, n, cfg.alloc, func(out []float64) {
			castInt32ToFloat64(out, raw)
		}, nullBuf, arr.NullN())
	case *array.Int64:
		raw := x.Int64Values()
		nullBuf := series.CopyValidityBitmap(arr, cfg.alloc)
		return series.BuildFloat64DirectWithValidity(name, n, cfg.alloc, func(out []float64) {
			castInt64ToFloat64(out, raw)
		}, nullBuf, arr.NullN())
	case *array.Uint64:
		raw := x.Uint64Values()
		nullBuf := series.CopyValidityBitmap(arr, cfg.alloc)
		return series.BuildFloat64DirectWithValidity(name, n, cfg.alloc, func(out []float64) {
			castUint64ToFloat64(out, raw)
		}, nullBuf, arr.NullN())
	case *array.Float32:
		raw := x.Float32Values()
		nullBuf := series.CopyValidityBitmap(arr, cfg.alloc)
		return series.BuildFloat64DirectWithValidity(name, n, cfg.alloc, func(out []float64) {
			castFloat32ToFloat64(out, raw)
		}, nullBuf, arr.NullN())
	case *array.Float64:
		raw := x.Float64Values()
		nullBuf := series.CopyValidityBitmap(arr, cfg.alloc)
		return series.BuildFloat64DirectWithValidity(name, n, cfg.alloc, func(out []float64) {
			copy(out, raw)
		}, nullBuf, arr.NullN())
	}
	// String / Bool paths can mint nulls (parse failures), so they
	// keep the []bool path.
	out := make([]float64, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Boolean:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			if x.Value(i) {
				out[i] = 1
			}
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseFloat(x.Value(i), 64)
			if err != nil {
				continue
			}
			out[i] = v
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> f64", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromFloat64(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToFloat32(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]float32, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = float32(raw[i])
			valid[i] = true
		}
	case *array.Float64:
		raw := x.Float64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = float32(raw[i])
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseFloat(x.Value(i), 32)
			if err != nil {
				continue
			}
			out[i] = float32(v)
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> f32", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromFloat32(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToBool(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]bool, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = raw[i] != 0
			valid[i] = true
		}
	case *array.Float64:
		raw := x.Float64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v := raw[i]
			out[i] = v != 0 && v == v // NaN -> false, but keep valid
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			v, err := strconv.ParseBool(x.Value(i))
			if err != nil {
				continue
			}
			out[i] = v
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> bool", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromBool(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

func castToString(name string, arr arrow.Array, cfg config) (*series.Series, error) {
	n := arr.Len()
	out := make([]string, n)
	valid := make([]bool, n)
	switch x := arr.(type) {
	case *array.Int32:
		raw := x.Int32Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatInt(int64(raw[i]), 10)
			valid[i] = true
		}
	case *array.Int64:
		raw := x.Int64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatInt(raw[i], 10)
			valid[i] = true
		}
	case *array.Uint64:
		raw := x.Uint64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatUint(raw[i], 10)
			valid[i] = true
		}
	case *array.Float64:
		raw := x.Float64Values()
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = strconv.FormatFloat(raw[i], 'g', -1, 64)
			valid[i] = true
		}
	case *array.Boolean:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			if x.Value(i) {
				out[i] = "true"
			} else {
				out[i] = "false"
			}
			valid[i] = true
		}
	case *array.String:
		for i := range out {
			if arr.IsNull(i) {
				continue
			}
			out[i] = x.Value(i)
			valid[i] = true
		}
	default:
		return nil, fmt.Errorf("%w: cast %s -> str", ErrUnsupportedDType, arr.DataType())
	}
	return series.FromString(name, out, compactValid(valid), series.WithAllocator(cfg.alloc))
}

// castInt64ToFloat64 is the specialized int64→float64 cast. Bypasses the
// generic fastCastFill so the compiler can inline the CVTSI2SD conversion
// directly into the tight loop. fastCastFill uses a closure for genericity,
// which blocks inlining and was measured at 0.7× bandwidth.
func castInt64ToFloat64(out []float64, src []int64) {
	n := len(out)
	if n < 128*1024 {
		if hasCastI64ToF64AVX2 {
			simdCastInt64ToFloat64AVX2(out, src)
			return
		}
		for i := range out {
			out[i] = float64(src[i])
		}
		return
	}
	k := min(8, runtime.GOMAXPROCS(0))
	chunkSize := (n + k - 1) / k
	var wg sync.WaitGroup
	// Large-N path: AVX2 uses streaming stores (VMOVNTPD) to bypass
	// write-allocate; ARM64 uses SCVTF.2D. Both measured to win over
	// autovec once the output no longer fits in L2.
	if n >= castNTThreshold {
		for w := range k {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				start := w * chunkSize
				end := min(start+chunkSize, n)
				simdCastInt64ToFloat64NT(out[start:end], src[start:end])
			}(w)
		}
		wg.Wait()
		return
	}
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := w * chunkSize
			end := min(start+chunkSize, n)
			if start < end {
				simdCastInt64ToFloat64AVX2(out[start:end], src[start:end])
			}
		}(w)
	}
	wg.Wait()
}

func castInt32ToFloat64(out []float64, src []int32) {
	n := len(out)
	if n < 128*1024 {
		for i := range out {
			out[i] = float64(src[i])
		}
		return
	}
	k := min(8, runtime.GOMAXPROCS(0))
	chunkSize := (n + k - 1) / k
	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := w * chunkSize
			end := min(start+chunkSize, n)
			for i := start; i < end; i++ {
				out[i] = float64(src[i])
			}
		}(w)
	}
	wg.Wait()
}

func castUint64ToFloat64(out []float64, src []uint64) {
	n := len(out)
	if n < 128*1024 {
		for i := range out {
			out[i] = float64(src[i])
		}
		return
	}
	k := min(8, runtime.GOMAXPROCS(0))
	chunkSize := (n + k - 1) / k
	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := w * chunkSize
			end := min(start+chunkSize, n)
			for i := start; i < end; i++ {
				out[i] = float64(src[i])
			}
		}(w)
	}
	wg.Wait()
}

func castFloat32ToFloat64(out []float64, src []float32) {
	n := len(out)
	if n < 128*1024 {
		for i := range out {
			out[i] = float64(src[i])
		}
		return
	}
	k := min(8, runtime.GOMAXPROCS(0))
	chunkSize := (n + k - 1) / k
	var wg sync.WaitGroup
	for w := range k {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := w * chunkSize
			end := min(start+chunkSize, n)
			for i := start; i < end; i++ {
				out[i] = float64(src[i])
			}
		}(w)
	}
	wg.Wait()
}

// compactValid returns nil when every entry is true, so the output Series
// is built without a validity bitmap.
func compactValid(v []bool) []bool {
	for _, ok := range v {
		if !ok {
			return v
		}
	}
	return nil
}
