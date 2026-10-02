package compute

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

// castScalar is one source value read at its exact width.
type castScalar struct {
	kind byte // 'i' signed, 'u' unsigned, 'f' float, 'b' bool
	i    int64
	u    uint64
	f    float64
}

func isGenericCastable(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.BOOL:
		return true
	}
	return false
}

func readCastScalar(arr arrow.Array, i int) castScalar {
	switch a := arr.(type) {
	case *array.Int8:
		return castScalar{kind: 'i', i: int64(a.Value(i))}
	case *array.Int16:
		return castScalar{kind: 'i', i: int64(a.Value(i))}
	case *array.Int32:
		return castScalar{kind: 'i', i: int64(a.Value(i))}
	case *array.Int64:
		return castScalar{kind: 'i', i: a.Value(i)}
	case *array.Uint8:
		return castScalar{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint16:
		return castScalar{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint32:
		return castScalar{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint64:
		return castScalar{kind: 'u', u: a.Value(i)}
	case *array.Float32:
		return castScalar{kind: 'f', f: float64(a.Value(i))}
	case *array.Float64:
		return castScalar{kind: 'f', f: a.Value(i)}
	case *array.Boolean:
		if a.Value(i) {
			return castScalar{kind: 'b', i: 1}
		}
		return castScalar{kind: 'b'}
	}
	return castScalar{}
}

// toSigned converts v to an int64 in [lo, hi]; ok is false when the
// value does not fit (NaN, infinities and out of range floats included).
// Floats truncate toward zero, as in polars.
func (v castScalar) toSigned(lo, hi int64) (int64, bool) {
	switch v.kind {
	case 'u':
		if v.u > uint64(hi) {
			return 0, false
		}
		return int64(v.u), true
	case 'f':
		f := math.Trunc(v.f)
		if f != f || f < float64(lo) || f >= -float64(lo) {
			return 0, false
		}
		x := int64(f)
		return x, x >= lo && x <= hi
	}
	return v.i, v.i >= lo && v.i <= hi
}

func (v castScalar) toUnsigned(hi uint64) (uint64, bool) {
	switch v.kind {
	case 'u':
		return v.u, v.u <= hi
	case 'f':
		f := math.Trunc(v.f)
		if f != f || f < 0 || f >= 18446744073709551616.0 {
			return 0, false
		}
		x := uint64(f)
		return x, x <= hi
	}
	if v.i < 0 {
		return 0, false
	}
	return uint64(v.i), uint64(v.i) <= hi
}

func (v castScalar) toFloat() float64 {
	switch v.kind {
	case 'u':
		return float64(v.u)
	case 'f':
		return v.f
	}
	return float64(v.i)
}

func (v castScalar) toBool() bool {
	switch v.kind {
	case 'u':
		return v.u != 0
	case 'f':
		return v.f != 0
	}
	return v.i != 0
}

// castGeneric covers the numeric and boolean cast pairs the typed
// kernels in cast.go do not handle (for example u32 to f64 or bool to
// u64). It is a plain per-value loop; the hot pairs keep their fast
// kernels. Values that do not fit the target become null, matching the
// other golars cast kernels. ok is false when the pair is not handled
// here either.
func castGeneric(arr arrow.Array, to dtype.DType, name string, cfg config) (*series.Series, bool, error) {
	from := arr.DataType().ID()
	if !isGenericCastable(from) {
		return nil, false, nil
	}
	if to.ID() == arrow.STRING {
		return castGenericString(arr, name, cfg)
	}
	if !isGenericCastable(to.ID()) {
		return nil, false, nil
	}
	b := array.NewBuilder(cfg.alloc, to.Arrow())
	defer b.Release()
	b.Reserve(arr.Len())
	for i := range arr.Len() {
		if arr.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := readCastScalar(arr, i)
		ok := true
		switch ob := b.(type) {
		case *array.Int8Builder:
			var x int64
			if x, ok = v.toSigned(math.MinInt8, math.MaxInt8); ok {
				ob.Append(int8(x))
			}
		case *array.Int16Builder:
			var x int64
			if x, ok = v.toSigned(math.MinInt16, math.MaxInt16); ok {
				ob.Append(int16(x))
			}
		case *array.Int32Builder:
			var x int64
			if x, ok = v.toSigned(math.MinInt32, math.MaxInt32); ok {
				ob.Append(int32(x))
			}
		case *array.Int64Builder:
			var x int64
			if x, ok = v.toSigned(math.MinInt64, math.MaxInt64); ok {
				ob.Append(x)
			}
		case *array.Uint8Builder:
			var x uint64
			if x, ok = v.toUnsigned(math.MaxUint8); ok {
				ob.Append(uint8(x))
			}
		case *array.Uint16Builder:
			var x uint64
			if x, ok = v.toUnsigned(math.MaxUint16); ok {
				ob.Append(uint16(x))
			}
		case *array.Uint32Builder:
			var x uint64
			if x, ok = v.toUnsigned(math.MaxUint32); ok {
				ob.Append(uint32(x))
			}
		case *array.Uint64Builder:
			var x uint64
			if x, ok = v.toUnsigned(math.MaxUint64); ok {
				ob.Append(x)
			}
		case *array.Float32Builder:
			ob.Append(float32(v.toFloat()))
		case *array.Float64Builder:
			ob.Append(v.toFloat())
		case *array.BooleanBuilder:
			ob.Append(v.toBool())
		}
		if !ok {
			b.AppendNull()
		}
	}
	out, err := series.New(name, b.NewArray())
	return out, true, err
}

func castGenericString(arr arrow.Array, name string, cfg config) (*series.Series, bool, error) {
	b := array.NewStringBuilder(cfg.alloc)
	defer b.Release()
	b.Reserve(arr.Len())
	_, f32 := arr.(*array.Float32)
	for i := range arr.Len() {
		if arr.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := readCastScalar(arr, i)
		switch v.kind {
		case 'i':
			b.Append(strconv.FormatInt(v.i, 10))
		case 'u':
			b.Append(strconv.FormatUint(v.u, 10))
		case 'b':
			b.Append(strconv.FormatBool(v.i != 0))
		case 'f':
			if f32 {
				b.Append(FormatFloatPolars(v.f, 32))
			} else {
				b.Append(FormatFloatPolars(v.f, 64))
			}
		}
	}
	out, err := series.New(name, b.NewArray())
	return out, true, err
}

// rescaleChecked multiplies the physical int64 values of s by factor
// and stores them as dtype to (an int64 backed temporal type). Products
// that overflow int64 become null, as in polars' non-strict cast;
// wrapping would silently produce a wrong timestamp.
func rescaleChecked(s *series.Series, to dtype.DType, mem memory.Allocator, factor int64) (*series.Series, error) {
	arr, err := extractChunk(s, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	vals := physicalValues(arr)
	if vals == nil && arr.Len() > 0 {
		return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, s.DType(), to)
	}
	lo, hi := int64(math.MinInt64)/factor, int64(math.MaxInt64)/factor
	b := array.NewBuilder(mem, to.Arrow())
	defer b.Release()
	b.Reserve(arr.Len())
	for i, v := range vals {
		if arr.IsNull(i) || v < lo || v > hi {
			b.AppendNull()
			continue
		}
		switch tb := b.(type) {
		case *array.TimestampBuilder:
			tb.Append(arrow.Timestamp(v * factor))
		case *array.DurationBuilder:
			tb.Append(arrow.Duration(v * factor))
		case *array.Time64Builder:
			tb.Append(arrow.Time64(v * factor))
		case *array.Int64Builder:
			tb.Append(v * factor)
		default:
			return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, s.DType(), to)
		}
	}
	return series.New(s.Name(), b.NewArray())
}

// viaInt64 casts s to i64 and reinterprets the result as the temporal
// dtype to.
func viaInt64(ctx context.Context, s *series.Series, to dtype.DType, cfg config) (*series.Series, error) {
	wide, err := Cast(ctx, s, dtype.Int64(), WithAllocator(cfg.alloc))
	if err != nil {
		return nil, err
	}
	defer wide.Release()
	return reinterpretPhysical(wide, to, cfg.alloc, nil)
}

// upscaleFactor is the multiplier from unit from to the finer unit to,
// or 1 when to is not finer.
func upscaleFactor(from, to dtype.TimeUnit) int64 {
	fp, tp := temporal.UnitsPerSecond(from), temporal.UnitsPerSecond(to)
	if tp > fp {
		return tp / fp
	}
	return 1
}
