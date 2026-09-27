package series

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// temporalKind classifies the four polars temporal dtypes.
type temporalKind int8

const (
	tkNone temporalKind = iota
	tkDate
	tkDatetime
	tkDuration
	tkTime
)

// tinfo describes a temporal dtype: its kind, resolution and zone.
type tinfo struct {
	kind temporalKind
	unit dtype.TimeUnit
	tz   string
}

func temporalInfoOf(dt dtype.DType) tinfo {
	switch dt.ID() {
	case arrow.DATE32:
		return tinfo{kind: tkDate, unit: dtype.Millisecond}
	case arrow.TIMESTAMP:
		u, _ := dt.TimeUnit()
		return tinfo{kind: tkDatetime, unit: u, tz: dt.TimeZone()}
	case arrow.DURATION:
		u, _ := dt.TimeUnit()
		return tinfo{kind: tkDuration, unit: u}
	case arrow.TIME32, arrow.TIME64:
		u, _ := dt.TimeUnit()
		return tinfo{kind: tkTime, unit: u}
	}
	return tinfo{}
}

func (t tinfo) dtype() dtype.DType {
	switch t.kind {
	case tkDate:
		return dtype.Date()
	case tkDatetime:
		return dtype.Datetime(t.unit, t.tz)
	case tkDuration:
		return dtype.Duration(t.unit)
	case tkTime:
		return dtype.Time(t.unit)
	}
	return dtype.Null()
}

// fixedNum is the set of physical element types the temporal builders
// write directly into arrow buffers.
type fixedNum interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint32 | ~float64
}

// parallelCutoff is the row count above which temporal kernels split
// their work across goroutines.
const temporalParallelCutoff = 1 << 15

// forRanges runs fn over [0, n) split into byte-aligned chunks so
// concurrent writers never touch the same validity byte.
func forRanges(n int, fn func(start, end int) error) error {
	if n < temporalParallelCutoff {
		return fn(0, n)
	}
	par := pool.DefaultParallelism()
	chunks := min(par*4, (n+8191)/8192)
	per := ((n+chunks-1)/chunks + 63) &^ 63
	blocks := (n + per - 1) / per
	return pool.ParallelFor(context.Background(), blocks, par, func(_ context.Context, s, e int) error {
		for b := s; b < e; b++ {
			lo := b * per
			hi := min(lo+per, n)
			if err := fn(lo, hi); err != nil {
				return err
			}
		}
		return nil
	})
}

// newValidity returns a writable validity bitmap of n bits initialised
// from src (all valid when src is nil or has no nulls).
func newValidity(n int, src arrow.Array, mem memory.Allocator) *memory.Buffer {
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	b := buf.Bytes()
	if src != nil && src.NullN() > 0 {
		copyBitmapInto(b, src, n)
		return buf
	}
	for i := range b {
		b[i] = 0xFF
	}
	return buf
}

// finishValidity counts the nulls in buf and drops it when there are
// none. It returns the buffer to hand to arrow (possibly nil).
func finishValidity(buf *memory.Buffer, n int) (*memory.Buffer, int) {
	nulls := n - bitutil.CountSetBits(buf.Bytes(), 0, n)
	if nulls == 0 {
		buf.Release()
		return nil, 0
	}
	return buf, nulls
}

// buildFixed allocates an n-element buffer of T, lets fill write the
// values and clear validity bits, and wraps the result as a dt array.
// src (optional) seeds the validity bitmap.
func buildFixed[T fixedNum](name string, n int, mem memory.Allocator, dt arrow.DataType, src arrow.Array, fill func(out []T, valid []byte) error) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var zero T
	size := int(unsafe.Sizeof(zero))
	data := memory.NewResizableBuffer(mem)
	data.Resize(n * size)
	defer data.Release()
	valid := newValidity(n, src, mem)
	if n > 0 {
		out := unsafe.Slice((*T)(unsafe.Pointer(&data.Bytes()[0])), n)
		if err := fill(out, valid.Bytes()); err != nil {
			valid.Release()
			return nil, err
		}
	}
	vb, nulls := finishValidity(valid, n)
	if vb != nil {
		defer vb.Release()
	}
	ad := array.NewData(dt, n, []*memory.Buffer{vb, data}, nil, nulls, 0)
	defer ad.Release()
	return New(name, array.MakeFromData(ad))
}

// buildBool is buildFixed for boolean outputs: fill sets value bits.
func buildBool(name string, n int, mem memory.Allocator, src arrow.Array, fill func(bits, valid []byte) error) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	data := memory.NewResizableBuffer(mem)
	data.Resize(int(bitutil.BytesForBits(int64(n))))
	defer data.Release()
	bits := data.Bytes()
	for i := range bits {
		bits[i] = 0
	}
	valid := newValidity(n, src, mem)
	if err := fill(bits, valid.Bytes()); err != nil {
		valid.Release()
		return nil, err
	}
	vb, nulls := finishValidity(valid, n)
	if vb != nil {
		defer vb.Release()
	}
	ad := array.NewData(arrow.FixedWidthTypes.Boolean, n, []*memory.Buffer{vb, data}, nil, nulls, 0)
	defer ad.Release()
	return New(name, array.MakeFromData(ad))
}

// physicalInt64 returns the int64 physical values of a temporal or
// integer chunk, widening int32-backed arrays (Date32, Time32) into a
// fresh slice.
func physicalInt64(arr arrow.Array) []int64 {
	switch a := arr.(type) {
	case *array.Timestamp:
		return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(a.TimestampValues()))), a.Len())
	case *array.Duration:
		return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(a.DurationValues()))), a.Len())
	case *array.Time64:
		return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(a.Time64Values()))), a.Len())
	case *array.Date64:
		return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(a.Date64Values()))), a.Len())
	case *array.Int64:
		return a.Int64Values()
	case *array.Date32:
		src := a.Date32Values()
		out := make([]int64, len(src))
		for i, v := range src {
			out[i] = int64(v)
		}
		return out
	case *array.Time32:
		src := a.Time32Values()
		out := make([]int64, len(src))
		for i, v := range src {
			out[i] = int64(v)
		}
		return out
	case *array.Int32:
		return widenToInt64(a.Int32Values())
	case *array.Int16:
		return widenToInt64(a.Int16Values())
	case *array.Int8:
		return widenToInt64(a.Int8Values())
	case *array.Uint8:
		return widenToInt64(a.Uint8Values())
	case *array.Uint16:
		return widenToInt64(a.Uint16Values())
	case *array.Uint32:
		return widenToInt64(a.Uint32Values())
	case *array.Uint64:
		return widenToInt64(a.Uint64Values())
	}
	return nil
}

func widenToInt64[T ~int8 | ~int16 | ~int32 | ~uint8 | ~uint16 | ~uint32 | ~uint64](src []T) []int64 {
	out := make([]int64, len(src))
	for i, v := range src {
		out[i] = int64(v)
	}
	return out
}

// date32Values returns the day numbers of a Date32 chunk.
func date32Values(arr arrow.Array) []int32 {
	a := arr.(*array.Date32)
	return unsafe.Slice((*int32)(unsafe.Pointer(unsafe.SliceData(a.Date32Values()))), a.Len())
}

func isValidBit(valid []byte, i int) bool { return bitutil.BitIsSet(valid, i) }

func clearValid(valid []byte, i int) { bitutil.ClearBit(valid, i) }

// zoneFor returns a per-goroutine zone cache for tz (nil for naive/UTC).
func zoneFor(tz string) (*temporal.Zone, error) { return temporal.NewZone(tz) }

// errTemporalOp is the error polars raises when a dt op meets a dtype
// it does not support.
func errTemporalOp(op string, dt dtype.DType) error {
	return fmt.Errorf("`%s` operation not supported for dtype `%s`", op, dt)
}

// timeOfDayNs returns nanoseconds since midnight for a Time value.
func timeOfDayNs(v int64, u dtype.TimeUnit) int64 { return v * temporal.NsPerUnit(u) }
