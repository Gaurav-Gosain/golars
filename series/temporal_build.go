package series

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
)

// BuildTypedInt64WithValidity builds an int64-backed array of dtype dt
// (Int64, Datetime, Duration, Time64) from fill and a caller-supplied
// validity bitmap, which it consumes (nil means no nulls).
func BuildTypedInt64WithValidity(name string, n int, mem memory.Allocator, dt arrow.DataType,
	fill func(out []int64), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	return buildTypedWithValidity(name, n, mem, dt, fill, nullBuf, nullCount)
}

// BuildTypedInt32WithValidity is the int32-backed sibling (Date32).
func BuildTypedInt32WithValidity(name string, n int, mem memory.Allocator, dt arrow.DataType,
	fill func(out []int32), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	return buildTypedWithValidity(name, n, mem, dt, fill, nullBuf, nullCount)
}

func buildTypedWithValidity[T fixedNum](name string, n int, mem memory.Allocator, dt arrow.DataType,
	fill func(out []T), nullBuf *memory.Buffer, nullCount int,
) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var zero T
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(n * int(unsafe.Sizeof(zero)))
	defer buf.Release()
	if n > 0 {
		fill(unsafe.Slice((*T)(unsafe.Pointer(&buf.Bytes()[0])), n))
	}
	if nullCount == 0 && nullBuf != nil {
		nullBuf.Release()
		nullBuf = nil
	}
	data := array.NewData(dt, n, []*memory.Buffer{nullBuf, buf}, nil, nullCount, 0)
	if nullBuf != nil {
		nullBuf.Release()
	}
	defer data.Release()
	return New(name, array.MakeFromData(data))
}

// TemporalScalar is a typed temporal literal: Value is the physical
// value of DType (days for Date, ticks of the unit otherwise). Null
// marks a typed null literal.
type TemporalScalar struct {
	DType dtype.DType
	Value int64
	Null  bool
	// FromGoTime marks literals built from a Go time.Time. They carry
	// an absolute instant, so comparing them with a zoned column uses
	// the instant instead of raising a time zone mismatch.
	FromGoTime bool
	Instant    int64 // UTC instant in the DType unit when FromGoTime
}

// Series materializes the scalar as a length-n Series.
func (t TemporalScalar) Series(name string, n int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	fillValid := func(vb []byte) {
		if t.Null {
			for i := range vb {
				vb[i] = 0
			}
		}
	}
	if t.DType.IsDate() || t.DType.ID() == arrow.TIME32 {
		return buildFixed(name, n, cfg.alloc, t.DType.Arrow(), nil, func(out []int32, vb []byte) error {
			for i := range out {
				out[i] = int32(t.Value)
			}
			fillValid(vb)
			return nil
		})
	}
	return buildFixed(name, n, cfg.alloc, t.DType.Arrow(), nil, func(out []int64, vb []byte) error {
		for i := range out {
			out[i] = t.Value
		}
		fillValid(vb)
		return nil
	})
}

// ScalarFromTime converts a Go time to a naive Datetime literal. The
// unit is microseconds unless the value has sub-microsecond precision.
func ScalarFromTime(t time.Time) TemporalScalar {
	unit := dtype.Microsecond
	if t.Nanosecond()%1000 != 0 {
		unit = dtype.Nanosecond
	}
	return TemporalScalar{
		DType:      dtype.Datetime(unit, ""),
		Value:      TimeToTicks(t, unit, true),
		FromGoTime: true,
		Instant:    TimeToTicks(t, unit, false),
	}
}

// ScalarFromDuration converts a Go duration to a Duration literal
// (microseconds unless it has sub-microsecond precision).
func ScalarFromDuration(d time.Duration) TemporalScalar {
	if d%time.Microsecond != 0 {
		return TemporalScalar{DType: dtype.Duration(dtype.Nanosecond), Value: int64(d)}
	}
	return TemporalScalar{DType: dtype.Duration(dtype.Microsecond), Value: int64(d / time.Microsecond)}
}

// ScalarDate returns a Date literal for the calendar day.
func ScalarDate(year, month, day int) (TemporalScalar, error) {
	if !temporal.ValidDate(int64(year), month, day) {
		return TemporalScalar{}, fmt.Errorf("invalid date components (%d, %d, %d) supplied", year, month, day)
	}
	return TemporalScalar{DType: dtype.Date(), Value: temporal.DaysFromCivil(int64(year), month, day)}, nil
}

// ToTemporalScalar converts supported Go literal values (time.Time,
// time.Duration, TemporalScalar) to a TemporalScalar.
func ToTemporalScalar(v any) (TemporalScalar, bool) {
	switch x := v.(type) {
	case TemporalScalar:
		return x, true
	case *TemporalScalar:
		return *x, true
	case time.Time:
		return ScalarFromTime(x), true
	case time.Duration:
		return ScalarFromDuration(x), true
	}
	return TemporalScalar{}, false
}
