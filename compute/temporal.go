package compute

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/pool"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

// Temporal arithmetic and comparison. polars' rules, which these
// kernels follow:
//
//   - datetime - datetime  -> duration in the coarser unit
//   - datetime +- duration -> datetime in the coarser unit, zone kept
//   - date +- duration     -> date (floored)
//   - date - date          -> duration[us]
//   - duration +- duration -> duration in the coarser unit
//   - duration * int|float, duration / int|float, duration / duration
//   - comparisons cast both sides to their supertype (coarser unit)
//
// Converting a datetime to a coarser unit floors; a duration truncates.

type tkind int8

const (
	kNone tkind = iota
	kDate
	kDatetime
	kDuration
	kTime
	kInt
	kFloat
)

// operand is one side of a temporal binary op: its physical values as
// int64 (or float64 for kFloat), length n or 1 (broadcast).
type operand struct {
	kind  tkind
	unit  dtype.TimeUnit
	tz    string
	arr   arrow.Array
	ints  []int64
	flts  []float64
	bcast bool
	// instant is set for literals built from Go time values; it holds
	// the UTC instant used when comparing against zoned columns.
	fromGo  bool
	instant int64
}

func (o operand) at(i int) int64 {
	if o.bcast {
		return o.ints[0]
	}
	return o.ints[i]
}

func (o operand) fat(i int) float64 {
	if o.bcast {
		return o.flts[0]
	}
	return o.flts[i]
}

func kindOf(dt dtype.DType) tkind {
	switch {
	case dt.IsDate():
		return kDate
	case dt.IsDatetime():
		return kDatetime
	case dt.IsDuration():
		return kDuration
	case dt.IsTime():
		return kTime
	case dt.IsInteger():
		return kInt
	case dt.IsFloating():
		return kFloat
	}
	return kNone
}

func isTemporalKind(k tkind) bool { return k >= kDate && k <= kTime }

func newOperand(arr arrow.Array, dt dtype.DType, bcast bool) (operand, error) {
	o := operand{kind: kindOf(dt), arr: arr, bcast: bcast, tz: dt.TimeZone()}
	if u, ok := dt.TimeUnit(); ok {
		o.unit = u
	}
	switch o.kind {
	case kFloat:
		switch a := arr.(type) {
		case *array.Float64:
			o.flts = a.Float64Values()
		case *array.Float32:
			src := a.Float32Values()
			o.flts = make([]float64, len(src))
			for i, v := range src {
				o.flts[i] = float64(v)
			}
		}
	case kNone:
		return o, fmt.Errorf("%w: %s", ErrUnsupportedDType, dt)
	default:
		o.ints = physicalValues(arr)
		if o.ints == nil && arr.Len() > 0 {
			return o, fmt.Errorf("%w: %s", ErrUnsupportedDType, dt)
		}
	}
	return o, nil
}

// physicalValues returns the values of an integer-backed array as
// int64, zero-copy for int64-backed types.
func physicalValues(arr arrow.Array) []int64 {
	switch a := arr.(type) {
	case *array.Int64:
		return a.Int64Values()
	case *array.Timestamp:
		v := a.TimestampValues()
		return unsafeInt64s(v)
	case *array.Duration:
		return unsafeInt64s(a.DurationValues())
	case *array.Time64:
		return unsafeInt64s(a.Time64Values())
	case *array.Date64:
		return unsafeInt64s(a.Date64Values())
	case *array.Date32:
		return widenInts(a.Date32Values())
	case *array.Time32:
		return widenInts(a.Time32Values())
	case *array.Int32:
		return widenInts(a.Int32Values())
	case *array.Int16:
		return widenInts(a.Int16Values())
	case *array.Int8:
		return widenInts(a.Int8Values())
	case *array.Uint8:
		return widenInts(a.Uint8Values())
	case *array.Uint16:
		return widenInts(a.Uint16Values())
	case *array.Uint32:
		return widenInts(a.Uint32Values())
	case *array.Uint64:
		return widenInts(a.Uint64Values())
	}
	return nil
}

func widenInts[T ~int8 | ~int16 | ~int32 | ~uint8 | ~uint16 | ~uint32 | ~uint64](src []T) []int64 {
	out := make([]int64, len(src))
	for i, v := range src {
		out[i] = int64(v)
	}
	return out
}

// rescale converts o's values to unit u (date days become ticks).
func (o operand) rescale(u dtype.TimeUnit) []int64 {
	switch {
	case o.kind == kDate:
		per := temporal.UnitsPerDay(u)
		out := make([]int64, len(o.ints))
		for i, v := range o.ints {
			out[i] = v * per
		}
		return out
	case o.unit == u:
		return o.ints
	}
	floor := o.kind == kDatetime
	out := make([]int64, len(o.ints))
	for i, v := range o.ints {
		out[i] = temporal.ConvertUnit(v, o.unit, u, floor)
	}
	return out
}

// binaryValidity returns the AND of both operands' validity, handling
// broadcast scalars. The buffer (nil when no nulls) is owned by caller.
func binaryValidity(a, b operand, n int, mem memory.Allocator) (*memory.Buffer, int) {
	allNull := func() (*memory.Buffer, int) {
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(int(bitutil.BytesForBits(int64(n))))
		for i := range buf.Bytes() {
			buf.Bytes()[i] = 0
		}
		return buf, n
	}
	switch {
	case a.bcast && b.bcast:
		if a.arr.IsNull(0) || b.arr.IsNull(0) {
			return allNull()
		}
		return nil, 0
	case a.bcast:
		if a.arr.IsNull(0) {
			return allNull()
		}
		return series.CopyValidityBitmap(b.arr, mem), b.arr.NullN()
	case b.bcast:
		if b.arr.IsNull(0) {
			return allNull()
		}
		return series.CopyValidityBitmap(a.arr, mem), a.arr.NullN()
	}
	return series.AndValidityBitmap(a.arr, b.arr, mem)
}

func opSymbol(op arithOp) string {
	switch op {
	case opAdd:
		return "+"
	case opSub:
		return "-"
	case opMul:
		return "*"
	}
	return "/"
}

// parallelFill runs fn over [0, n) in parallel for large n.
func parallelFill(ctx context.Context, n int, fn func(lo, hi int)) {
	if n < minParallelRows {
		fn(0, n)
		return
	}
	_ = pool.ParallelFor(ctx, n, 0, func(_ context.Context, lo, hi int) error {
		fn(lo, hi)
		return nil
	})
}

// prepareBinary turns (a, b) into operands, broadcasting length-1 sides.
func prepareBinary(a, b *series.Series, mem memory.Allocator) (operand, operand, int, func(), error) {
	aArr, err := extractChunk(a, mem)
	if err != nil {
		return operand{}, operand{}, 0, nil, err
	}
	bArr, err := extractChunk(b, mem)
	if err != nil {
		aArr.Release()
		return operand{}, operand{}, 0, nil, err
	}
	release := func() { aArr.Release(); bArr.Release() }
	n := a.Len()
	aB, bB := false, false
	switch {
	case a.Len() == b.Len():
	case b.Len() == 1:
		bB = true
	case a.Len() == 1:
		aB = true
		n = b.Len()
	default:
		release()
		return operand{}, operand{}, 0, nil, fmt.Errorf("%w: %d vs %d", ErrLengthMismatch, a.Len(), b.Len())
	}
	ao, err := newOperand(aArr, a.DType(), aB)
	if err != nil {
		release()
		return operand{}, operand{}, 0, nil, err
	}
	bo, err := newOperand(bArr, b.DType(), bB)
	if err != nil {
		release()
		return operand{}, operand{}, 0, nil, err
	}
	return ao, bo, n, release, nil
}

func involvesTemporal(a, b dtype.DType) bool {
	return a.IsTemporal() || b.IsTemporal()
}

// temporalArith implements Add/Sub/Mul/Div when either side is
// temporal. ok is false when neither side is temporal.
func temporalArith(ctx context.Context, a, b *series.Series, opts []Option, op arithOp) (*series.Series, bool, error) {
	if !involvesTemporal(a.DType(), b.DType()) {
		return nil, false, nil
	}
	cfg := resolve(opts)
	mem := cfg.alloc
	ao, bo, n, release, err := prepareBinary(a, b, mem)
	if err != nil {
		return nil, true, err
	}
	defer release()
	name := cfg.outName(a.Name())
	if ao.bcast && !bo.bcast {
		name = cfg.outName(b.Name())
		if a.Len() == 1 && b.Len() == 1 {
			name = cfg.outName(a.Name())
		}
	}
	out, err := temporalArithOperands(ctx, name, ao, bo, n, op, mem)
	return out, true, err
}

func notAllowed(op arithOp, a, b operand) error {
	return fmt.Errorf("%s not allowed on %s and %s", opSymbol(op), describe(a), describe(b))
}

func describe(o operand) string {
	switch o.kind {
	case kDate:
		return "date"
	case kDatetime:
		return dtype.Datetime(o.unit, o.tz).String()
	case kDuration:
		return dtype.Duration(o.unit).String()
	case kTime:
		return "time"
	case kInt:
		return "int"
	case kFloat:
		return "float"
	}
	return "unknown"
}

func temporalArithOperands(ctx context.Context, name string, a, b operand, n int, op arithOp, mem memory.Allocator) (*series.Series, error) {
	valid, nulls := binaryValidity(a, b, n, mem)
	emitInt := func(dt dtype.DType, av, bv []int64, f func(x, y int64) int64) (*series.Series, error) {
		ab, bb := a.bcast, b.bcast
		return series.BuildTypedInt64WithValidity(name, n, mem, dt.Arrow(), func(out []int64) {
			parallelFill(ctx, n, func(lo, hi int) {
				for i := lo; i < hi; i++ {
					x, y := av[0], bv[0]
					if !ab {
						x = av[i]
					}
					if !bb {
						y = bv[i]
					}
					out[i] = f(x, y)
				}
			})
		}, valid, nulls)
	}
	switch {
	case a.kind == kDatetime && b.kind == kDatetime && op == opSub:
		if a.tz != b.tz {
			valid.Release()
			return nil, fmt.Errorf("failed to determine supertype of %s and %s", describe(a), describe(b))
		}
		u := temporal.CoarserUnit(a.unit, b.unit)
		return emitInt(dtype.Duration(u), a.rescale(u), b.rescale(u), func(x, y int64) int64 { return x - y })
	case op == opSub && ((a.kind == kDatetime && b.kind == kDate) || (a.kind == kDate && b.kind == kDatetime)):
		u := a.unit
		if b.kind == kDatetime {
			u = b.unit
		}
		return emitInt(dtype.Duration(u), a.rescale(u), b.rescale(u), func(x, y int64) int64 { return x - y })
	case a.kind == kDate && b.kind == kDate && op == opSub:
		per := temporal.UnitsPerDay(dtype.Microsecond)
		return emitInt(dtype.Duration(dtype.Microsecond), a.ints, b.ints, func(x, y int64) int64 { return (x - y) * per })
	case (a.kind == kDatetime && b.kind == kDuration && (op == opAdd || op == opSub)) ||
		(a.kind == kDuration && b.kind == kDatetime && op == opAdd):
		dtOp, duOp := a, b
		if a.kind == kDuration {
			dtOp, duOp = b, a
		}
		u := temporal.CoarserUnit(dtOp.unit, duOp.unit)
		av, bv := a.rescale(u), b.rescale(u)
		if op == opSub {
			return emitInt(dtype.Datetime(u, dtOp.tz), av, bv, func(x, y int64) int64 { return x - y })
		}
		return emitInt(dtype.Datetime(u, dtOp.tz), av, bv, func(x, y int64) int64 { return x + y })
	case (a.kind == kDate && b.kind == kDuration && (op == opAdd || op == opSub)) ||
		(a.kind == kDuration && b.kind == kDate && op == opAdd):
		dateOp, duOp := a, b
		if a.kind == kDuration {
			dateOp, duOp = b, a
		}
		// polars computes date +/- duration in datetime[us] or
		// datetime[ms] (a ns duration is first truncated to ms), floors
		// back to days and gives null on overflow. For example
		// date + (-1ns) keeps the date while date + (-1us) is the
		// previous day.
		unit := duOp.unit
		div := int64(1)
		if unit == dtype.Nanosecond {
			unit, div = dtype.Millisecond, 1_000_000
		}
		per := temporal.UnitsPerDay(unit)
		sign := int64(1)
		if op == opSub {
			sign = -1
		}
		dv, uv := dateOp.ints, duOp.ints
		db, ub := dateOp.bcast, duOp.bcast
		vals := make([]int32, n)
		ok := make([]bool, n)
		var vbits []byte
		if valid != nil {
			vbits = valid.Bytes()
			defer valid.Release()
		}
		for i := range n {
			if vbits != nil && !bitutil.BitIsSet(vbits, i) {
				continue
			}
			d, du := dv[0], uv[0]
			if !db {
				d = dv[i]
			}
			if !ub {
				du = uv[i]
			}
			ticks, ok1 := mulOK(d, per)
			sum, ok2 := addOK(ticks, sign*(du/div))
			if !ok1 || !ok2 {
				continue
			}
			r := temporal.FloorDiv(sum, per)
			if r < math.MinInt32 || r > math.MaxInt32 {
				continue
			}
			vals[i], ok[i] = int32(r), true
		}
		b := array.NewDate32Builder(mem)
		defer b.Release()
		for i, v := range vals {
			if ok[i] {
				b.Append(arrow.Date32(v))
			} else {
				b.AppendNull()
			}
		}
		return series.New(name, b.NewArray())
	case a.kind == kDuration && b.kind == kDuration:
		u := temporal.CoarserUnit(a.unit, b.unit)
		if op == opDiv {
			// polars divides in the finer unit.
			u = max(a.unit, b.unit)
		}
		av, bv := a.rescale(u), b.rescale(u)
		switch op {
		case opAdd:
			return emitInt(dtype.Duration(u), av, bv, func(x, y int64) int64 { return x + y })
		case opSub:
			return emitInt(dtype.Duration(u), av, bv, func(x, y int64) int64 { return x - y })
		case opDiv:
			ab, bb := a.bcast, b.bcast
			return series.BuildFloat64DirectWithValidity(name, n, mem, func(out []float64) {
				for i := range out {
					x, y := av[0], bv[0]
					if !ab {
						x = av[i]
					}
					if !bb {
						y = bv[i]
					}
					out[i] = float64(x) / float64(y)
				}
			}, valid, nulls)
		}
	case (a.kind == kDuration && b.kind == kInt && (op == opMul || op == opDiv)) ||
		(a.kind == kInt && b.kind == kDuration && op == opMul):
		du, iv := a, b
		if a.kind == kInt {
			du, iv = b, a
		}
		av, bv := du.ints, iv.ints
		if op == opMul {
			if a.kind == kInt {
				return emitInt(dtype.Duration(du.unit), bv, av, func(x, y int64) int64 { return y * x })
			}
			return emitInt(dtype.Duration(du.unit), av, bv, func(x, y int64) int64 { return x * y })
		}
		// Integer division by zero gives null; floor division otherwise.
		if valid == nil {
			valid, nulls = newAllValid(n, mem)
		}
		vb := valid.Bytes()
		for i := range n {
			y := bv[0]
			if !iv.bcast {
				y = bv[i]
			}
			if y == 0 && bitutil.BitIsSet(vb, i) {
				bitutil.ClearBit(vb, i)
				nulls++
			}
		}
		return emitInt(dtype.Duration(du.unit), av, bv, func(x, y int64) int64 {
			if y == 0 {
				return 0
			}
			return temporal.FloorDiv(x, y)
		})
	case (a.kind == kDuration && b.kind == kFloat && (op == opMul || op == opDiv)) ||
		(a.kind == kFloat && b.kind == kDuration && op == opMul):
		du, fl := a, b
		if a.kind == kFloat {
			du, fl = b, a
		}
		dv := du.ints
		return series.BuildTypedInt64WithValidity(name, n, mem, dtype.Duration(du.unit).Arrow(), func(out []int64) {
			for i := range out {
				x := dv[0]
				if !du.bcast {
					x = dv[i]
				}
				y := fl.fat(i)
				var r float64
				if op == opMul {
					r = float64(x) * y
				} else {
					r = float64(x) / y
				}
				if math.IsNaN(r) || math.IsInf(r, 0) {
					out[i] = 0
					continue
				}
				out[i] = int64(r)
			}
		}, valid, nulls)
	}
	if valid != nil {
		valid.Release()
	}
	return nil, notAllowed(op, a, b)
}

func newAllValid(n int, mem memory.Allocator) (*memory.Buffer, int) {
	buf := memory.NewResizableBuffer(mem)
	buf.Resize(int(bitutil.BytesForBits(int64(n))))
	for i := range buf.Bytes() {
		buf.Bytes()[i] = 0xFF
	}
	return buf, 0
}

// literalToSeries turns a Go literal into a length-1 Series (temporal
// scalars, time.Time, time.Duration, ints and floats).
func literalToSeries(lit any, mem memory.Allocator) (*series.Series, bool, error) {
	if ts, ok := series.ToTemporalScalar(lit); ok {
		s, err := ts.Series("literal", 1, series.WithAllocator(mem))
		return s, true, err
	}
	switch v := lit.(type) {
	case int:
		s, err := series.FromInt64("literal", []int64{int64(v)}, nil, series.WithAllocator(mem))
		return s, false, err
	case int64:
		s, err := series.FromInt64("literal", []int64{v}, nil, series.WithAllocator(mem))
		return s, false, err
	case int32:
		s, err := series.FromInt64("literal", []int64{int64(v)}, nil, series.WithAllocator(mem))
		return s, false, err
	case float64:
		s, err := series.FromFloat64("literal", []float64{v}, nil, series.WithAllocator(mem))
		return s, false, err
	case float32:
		s, err := series.FromFloat64("literal", []float64{float64(v)}, nil, series.WithAllocator(mem))
		return s, false, err
	}
	return nil, false, fmt.Errorf("compute: unsupported literal %T", lit)
}

// temporalArithLit handles `a OP lit` when a or lit is temporal.
func temporalArithLit(ctx context.Context, a *series.Series, lit any, opts []Option, op arithOp) (*series.Series, bool, error) {
	_, litTemporal := series.ToTemporalScalar(lit)
	if !a.DType().IsTemporal() && !litTemporal {
		return nil, false, nil
	}
	cfg := resolve(opts)
	ls, _, err := literalToSeries(lit, cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer ls.Release()
	out, _, err := temporalArith(ctx, a, ls, append(opts, WithName(a.Name())), op)
	return out, true, err
}

// temporalCompare handles comparisons where either side is temporal.
func temporalCompare(ctx context.Context, a, b *series.Series, opts []Option, op compareOp, bInstant *series.TemporalScalar) (*series.Series, bool, error) {
	if !involvesTemporal(a.DType(), b.DType()) {
		return nil, false, nil
	}
	cfg := resolve(opts)
	if bInstant == nil {
		if out, ok, err := dateCompareScalar(a, b, &cfg, op); ok {
			return out, true, err
		}
		if out, ok, err := dateComparePair(a, b, &cfg, op); ok {
			return out, true, err
		}
	}
	mem := cfg.alloc
	ao, bo, n, release, err := prepareBinary(a, b, mem)
	if err != nil {
		return nil, true, err
	}
	defer release()
	if bInstant != nil {
		bo.fromGo, bo.instant = true, bInstant.Instant
	}
	name := cfg.outName(a.Name())
	av, bv, err := compareCommon(ao, bo)
	if err != nil {
		return nil, true, err
	}
	valid, nulls := binaryValidity(ao, bo, n, mem)
	pred := orderedPredicate[int64](op)
	ab, bb := ao.bcast, bo.bcast
	out, err := series.BuildBoolDirectWithValidity(name, n, mem, func(bits []byte) {
		for i := range n {
			x, y := av[0], bv[0]
			if !ab {
				x = av[i]
			}
			if !bb {
				y = bv[i]
			}
			if pred(x, y) {
				bits[i>>3] |= 1 << uint(i&7)
			}
		}
	}, valid, nulls)
	return out, true, err
}

// compareCommon casts both operands to their comparison supertype.
func compareCommon(a, b operand) ([]int64, []int64, error) {
	mismatch := func() error {
		return fmt.Errorf("cannot compare %s with %s", describe(a), describe(b))
	}
	switch {
	case a.kind == kDatetime && b.kind == kDatetime:
		if a.tz != b.tz {
			if b.fromGo && b.bcast {
				// A Go time literal is an absolute instant: compare it with
				// the zoned column physically.
				u := temporal.CoarserUnit(a.unit, b.unit)
				return a.rescale(u), []int64{temporal.ConvertUnit(b.instant, b.unit, u, true)}, nil
			}
			return nil, nil, fmt.Errorf("could not evaluate comparison between %s and %s", describe(a), describe(b))
		}
		u := temporal.CoarserUnit(a.unit, b.unit)
		return a.rescale(u), b.rescale(u), nil
	case a.kind == kDatetime && b.kind == kDate:
		return a.ints, b.rescale(a.unit), nil
	case a.kind == kDate && b.kind == kDatetime:
		return a.rescale(b.unit), b.ints, nil
	case a.kind == kDate && b.kind == kDate:
		return a.ints, b.ints, nil
	case a.kind == kDuration && b.kind == kDuration:
		u := temporal.CoarserUnit(a.unit, b.unit)
		return a.rescale(u), b.rescale(u), nil
	case a.kind == kTime && b.kind == kTime:
		u := a.unit
		if b.unit > u {
			u = b.unit
		}
		return a.rescale(u), b.rescale(u), nil
	}
	return nil, nil, mismatch()
}

// temporalCompareLit handles `a OP lit` for temporal a or lit.
func temporalCompareLit(ctx context.Context, a *series.Series, lit any, opts []Option, op compareOp) (*series.Series, bool, error) {
	ts, litTemporal := series.ToTemporalScalar(lit)
	if !a.DType().IsTemporal() && !litTemporal {
		return nil, false, nil
	}
	cfg := resolve(opts)
	ls, _, err := literalToSeries(lit, cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer ls.Release()
	var inst *series.TemporalScalar
	if litTemporal && ts.FromGoTime {
		inst = &ts
	}
	out, _, err := temporalCompare(ctx, a, ls, append(opts, WithName(a.Name())), op, inst)
	return out, true, err
}

func unsafeInt64s[T ~int64](v []T) []int64 {
	if len(v) == 0 {
		return []int64{}
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(v))), len(v))
}

// temporalSortKeys returns the physical int64 values of a temporal
// array so the sort kernels can order it; nil for other dtypes.
func temporalSortKeys(arr arrow.Array) []int64 {
	switch arr.(type) {
	case *array.Timestamp, *array.Date32, *array.Date64, *array.Duration, *array.Time32, *array.Time64:
		return physicalValues(arr)
	}
	return nil
}

// mulOK and addOK report int64 overflow.
func mulOK(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	c := a * b
	return c, c/b == a && !(a == -1 && b == math.MinInt64) && !(b == -1 && a == math.MinInt64)
}

func addOK(a, b int64) (int64, bool) {
	c := a + b
	return c, (c > a) == (b > 0)
}
