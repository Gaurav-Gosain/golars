package compute

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/temporal"
	"github.com/Gaurav-Gosain/golars/series"
)

func isSmallInt(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16:
		return true
	}
	return false
}

// castExtra covers the casts the core kernels in cast.go do not:
// anything involving a temporal dtype and the narrow integer widths
// (i8, i16, u8, u16) that temporal field accessors produce. ok reports
// whether the cast was handled here.
func castExtra(ctx context.Context, s *series.Series, to dtype.DType, cfg config) (*series.Series, bool, error) {
	from := s.DType()
	name := cfg.outName(s.Name())
	switch {
	case from.IsTemporal() || to.IsTemporal():
		out, err := castTemporal(ctx, s, to, cfg)
		if err != nil {
			return nil, true, err
		}
		if out.Name() != name {
			renamed := out.Rename(name)
			out.Release()
			out = renamed
		}
		return out, true, nil
	case isSmallInt(to.ID()) && (from.IsNumeric() || from.IsBool()):
		out, err := castToSmallInt(s, to, name, cfg.alloc)
		return out, true, err
	case isSmallInt(to.ID()) && from.IsString():
		// Parse as i64 first, then narrow (out of range becomes null).
		wide, err := Cast(ctx, s, dtype.Int64(), WithAllocator(cfg.alloc))
		if err != nil {
			return nil, true, err
		}
		defer wide.Release()
		out, err := castToSmallInt(wide, to, name, cfg.alloc)
		return out, true, err
	case isSmallInt(from.ID()):
		wide, err := widenToInt64(s, cfg.alloc)
		if err != nil {
			return nil, true, err
		}
		defer wide.Release()
		out, err := Cast(ctx, wide, to, WithAllocator(cfg.alloc), WithName(name))
		return out, true, err
	}
	return nil, false, nil
}

// widenToInt64 converts any integer-backed Series to Int64.
func widenToInt64(s *series.Series, mem memory.Allocator) (*series.Series, error) {
	arr, err := extractChunk(s, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	vals := physicalValues(arr)
	if vals == nil && arr.Len() > 0 {
		return nil, fmt.Errorf("%w: cannot widen %s to i64", ErrUnsupportedDType, s.DType())
	}
	return series.BuildTypedInt64WithValidity(s.Name(), arr.Len(), mem, arrow.PrimitiveTypes.Int64,
		func(out []int64) { copy(out, vals) }, series.CopyValidityBitmap(arr, mem), arr.NullN())
}

func castToSmallInt(s *series.Series, to dtype.DType, name string, mem memory.Allocator) (*series.Series, error) {
	arr, err := extractChunk(s, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	n := arr.Len()
	var lo, hi int64
	switch to.ID() {
	case arrow.INT8:
		lo, hi = math.MinInt8, math.MaxInt8
	case arrow.INT16:
		lo, hi = math.MinInt16, math.MaxInt16
	case arrow.UINT8:
		lo, hi = 0, math.MaxUint8
	default:
		lo, hi = 0, math.MaxUint16
	}
	vals := make([]int64, n)
	valid := make([]bool, n)
	for i := range n {
		if arr.IsNull(i) {
			continue
		}
		var v int64
		switch a := arr.(type) {
		case *array.Float64:
			f := a.Value(i)
			if f != f || f < float64(lo) || f > float64(hi) {
				continue
			}
			v = int64(f)
		case *array.Float32:
			f := float64(a.Value(i))
			if f != f || f < float64(lo) || f > float64(hi) {
				continue
			}
			v = int64(f)
		case *array.Boolean:
			if a.Value(i) {
				v = 1
			}
		case *array.Uint64:
			u := a.Value(i)
			if u > uint64(hi) {
				continue
			}
			v = int64(u)
		default:
			pv := physicalValues(arr)
			if pv == nil {
				return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, s.DType(), to)
			}
			v = pv[i]
		}
		if v < lo || v > hi {
			continue
		}
		vals[i] = v
		valid[i] = true
	}
	switch to.ID() {
	case arrow.INT8:
		b := array.NewInt8Builder(mem)
		defer b.Release()
		for i, v := range vals {
			if valid[i] {
				b.Append(int8(v))
			} else {
				b.AppendNull()
			}
		}
		return series.New(name, b.NewArray())
	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		defer b.Release()
		for i, v := range vals {
			if valid[i] {
				b.Append(int16(v))
			} else {
				b.AppendNull()
			}
		}
		return series.New(name, b.NewArray())
	case arrow.UINT8:
		b := array.NewUint8Builder(mem)
		defer b.Release()
		for i, v := range vals {
			if valid[i] {
				b.Append(uint8(v))
			} else {
				b.AppendNull()
			}
		}
		return series.New(name, b.NewArray())
	}
	b := array.NewUint16Builder(mem)
	defer b.Release()
	for i, v := range vals {
		if valid[i] {
			b.Append(uint16(v))
		} else {
			b.AppendNull()
		}
	}
	return series.New(name, b.NewArray())
}

// reinterpretPhysical copies the integer physical values of s into a
// new array of dtype to (int64 or int32 backed).
func reinterpretPhysical(s *series.Series, to dtype.DType, mem memory.Allocator, conv func(int64) int64) (*series.Series, error) {
	arr, err := extractChunk(s, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	vals := physicalValues(arr)
	if vals == nil && arr.Len() > 0 {
		if f, ok := arr.(*array.Float64); ok {
			fv := f.Float64Values()
			vals = make([]int64, len(fv))
			for i, x := range fv {
				vals[i] = int64(x)
			}
		} else {
			return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, s.DType(), to)
		}
	}
	if conv == nil {
		conv = func(v int64) int64 { return v }
	}
	valid := series.CopyValidityBitmap(arr, mem)
	n := arr.Len()
	if to.IsDate() || to.ID() == arrow.TIME32 || to.ID() == arrow.INT32 {
		return series.BuildTypedInt32WithValidity(s.Name(), n, mem, to.Arrow(), func(out []int32) {
			for i, v := range vals {
				out[i] = int32(conv(v))
			}
		}, valid, arr.NullN())
	}
	if to.ID() == arrow.FLOAT64 {
		return series.BuildFloat64DirectWithValidity(s.Name(), n, mem, func(out []float64) {
			for i, v := range vals {
				out[i] = float64(conv(v))
			}
		}, valid, arr.NullN())
	}
	return series.BuildTypedInt64WithValidity(s.Name(), n, mem, to.Arrow(), func(out []int64) {
		for i, v := range vals {
			out[i] = conv(v)
		}
	}, valid, arr.NullN())
}

func castTemporal(ctx context.Context, s *series.Series, to dtype.DType, cfg config) (*series.Series, error) {
	from := s.DType()
	mem := cfg.alloc
	opt := series.WithAllocator(mem)
	unsupported := func() error {
		return fmt.Errorf("%w: casting from %s to %s not supported", ErrUnsupportedDType, from, to)
	}
	fu, _ := from.TimeUnit()
	tu, _ := to.TimeUnit()
	switch {
	case to.ID() == arrow.STRING:
		switch {
		case from.IsDuration():
			return nil, unsupported()
		case from.IsTime():
			return s.Dt().Strftime("%T", opt)
		}
		return s.Dt().ToString("", opt)
	case from.ID() == arrow.STRING:
		switch {
		case to.IsDuration():
			// polars parses the string as an integer tick count.
			return viaInt64(ctx, s, to, cfg)
		case to.IsDate():
			return s.Str().ToDate("%Y-%m-%d", series.StrptimeOptions{Exact: true}, opt)
		case to.IsDatetime():
			f := "%Y-%m-%dT%H:%M:%S%.f"
			if to.TimeZone() != "" {
				f += "%:z"
			}
			out, err := s.Str().ToDatetime(f, tu.String(), "", series.StrptimeOptions{Exact: true}, opt)
			if err != nil || to.TimeZone() == "" {
				return out, err
			}
			defer out.Release()
			return out.Dt().ConvertTimeZone(to.TimeZone())
		}
		return nil, unsupported()
	case from.IsDate():
		switch {
		case to.IsDatetime():
			return rescaleChecked(s, to, mem, temporal.UnitsPerDay(tu))
		case to.IsInteger() || to.IsFloating():
			return castNumericTarget(ctx, s, to, cfg)
		}
	case from.IsDatetime():
		switch {
		case to.IsDatetime():
			if up := upscaleFactor(fu, tu); up > 1 {
				return rescaleChecked(s, to, mem, up)
			}
			return reinterpretPhysical(s, to, mem, func(v int64) int64 { return temporal.ConvertUnit(v, fu, tu, true) })
		case to.IsDate():
			return s.Dt().Date(opt)
		case to.IsTime():
			t, err := s.Dt().Time(opt)
			if err != nil || tu == dtype.Nanosecond {
				return t, err
			}
			defer t.Release()
			return reinterpretPhysical(t, to, mem, func(v int64) int64 { return temporal.ConvertUnit(v, dtype.Nanosecond, tu, true) })
		case to.IsInteger() || to.IsFloating():
			return castNumericTarget(ctx, s, to, cfg)
		}
	case from.IsDuration():
		switch {
		case to.IsDuration():
			if up := upscaleFactor(fu, tu); up > 1 {
				return rescaleChecked(s, to, mem, up)
			}
			return reinterpretPhysical(s, to, mem, func(v int64) int64 { return temporal.ConvertUnit(v, fu, tu, false) })
		case to.IsInteger() || to.IsFloating():
			return castNumericTarget(ctx, s, to, cfg)
		}
	case from.IsTime():
		switch {
		case to.IsTime():
			return reinterpretPhysical(s, to, mem, func(v int64) int64 { return temporal.ConvertUnit(v, fu, tu, true) })
		case to.IsDuration():
			return reinterpretPhysical(s, to, mem, func(v int64) int64 { return temporal.ConvertUnit(v, fu, tu, false) })
		case to.IsInteger() || to.IsFloating():
			return castNumericTarget(ctx, s, to, cfg)
		}
	case from.IsInteger():
		if to.IsTemporal() {
			return reinterpretPhysical(s, to, mem, nil)
		}
	case from.IsFloating() || from.IsBool():
		if to.IsTemporal() {
			// polars truncates floats (NaN and out of range become null)
			// and maps bools to 0 and 1 before reinterpreting.
			return viaInt64(ctx, s, to, cfg)
		}
	}
	return nil, unsupported()
}

// castNumericTarget casts a temporal Series to a numeric dtype through
// its physical int64 value.
func castNumericTarget(ctx context.Context, s *series.Series, to dtype.DType, cfg config) (*series.Series, error) {
	phys, err := reinterpretPhysical(s, dtype.Int64(), cfg.alloc, nil)
	if err != nil {
		return nil, err
	}
	if to.ID() == arrow.INT64 {
		return phys, nil
	}
	defer phys.Release()
	return Cast(ctx, phys, to, WithAllocator(cfg.alloc))
}

// widenableInt reports integer widths the core kernels lack scalar
// paths for; they are widened to i64 for literal ops.
func widenableInt(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return true
	}
	return false
}

// extraArithLit handles `a OP lit` for temporal operands and for the
// narrow integer widths (computed in i64, cast back to a's dtype).
func extraArithLit(ctx context.Context, a *series.Series, lit any, opts []Option, op arithOp) (*series.Series, bool, error) {
	if out, ok, err := temporalArithLit(ctx, a, lit, opts, op); ok {
		return out, true, err
	}
	if !widenableInt(a.DType().ID()) {
		return nil, false, nil
	}
	if _, isInt := toInt64Scalar(lit); !isInt {
		return nil, false, nil
	}
	cfg := resolve(opts)
	wide, err := widenToInt64(a, cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer wide.Release()
	res, err := runArithLit(ctx, wide, lit, opts, "ArithLit", op)
	if err != nil {
		return nil, true, err
	}
	defer res.Release()
	out, err := Cast(ctx, res, a.DType(), WithAllocator(cfg.alloc), WithName(cfg.outName(a.Name())))
	return out, true, err
}

// extraCompare handles comparisons with a temporal operand.
func extraCompare(ctx context.Context, a, b *series.Series, opts []Option, op compareOp) (*series.Series, bool, error) {
	return temporalCompare(ctx, a, b, opts, op, nil)
}

// extraCompareLit handles `a OP lit` for temporal operands and narrow
// integer columns.
func extraCompareLit(ctx context.Context, a *series.Series, lit any, opts []Option, op compareOp) (*series.Series, bool, error) {
	if out, ok, err := temporalCompareLit(ctx, a, lit, opts, op); ok {
		return out, true, err
	}
	if !widenableInt(a.DType().ID()) && a.DType().ID() != arrow.UINT64 {
		return nil, false, nil
	}
	cfg := resolve(opts)
	wide, err := widenToInt64(a, cfg.alloc)
	if err != nil {
		return nil, true, err
	}
	defer wide.Release()
	out, err := runCompareLit(ctx, wide, lit, opts, op)
	return out, true, err
}
