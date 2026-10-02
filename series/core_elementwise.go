package series

import (
	"cmp"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// pairCmp returns a comparator between row i of a and row j of b for
// compatible dtypes: any two numerics (NaN sorts last), strings,
// booleans, or two columns of the same temporal dtype. Callers handle
// nulls.
func pairCmp(a, b arrow.Array) (func(i, j int) int, error) {
	da, db := a.DataType(), b.DataType()
	switch {
	case isNumericType(da) && isNumericType(db):
		if isIntType(da) && isIntType(db) && da.ID() != arrow.UINT64 && db.ID() != arrow.UINT64 {
			x, err := intVals(a)
			if err != nil {
				return nil, err
			}
			y, err := intVals(b)
			if err != nil {
				return nil, err
			}
			return func(i, j int) int { return cmp.Compare(x[i], y[j]) }, nil
		}
		x, err := floatVals(a)
		if err != nil {
			return nil, err
		}
		y, err := floatVals(b)
		if err != nil {
			return nil, err
		}
		return func(i, j int) int { return cmpTotalFloat(x[i], y[j]) }, nil
	case da.ID() == arrow.STRING && db.ID() == arrow.STRING:
		x, y := a.(*array.String), b.(*array.String)
		return func(i, j int) int { return strings.Compare(x.Value(i), y.Value(j)) }, nil
	case da.ID() == arrow.BOOL && db.ID() == arrow.BOOL:
		x, y := a.(*array.Boolean), b.(*array.Boolean)
		return func(i, j int) int {
			return cmp.Compare(b2i(x.Value(i)), b2i(y.Value(j)))
		}, nil
	case arrow.TypeEqual(da, db) && fixedWidthBytes(da) > 0:
		x, err := intVals(a)
		if err != nil {
			return nil, err
		}
		y, err := intVals(b)
		if err != nil {
			return nil, err
		}
		return func(i, j int) int { return cmp.Compare(x[i], y[j]) }, nil
	}
	return nil, fmt.Errorf("series: cannot compare %s with %s", da, db)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// twoArrays consolidates s and other, checking equal length.
func twoArrays(s, other *Series, mem memory.Allocator) (arrow.Array, arrow.Array, error) {
	if s.Len() != other.Len() {
		return nil, nil, fmt.Errorf("series: length mismatch %d vs %d", s.Len(), other.Len())
	}
	a, err := s.single(mem)
	if err != nil {
		return nil, nil, err
	}
	b, err := other.single(mem)
	if err != nil {
		a.Release()
		return nil, nil, err
	}
	return a, b, nil
}

// EqMissing compares element-wise where null equals null and null never
// equals a value. The result has no nulls. NaN equals NaN. Mirrors
// polars' eq_missing.
func (s *Series) EqMissing(other *Series, opts ...Option) (*Series, error) {
	return s.eqMissing(other, false, opts)
}

// NeMissing is the negation of EqMissing.
func (s *Series) NeMissing(other *Series, opts ...Option) (*Series, error) {
	return s.eqMissing(other, true, opts)
}

func (s *Series) eqMissing(other *Series, negate bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, b, err := twoArrays(s, other, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	n := a.Len()
	out := make([]bool, n)
	var c func(i, j int) int
	if a.DataType().ID() != arrow.NULL && b.DataType().ID() != arrow.NULL {
		c, err = pairCmp(a, b)
		if err != nil {
			return nil, err
		}
	}
	for i := range n {
		an, bn := a.IsNull(i), b.IsNull(i)
		var eq bool
		switch {
		case an || bn:
			eq = an && bn
		default:
			eq = c(i, i) == 0
		}
		out[i] = eq != negate
	}
	return FromBool(s.name, out, nil, WithAllocator(cfg.alloc))
}

// IsBetween tests lower <= s <= upper with closed choosing inclusive
// bounds ("both", "left", "right", "none"). Nulls propagate.
func (s *Series) IsBetween(lower, upper *Series, closed string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, lo, err := twoArrays(s, lower, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer lo.Release()
	if upper.Len() != s.Len() {
		return nil, fmt.Errorf("series: length mismatch %d vs %d", s.Len(), upper.Len())
	}
	hi, err := upper.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer hi.Release()
	cl, err := pairCmp(a, lo)
	if err != nil {
		return nil, err
	}
	ch, err := pairCmp(a, hi)
	if err != nil {
		return nil, err
	}
	leftInc := closed == "both" || closed == "left" || closed == ""
	rightInc := closed == "both" || closed == "right" || closed == ""
	n := a.Len()
	out := make([]bool, n)
	valid := make([]bool, n)
	for i := range n {
		if a.IsNull(i) || lo.IsNull(i) || hi.IsNull(i) {
			continue
		}
		valid[i] = true
		l := cl(i, i)
		h := ch(i, i)
		okL := l > 0 || (leftInc && l == 0)
		okH := h < 0 || (rightInc && h == 0)
		out[i] = okL && okH
	}
	return FromBool(s.name, out, validOrNil(valid), WithAllocator(cfg.alloc))
}

// IsClose reports |a-b| <= max(relTol*max(|a|,|b|), absTol). Infinite
// values are close only when equal. NaNs are close to each other only
// with nansEqual. Nulls propagate.
func (s *Series) IsClose(other *Series, absTol, relTol float64, nansEqual bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, b, err := twoArrays(s, other, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	x, err := floatVals(a)
	if err != nil {
		return nil, err
	}
	y, err := floatVals(b)
	if err != nil {
		return nil, err
	}
	n := a.Len()
	out := make([]bool, n)
	for i := range n {
		xi, yi := x[i], y[i]
		switch {
		case math.IsNaN(xi) || math.IsNaN(yi):
			out[i] = nansEqual && math.IsNaN(xi) && math.IsNaN(yi)
		case xi == yi:
			out[i] = true
		case math.IsInf(xi, 0) || math.IsInf(yi, 0):
			out[i] = false
		default:
			tol := math.Max(relTol*math.Max(math.Abs(xi), math.Abs(yi)), absTol)
			out[i] = math.Abs(xi-yi) <= tol
		}
	}
	return FromBool(s.name, out, andValid(validSlice(a), validSlice(b)), WithAllocator(cfg.alloc))
}

// NaNCheck evaluates one of "is_nan", "is_not_nan", "is_finite",
// "is_infinite". Integers are never NaN or infinite. Nulls stay null.
// Non-numeric inputs are an error, as in polars.
func (s *Series) NaNCheck(kind string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	dt := arr.DataType()
	if !isNumericType(dt) {
		return nil, fmt.Errorf("series: %s not supported for dtype %s", kind, s.DType())
	}
	n := arr.Len()
	out := make([]bool, n)
	if isFloatType(dt) {
		vals, _ := floatVals(arr)
		for i, v := range vals {
			switch kind {
			case "is_nan":
				out[i] = math.IsNaN(v)
			case "is_not_nan":
				out[i] = !math.IsNaN(v)
			case "is_finite":
				out[i] = !math.IsNaN(v) && !math.IsInf(v, 0)
			case "is_infinite":
				out[i] = math.IsInf(v, 0)
			}
		}
	} else {
		fill := kind == "is_not_nan" || kind == "is_finite"
		for i := range out {
			out[i] = fill
		}
	}
	return FromBool(s.name, out, validSlice(arr), WithAllocator(cfg.alloc))
}

// arithKind is the output class of a binary numeric op.
type arithKind int

const (
	arithInt arithKind = iota
	arithF32
	arithF64
)

func arithOutKind(a, b arrow.DataType) (arithKind, error) {
	if !isNumericType(a) && a.ID() != arrow.BOOL || !isNumericType(b) && b.ID() != arrow.BOOL {
		return 0, fmt.Errorf("series: arithmetic needs numeric inputs, got %s and %s", a, b)
	}
	if isFloatType(a) || isFloatType(b) {
		if (a.ID() == arrow.FLOAT32 || !isFloatType(a) && fixedWidthBytes(a) <= 2) &&
			(b.ID() == arrow.FLOAT32 || !isFloatType(b) && fixedWidthBytes(b) <= 2) {
			return arithF32, nil
		}
		return arithF64, nil
	}
	return arithInt, nil
}

// intOutType picks the integer output dtype for an int op int.
func intOutType(a, b arrow.DataType) arrow.DataType {
	if arrow.TypeEqual(a, b) && a.ID() != arrow.BOOL {
		return a
	}
	// Mixed widths take the polars supertype (i8 % i16 is i16).
	if st, ok := dtype.NumericSupertype(dtype.FromArrow(a), dtype.FromArrow(b)); ok && st.IsInteger() {
		return st.Arrow()
	}
	return arrow.PrimitiveTypes.Int64
}

// binaryArith applies intFn or floatFn row-wise. intFn may report
// ok=false to produce a null (division by zero).
func (s *Series) binaryArith(other *Series, name string, intFn func(x, y int64) (int64, bool), floatFn func(x, y float64) float64, forceFloat bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, b, err := twoArrays(s, other, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	kind, err := arithOutKind(a.DataType(), b.DataType())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	valid := andValid(validSlice(a), validSlice(b))
	n := a.Len()
	if kind == arithInt && !forceFloat {
		x, err := intVals(a)
		if err != nil {
			return nil, err
		}
		y, err := intVals(b)
		if err != nil {
			return nil, err
		}
		out := make([]int64, n)
		for i := range n {
			if valid != nil && !valid[i] {
				continue
			}
			v, ok := intFn(x[i], y[i])
			if !ok {
				if valid == nil {
					valid = make([]bool, n)
					for k := range valid {
						valid[k] = true
					}
				}
				valid[i] = false
				continue
			}
			out[i] = v
		}
		return intSeriesAs(s.name, out, valid, intOutType(a.DataType(), b.DataType()), cfg.alloc)
	}
	x, err := floatVals(a)
	if err != nil {
		return nil, err
	}
	y, err := floatVals(b)
	if err != nil {
		return nil, err
	}
	out := make([]float64, n)
	for i := range n {
		out[i] = floatFn(x[i], y[i])
	}
	return floatSeries(s.name, out, valid, kind == arithF32, cfg.alloc)
}

// intSeriesAs stores int64 values as the integer dtype dt (values are
// truncated to the width, callers guarantee they fit).
func intSeriesAs(name string, vals []int64, valid []bool, dt arrow.DataType, mem memory.Allocator) (*Series, error) {
	n := len(vals)
	switch dt.ID() {
	case arrow.INT8:
		return buildFixedValid(name, dt, n, valid, mem, func(out []int8) { narrowInto(out, vals) })
	case arrow.INT16:
		return buildFixedValid(name, dt, n, valid, mem, func(out []int16) { narrowInto(out, vals) })
	case arrow.INT32:
		return buildFixedValid(name, dt, n, valid, mem, func(out []int32) { narrowInto(out, vals) })
	case arrow.UINT8:
		return buildFixedValid(name, dt, n, valid, mem, func(out []uint8) { narrowInto(out, vals) })
	case arrow.UINT16:
		return buildFixedValid(name, dt, n, valid, mem, func(out []uint16) { narrowInto(out, vals) })
	case arrow.UINT32:
		return buildFixedValid(name, dt, n, valid, mem, func(out []uint32) { narrowInto(out, vals) })
	case arrow.UINT64:
		return buildFixedValid(name, dt, n, valid, mem, func(out []uint64) { narrowInto(out, vals) })
	}
	return buildFixedValid(name, arrow.PrimitiveTypes.Int64, n, valid, mem, func(out []int64) { copy(out, vals) })
}

func narrowInto[T fixedWidth](dst []T, src []int64) {
	for i, v := range src {
		dst[i] = T(v)
	}
}

// FloorDiv returns floor(s / other). Integer division by zero gives
// null. Mirrors polars' floordiv.
func (s *Series) FloorDiv(other *Series, opts ...Option) (*Series, error) {
	return s.binaryArith(other, "floordiv", func(x, y int64) (int64, bool) {
		if y == 0 {
			return 0, false
		}
		q := x / y
		if (x%y != 0) && ((x < 0) != (y < 0)) {
			q--
		}
		return q, true
	}, func(x, y float64) float64 { return math.Floor(x / y) }, false, opts)
}

// Mod returns the remainder with the sign of the divisor (Python
// semantics). Integer modulo by zero gives null.
func (s *Series) Mod(other *Series, opts ...Option) (*Series, error) {
	return s.binaryArith(other, "mod", func(x, y int64) (int64, bool) {
		if y == 0 {
			return 0, false
		}
		r := x % y
		if r != 0 && (r < 0) != (y < 0) {
			r += y
		}
		return r, true
	}, floatMod(s.DType(), other.DType()), false, opts)
}

// floatMod is polars' float modulo, l - r*floor(l/r), evaluated in f32
// when the result is f32 so the rounding matches.
func floatMod(a, b dtype.DType) func(x, y float64) float64 {
	if st, ok := dtype.NumericSupertype(a, b); ok && st.ID() == arrow.FLOAT32 {
		return func(x, y float64) float64 {
			l, r := float32(x), float32(y)
			q := float32(math.Floor(float64(l / r)))
			return float64(l - float32(r*q))
		}
	}
	return func(x, y float64) float64 {
		return x - float64(y*math.Floor(x/y))
	}
}

// TrueDiv divides as floating point.
func (s *Series) TrueDiv(other *Series, opts ...Option) (*Series, error) {
	return s.binaryArith(other, "truediv", nil, func(x, y float64) float64 { return x / y }, true, opts)
}

// PowSeries raises s to a per-row exponent. Integer base and
// non-negative integer exponent stay integer.
func (s *Series) PowSeries(exponent *Series, opts ...Option) (*Series, error) {
	return s.binaryArith(exponent, "pow", func(x, y int64) (int64, bool) {
		if y < 0 {
			return 0, false
		}
		out := int64(1)
		for y > 0 {
			if y&1 == 1 {
				out *= x
			}
			x *= x
			y >>= 1
		}
		return out, true
	}, math.Pow, false, opts)
}

// LogicOp applies "and", "or" or "xor" element-wise: Kleene logic for
// booleans (xor is null when either side is null) and bitwise for
// integers (null when either side is null).
func (s *Series) LogicOp(op string, other *Series, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, b, err := twoArrays(s, other, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	defer b.Release()
	n := a.Len()
	if a.DataType().ID() == arrow.BOOL && b.DataType().ID() == arrow.BOOL {
		x, y := a.(*array.Boolean), b.(*array.Boolean)
		out := make([]bool, n)
		valid := make([]bool, n)
		for i := range n {
			xn, yn := x.IsNull(i), y.IsNull(i)
			xv, yv := !xn && x.Value(i), !yn && y.Value(i)
			switch op {
			case "and":
				switch {
				case (!xn && !xv) || (!yn && !yv):
					out[i], valid[i] = false, true
				case xn || yn:
				default:
					out[i], valid[i] = true, true
				}
			case "or":
				switch {
				case xv || yv:
					out[i], valid[i] = true, true
				case xn || yn:
				default:
					out[i], valid[i] = false, true
				}
			case "xor":
				if !xn && !yn {
					out[i], valid[i] = xv != yv, true
				}
			default:
				return nil, fmt.Errorf("series: unknown logic op %q", op)
			}
		}
		return FromBool(s.name, out, validOrNil(valid), WithAllocator(cfg.alloc))
	}
	if !isIntType(a.DataType()) || !isIntType(b.DataType()) {
		return nil, fmt.Errorf("series: %s needs bool or integer inputs, got %s and %s", op, a.DataType(), b.DataType())
	}
	return s.binaryArith(other, op, func(x, y int64) (int64, bool) {
		switch op {
		case "and":
			return x & y, true
		case "or":
			return x | y, true
		}
		return x ^ y, true
	}, nil, false, opts)
}

// LogBase returns the logarithm in the given base. f32 stays f32;
// every other numeric input gives f64.
func (s *Series) LogBase(base float64, opts ...Option) (*Series, error) {
	lb := math.Log(base)
	if s.data.DataType().ID() == arrow.FLOAT32 {
		// polars divides in f32: round ln(x) and ln(base) first.
		lb32 := float32(lb)
		return s.floatUnary("log", func(x float64) float64 {
			return float64(float32(math.Log(x)) / lb32)
		}, opts)
	}
	return s.floatUnary("log", func(x float64) float64 { return math.Log(x) / lb }, opts)
}

// RoundSigFigs rounds floats to the given number of significant digits.
// Integer inputs are returned unchanged.
func (s *Series) RoundSigFigs(digits int, opts ...Option) (*Series, error) {
	if digits < 1 {
		return nil, fmt.Errorf("series: round_sig_figs digits must be at least 1")
	}
	if isIntType(s.data.DataType()) {
		return s.Clone(), nil
	}
	return s.floatUnary("round_sig_figs", func(x float64) float64 {
		if x == 0 || math.IsNaN(x) || math.IsInf(x, 0) {
			return x
		}
		mag := math.Pow(10, float64(digits-1)-math.Floor(math.Log10(math.Abs(x))))
		return math.Round(x*mag) / mag
	}, opts)
}

// floatUnary maps a float function over a numeric Series, keeping f32
// as f32 (computed in f64, then rounded) and producing f64 otherwise.
// The null pattern is preserved.
func (s *Series) floatUnary(name string, fn func(float64) float64, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	if !isNumericType(arr.DataType()) {
		return nil, fmt.Errorf("series: %s needs numeric input, got %s", name, s.DType())
	}
	n := arr.Len()
	bits := CopyValidityBitmap(arr, cfg.alloc)
	if bits != nil {
		defer bits.Release()
	}
	nulls := arr.NullN()
	switch a := arr.(type) {
	case *array.Float64:
		vals := a.Float64Values()
		return buildFixedBits(s.name, arrow.PrimitiveTypes.Float64, n, bits, nulls, cfg.alloc, func(out []float64) {
			for i, v := range vals {
				out[i] = fn(v)
			}
		})
	case *array.Float32:
		vals := a.Float32Values()
		return buildFixedBits(s.name, arrow.PrimitiveTypes.Float32, n, bits, nulls, cfg.alloc, func(out []float32) {
			for i, v := range vals {
				out[i] = float32(fn(float64(v)))
			}
		})
	}
	vals, err := floatVals(arr)
	if err != nil {
		return nil, err
	}
	return buildFixedBits(s.name, arrow.PrimitiveTypes.Float64, n, bits, nulls, cfg.alloc, func(out []float64) {
		for i, v := range vals {
			out[i] = fn(v)
		}
	})
}

// FloatUnary is the exported form of floatUnary: it applies fn to every
// value of a numeric Series with polars' dtype rules for float-valued
// math (f32 stays f32, everything else becomes f64, nulls kept).
func (s *Series) FloatUnary(name string, fn func(float64) float64, opts ...Option) (*Series, error) {
	return s.floatUnary(name, fn, opts)
}

// Reinterpret reinterprets the bits of an integer column as the signed
// or unsigned integer of the same width.
func (s *Series) Reinterpret(signed bool, opts ...Option) (*Series, error) {
	dt := s.data.DataType()
	var to arrow.DataType
	switch dt.ID() {
	case arrow.INT8, arrow.UINT8:
		to = pick[arrow.DataType](signed, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Uint8)
	case arrow.INT16, arrow.UINT16:
		to = pick[arrow.DataType](signed, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint16)
	case arrow.INT32, arrow.UINT32:
		to = pick[arrow.DataType](signed, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32)
	case arrow.INT64, arrow.UINT64:
		to = pick[arrow.DataType](signed, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64)
	default:
		return nil, fmt.Errorf("series: reinterpret needs an integer column, got %s", s.DType())
	}
	return s.retype(to, opts)
}

// ToPhysical returns the physical representation: dates become i32,
// datetimes, durations and 64-bit times become i64, 32-bit times i32.
// Other dtypes are returned as a clone.
func (s *Series) ToPhysical(opts ...Option) (*Series, error) {
	switch s.data.DataType().ID() {
	case arrow.DATE32, arrow.TIME32:
		return s.retype(arrow.PrimitiveTypes.Int32, opts)
	case arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		return s.retype(arrow.PrimitiveTypes.Int64, opts)
	}
	return s.Clone(), nil
}

// retype relabels the buffers of a fixed-width column with another
// dtype of the same width (zero copy).
func (s *Series) retype(to arrow.DataType, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	arr, err := s.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	d := arr.Data()
	data := array.NewData(to, d.Len(), d.Buffers(), nil, d.NullN(), d.Offset())
	defer data.Release()
	return New(s.name, array.MakeFromData(data))
}
