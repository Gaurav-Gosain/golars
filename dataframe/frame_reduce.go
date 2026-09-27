package dataframe

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// reduceOp names a column-wise reduction producing one value per column.
type reduceOp uint8

const (
	reduceSum reduceOp = iota
	reduceMean
	reduceMedian
	reduceMin
	reduceMax
	reduceStd
	reduceVar
	reduceProduct
	reduceQuantile
	reduceCount
	reduceNullCount
)

func (o reduceOp) String() string {
	return [...]string{"sum", "mean", "median", "min", "max", "std", "var",
		"product", "quantile", "count", "null_count"}[o]
}

// reduceArgs carries the optional parameters of a reduction.
type reduceArgs struct {
	ddof   int
	q      float64
	method QuantileMethod
}

// reduceFrame applies op to every column of df and returns a one-row
// frame with the same column names. The dtype of each output column
// follows polars' DataFrame.<op>() rules.
func (df *DataFrame) reduceFrame(ctx context.Context, op reduceOp, args reduceArgs) (*DataFrame, error) {
	mem := memory.DefaultAllocator
	out := make([]*series.Series, 0, len(df.cols))
	for _, c := range df.cols {
		if err := ctx.Err(); err != nil {
			releaseAll(out)
			return nil, err
		}
		s, err := reduceColumn(c, op, args, mem)
		if err != nil {
			releaseAll(out)
			return nil, fmt.Errorf("dataframe.%s: column %q: %w", op, c.Name(), err)
		}
		out = append(out, s)
	}
	return New(out...)
}

// numeric is every fixed-width primitive the reducers read directly.
type numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// reduceColumn reduces one column. The returned Series has length 1 and
// the column's name.
func reduceColumn(s *series.Series, op reduceOp, args reduceArgs, mem memory.Allocator) (*series.Series, error) {
	name := s.Name()
	a := firstChunk(s)
	dt := a.DataType()

	switch op {
	case reduceCount:
		return scalarSeries(name, arrow.PrimitiveTypes.Uint32, uint64(a.Len()-a.NullN()), true, mem)
	case reduceNullCount:
		return scalarSeries(name, arrow.PrimitiveTypes.Uint32, uint64(a.NullN()), true, mem)
	}

	switch dt.ID() {
	case arrow.INT8:
		return reduceInts(name, rawValues[int8](a), a, dt, op, args, mem)
	case arrow.INT16:
		return reduceInts(name, rawValues[int16](a), a, dt, op, args, mem)
	case arrow.INT32:
		return reduceInts(name, rawValues[int32](a), a, dt, op, args, mem)
	case arrow.INT64:
		return reduceInts(name, rawValues[int64](a), a, dt, op, args, mem)
	case arrow.UINT8:
		return reduceInts(name, rawValues[uint8](a), a, dt, op, args, mem)
	case arrow.UINT16:
		return reduceInts(name, rawValues[uint16](a), a, dt, op, args, mem)
	case arrow.UINT32:
		return reduceInts(name, rawValues[uint32](a), a, dt, op, args, mem)
	case arrow.UINT64:
		return reduceInts(name, rawValues[uint64](a), a, dt, op, args, mem)
	case arrow.FLOAT32:
		return reduceFloats(name, rawValues[float32](a), a, dt, op, args, mem)
	case arrow.FLOAT64:
		return reduceFloats(name, rawValues[float64](a), a, dt, op, args, mem)
	case arrow.BOOL:
		return reduceBool(name, a.(*array.Boolean), op, args, mem)
	case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		return reduceBytes(name, a, op, mem)
	case arrow.DATE32, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME32, arrow.TIME64, arrow.DATE64:
		return reduceTemporal(name, a, op, args, mem)
	case arrow.NULL:
		return nullScalar(name, arrow.Null, mem), nil
	}
	// Nested and other dtypes: every reduction is null. Product yields
	// the Null dtype, the rest keep the input dtype.
	if op == reduceProduct {
		return nullScalar(name, arrow.Null, mem), nil
	}
	return nullScalar(name, dt, mem), nil
}

func isUnsigned(id arrow.Type) bool {
	switch id {
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

// reduceInts handles every integer width. Sums wrap like polars.
func reduceInts[T numeric](name string, vals []T, a arrow.Array, dt arrow.DataType, op reduceOp, args reduceArgs, mem memory.Allocator) (*series.Series, error) {
	hasNulls := a.NullN() > 0
	valid := a.Len() - a.NullN()
	switch op {
	case reduceSum:
		// Two's complement addition wraps identically for signed and
		// unsigned inputs once each value is widened with its own sign.
		var acc uint64
		unsigned := isUnsigned(dt.ID())
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				continue
			}
			if unsigned {
				acc += uint64(v)
			} else {
				acc += uint64(int64(v))
			}
		}
		switch dt.ID() {
		case arrow.INT32:
			return scalarSeries(name, dt, uint64(int64(int32(acc))), true, mem)
		case arrow.UINT32:
			return scalarSeries(name, dt, uint64(uint32(acc)), true, mem)
		case arrow.UINT64:
			return scalarSeries(name, dt, acc, true, mem)
		}
		return scalarSeries(name, arrow.PrimitiveTypes.Int64, acc, true, mem)
	case reduceProduct:
		if dt.ID() == arrow.UINT64 {
			acc := uint64(1)
			for i, v := range vals {
				if hasNulls && a.IsNull(i) {
					continue
				}
				acc *= uint64(v)
			}
			return scalarSeries(name, dt, acc, true, mem)
		}
		acc := int64(1)
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				continue
			}
			acc *= int64(v)
		}
		return scalarSeries(name, arrow.PrimitiveTypes.Int64, uint64(acc), true, mem)
	case reduceMin, reduceMax:
		if valid == 0 {
			return nullScalar(name, dt, mem), nil
		}
		first := true
		var best T
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				continue
			}
			if first || (op == reduceMin && v < best) || (op == reduceMax && v > best) {
				best = v
				first = false
			}
		}
		if isUnsigned(dt.ID()) {
			return scalarSeries(name, dt, uint64(best), true, mem)
		}
		return scalarSeries(name, dt, uint64(int64(best)), true, mem)
	}
	f64 := arrow.PrimitiveTypes.Float64
	fs := make([]float64, 0, valid)
	for i, v := range vals {
		if hasNulls && a.IsNull(i) {
			continue
		}
		fs = append(fs, float64(v))
	}
	v, ok := floatStat(fs, op, args)
	return floatScalar(name, f64, v, ok, mem)
}

// floatStat computes mean, median, std, var or quantile over fs. fs is
// reordered in place for order statistics. ok=false means null.
func floatStat(fs []float64, op reduceOp, args reduceArgs) (float64, bool) {
	switch op {
	case reduceMean:
		if len(fs) == 0 {
			return 0, false
		}
		var sum float64
		for _, v := range fs {
			sum += v
		}
		return sum / float64(len(fs)), true
	case reduceStd, reduceVar:
		n := len(fs)
		if n-args.ddof <= 0 {
			return 0, false
		}
		var sum float64
		for _, v := range fs {
			sum += v
		}
		mean := sum / float64(n)
		var sq float64
		for _, v := range fs {
			d := v - mean
			sq += d * d
		}
		v := sq / float64(n-args.ddof)
		if op == reduceStd {
			v = math.Sqrt(v)
		}
		return v, true
	case reduceMedian:
		sortFloatsNaNLast(fs)
		return quantileSorted(fs, 0.5, QuantileLinear)
	case reduceQuantile:
		sortFloatsNaNLast(fs)
		return quantileSorted(fs, args.q, args.method)
	}
	return 0, false
}

func reduceFloats[T float32 | float64](name string, vals []T, a arrow.Array, dt arrow.DataType, op reduceOp, args reduceArgs, mem memory.Allocator) (*series.Series, error) {
	hasNulls := a.NullN() > 0
	valid := a.Len() - a.NullN()
	switch op {
	case reduceSum, reduceProduct:
		acc := 0.0
		if op == reduceProduct {
			acc = 1
		}
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				continue
			}
			if op == reduceSum {
				acc += float64(v)
			} else {
				acc *= float64(v)
			}
		}
		return floatScalar(name, dt, acc, true, mem)
	case reduceMin, reduceMax:
		// NaN is ignored unless every valid value is NaN.
		found, sawNaN := false, false
		var best float64
		for i, v := range vals {
			if hasNulls && a.IsNull(i) {
				continue
			}
			f := float64(v)
			if math.IsNaN(f) {
				sawNaN = true
				continue
			}
			if !found || (op == reduceMin && f < best) || (op == reduceMax && f > best) {
				best = f
				found = true
			}
		}
		if !found {
			if sawNaN {
				return floatScalar(name, dt, math.NaN(), true, mem)
			}
			return nullScalar(name, dt, mem), nil
		}
		return floatScalar(name, dt, best, true, mem)
	}
	fs := make([]float64, 0, valid)
	for i, v := range vals {
		if hasNulls && a.IsNull(i) {
			continue
		}
		fs = append(fs, float64(v))
	}
	v, ok := floatStat(fs, op, args)
	return floatScalar(name, dt, v, ok, mem)
}

func reduceBool(name string, a *array.Boolean, op reduceOp, args reduceArgs, mem memory.Allocator) (*series.Series, error) {
	n := a.Len()
	hasNulls := a.NullN() > 0
	trues, valid := 0, 0
	for i := range n {
		if hasNulls && a.IsNull(i) {
			continue
		}
		valid++
		if a.Value(i) {
			trues++
		}
	}
	dt := a.DataType()
	switch op {
	case reduceSum:
		return scalarSeries(name, arrow.PrimitiveTypes.Uint32, uint64(trues), true, mem)
	case reduceProduct:
		p := int64(1)
		if trues < valid {
			p = 0
		}
		return scalarSeries(name, arrow.PrimitiveTypes.Int64, uint64(p), true, mem)
	case reduceMin:
		if valid == 0 {
			return nullScalar(name, dt, mem), nil
		}
		return boolScalar(name, trues == valid, mem)
	case reduceMax:
		if valid == 0 {
			return nullScalar(name, dt, mem), nil
		}
		return boolScalar(name, trues > 0, mem)
	case reduceQuantile:
		return nullScalar(name, dt, mem), nil
	}
	fs := make([]float64, 0, valid)
	for i := range n {
		if hasNulls && a.IsNull(i) {
			continue
		}
		if a.Value(i) {
			fs = append(fs, 1)
		} else {
			fs = append(fs, 0)
		}
	}
	v, ok := floatStat(fs, op, args)
	return floatScalar(name, arrow.PrimitiveTypes.Float64, v, ok, mem)
}

// bytesArray is the common read surface of string and binary arrays.
type bytesArray interface {
	arrow.Array
	ValueStr(int) string
}

func reduceBytes(name string, a arrow.Array, op reduceOp, mem memory.Allocator) (*series.Series, error) {
	dt := a.DataType()
	switch op {
	case reduceProduct:
		return nullScalar(name, arrow.Null, mem), nil
	case reduceMin, reduceMax:
	default:
		return nullScalar(name, dt, mem), nil
	}
	value := bytesAccessor(a)
	best, found := "", false
	for i := range a.Len() {
		if a.IsNull(i) {
			continue
		}
		v := value(i)
		if !found || (op == reduceMin && v < best) || (op == reduceMax && v > best) {
			best = v
			found = true
		}
	}
	if !found {
		return nullScalar(name, dt, mem), nil
	}
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	switch bb := b.(type) {
	case *array.StringBuilder:
		bb.Append(best)
	case *array.LargeStringBuilder:
		bb.Append(best)
	case *array.BinaryBuilder:
		bb.Append([]byte(best))
	}
	arr := b.NewArray()
	return seriesFromArray(name, arr)
}

// bytesAccessor returns a zero-copy string view of element i.
func bytesAccessor(a arrow.Array) func(int) string {
	switch t := a.(type) {
	case *array.String:
		return t.Value
	case *array.LargeString:
		return t.Value
	case *array.Binary:
		return func(i int) string { return unsafeBytesString(t.Value(i)) }
	case *array.LargeBinary:
		return func(i int) string { return unsafeBytesString(t.Value(i)) }
	}
	return a.ValueStr
}

func unsafeBytesString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// reduceTemporal covers date, datetime, duration and time columns.
func reduceTemporal(name string, a arrow.Array, op reduceOp, args reduceArgs, mem memory.Allocator) (*series.Series, error) {
	dt := a.DataType()
	var vals []int64
	isDate := dt.ID() == arrow.DATE32
	switch dt.ID() {
	case arrow.DATE32, arrow.TIME32:
		raw := rawValues[int32](a)
		vals = make([]int64, 0, a.Len()-a.NullN())
		for i, v := range raw {
			if a.IsValid(i) {
				vals = append(vals, int64(v))
			}
		}
	default:
		raw := rawValues[int64](a)
		vals = make([]int64, 0, a.Len()-a.NullN())
		for i, v := range raw {
			if a.IsValid(i) {
				vals = append(vals, v)
			}
		}
	}
	isDuration := dt.ID() == arrow.DURATION
	switch op {
	case reduceSum:
		if !isDuration {
			return nullScalar(name, dt, mem), nil
		}
		var acc int64
		for _, v := range vals {
			acc += v
		}
		return scalarSeries(name, dt, uint64(acc), true, mem)
	case reduceProduct:
		return nullScalar(name, arrow.Null, mem), nil
	case reduceStd, reduceVar:
		return nullScalar(name, dt, mem), nil
	case reduceMin, reduceMax:
		if len(vals) == 0 {
			return nullScalar(name, dt, mem), nil
		}
		best := vals[0]
		for _, v := range vals[1:] {
			if (op == reduceMin && v < best) || (op == reduceMax && v > best) {
				best = v
			}
		}
		return scalarSeries(name, dt, uint64(best), true, mem)
	}
	// mean, median, quantile: dates promote to datetime[us].
	outDT := dt
	scale := 1.0
	if isDate {
		outDT = &arrow.TimestampType{Unit: arrow.Microsecond}
		scale = 86400e6
	}
	fs := make([]float64, len(vals))
	for i, v := range vals {
		fs[i] = float64(v) * scale
	}
	v, ok := floatStat(fs, op, args)
	if !ok || math.IsNaN(v) {
		return nullScalar(name, outDT, mem), nil
	}
	return scalarSeries(name, outDT, uint64(int64(v)), true, mem)
}

// nullScalar returns a one-row all-null Series of dtype dt.
func nullScalar(name string, dt arrow.DataType, mem memory.Allocator) *series.Series {
	arr := array.MakeArrayOfNull(mem, dt, 1)
	s, _ := seriesFromArray(name, arr)
	return s
}

func boolScalar(name string, v bool, mem memory.Allocator) (*series.Series, error) {
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Append(v)
	arr := b.NewArray()
	return seriesFromArray(name, arr)
}

func floatScalar(name string, dt arrow.DataType, v float64, ok bool, mem memory.Allocator) (*series.Series, error) {
	if !ok {
		return nullScalar(name, dt, mem), nil
	}
	switch dt.ID() {
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		defer b.Release()
		b.Append(float32(v))
		arr := b.NewArray()
		return seriesFromArray(name, arr)
	}
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(v)
	arr := b.NewArray()
	return seriesFromArray(name, arr)
}

// scalarSeries builds a one-row Series of an integer-backed dtype from
// the bit pattern bits (sign-extended int64 for signed types).
func scalarSeries(name string, dt arrow.DataType, bits uint64, ok bool, mem memory.Allocator) (*series.Series, error) {
	if !ok {
		return nullScalar(name, dt, mem), nil
	}
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	switch bb := b.(type) {
	case *array.Int8Builder:
		bb.Append(int8(bits))
	case *array.Int16Builder:
		bb.Append(int16(bits))
	case *array.Int32Builder:
		bb.Append(int32(bits))
	case *array.Int64Builder:
		bb.Append(int64(bits))
	case *array.Uint8Builder:
		bb.Append(uint8(bits))
	case *array.Uint16Builder:
		bb.Append(uint16(bits))
	case *array.Uint32Builder:
		bb.Append(uint32(bits))
	case *array.Uint64Builder:
		bb.Append(bits)
	case *array.Date32Builder:
		bb.Append(arrow.Date32(int32(bits)))
	case *array.Date64Builder:
		bb.Append(arrow.Date64(int64(bits)))
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(int64(bits)))
	case *array.DurationBuilder:
		bb.Append(arrow.Duration(int64(bits)))
	case *array.Time32Builder:
		bb.Append(arrow.Time32(int32(bits)))
	case *array.Time64Builder:
		bb.Append(arrow.Time64(int64(bits)))
	default:
		return nil, fmt.Errorf("dataframe: unsupported scalar dtype %s", dt)
	}
	arr := b.NewArray()
	return seriesFromArray(name, arr)
}
