package series

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// JSONEncode serialises each struct row to a JSON object string.
// Mirrors polars' struct.json_encode: keys follow the struct's field
// order, a null struct row becomes the text "null" (not a null
// string), floats use polars' JSON float formatting (2.0 stays "2.0",
// NaN and infinities become null), dates render as "YYYY-MM-DD" and
// datetimes as "YYYY-MM-DD HH:MM:SS[.fff]" (RFC 3339 with an offset
// when a time zone is set).
//
// Supported field dtypes: booleans, integers, floats, strings,
// dates, datetimes, lists, fixed-size lists and nested structs.
// Other dtypes are an error.
func (o StructOps) JSONEncode(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if !o.s.DType().IsStruct() {
		return nil, fmt.Errorf("series.struct.json_encode: %q has dtype %s (need struct)",
			o.s.Name(), o.s.DType())
	}
	if err := jsonCheckEncodable(o.s.DType().Arrow()); err != nil {
		return nil, err
	}
	a := binChunk(o.s, cfg.alloc)
	defer a.Release()
	n := a.Len()
	out := newBinOut(cfg.alloc, n, 0)
	w := jsonWriter{}
	for i := range n {
		w.buf = w.value(w.buf[:0], a, i)
		out.appendBytes(w.buf)
		out.endRow()
	}
	return New(o.s.Name(), out.finish(arrow.BinaryTypes.String))
}

func jsonCheckEncodable(dt arrow.DataType) error {
	switch t := dt.(type) {
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if err := jsonCheckEncodable(f.Type); err != nil {
				return err
			}
		}
		return nil
	case *arrow.ListType:
		return jsonCheckEncodable(t.Elem())
	case *arrow.LargeListType:
		return jsonCheckEncodable(t.Elem())
	case *arrow.FixedSizeListType:
		return jsonCheckEncodable(t.Elem())
	}
	switch dt.ID() {
	case arrow.NULL, arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING,
		arrow.DATE32, arrow.TIMESTAMP:
		return nil
	}
	return fmt.Errorf("series.struct.json_encode: unsupported field dtype %s", dt)
}

type jsonWriter struct {
	buf []byte
	tz  map[string]*time.Location
}

// value appends the JSON form of a[i] to dst.
func (w *jsonWriter) value(dst []byte, a arrow.Array, i int) []byte {
	if a.IsNull(i) {
		return append(dst, "null"...)
	}
	switch t := a.(type) {
	case *array.Struct:
		st := t.DataType().(*arrow.StructType)
		dst = append(dst, '{')
		for j, f := range st.Fields() {
			if j > 0 {
				dst = append(dst, ',')
			}
			dst = jsonAppendEscaped(dst, f.Name)
			dst = append(dst, ':')
			dst = w.value(dst, t.Field(j), i)
		}
		return append(dst, '}')
	case *array.List:
		s, e := t.ValueOffsets(i)
		return w.list(dst, t.ListValues(), int(s), int(e))
	case *array.LargeList:
		s, e := t.ValueOffsets(i)
		return w.list(dst, t.ListValues(), int(s), int(e))
	case *array.FixedSizeList:
		s, e := t.ValueOffsets(i)
		return w.list(dst, t.ListValues(), int(s), int(e))
	case *array.Boolean:
		if t.Value(i) {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case *array.Int8:
		return strconv.AppendInt(dst, int64(t.Value(i)), 10)
	case *array.Int16:
		return strconv.AppendInt(dst, int64(t.Value(i)), 10)
	case *array.Int32:
		return strconv.AppendInt(dst, int64(t.Value(i)), 10)
	case *array.Int64:
		return strconv.AppendInt(dst, t.Value(i), 10)
	case *array.Uint8:
		return strconv.AppendUint(dst, uint64(t.Value(i)), 10)
	case *array.Uint16:
		return strconv.AppendUint(dst, uint64(t.Value(i)), 10)
	case *array.Uint32:
		return strconv.AppendUint(dst, uint64(t.Value(i)), 10)
	case *array.Uint64:
		return strconv.AppendUint(dst, t.Value(i), 10)
	case *array.Float32:
		return jsonAppendFloatOrNull(dst, float64(t.Value(i)), 32)
	case *array.Float64:
		return jsonAppendFloatOrNull(dst, t.Value(i), 64)
	case *array.String:
		return jsonAppendEscaped(dst, t.Value(i))
	case *array.LargeString:
		return jsonAppendEscaped(dst, t.Value(i))
	case *array.Date32:
		d := time.Unix(int64(t.Value(i))*86400, 0).UTC()
		dst = append(dst, '"')
		dst = d.AppendFormat(dst, "2006-01-02")
		return append(dst, '"')
	case *array.Timestamp:
		return w.timestamp(dst, t, i)
	}
	return append(dst, "null"...)
}

func (w *jsonWriter) list(dst []byte, vals arrow.Array, s, e int) []byte {
	dst = append(dst, '[')
	for k := s; k < e; k++ {
		if k > s {
			dst = append(dst, ',')
		}
		dst = w.value(dst, vals, k)
	}
	return append(dst, ']')
}

func (w *jsonWriter) timestamp(dst []byte, a *array.Timestamp, i int) []byte {
	tt := a.DataType().(*arrow.TimestampType)
	v := int64(a.Value(i))
	var ts time.Time
	switch tt.Unit {
	case arrow.Second:
		ts = time.Unix(v, 0)
	case arrow.Millisecond:
		ts = time.UnixMilli(v)
	case arrow.Microsecond:
		ts = time.UnixMicro(v)
	default:
		ts = time.Unix(0, v)
	}
	ts = ts.UTC()
	dst = append(dst, '"')
	if tt.TimeZone == "" {
		dst = ts.AppendFormat(dst, "2006-01-02 15:04:05")
		dst = jsonAppendFraction(dst, ts.Nanosecond())
		return append(dst, '"')
	}
	if w.tz == nil {
		w.tz = map[string]*time.Location{}
	}
	loc, ok := w.tz[tt.TimeZone]
	if !ok {
		var err error
		if loc, err = time.LoadLocation(tt.TimeZone); err != nil {
			loc = time.UTC
		}
		w.tz[tt.TimeZone] = loc
	}
	ts = ts.In(loc)
	dst = ts.AppendFormat(dst, "2006-01-02T15:04:05")
	dst = jsonAppendFraction(dst, ts.Nanosecond())
	dst = ts.AppendFormat(dst, "-07:00")
	return append(dst, '"')
}

// jsonAppendFraction mirrors chrono's "%.f": nothing for whole
// seconds, otherwise 3, 6 or 9 digits, whichever is the shortest
// exact form.
func jsonAppendFraction(dst []byte, ns int) []byte {
	switch {
	case ns == 0:
		return dst
	case ns%1_000_000 == 0:
		return jsonAppendPadded(append(dst, '.'), ns/1_000_000, 3)
	case ns%1_000 == 0:
		return jsonAppendPadded(append(dst, '.'), ns/1_000, 6)
	}
	return jsonAppendPadded(append(dst, '.'), ns, 9)
}

func jsonAppendPadded(dst []byte, v, width int) []byte {
	var tmp [10]byte
	s := strconv.AppendInt(tmp[:0], int64(v), 10)
	for range width - len(s) {
		dst = append(dst, '0')
	}
	return append(dst, s...)
}

func jsonAppendFloatOrNull(dst []byte, f float64, bits int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(dst, "null"...)
	}
	return jsonAppendFloat(dst, f, bits)
}
