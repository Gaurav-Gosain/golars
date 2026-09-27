// Package framerepr renders DataFrames and Series into a canonical text
// form used by parity tests. The same format is produced by a small
// polars helper (documented below), so expected values in tests are
// pasted verbatim from polars output.
//
// Format: the first line is "name:dtype" pairs joined by "|"; each
// following line is one row with cells joined by "|". Cells use:
// null for nulls; true/false; decimal integers; floats in shortest
// round-trip form with a trailing ".0" when integral (nan, inf, -inf
// for specials); strings wrapped in double quotes without escaping;
// lists as [a,b]; structs as {f=v,g=w}; dates as YYYY-MM-DD; datetimes
// as YYYY-MM-DDTHH:MM:SS.ffffff; durations as integer microseconds with
// a "us" suffix; times as HH:MM:SS.ffffff.
//
// The polars side of the format:
//
//	def cell(v, dt):
//	    if v is None: return "null"
//	    if dt == pl.Boolean: return "true" if v else "false"
//	    if dt.is_float():
//	        if v != v: return "nan"
//	        if v in (float("inf"), float("-inf")): return "inf" if v > 0 else "-inf"
//	        return repr(float(v))
//	    if dt == pl.String: return '"' + v + '"'
//	    if isinstance(dt, (pl.List, pl.Array)): return "[" + ",".join(cell(x, dt.inner) for x in v) + "]"
//	    if isinstance(dt, pl.Struct): return "{" + ",".join(f.name + "=" + cell(v[f.name], f.dtype) for f in dt.fields) + "}"
//	    if dt == pl.Date: return v.isoformat()
//	    if isinstance(dt, pl.Datetime): return v.strftime("%Y-%m-%dT%H:%M:%S.%f")
//	    if isinstance(dt, pl.Duration): return str(v // timedelta(microseconds=1)) + "us"
//	    if dt == pl.Time: return v.strftime("%H:%M:%S.%f")
//	    return str(v)
package framerepr

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Frame renders df in the canonical parity format.
func Frame(df *dataframe.DataFrame) string {
	var b strings.Builder
	cols := df.Columns()
	for i, c := range cols {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(c.Name())
		b.WriteByte(':')
		b.WriteString(c.DType().String())
	}
	arrs := make([]arrow.Array, len(cols))
	for i, c := range cols {
		arrs[i] = chunk0(c)
	}
	for r := range df.Height() {
		b.WriteByte('\n')
		for i, a := range arrs {
			if i > 0 {
				b.WriteByte('|')
			}
			b.WriteString(Cell(a, r))
		}
	}
	return b.String()
}

// Series renders s as a single-column frame.
func Series(s *series.Series) string {
	var b strings.Builder
	b.WriteString(s.Name())
	b.WriteByte(':')
	b.WriteString(s.DType().String())
	a := chunk0(s)
	for r := range s.Len() {
		b.WriteByte('\n')
		b.WriteString(Cell(a, r))
	}
	return b.String()
}

// Cell renders one value of a.
func Cell(a arrow.Array, i int) string {
	if a.IsNull(i) {
		return "null"
	}
	switch v := a.(type) {
	case *array.Null:
		return "null"
	case *array.Boolean:
		if v.Value(i) {
			return "true"
		}
		return "false"
	case *array.Int8:
		return strconv.FormatInt(int64(v.Value(i)), 10)
	case *array.Int16:
		return strconv.FormatInt(int64(v.Value(i)), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(v.Value(i)), 10)
	case *array.Int64:
		return strconv.FormatInt(v.Value(i), 10)
	case *array.Uint8:
		return strconv.FormatUint(uint64(v.Value(i)), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(v.Value(i)), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(v.Value(i)), 10)
	case *array.Uint64:
		return strconv.FormatUint(v.Value(i), 10)
	case *array.Float32:
		return Float(float64(v.Value(i)))
	case *array.Float64:
		return Float(v.Value(i))
	case *array.String:
		return `"` + v.Value(i) + `"`
	case *array.LargeString:
		return `"` + v.Value(i) + `"`
	case *array.Binary:
		return `b"` + string(v.Value(i)) + `"`
	case *array.Date32:
		return time.Unix(int64(v.Value(i))*86400, 0).UTC().Format("2006-01-02")
	case *array.Timestamp:
		unit := v.DataType().(*arrow.TimestampType).Unit
		return unitTime(int64(v.Value(i)), unit).Format("2006-01-02T15:04:05.000000")
	case *array.Duration:
		unit := v.DataType().(*arrow.DurationType).Unit
		return strconv.FormatInt(toMicros(int64(v.Value(i)), unit), 10) + "us"
	case *array.Time64:
		unit := v.DataType().(*arrow.Time64Type).Unit
		return unitTime(int64(v.Value(i)), unit).Format("15:04:05.000000")
	case *array.Time32:
		unit := v.DataType().(*arrow.Time32Type).Unit
		return unitTime(int64(v.Value(i)), unit).Format("15:04:05.000000")
	case array.ListLike:
		start, end := v.ValueOffsets(i)
		inner := v.ListValues()
		parts := make([]string, 0, end-start)
		for j := start; j < end; j++ {
			parts = append(parts, Cell(inner, int(j)))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *array.Struct:
		st := v.DataType().(*arrow.StructType)
		parts := make([]string, st.NumFields())
		for f := range st.NumFields() {
			parts[f] = st.Field(f).Name + "=" + Cell(v.Field(f), i)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return a.ValueStr(i)
}

// Float formats v the way Python's repr does for the common range.
func Float(v float64) string {
	switch {
	case math.IsNaN(v):
		return "nan"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(v, 'g', -1, 64)
	if !strings.ContainsAny(s, ".e") {
		s += ".0"
	}
	return s
}

func toMicros(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1_000
	case arrow.Nanosecond:
		return v / 1_000
	}
	return v
}

func unitTime(v int64, unit arrow.TimeUnit) time.Time {
	switch unit {
	case arrow.Second:
		return time.Unix(v, 0).UTC()
	case arrow.Millisecond:
		return time.UnixMilli(v).UTC()
	case arrow.Nanosecond:
		return time.Unix(0, v).UTC()
	}
	return time.UnixMicro(v).UTC()
}

// chunk0 returns the first chunk, or an empty array for a chunkless Series.
func chunk0(s *series.Series) arrow.Array {
	if s.NumChunks() == 0 {
		return array.MakeArrayOfNull(memory.DefaultAllocator, s.DType().Arrow(), 0)
	}
	return s.Chunk(0)
}
