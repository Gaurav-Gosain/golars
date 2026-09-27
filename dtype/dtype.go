// Package dtype defines the logical data types used by golars.
//
// A DType is a thin wrapper over arrow.DataType with a polars-style string
// representation and classification helpers. The zero value is invalid;
// callers construct a DType through one of the package constructors.
package dtype

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// TimeUnit is the resolution of a temporal value.
type TimeUnit int8

const (
	Second TimeUnit = iota
	Millisecond
	Microsecond
	Nanosecond
)

// String returns the short polars-style name (s, ms, us, ns).
func (u TimeUnit) String() string {
	switch u {
	case Second:
		return "s"
	case Millisecond:
		return "ms"
	case Microsecond:
		return "us"
	case Nanosecond:
		return "ns"
	}
	return fmt.Sprintf("TimeUnit(%d)", int8(u))
}

// toArrow maps to arrow-go's TimeUnit enum.
func (u TimeUnit) toArrow() arrow.TimeUnit {
	switch u {
	case Second:
		return arrow.Second
	case Millisecond:
		return arrow.Millisecond
	case Microsecond:
		return arrow.Microsecond
	case Nanosecond:
		return arrow.Nanosecond
	}
	panic(fmt.Sprintf("dtype: unknown TimeUnit %d", u))
}

// timeUnitFromArrow maps arrow-go's TimeUnit back.
func timeUnitFromArrow(u arrow.TimeUnit) TimeUnit {
	switch u {
	case arrow.Second:
		return Second
	case arrow.Millisecond:
		return Millisecond
	case arrow.Microsecond:
		return Microsecond
	case arrow.Nanosecond:
		return Nanosecond
	}
	panic(fmt.Sprintf("dtype: unknown arrow.TimeUnit %d", u))
}

// DType is the logical type of a Series column.
//
// A DType is immutable and inexpensive to copy. Two DType values are equal
// when Equal returns true; they are not required to be == comparable.
type DType struct {
	inner arrow.DataType
	// enum carries the fixed categories of a dtype built by Enum. It is
	// nil for every other dtype, including an Enum dtype read back from
	// an arrow array (the categories then live in the array dictionary).
	enum *enumInfo
}

// FromArrow wraps an existing arrow.DataType into a DType.
// The input must not be nil.
func FromArrow(t arrow.DataType) DType {
	if t == nil {
		panic("dtype: FromArrow received nil arrow.DataType")
	}
	return DType{inner: t}
}

// Arrow returns the underlying arrow.DataType.
func (d DType) Arrow() arrow.DataType { return d.inner }

// IsValid reports whether this DType was constructed through a package constructor.
func (d DType) IsValid() bool { return d.inner != nil }

// ID returns the arrow type identifier.
func (d DType) ID() arrow.Type {
	if d.inner == nil {
		return arrow.NULL
	}
	return d.inner.ID()
}

// Equal reports structural equality. Metadata on nested fields is ignored.
func (d DType) Equal(other DType) bool {
	if d.inner == nil || other.inner == nil {
		return d.inner == nil && other.inner == nil
	}
	if d.enum != nil && other.enum != nil && !d.enum.equal(other.enum) {
		return false
	}
	return arrow.TypeEqual(d.inner, other.inner)
}

// String returns a polars-style representation: i64, f32, str, bool,
// datetime[us, UTC], list[i32], and so on.
func (d DType) String() string {
	if d.inner == nil {
		return "<invalid>"
	}
	return reprArrow(d.inner)
}

// Classification helpers. These mirror the predicates polars exposes on its
// own DataType enum.

func (d DType) IsNull() bool     { return d.ID() == arrow.NULL }
func (d DType) IsBool() bool     { return d.ID() == arrow.BOOL }
func (d DType) IsInteger() bool  { return arrow.IsInteger(d.ID()) }
func (d DType) IsFloating() bool { return arrow.IsFloating(d.ID()) }
func (d DType) IsNumeric() bool  { return d.IsInteger() || d.IsFloating() }
func (d DType) IsString() bool {
	id := d.ID()
	return id == arrow.STRING || id == arrow.LARGE_STRING || id == arrow.STRING_VIEW
}
func (d DType) IsBinary() bool { return arrow.IsBinaryLike(d.ID()) && !d.IsString() }
func (d DType) IsTemporal() bool {
	switch d.ID() {
	case arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.TIME32, arrow.TIME64, arrow.DURATION:
		return true
	}
	return false
}
func (d DType) IsNested() bool    { return arrow.IsNested(d.ID()) }
func (d DType) IsStruct() bool    { return d.ID() == arrow.STRUCT }
func (d DType) IsList() bool      { return d.ID() == arrow.LIST || d.ID() == arrow.LARGE_LIST }
func (d DType) IsFixedList() bool { return d.ID() == arrow.FIXED_SIZE_LIST }

// Null returns the dtype representing an all-null column of unknown logical type.
func Null() DType { return DType{inner: arrow.Null} }

// Bool returns the boolean dtype.
func Bool() DType { return DType{inner: arrow.FixedWidthTypes.Boolean} }

// Signed integer dtypes.
func Int8() DType  { return DType{inner: arrow.PrimitiveTypes.Int8} }
func Int16() DType { return DType{inner: arrow.PrimitiveTypes.Int16} }
func Int32() DType { return DType{inner: arrow.PrimitiveTypes.Int32} }
func Int64() DType { return DType{inner: arrow.PrimitiveTypes.Int64} }

// Unsigned integer dtypes.
func Uint8() DType  { return DType{inner: arrow.PrimitiveTypes.Uint8} }
func Uint16() DType { return DType{inner: arrow.PrimitiveTypes.Uint16} }
func Uint32() DType { return DType{inner: arrow.PrimitiveTypes.Uint32} }
func Uint64() DType { return DType{inner: arrow.PrimitiveTypes.Uint64} }

// Floating-point dtypes.
func Float32() DType { return DType{inner: arrow.PrimitiveTypes.Float32} }
func Float64() DType { return DType{inner: arrow.PrimitiveTypes.Float64} }

// String returns the UTF-8 string dtype (arrow STRING, not STRING_VIEW).
func String() DType { return DType{inner: arrow.BinaryTypes.String} }

// Binary returns the opaque binary dtype.
func Binary() DType { return DType{inner: arrow.BinaryTypes.Binary} }

// Date returns the date-only dtype (days since epoch, 32-bit).
func Date() DType { return DType{inner: arrow.FixedWidthTypes.Date32} }

// Datetime returns the timestamp dtype at the given resolution. tz may be
// empty for a time-zone-naive timestamp.
func Datetime(unit TimeUnit, tz string) DType {
	return DType{inner: &arrow.TimestampType{Unit: unit.toArrow(), TimeZone: tz}}
}

// Duration returns the elapsed-time dtype at the given resolution.
func Duration(unit TimeUnit) DType {
	return DType{inner: &arrow.DurationType{Unit: unit.toArrow()}}
}

// Time returns the time-of-day dtype at the given resolution. Second
// and Millisecond resolutions map to Time32; Microsecond and
// Nanosecond map to Time64, matching arrow's storage rules.
func Time(unit TimeUnit) DType {
	switch unit {
	case Second, Millisecond:
		return DType{inner: &arrow.Time32Type{Unit: unit.toArrow()}}
	default:
		return DType{inner: &arrow.Time64Type{Unit: unit.toArrow()}}
	}
}

// List returns the variable-length list dtype with the given inner type.
func List(inner DType) DType {
	if !inner.IsValid() {
		panic("dtype: List received invalid inner dtype")
	}
	return DType{inner: arrow.ListOf(inner.inner)}
}

// FixedList returns the fixed-size list dtype with the given inner type and size.
func FixedList(inner DType, size int) DType {
	if !inner.IsValid() {
		panic("dtype: FixedList received invalid inner dtype")
	}
	if size <= 0 {
		panic(fmt.Sprintf("dtype: FixedList size must be positive, got %d", size))
	}
	return DType{inner: arrow.FixedSizeListOf(int32(size), inner.inner)}
}

// Struct returns the struct dtype with the given named fields. Field order is
// significant.
func Struct(fields ...StructField) DType {
	if len(fields) == 0 {
		panic("dtype: Struct requires at least one field")
	}
	arrowFields := make([]arrow.Field, len(fields))
	for i, f := range fields {
		if !f.DType.IsValid() {
			panic(fmt.Sprintf("dtype: Struct field %q has invalid dtype", f.Name))
		}
		arrowFields[i] = arrow.Field{Name: f.Name, Type: f.DType.inner, Nullable: true}
	}
	return DType{inner: arrow.StructOf(arrowFields...)}
}

// StructField names a field inside a struct dtype.
type StructField struct {
	Name  string
	DType DType
}

// reprArrow produces a polars-style short string for an arrow DataType.
func reprArrow(t arrow.DataType) string {
	switch t := t.(type) {
	case *arrow.NullType:
		return "null"
	case *arrow.BooleanType:
		return "bool"
	case *arrow.Int8Type:
		return "i8"
	case *arrow.Int16Type:
		return "i16"
	case *arrow.Int32Type:
		return "i32"
	case *arrow.Int64Type:
		return "i64"
	case *arrow.Uint8Type:
		return "u8"
	case *arrow.Uint16Type:
		return "u16"
	case *arrow.Uint32Type:
		return "u32"
	case *arrow.Uint64Type:
		return "u64"
	case *arrow.Float32Type:
		return "f32"
	case *arrow.Float64Type:
		return "f64"
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return "str"
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		return "binary"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "date"
	case *arrow.TimestampType:
		if t.TimeZone == "" {
			return fmt.Sprintf("datetime[%s]", timeUnitFromArrow(t.Unit))
		}
		return fmt.Sprintf("datetime[%s, %s]", timeUnitFromArrow(t.Unit), t.TimeZone)
	case *arrow.DurationType:
		return fmt.Sprintf("duration[%s]", timeUnitFromArrow(t.Unit))
	case *arrow.Time32Type:
		return fmt.Sprintf("time32[%s]", timeUnitFromArrow(t.Unit))
	case *arrow.Time64Type:
		return fmt.Sprintf("time64[%s]", timeUnitFromArrow(t.Unit))
	case *arrow.ListType:
		return fmt.Sprintf("list[%s]", reprArrow(t.Elem()))
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("list[%s; %d]", reprArrow(t.Elem()), t.Len())
	case *arrow.DictionaryType:
		return reprDictionary(t)
	case *arrow.StructType:
		var b strings.Builder
		b.WriteString("struct{")
		for i, f := range t.Fields() {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(f.Name)
			b.WriteString(": ")
			b.WriteString(reprArrow(f.Type))
		}
		b.WriteByte('}')
		return b.String()
	}
	return t.String()
}
