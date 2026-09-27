// Package difftest is a differential test harness that runs random
// queries through golars and through polars (the oracle) and compares
// the results. See docs/testing.md for how to run it.
//
// A case is a JSON plan (plan.go) plus one or two input frames written
// as Arrow IPC streams. The Go side builds the frames, generates the
// plan, runs it on golars, asks a long-lived Python process
// (bench/polars-compare/difftest/runner.py) to run the same plan on
// polars, and compares the two results with the rules in compare.go.
package difftest

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// Kind is the logical type family of a generated column or expression.
type Kind uint8

const (
	KNull Kind = iota
	KBool
	KInt
	KUint
	KFloat
	KStr
	KDate
	KDatetime
	KDuration
	KTime
	KList
	KStruct
	KCat
)

// Type is a logical dtype in the harness. It is intentionally close to
// the polars dtype lattice so both sides agree on names.
type Type struct {
	K      Kind
	Bits   int    // 8/16/32/64 for ints, 32/64 for floats
	Unit   string // "ms", "us", "ns" for datetime and duration
	TZ     string // datetime time zone, "" for naive
	Elem   *Type  // list element
	Fields []Field
}

// Field is one struct field.
type Field struct {
	Name string
	T    Type
}

var (
	TBool    = Type{K: KBool}
	TI8      = Type{K: KInt, Bits: 8}
	TI16     = Type{K: KInt, Bits: 16}
	TI32     = Type{K: KInt, Bits: 32}
	TI64     = Type{K: KInt, Bits: 64}
	TU8      = Type{K: KUint, Bits: 8}
	TU16     = Type{K: KUint, Bits: 16}
	TU32     = Type{K: KUint, Bits: 32}
	TU64     = Type{K: KUint, Bits: 64}
	TF32     = Type{K: KFloat, Bits: 32}
	TF64     = Type{K: KFloat, Bits: 64}
	TStr     = Type{K: KStr}
	TDate    = Type{K: KDate}
	TTime    = Type{K: KTime}
	TCat     = Type{K: KCat}
	TNull    = Type{K: KNull}
	intTypes = []Type{TI8, TI16, TI32, TI64, TU8, TU16, TU32, TU64}
)

// ListOf returns list[t].
func ListOf(t Type) Type { return Type{K: KList, Elem: &t} }

// Datetime returns datetime[unit, tz].
func Datetime(unit, tz string) Type { return Type{K: KDatetime, Unit: unit, TZ: tz} }

// Duration returns duration[unit].
func Duration(unit string) Type { return Type{K: KDuration, Unit: unit} }

func (t Type) IsInt() bool     { return t.K == KInt || t.K == KUint }
func (t Type) IsFloat() bool   { return t.K == KFloat }
func (t Type) IsNumeric() bool { return t.IsInt() || t.IsFloat() }
func (t Type) IsTemporal() bool {
	return t.K == KDate || t.K == KDatetime || t.K == KDuration || t.K == KTime
}

// Orderable reports whether min/max/sort make sense for t in both
// engines.
func (t Type) Orderable() bool {
	return t.IsNumeric() || t.K == KStr || t.K == KBool || t.IsTemporal()
}

// Name is the canonical dtype name, shared with the Python runner. It
// is also a valid glr cast target for the types glr can express.
func (t Type) Name() string {
	switch t.K {
	case KNull:
		return "null"
	case KBool:
		return "bool"
	case KInt:
		return fmt.Sprintf("i%d", t.Bits)
	case KUint:
		return fmt.Sprintf("u%d", t.Bits)
	case KFloat:
		return fmt.Sprintf("f%d", t.Bits)
	case KStr:
		return "str"
	case KDate:
		return "date"
	case KTime:
		return "time"
	case KCat:
		return "cat"
	case KDatetime:
		if t.TZ == "" {
			return "datetime[" + t.Unit + "]"
		}
		return "datetime[" + t.Unit + ", " + t.TZ + "]"
	case KDuration:
		return "duration[" + t.Unit + "]"
	case KList:
		return "list[" + t.Elem.Name() + "]"
	case KStruct:
		parts := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			parts[i] = f.Name + ": " + f.T.Name()
		}
		return "struct{" + strings.Join(parts, ", ") + "}"
	}
	return "?"
}

// Equal reports structural equality.
func (t Type) Equal(o Type) bool { return t.Name() == o.Name() }

func timeUnit(u string) arrow.TimeUnit {
	switch u {
	case "ms":
		return arrow.Millisecond
	case "ns":
		return arrow.Nanosecond
	case "s":
		return arrow.Second
	}
	return arrow.Microsecond
}

func unitName(u arrow.TimeUnit) string {
	switch u {
	case arrow.Millisecond:
		return "ms"
	case arrow.Nanosecond:
		return "ns"
	case arrow.Second:
		return "s"
	}
	return "us"
}

// Arrow returns the arrow type golars uses for t.
func (t Type) Arrow() arrow.DataType {
	switch t.K {
	case KNull:
		return arrow.Null
	case KBool:
		return arrow.FixedWidthTypes.Boolean
	case KInt:
		switch t.Bits {
		case 8:
			return arrow.PrimitiveTypes.Int8
		case 16:
			return arrow.PrimitiveTypes.Int16
		case 32:
			return arrow.PrimitiveTypes.Int32
		}
		return arrow.PrimitiveTypes.Int64
	case KUint:
		switch t.Bits {
		case 8:
			return arrow.PrimitiveTypes.Uint8
		case 16:
			return arrow.PrimitiveTypes.Uint16
		case 32:
			return arrow.PrimitiveTypes.Uint32
		}
		return arrow.PrimitiveTypes.Uint64
	case KFloat:
		if t.Bits == 32 {
			return arrow.PrimitiveTypes.Float32
		}
		return arrow.PrimitiveTypes.Float64
	case KStr:
		return arrow.BinaryTypes.String
	case KDate:
		return arrow.FixedWidthTypes.Date32
	case KTime:
		return &arrow.Time64Type{Unit: arrow.Nanosecond}
	case KDatetime:
		return &arrow.TimestampType{Unit: timeUnit(t.Unit), TimeZone: t.TZ}
	case KDuration:
		return &arrow.DurationType{Unit: timeUnit(t.Unit)}
	case KList:
		return arrow.ListOf(t.Elem.Arrow())
	case KStruct:
		fs := make([]arrow.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = arrow.Field{Name: f.Name, Type: f.T.Arrow(), Nullable: true}
		}
		return arrow.StructOf(fs...)
	case KCat:
		return &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String}
	}
	panic("difftest: unknown kind")
}

// ArrowName is the canonical dtype name of an arrow type, using the
// same spelling as Type.Name. Large and view variants collapse onto
// their plain form because polars writes those and golars reads them
// back as the same logical type.
func ArrowName(dt arrow.DataType) string {
	switch t := dt.(type) {
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
	case *arrow.Float16Type:
		return "f16"
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
	case *arrow.Time64Type:
		if t.Unit == arrow.Nanosecond {
			return "time"
		}
		return "time64[" + unitName(t.Unit) + "]"
	case *arrow.Time32Type:
		return "time32[" + unitName(t.Unit) + "]"
	case *arrow.TimestampType:
		if t.TimeZone == "" {
			return "datetime[" + unitName(t.Unit) + "]"
		}
		return "datetime[" + unitName(t.Unit) + ", " + t.TimeZone + "]"
	case *arrow.DurationType:
		return "duration[" + unitName(t.Unit) + "]"
	case *arrow.ListType:
		return "list[" + ArrowName(t.Elem()) + "]"
	case *arrow.LargeListType:
		return "list[" + ArrowName(t.Elem()) + "]"
	case *arrow.ListViewType:
		return "list[" + ArrowName(t.Elem()) + "]"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("array[%s, %d]", ArrowName(t.Elem()), t.Len())
	case *arrow.StructType:
		parts := make([]string, t.NumFields())
		for i, f := range t.Fields() {
			parts[i] = f.Name + ": " + ArrowName(f.Type)
		}
		return "struct{" + strings.Join(parts, ", ") + "}"
	case *arrow.DictionaryType:
		if t.Ordered {
			return "enum"
		}
		return "cat"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("decimal[%d, %d]", t.Precision, t.Scale)
	}
	return dt.String()
}
