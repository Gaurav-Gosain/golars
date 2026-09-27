package testutil

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// PyRepr renders an arrow array the way Python prints
// `repr(polars_series.to_list())`: lists as [...], structs as dicts,
// nulls as None, strings in Python quoting. Parity tests compare this
// against output captured from polars, which keeps expected values
// for nested results readable and exact.
func PyRepr(a arrow.Array) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := range a.Len() {
		if i > 0 {
			b.WriteString(", ")
		}
		pyValue(&b, a, i)
	}
	b.WriteByte(']')
	return b.String()
}

func pyValue(b *strings.Builder, a arrow.Array, i int) {
	if a.IsNull(i) {
		b.WriteString("None")
		return
	}
	switch v := a.(type) {
	case *array.Boolean:
		if v.Value(i) {
			b.WriteString("True")
		} else {
			b.WriteString("False")
		}
	case *array.Int8:
		b.WriteString(strconv.FormatInt(int64(v.Value(i)), 10))
	case *array.Int16:
		b.WriteString(strconv.FormatInt(int64(v.Value(i)), 10))
	case *array.Int32:
		b.WriteString(strconv.FormatInt(int64(v.Value(i)), 10))
	case *array.Int64:
		b.WriteString(strconv.FormatInt(v.Value(i), 10))
	case *array.Uint8:
		b.WriteString(strconv.FormatUint(uint64(v.Value(i)), 10))
	case *array.Uint16:
		b.WriteString(strconv.FormatUint(uint64(v.Value(i)), 10))
	case *array.Uint32:
		b.WriteString(strconv.FormatUint(uint64(v.Value(i)), 10))
	case *array.Uint64:
		b.WriteString(strconv.FormatUint(v.Value(i), 10))
	case *array.Float32:
		b.WriteString(pyFloat(float64(v.Value(i)), 32))
	case *array.Float64:
		b.WriteString(pyFloat(v.Value(i), 64))
	case *array.String:
		b.WriteString(PyStr(v.Value(i)))
	case *array.LargeString:
		b.WriteString(PyStr(v.Value(i)))
	case *array.Binary:
		b.WriteString(pyBytes(v.Value(i)))
	case *array.List:
		start, end := v.ValueOffsets(i)
		pyRange(b, v.ListValues(), int(start), int(end))
	case *array.LargeList:
		start, end := v.ValueOffsets(i)
		pyRange(b, v.ListValues(), int(start), int(end))
	case *array.FixedSizeList:
		start, end := v.ValueOffsets(i)
		pyRange(b, v.ListValues(), int(start), int(end))
	case *array.Struct:
		st := v.DataType().(*arrow.StructType)
		b.WriteByte('{')
		for f := range st.NumFields() {
			if f > 0 {
				b.WriteString(", ")
			}
			b.WriteString(PyStr(st.Field(f).Name))
			b.WriteString(": ")
			pyValue(b, v.Field(f), i)
		}
		b.WriteByte('}')
	case *array.Dictionary:
		pyValue(b, v.Dictionary(), v.GetValueIndex(i))
	default:
		fmt.Fprintf(b, "%v", a.GetOneForMarshal(i))
	}
}

func pyRange(b *strings.Builder, values arrow.Array, start, end int) {
	b.WriteByte('[')
	for j := start; j < end; j++ {
		if j > start {
			b.WriteString(", ")
		}
		pyValue(b, values, j)
	}
	b.WriteByte(']')
}

// pyFloat mirrors Python's float repr: shortest round-trip digits,
// scientific notation outside 1e-4 <= |x| < 1e16, and a trailing ".0"
// on integral values.
func pyFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	if bits == 32 {
		// Python widens f32 to a double before printing.
		bits = 64
	}
	e := strconv.FormatFloat(f, 'e', -1, bits)
	mant, expStr, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expStr)
	if f != 0 && (exp < -4 || exp >= 16) {
		sign := "+"
		if exp < 0 {
			sign = "-"
			exp = -exp
		}
		return fmt.Sprintf("%se%s%02d", mant, sign, exp)
	}
	s := strconv.FormatFloat(f, 'f', -1, bits)
	if !strings.ContainsAny(s, ".") {
		s += ".0"
	}
	return s
}

// PyStr renders s with Python's str repr quoting rules.
func PyStr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r >= 0x80 && !unicode.IsPrint(r) && !unicode.In(r, unicode.Mn, unicode.Me, unicode.Mc):
			switch {
			case r <= 0xff:
				fmt.Fprintf(&b, `\x%02x`, r)
			case r <= 0xffff:
				fmt.Fprintf(&b, `\u%04x`, r)
			default:
				fmt.Fprintf(&b, `\U%08x`, r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

func pyBytes(p []byte) string {
	var b strings.Builder
	b.WriteString("b'")
	for _, c := range p {
		switch {
		case c == '\'' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
