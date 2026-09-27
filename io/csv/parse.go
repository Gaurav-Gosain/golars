package csv

import (
	"strconv"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

// kind is the physical column kind the native reader builds.
type kind uint8

const (
	kInt64 kind = iota
	kBool
	kDate32
	kTime32
	kTsS
	kTsNs
	kTsSUTC
	kTsNsUTC
	kFloat64
	kString
	// kGeneric covers explicit schema types without a dedicated fast
	// path. Values go through the arrow builder's AppendValueFromString.
	kGeneric
)

// inferOrder is the candidate order for type inference. It follows
// arrow-go's csv inference (which golars used before the native reader)
// so existing files keep their dtypes: the first kind that parses every
// sampled value wins.
var inferOrder = [...]kind{kInt64, kBool, kDate32, kTime32, kTsS, kTsNs, kTsSUTC, kTsNsUTC, kFloat64, kString}

func kindArrow(k kind) arrow.DataType {
	switch k {
	case kInt64:
		return arrow.PrimitiveTypes.Int64
	case kBool:
		return arrow.FixedWidthTypes.Boolean
	case kDate32:
		return arrow.FixedWidthTypes.Date32
	case kTime32:
		return arrow.FixedWidthTypes.Time32s
	case kTsS:
		return &arrow.TimestampType{Unit: arrow.Second}
	case kTsNs:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case kTsSUTC:
		return &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}
	case kTsNsUTC:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	case kFloat64:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

// kindFor maps an explicit schema type to the kind that parses it.
func kindFor(dt arrow.DataType) kind {
	switch dt.ID() {
	case arrow.INT64:
		return kInt64
	case arrow.FLOAT64:
		return kFloat64
	case arrow.BOOL:
		return kBool
	case arrow.STRING:
		return kString
	case arrow.DATE32:
		return kDate32
	}
	return kGeneric
}

// isGenericKind reports whether k is built through an arrow builder.
func isGenericKind(k kind) bool {
	switch k {
	case kTime32, kTsS, kTsNs, kTsSUTC, kTsNsUTC, kGeneric:
		return true
	}
	return false
}

// parsesAs reports whether s is a valid value of kind k during inference.
// Booleans are strict here ("1" and "0" stay integers).
func parsesAs(s string, k kind) bool {
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	switch k {
	case kInt64:
		_, ok := parseInt64(b)
		return ok
	case kBool:
		_, ok := parseBool(b, false)
		return ok
	case kDate32:
		_, ok := parseDate32(b)
		return ok
	case kTime32:
		_, err := arrow.Time32FromString(s, arrow.Second)
		return err == nil
	case kTsS, kTsNs, kTsSUTC, kTsNsUTC:
		dt := kindArrow(k).(*arrow.TimestampType)
		loc, err := dt.GetZone()
		if err != nil {
			return false
		}
		_, zonePresent, err := arrow.TimestampFromStringInLocation(s, dt.Unit, loc)
		return err == nil && zonePresent == (dt.TimeZone != "")
	case kFloat64:
		_, ok := parseFloat64(b)
		return ok
	}
	return true
}

func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// parseInt64 parses an optionally signed base-10 integer. Up to 18
// digits cannot overflow and take a tight loop; longer inputs defer to
// strconv for exact overflow handling.
func parseInt64(b []byte) (int64, bool) {
	n := len(b)
	if n == 0 {
		return 0, false
	}
	i := 0
	neg := false
	if c := b[0]; c == '-' || c == '+' {
		neg = c == '-'
		i = 1
		if n == 1 {
			return 0, false
		}
	}
	if n-i > 18 {
		v, err := strconv.ParseInt(unsafeString(b), 10, 64)
		return v, err == nil
	}
	var v int64
	for ; i < n; i++ {
		d := b[i] - '0'
		if d > 9 {
			return 0, false
		}
		v = v*10 + int64(d)
	}
	if neg {
		v = -v
	}
	return v, true
}

// parseBool accepts the strconv.ParseBool spellings. "1" and "0" are
// only accepted when lenient is set (explicit Boolean schema columns).
func parseBool(b []byte, lenient bool) (bool, bool) {
	switch unsafeString(b) {
	case "true", "TRUE", "True", "t", "T":
		return true, true
	case "false", "FALSE", "False", "f", "F":
		return false, true
	case "1":
		return true, lenient
	case "0":
		return false, lenient
	}
	return false, false
}

// parseDate32 parses YYYY-MM-DD into days since the unix epoch, with the
// same validation as time.Parse("2006-01-02", s).
func parseDate32(b []byte) (int32, bool) {
	if len(b) != 10 || b[4] != '-' || b[7] != '-' {
		return 0, false
	}
	y, ok1 := digits(b[0:4])
	m, ok2 := digits(b[5:7])
	d, ok3 := digits(b[8:10])
	if !ok1 || !ok2 || !ok3 || m < 1 || m > 12 || d < 1 || d > daysIn(time.Month(m), y) {
		return 0, false
	}
	return int32(daysFromCivil(y, m, d)), true
}

func digits(b []byte) (int, bool) {
	v := 0
	for _, c := range b {
		d := c - '0'
		if d > 9 {
			return 0, false
		}
		v = v*10 + int(d)
	}
	return v, true
}

func daysIn(m time.Month, year int) int {
	switch m {
	case time.February:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case time.April, time.June, time.September, time.November:
		return 30
	}
	return 31
}

// daysFromCivil converts a proleptic Gregorian date to days since
// 1970-01-01 (Howard Hinnant's algorithm).
func daysFromCivil(y, m, d int) int {
	if m <= 2 {
		y--
	}
	era := y
	if era < 0 {
		era -= 399
	}
	era /= 400
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}
