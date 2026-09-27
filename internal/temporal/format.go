package temporal

import (
	"fmt"
	"strconv"
)

// itemKind enumerates the chrono format specifiers golars supports.
type itemKind uint8

const (
	itLiteral itemKind = iota
	itSpace
	// numeric
	itYear
	itYearDiv100
	itYearMod100
	itIsoYear
	itIsoYearDiv100
	itIsoYearMod100
	itQuarter
	itMonth
	itDay
	itWeekFromSun
	itWeekFromMon
	itIsoWeek
	itNumDaysFromSun
	itWeekdayFromMon
	itOrdinal
	itHour
	itHour12
	itMinute
	itSecond
	itNanosecond
	itTimestamp
	// fixed
	itShortMonth
	itLongMonth
	itShortWeekday
	itLongWeekday
	itLowerAmPm
	itUpperAmPm
	itFracAuto
	itFrac3
	itFrac6
	itFrac9
	itFrac3NoDot
	itFrac6NoDot
	itFrac9NoDot
	itTZName
	itTZOffset
	itTZOffsetColon
	itTZOffsetDoubleColon
	itTZOffsetTripleColon
	itTZOffsetPermissive
	itRFC3339
)

// Padding modes of numeric items.
const (
	padZero  byte = '0'
	padSpace byte = '_'
	padNone  byte = '-'
)

type item struct {
	kind itemKind
	pad  byte
	lit  string
}

// Format is a compiled chrono-style format string (the dialect polars
// uses for strftime and strptime).
type Format struct {
	items []item
	// Summary flags used by parsers and callers.
	HasTime, HasDate, HasOffset, HasHour, HasMinute bool
	// FracDigits is 3, 6 or 9 when the format pins the fractional width
	// (used to infer the output time unit of strptime), else 0.
	FracDigits int
	src        string
	// lenientFrac lets %.3f / %.6f / %.9f accept any number of digits
	// (chrono's rule, used by format inference). Explicit user formats
	// require the exact width, like polars.
	lenientFrac bool
}

// Source returns the original format string.
func (fm *Format) Source() string { return fm.src }

func num(k itemKind, pad byte) item { return item{kind: k, pad: pad} }

// CompileFormat parses a chrono format string.
func CompileFormat(s string) (*Format, error) {
	f := &Format{src: s}
	i := 0
	add := func(its ...item) { f.items = append(f.items, its...) }
	for i < len(s) {
		c := s[i]
		switch {
		case c == '%':
			i++
			if i >= len(s) {
				return nil, fmt.Errorf("invalid format string %q: trailing '%%'", s)
			}
			spec := s[i]
			var padOverride byte
			alternate := false
			switch spec {
			case '-', '0', '_':
				padOverride = spec
				if spec == '0' {
					padOverride = padZero
				}
			case '#':
				alternate = true
			}
			if padOverride != 0 || alternate {
				i++
				if i >= len(s) {
					return nil, fmt.Errorf("invalid format string %q", s)
				}
				spec = s[i]
			}
			i++
			start := len(f.items)
			switch spec {
			case 'A':
				add(item{kind: itLongWeekday})
			case 'B':
				add(item{kind: itLongMonth})
			case 'C':
				add(num(itYearDiv100, padZero))
			case 'D', 'x':
				add(num(itMonth, padZero), item{kind: itLiteral, lit: "/"}, num(itDay, padZero), item{kind: itLiteral, lit: "/"}, num(itYearMod100, padZero))
			case 'F':
				add(num(itYear, padZero), item{kind: itLiteral, lit: "-"}, num(itMonth, padZero), item{kind: itLiteral, lit: "-"}, num(itDay, padZero))
			case 'G':
				add(num(itIsoYear, padZero))
			case 'H':
				add(num(itHour, padZero))
			case 'I':
				add(num(itHour12, padZero))
			case 'M':
				add(num(itMinute, padZero))
			case 'P':
				add(item{kind: itLowerAmPm})
			case 'R':
				add(num(itHour, padZero), item{kind: itLiteral, lit: ":"}, num(itMinute, padZero))
			case 'S':
				add(num(itSecond, padZero))
			case 'T', 'X':
				add(num(itHour, padZero), item{kind: itLiteral, lit: ":"}, num(itMinute, padZero), item{kind: itLiteral, lit: ":"}, num(itSecond, padZero))
			case 'U':
				add(num(itWeekFromSun, padZero))
			case 'V':
				add(num(itIsoWeek, padZero))
			case 'W':
				add(num(itWeekFromMon, padZero))
			case 'Y':
				add(num(itYear, padZero))
			case 'Z':
				add(item{kind: itTZName})
			case 'a':
				add(item{kind: itShortWeekday})
			case 'b', 'h':
				add(item{kind: itShortMonth})
			case 'c':
				add(item{kind: itShortWeekday}, item{kind: itSpace, lit: " "}, item{kind: itShortMonth}, item{kind: itSpace, lit: " "},
					num(itDay, padSpace), item{kind: itSpace, lit: " "}, num(itHour, padZero), item{kind: itLiteral, lit: ":"},
					num(itMinute, padZero), item{kind: itLiteral, lit: ":"}, num(itSecond, padZero), item{kind: itSpace, lit: " "}, num(itYear, padZero))
			case 'd':
				add(num(itDay, padZero))
			case 'e':
				add(num(itDay, padSpace))
			case 'f':
				add(num(itNanosecond, padZero))
			case 'g':
				add(num(itIsoYearMod100, padZero))
			case 'j':
				add(num(itOrdinal, padZero))
			case 'k':
				add(num(itHour, padSpace))
			case 'l':
				add(num(itHour12, padSpace))
			case 'm':
				add(num(itMonth, padZero))
			case 'n':
				add(item{kind: itSpace, lit: "\n"})
			case 'p':
				add(item{kind: itUpperAmPm})
			case 'q':
				add(num(itQuarter, padNone))
			case 'r':
				add(num(itHour12, padZero), item{kind: itLiteral, lit: ":"}, num(itMinute, padZero), item{kind: itLiteral, lit: ":"},
					num(itSecond, padZero), item{kind: itSpace, lit: " "}, item{kind: itUpperAmPm})
			case 's':
				add(num(itTimestamp, padNone))
			case 't':
				add(item{kind: itSpace, lit: "\t"})
			case 'u':
				add(num(itWeekdayFromMon, padNone))
			case 'v':
				add(num(itDay, padSpace), item{kind: itLiteral, lit: "-"}, item{kind: itShortMonth}, item{kind: itLiteral, lit: "-"}, num(itYear, padZero))
			case 'w':
				add(num(itNumDaysFromSun, padNone))
			case 'y':
				add(num(itYearMod100, padZero))
			case 'z':
				if alternate {
					add(item{kind: itTZOffsetPermissive})
				} else {
					add(item{kind: itTZOffset})
				}
			case '+':
				add(item{kind: itRFC3339})
			case ':':
				switch {
				case len(s) >= i+3 && s[i:i+3] == "::z":
					i += 3
					add(item{kind: itTZOffsetTripleColon})
				case len(s) >= i+2 && s[i:i+2] == ":z":
					i += 2
					add(item{kind: itTZOffsetDoubleColon})
				case len(s) >= i+1 && s[i] == 'z':
					i++
					add(item{kind: itTZOffsetColon})
				default:
					return nil, fmt.Errorf("invalid format string %q", s)
				}
			case '.':
				switch {
				case len(s) >= i+2 && s[i:i+2] == "3f":
					i += 2
					add(item{kind: itFrac3})
				case len(s) >= i+2 && s[i:i+2] == "6f":
					i += 2
					add(item{kind: itFrac6})
				case len(s) >= i+2 && s[i:i+2] == "9f":
					i += 2
					add(item{kind: itFrac9})
				case len(s) >= i+1 && s[i] == 'f':
					i++
					add(item{kind: itFracAuto})
				default:
					return nil, fmt.Errorf("invalid format string %q", s)
				}
			case '3', '6', '9':
				if i >= len(s) || s[i] != 'f' {
					return nil, fmt.Errorf("invalid format string %q", s)
				}
				i++
				switch spec {
				case '3':
					add(item{kind: itFrac3NoDot})
				case '6':
					add(item{kind: itFrac6NoDot})
				default:
					add(item{kind: itFrac9NoDot})
				}
			case '%':
				add(item{kind: itLiteral, lit: "%"})
			default:
				return nil, fmt.Errorf("invalid format string %q: unknown specifier %%%c", s, spec)
			}
			if padOverride != 0 {
				if len(f.items)-start != 1 || f.items[start].kind < itYear || f.items[start].kind > itTimestamp {
					return nil, fmt.Errorf("invalid format string %q: padding modifier on non-numeric specifier", s)
				}
				f.items[start].pad = padOverride
			}
		case isSpace(c):
			j := i
			for j < len(s) && isSpace(s[j]) {
				j++
			}
			add(item{kind: itSpace, lit: s[i:j]})
			i = j
		default:
			j := i
			for j < len(s) && s[j] != '%' && !isSpace(s[j]) {
				j++
			}
			add(item{kind: itLiteral, lit: s[i:j]})
			i = j
		}
	}
	for _, it := range f.items {
		switch it.kind {
		case itYear, itYearDiv100, itYearMod100, itIsoYear, itIsoYearDiv100, itIsoYearMod100, itQuarter,
			itMonth, itDay, itWeekFromSun, itWeekFromMon, itIsoWeek, itNumDaysFromSun, itWeekdayFromMon,
			itOrdinal, itShortMonth, itLongMonth, itShortWeekday, itLongWeekday:
			f.HasDate = true
		case itHour, itHour12:
			f.HasTime, f.HasHour = true, true
		case itMinute:
			f.HasTime, f.HasMinute = true, true
		case itSecond, itNanosecond, itLowerAmPm, itUpperAmPm, itFracAuto:
			f.HasTime = true
		case itFrac3, itFrac3NoDot:
			f.HasTime = true
			f.FracDigits = 3
		case itFrac6, itFrac6NoDot:
			f.HasTime = true
			f.FracDigits = 6
		case itFrac9, itFrac9NoDot:
			f.HasTime = true
			f.FracDigits = 9
		case itTZOffset, itTZOffsetColon, itTZOffsetDoubleColon, itTZOffsetTripleColon, itTZOffsetPermissive:
			f.HasOffset = true
		case itRFC3339:
			f.HasDate, f.HasTime, f.HasHour, f.HasMinute, f.HasOffset = true, true, true, true, true
		case itTimestamp:
			f.HasDate, f.HasTime, f.HasHour, f.HasMinute = true, true, true, true
		}
	}
	return f, nil
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

var (
	shortMonths   = [12]string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
	longMonths    = [12]string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}
	shortWeekdays = [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	longWeekdays  = [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
)

// Fields is the broken-down value handed to Format.Append.
type Fields struct {
	Civil
	// Days is the day number since the epoch (for weekday math).
	Days int64
	// Unix is seconds since the epoch of the UTC instant (for %s).
	Unix int64
	// Offset is the UTC offset in seconds; Abbr the zone abbreviation.
	Offset int
	Abbr   string
	HasTZ  bool
}

// set fills the calendar fields from a day number and the nanoseconds
// elapsed within that day.
func (f *Fields) set(days, nsOfDay int64) {
	f.Days = days
	y, m, d := CivilFromDays(days)
	sec := nsOfDay / NsSecond
	f.Year, f.Month, f.Day = y, m, d
	f.Hour = int(sec / 3600)
	f.Minute = int(sec / 60 % 60)
	f.Second = int(sec % 60)
	f.Nanosecond = int(nsOfDay % NsSecond)
}

// SetDays fills f for a date value.
func (f *Fields) SetDays(days int64) {
	f.set(days, 0)
	f.Unix = days * SecondsPerDay
}

// SetLocal fills f from a naive wall-clock tick value; utc is the
// matching UTC instant in the same unit (equal to local for naive data).
func (f *Fields) SetLocal(local, utc int64, per int64) {
	perDay := per * SecondsPerDay
	days := FloorDiv(local, perDay)
	rem := local - days*perDay
	f.set(days, rem*(NsSecond/per))
	f.Unix = FloorDiv(utc, per)
}

// SetTimeOfDay fills f from nanoseconds since midnight.
func (f *Fields) SetTimeOfDay(ns int64) {
	f.set(0, ns)
}

func appendPadded(dst []byte, v int64, width int, pad byte) []byte {
	var buf [24]byte
	s := strconv.AppendInt(buf[:0], v, 10)
	neg := false
	if v < 0 {
		neg = true
		s = s[1:]
	}
	if neg {
		dst = append(dst, '-')
	}
	switch pad {
	case padZero:
		for k := len(s); k < width; k++ {
			dst = append(dst, '0')
		}
	case padSpace:
		for k := len(s); k < width; k++ {
			dst = append(dst, ' ')
		}
	}
	return append(dst, s...)
}

func appendTwo(dst []byte, v int, pad byte) []byte {
	if v < 10 {
		switch pad {
		case padZero:
			dst = append(dst, '0')
		case padSpace:
			dst = append(dst, ' ')
		}
		return append(dst, byte('0'+v))
	}
	return strconv.AppendInt(dst, int64(v), 10)
}

func appendYear(dst []byte, y int64, pad byte) []byte {
	if y >= 1000 && y <= 9999 {
		return strconv.AppendInt(dst, y, 10)
	}
	if y < 0 || y >= 10000 {
		if y >= 0 {
			dst = append(dst, '+')
		} else {
			dst = append(dst, '-')
			y = -y
		}
		// Width includes the sign in Rust's {:+05}; digits pad to 4.
		return appendPadded(dst, y, 4, pad)
	}
	return appendPadded(dst, y, 4, pad)
}

func appendOffset(dst []byte, off int, colon bool, withSeconds bool, hoursOnly bool) []byte {
	sign := byte('+')
	if off < 0 {
		sign = '-'
		off = -off
	}
	dst = append(dst, sign)
	h, m, s := off/3600, off/60%60, off%60
	dst = appendTwo(dst, h, padZero)
	if hoursOnly {
		return dst
	}
	if colon {
		dst = append(dst, ':')
	}
	dst = appendTwo(dst, m, padZero)
	if withSeconds {
		dst = append(dst, ':')
		dst = appendTwo(dst, s, padZero)
	}
	return dst
}

func weeksFrom(f *Fields, sunday bool) int {
	ord := OrdinalDay(f.Days, f.Year)
	wd := ISOWeekday(f.Days) // 1..7 Mon..Sun
	var daysFrom int         // days since the week start
	if sunday {
		daysFrom = wd % 7
	} else {
		daysFrom = wd - 1
	}
	return (ord - 1 - daysFrom + 7) / 7
}

// Append renders f into dst. It returns an error when the format needs
// a component the value does not have (a time zone on naive data).
func (fm *Format) Append(dst []byte, f *Fields) ([]byte, error) {
	for _, it := range fm.items {
		switch it.kind {
		case itLiteral, itSpace:
			dst = append(dst, it.lit...)
		case itYear:
			dst = appendYear(dst, f.Year, it.pad)
		case itYearDiv100:
			dst = appendTwo(dst, int(FloorDiv(f.Year, 100)), it.pad)
		case itYearMod100:
			dst = appendTwo(dst, int(FloorMod(f.Year, 100)), it.pad)
		case itIsoYear, itIsoYearDiv100, itIsoYearMod100:
			iy, _ := ISOWeek(f.Days)
			switch it.kind {
			case itIsoYear:
				dst = appendYear(dst, iy, it.pad)
			case itIsoYearDiv100:
				dst = appendTwo(dst, int(FloorDiv(iy, 100)), it.pad)
			default:
				dst = appendTwo(dst, int(FloorMod(iy, 100)), it.pad)
			}
		case itQuarter:
			dst = strconv.AppendInt(dst, int64((f.Month-1)/3+1), 10)
		case itMonth:
			dst = appendTwo(dst, f.Month, it.pad)
		case itDay:
			dst = appendTwo(dst, f.Day, it.pad)
		case itWeekFromSun:
			dst = appendTwo(dst, weeksFrom(f, true), it.pad)
		case itWeekFromMon:
			dst = appendTwo(dst, weeksFrom(f, false), it.pad)
		case itIsoWeek:
			_, w := ISOWeek(f.Days)
			dst = appendTwo(dst, w, it.pad)
		case itNumDaysFromSun:
			dst = strconv.AppendInt(dst, int64(ISOWeekday(f.Days)%7), 10)
		case itWeekdayFromMon:
			dst = strconv.AppendInt(dst, int64(ISOWeekday(f.Days)), 10)
		case itOrdinal:
			dst = appendPadded(dst, int64(OrdinalDay(f.Days, f.Year)), 3, it.pad)
		case itHour:
			dst = appendTwo(dst, f.Hour, it.pad)
		case itHour12:
			h := f.Hour % 12
			if h == 0 {
				h = 12
			}
			dst = appendTwo(dst, h, it.pad)
		case itMinute:
			dst = appendTwo(dst, f.Minute, it.pad)
		case itSecond:
			dst = appendTwo(dst, f.Second, it.pad)
		case itNanosecond:
			dst = appendPadded(dst, int64(f.Nanosecond), 9, it.pad)
		case itTimestamp:
			dst = strconv.AppendInt(dst, f.Unix, 10)
		case itShortMonth:
			dst = append(dst, shortMonths[f.Month-1]...)
		case itLongMonth:
			dst = append(dst, longMonths[f.Month-1]...)
		case itShortWeekday:
			dst = append(dst, shortWeekdays[ISOWeekday(f.Days)%7]...)
		case itLongWeekday:
			dst = append(dst, longWeekdays[ISOWeekday(f.Days)%7]...)
		case itLowerAmPm:
			if f.Hour >= 12 {
				dst = append(dst, "pm"...)
			} else {
				dst = append(dst, "am"...)
			}
		case itUpperAmPm:
			if f.Hour >= 12 {
				dst = append(dst, "PM"...)
			} else {
				dst = append(dst, "AM"...)
			}
		case itFracAuto:
			dst = AppendFracAuto(dst, f.Nanosecond)
		case itFrac3:
			dst = append(dst, '.')
			dst = appendPadded(dst, int64(f.Nanosecond/1_000_000), 3, padZero)
		case itFrac6:
			dst = append(dst, '.')
			dst = appendPadded(dst, int64(f.Nanosecond/1_000), 6, padZero)
		case itFrac9:
			dst = append(dst, '.')
			dst = appendPadded(dst, int64(f.Nanosecond), 9, padZero)
		case itFrac3NoDot:
			dst = appendPadded(dst, int64(f.Nanosecond/1_000_000), 3, padZero)
		case itFrac6NoDot:
			dst = appendPadded(dst, int64(f.Nanosecond/1_000), 6, padZero)
		case itFrac9NoDot:
			dst = appendPadded(dst, int64(f.Nanosecond), 9, padZero)
		case itTZName:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = append(dst, f.Abbr...)
		case itTZOffset, itTZOffsetPermissive:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = appendOffset(dst, f.Offset, false, false, false)
		case itTZOffsetColon:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = appendOffset(dst, f.Offset, true, false, false)
		case itTZOffsetDoubleColon:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = appendOffset(dst, f.Offset, true, true, false)
		case itTZOffsetTripleColon:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = appendOffset(dst, f.Offset, false, false, true)
		case itRFC3339:
			if !f.HasTZ {
				return dst, errNoTZ
			}
			dst = appendYear(dst, f.Year, padZero)
			dst = append(dst, '-')
			dst = appendTwo(dst, f.Month, padZero)
			dst = append(dst, '-')
			dst = appendTwo(dst, f.Day, padZero)
			dst = append(dst, 'T')
			dst = appendTwo(dst, f.Hour, padZero)
			dst = append(dst, ':')
			dst = appendTwo(dst, f.Minute, padZero)
			dst = append(dst, ':')
			dst = appendTwo(dst, f.Second, padZero)
			dst = AppendFracAuto(dst, f.Nanosecond)
			dst = appendOffset(dst, f.Offset, true, false, false)
		}
	}
	return dst, nil
}

var errNoTZ = fmt.Errorf("cannot format timezone-naive value with a time zone specifier (%%Z, %%z, %%+)")

// AppendFracAuto writes chrono's %.f: nothing for whole seconds, else a
// dot and 3, 6 or 9 digits.
func AppendFracAuto(dst []byte, nano int) []byte {
	if nano == 0 {
		return dst
	}
	dst = append(dst, '.')
	switch {
	case nano%1_000_000 == 0:
		return appendPadded(dst, int64(nano/1_000_000), 3, padZero)
	case nano%1_000 == 0:
		return appendPadded(dst, int64(nano/1_000), 6, padZero)
	}
	return appendPadded(dst, int64(nano), 9, padZero)
}
