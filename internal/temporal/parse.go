package temporal

import (
	"strings"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// parsed collects the fields found while scanning a string with a
// Format. It mirrors chrono's Parsed.
type parsed struct {
	year, yDiv100, yMod100    int64
	hasYear, hasYDiv, hasYMod bool

	month, day, ordinal          int
	hasMonth, hasDay, hasOrdinal bool

	hour, hour12       int
	hasHour, hasHour12 bool
	pm                 int // -1 unset, 0 am, 1 pm

	minute, second, nano int
	hasMinute, hasSecond bool

	weekday int // ISO 1..7, 0 when unset

	offset    int
	hasOffset bool

	ts    int64
	hasTS bool
}

// ParseResult is the outcome of a successful parse: a naive date and
// time of day, plus the UTC offset when the input carried one.
type ParseResult struct {
	Days      int64
	NsOfDay   int64
	Offset    int
	HasOffset bool
}

// UTCNs returns the result as nanoseconds since the epoch, applying the
// parsed offset (if any) to get a UTC instant.
func (r ParseResult) UTCNs() int64 {
	return r.Days*NsDay + r.NsOfDay - int64(r.Offset)*NsSecond
}

// Ticks returns the result in unit u without overflowing for far dates
// (sub-unit precision floors). With utc set the parsed offset is
// applied to produce a UTC instant; otherwise the naive wall clock.
func (r ParseResult) Ticks(u dtype.TimeUnit, utc bool) int64 {
	v := r.Days*UnitsPerDay(u) + FloorDiv(r.NsOfDay, NsPerUnit(u))
	if utc {
		v -= int64(r.Offset) * UnitsPerSecond(u)
	}
	return v
}

func trimSpaceLeft(s string) string {
	i := 0
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	return s[i:]
}

// scanNumber reads between minDigits and maxDigits ASCII digits.
func scanNumber(s string, minDigits, maxDigits int) (string, int64, bool) {
	var n int64
	i := 0
	for i < len(s) && i < maxDigits && s[i] >= '0' && s[i] <= '9' {
		if n > (1<<62)/10 {
			return s, 0, false
		}
		n = n*10 + int64(s[i]-'0')
		i++
	}
	if i < minDigits {
		return s, 0, false
	}
	return s[i:], n, true
}

func hasPrefixFold(s, p string) bool {
	return len(s) >= len(p) && strings.EqualFold(s[:len(p)], p)
}

func scanMonth(s string, long bool) (string, int, bool) {
	for i, m := range shortMonths {
		if !hasPrefixFold(s, m) {
			continue
		}
		if long {
			if hasPrefixFold(s, longMonths[i]) {
				return s[len(longMonths[i]):], i + 1, true
			}
		}
		return s[3:], i + 1, true
	}
	return s, 0, false
}

func scanWeekday(s string, long bool) (string, int, bool) {
	for i, w := range shortWeekdays {
		if !hasPrefixFold(s, w) {
			continue
		}
		iso := i
		if iso == 0 {
			iso = 7
		}
		if long && hasPrefixFold(s, longWeekdays[i]) {
			return s[len(longWeekdays[i]):], iso, true
		}
		return s[3:], iso, true
	}
	return s, 0, false
}

// scanFraction reads 1..9 digits after an optional-dot context and
// returns nanoseconds; extra digits beyond 9 are consumed and ignored.
func scanFraction(s string, exact int) (string, int, bool) {
	i := 0
	var v int64
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		if i < 9 {
			v = v*10 + int64(s[i]-'0')
		}
		i++
		if exact > 0 && i == exact {
			break
		}
	}
	if i == 0 || (exact > 0 && i != exact) {
		return s, 0, false
	}
	digits := min(i, 9)
	for k := digits; k < 9; k++ {
		v *= 10
	}
	return s[i:], int(v), true
}

func scanOffset(s string, allowZulu, allowMissingMinutes bool) (string, int, bool) {
	if allowZulu && len(s) > 0 && (s[0] == 'Z' || s[0] == 'z') {
		return s[1:], 0, true
	}
	if len(s) == 0 {
		return s, 0, false
	}
	sign := 1
	switch s[0] {
	case '+':
	case '-':
		sign = -1
	default:
		if strings.HasPrefix(s, "−") {
			sign = -1
			s = s[len("−")-1:]
		} else {
			return s, 0, false
		}
	}
	s = s[1:]
	s, h, ok := scanNumber(s, 2, 2)
	if !ok {
		return s, 0, false
	}
	rest := strings.TrimLeft(s, ": \t")
	r2, m, ok := scanNumber(rest, 2, 2)
	if !ok {
		if !allowMissingMinutes {
			return s, 0, false
		}
		m = 0
		r2 = s
	}
	if m >= 60 {
		return s, 0, false
	}
	return r2, sign * int(h*3600+m*60), true
}

// scan walks the format items over s and returns the unconsumed rest.
func (fm *Format) scan(s string, p *parsed) (string, bool) {
	p.pm = -1
	var ok bool
	var v int64
	for _, it := range fm.items {
		switch it.kind {
		case itLiteral:
			if !strings.HasPrefix(s, it.lit) {
				return s, false
			}
			s = s[len(it.lit):]
		case itSpace:
			s = trimSpaceLeft(s)
		case itYear:
			s = trimSpaceLeft(s)
			switch {
			case strings.HasPrefix(s, "-"):
				if s, v, ok = scanNumber(s[1:], 1, 18); !ok {
					return s, false
				}
				v = -v
			case strings.HasPrefix(s, "+"):
				if s, v, ok = scanNumber(s[1:], 1, 18); !ok {
					return s, false
				}
			default:
				if s, v, ok = scanNumber(s, 1, 4); !ok {
					return s, false
				}
			}
			p.year, p.hasYear = v, true
		case itIsoYear, itIsoYearDiv100, itIsoYearMod100, itWeekFromSun, itWeekFromMon, itIsoWeek, itQuarter:
			// Parsed for validation of the shape only; week based dates
			// are not resolved.
			width := 2
			switch it.kind {
			case itIsoYear:
				width = 4
			case itQuarter:
				width = 1
			}
			s = trimSpaceLeft(s)
			if s, _, ok = scanNumber(s, 1, width); !ok {
				return s, false
			}
		case itYearDiv100:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok {
				return s, false
			}
			p.yDiv100, p.hasYDiv = v, true
		case itYearMod100:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok {
				return s, false
			}
			p.yMod100, p.hasYMod = v, true
		case itMonth:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v < 1 || v > 12 {
				return s, false
			}
			p.month, p.hasMonth = int(v), true
		case itDay:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v < 1 || v > 31 {
				return s, false
			}
			p.day, p.hasDay = int(v), true
		case itNumDaysFromSun:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 1); !ok || v > 6 {
				return s, false
			}
			p.weekday = int(v)
			if p.weekday == 0 {
				p.weekday = 7
			}
		case itWeekdayFromMon:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 1); !ok || v < 1 || v > 7 {
				return s, false
			}
			p.weekday = int(v)
		case itOrdinal:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 3); !ok || v < 1 || v > 366 {
				return s, false
			}
			p.ordinal, p.hasOrdinal = int(v), true
		case itHour:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v > 23 {
				return s, false
			}
			p.hour, p.hasHour = int(v), true
		case itHour12:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v < 1 || v > 12 {
				return s, false
			}
			p.hour12, p.hasHour12 = int(v), true
		case itMinute:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v > 59 {
				return s, false
			}
			p.minute, p.hasMinute = int(v), true
		case itSecond:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 2); !ok || v > 60 {
				return s, false
			}
			p.second, p.hasSecond = int(v), true
		case itNanosecond:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 9); !ok {
				return s, false
			}
			p.nano = int(v)
		case itTimestamp:
			s = trimSpaceLeft(s)
			if s, v, ok = scanNumber(s, 1, 19); !ok {
				return s, false
			}
			p.ts, p.hasTS = v, true
		case itShortMonth, itLongMonth:
			var m int
			if s, m, ok = scanMonth(s, it.kind == itLongMonth); !ok {
				return s, false
			}
			p.month, p.hasMonth = m, true
		case itShortWeekday, itLongWeekday:
			var w int
			if s, w, ok = scanWeekday(s, it.kind == itLongWeekday); !ok {
				return s, false
			}
			p.weekday = w
		case itLowerAmPm, itUpperAmPm:
			switch {
			case hasPrefixFold(s, "am"):
				p.pm = 0
			case hasPrefixFold(s, "pm"):
				p.pm = 1
			default:
				return s, false
			}
			s = s[2:]
		case itFracAuto:
			if strings.HasPrefix(s, ".") {
				var n int
				if s, n, ok = scanFraction(s[1:], 0); !ok {
					return s, false
				}
				p.nano = n
			}
		case itFrac3, itFrac6, itFrac9:
			want := fracWidth(it.kind)
			if fm.lenientFrac {
				want = 0
			}
			if strings.HasPrefix(s, ".") {
				var n int
				if s, n, ok = scanFraction(s[1:], want); !ok {
					return s, false
				}
				p.nano = n
			}
		case itFrac3NoDot, itFrac6NoDot, itFrac9NoDot:
			want := fracWidth(it.kind)
			var n int
			if s, n, ok = scanFraction(s, want); !ok {
				return s, false
			}
			p.nano = n
		case itTZName:
			i := 0
			for i < len(s) && !isSpace(s[i]) {
				i++
			}
			s = s[i:]
		case itTZOffset, itTZOffsetColon, itTZOffsetDoubleColon, itTZOffsetTripleColon:
			var off int
			if s, off, ok = scanOffset(trimSpaceLeft(s), false, false); !ok {
				return s, false
			}
			p.offset, p.hasOffset = off, true
		case itTZOffsetPermissive:
			var off int
			if s, off, ok = scanOffset(trimSpaceLeft(s), true, true); !ok {
				return s, false
			}
			p.offset, p.hasOffset = off, true
		case itRFC3339:
			if s, ok = scanRFC3339(s, p); !ok {
				return s, false
			}
		}
	}
	return s, true
}

func fracWidth(k itemKind) int {
	switch k {
	case itFrac3, itFrac3NoDot:
		return 3
	case itFrac6, itFrac6NoDot:
		return 6
	}
	return 9
}

func scanRFC3339(s string, p *parsed) (string, bool) {
	var v int64
	var ok bool
	num := func(minD, maxD int) bool {
		s = trimSpaceLeft(s)
		s, v, ok = scanNumber(s, minD, maxD)
		return ok
	}
	lit := func(c byte) bool {
		if len(s) == 0 || s[0] != c {
			return false
		}
		s = s[1:]
		return true
	}
	neg := false
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		neg = s[0] == '-'
		s = s[1:]
		if !num(1, 18) {
			return s, false
		}
	} else if !num(4, 4) {
		return s, false
	}
	p.year, p.hasYear = v, true
	if neg {
		p.year = -v
	}
	if !lit('-') || !num(1, 2) {
		return s, false
	}
	p.month, p.hasMonth = int(v), true
	if !lit('-') || !num(1, 2) {
		return s, false
	}
	p.day, p.hasDay = int(v), true
	if len(s) == 0 || (s[0] != 'T' && s[0] != 't' && s[0] != ' ') {
		return s, false
	}
	s = s[1:]
	if !num(1, 2) {
		return s, false
	}
	p.hour, p.hasHour = int(v), true
	if !lit(':') || !num(1, 2) {
		return s, false
	}
	p.minute, p.hasMinute = int(v), true
	if !lit(':') || !num(1, 2) {
		return s, false
	}
	p.second, p.hasSecond = int(v), true
	if strings.HasPrefix(s, ".") {
		var n int
		if s, n, ok = scanFraction(s[1:], 0); !ok {
			return s, false
		}
		p.nano = n
	}
	s = trimSpaceLeft(s)
	if hasPrefixFold(s, "UTC") {
		p.offset, p.hasOffset = 0, true
		return s[3:], true
	}
	var off int
	if s, off, ok = scanOffset(s, true, false); !ok {
		return s, false
	}
	p.offset, p.hasOffset = off, true
	return s, true
}

// resolve turns the scanned fields into a date and time of day.
func (p *parsed) resolve() (ParseResult, bool) {
	var r ParseResult
	r.Offset, r.HasOffset = p.offset, p.hasOffset
	if p.hasTS {
		ns := p.ts*NsSecond + int64(p.nano)
		// %s is a UTC instant; express it as the local wall clock so
		// UTCNs recovers it.
		ns += int64(p.offset) * NsSecond
		r.Days = FloorDiv(ns, NsDay)
		r.NsOfDay = ns - r.Days*NsDay
		return r, true
	}
	year := p.year
	switch {
	case p.hasYear:
	case p.hasYDiv && p.hasYMod:
		year = p.yDiv100*100 + p.yMod100
	case p.hasYMod:
		if p.yMod100 < 70 {
			year = 2000 + p.yMod100
		} else {
			year = 1900 + p.yMod100
		}
	default:
		year = 1970
		if !p.hasMonth && !p.hasDay && !p.hasOrdinal {
			return r, false
		}
	}
	var days int64
	if p.hasOrdinal && !p.hasMonth {
		dayCount := int64(365)
		if IsLeapYear(year) {
			dayCount = 366
		}
		if int64(p.ordinal) > dayCount {
			return r, false
		}
		days = DaysFromCivil(year, 1, 1) + int64(p.ordinal) - 1
	} else {
		m, d := p.month, p.day
		if !p.hasMonth {
			m = 1
		}
		if !p.hasDay {
			d = 1
		}
		if !ValidDate(year, m, d) {
			return r, false
		}
		days = DaysFromCivil(year, m, d)
		if p.hasOrdinal && OrdinalDay(days, year) != p.ordinal {
			return r, false
		}
	}
	if p.weekday != 0 && ISOWeekday(days) != p.weekday {
		return r, false
	}
	hour := p.hour
	switch {
	case p.hasHour12:
		if p.pm < 0 {
			return r, false
		}
		hour = p.hour12 % 12
		if p.pm == 1 {
			hour += 12
		}
		if p.hasHour && hour != p.hour {
			return r, false
		}
	case p.hasHour && p.pm >= 0:
		if (p.hour >= 12) != (p.pm == 1) {
			return r, false
		}
	}
	sec := p.second
	if sec == 60 {
		sec = 59
	}
	r.Days = days
	r.NsOfDay = int64(hour)*NsHour + int64(p.minute)*NsMinute + int64(sec)*NsSecond + int64(p.nano)
	return r, true
}

// Parse parses s, requiring the whole string to match.
func (fm *Format) Parse(s string) (ParseResult, bool) {
	var p parsed
	rest, ok := fm.scan(s, &p)
	if !ok || rest != "" {
		return ParseResult{}, false
	}
	return p.resolve()
}

// ParseSearch finds the first position in s where the format matches,
// ignoring any trailing text (polars' exact=False).
func (fm *Format) ParseSearch(s string) (ParseResult, bool) {
	for i := 0; i < len(s); i++ {
		if i > 0 && s[i]&0xC0 == 0x80 {
			continue // not at a UTF-8 boundary
		}
		var p parsed
		if _, ok := fm.scan(s[i:], &p); ok {
			if r, ok := p.resolve(); ok {
				return r, true
			}
		}
	}
	return ParseResult{}, false
}

// ParseTime parses a time of day, returning nanoseconds since midnight.
func (fm *Format) ParseTime(s string) (int64, bool) {
	var p parsed
	rest, ok := fm.scan(s, &p)
	if !ok || rest != "" {
		return 0, false
	}
	if !p.hasHour && !p.hasHour12 {
		return 0, false
	}
	p.hasYear, p.year = true, 1970
	r, ok := p.resolve()
	if !ok {
		return 0, false
	}
	return r.NsOfDay, true
}
