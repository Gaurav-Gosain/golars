package series

import (
	"fmt"
	"math"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// A small allocation-free JSON scanner shared by str.json_decode,
// str.json_path_match and struct.json_encode. It works on byte
// offsets into the source string: values are identified by their
// [start, end) span, and nothing is materialised unless a caller asks
// for it (string unescaping, number parsing). encoding/json is not
// used because it loses object key order and allocates per value.

// jsonMaxDepth bounds nesting so hostile input cannot blow the stack.
const jsonMaxDepth = 512

type jsonSyntaxError struct {
	pos int
	msg string
}

func (e *jsonSyntaxError) Error() string {
	return fmt.Sprintf("json parsing error: %s at character %d", e.msg, e.pos)
}

func jsonErrAt(pos int, msg string) error { return &jsonSyntaxError{pos: pos, msg: msg} }

func jsonSkipWS(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// jsonValueSpan validates that s holds exactly one JSON value (with
// optional surrounding whitespace) and returns its span. empty is true
// when s is blank.
func jsonValueSpan(s string) (start, end int, empty bool, err error) {
	start = jsonSkipWS(s, 0)
	if start == len(s) {
		return 0, 0, true, nil
	}
	end, err = jsonSkipValue(s, start, 0)
	if err != nil {
		return 0, 0, false, err
	}
	if j := jsonSkipWS(s, end); j != len(s) {
		return 0, 0, false, jsonErrAt(j, "trailing characters")
	}
	return start, end, false, nil
}

// jsonSkipValue validates the value starting at s[i] (no leading
// whitespace) and returns the offset just past it.
func jsonSkipValue(s string, i, depth int) (int, error) {
	if i >= len(s) {
		return i, jsonErrAt(i, "unexpected end of input")
	}
	if depth > jsonMaxDepth {
		return i, jsonErrAt(i, "nesting too deep")
	}
	switch c := s[i]; c {
	case '{':
		i = jsonSkipWS(s, i+1)
		if i < len(s) && s[i] == '}' {
			return i + 1, nil
		}
		for {
			if i >= len(s) || s[i] != '"' {
				return i, jsonErrAt(i, "expected object key")
			}
			e, _, err := jsonSkipString(s, i)
			if err != nil {
				return e, err
			}
			i = jsonSkipWS(s, e)
			if i >= len(s) || s[i] != ':' {
				return i, jsonErrAt(i, "expected ':'")
			}
			i = jsonSkipWS(s, i+1)
			if i, err = jsonSkipValue(s, i, depth+1); err != nil {
				return i, err
			}
			i = jsonSkipWS(s, i)
			if i < len(s) && s[i] == ',' {
				i = jsonSkipWS(s, i+1)
				continue
			}
			if i < len(s) && s[i] == '}' {
				return i + 1, nil
			}
			return i, jsonErrAt(i, "expected ',' or '}'")
		}
	case '[':
		i = jsonSkipWS(s, i+1)
		if i < len(s) && s[i] == ']' {
			return i + 1, nil
		}
		for {
			var err error
			if i, err = jsonSkipValue(s, i, depth+1); err != nil {
				return i, err
			}
			i = jsonSkipWS(s, i)
			if i < len(s) && s[i] == ',' {
				i = jsonSkipWS(s, i+1)
				continue
			}
			if i < len(s) && s[i] == ']' {
				return i + 1, nil
			}
			return i, jsonErrAt(i, "expected ',' or ']'")
		}
	case '"':
		e, _, err := jsonSkipString(s, i)
		return e, err
	case 't':
		return jsonSkipLiteral(s, i, "true")
	case 'f':
		return jsonSkipLiteral(s, i, "false")
	case 'n':
		return jsonSkipLiteral(s, i, "null")
	default:
		e, _, err := jsonScanNumber(s, i)
		return e, err
	}
}

func jsonSkipLiteral(s string, i int, lit string) (int, error) {
	if len(s)-i >= len(lit) && s[i:i+len(lit)] == lit {
		return i + len(lit), nil
	}
	return i, jsonErrAt(i, "invalid literal")
}

// jsonSkipString scans the string starting at the quote s[i] and
// returns the offset past the closing quote. esc reports whether the
// body contains escape sequences.
func jsonSkipString(s string, i int) (end int, esc bool, err error) {
	j := i + 1
	for j < len(s) {
		c := s[j]
		switch {
		case c == '"':
			return j + 1, esc, nil
		case c == '\\':
			esc = true
			if j+1 >= len(s) {
				return j, esc, jsonErrAt(j, "unterminated escape")
			}
			switch s[j+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				j += 2
			case 'u':
				if j+6 > len(s) {
					return j, esc, jsonErrAt(j, "invalid unicode escape")
				}
				for k := j + 2; k < j+6; k++ {
					if jsonHexVal(s[k]) < 0 {
						return k, esc, jsonErrAt(k, "invalid unicode escape")
					}
				}
				j += 6
			default:
				return j, esc, jsonErrAt(j, "invalid escape")
			}
		case c < 0x20:
			return j, esc, jsonErrAt(j, "control character in string")
		default:
			j++
		}
	}
	return j, esc, jsonErrAt(j, "unterminated string")
}

func jsonHexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// jsonScanNumber validates the JSON number grammar at s[i].
func jsonScanNumber(s string, i int) (end int, isFloat bool, err error) {
	j := i
	if j < len(s) && s[j] == '-' {
		j++
	}
	switch {
	case j < len(s) && s[j] == '0':
		j++
	case j < len(s) && s[j] >= '1' && s[j] <= '9':
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
	default:
		return j, false, jsonErrAt(j, "invalid value")
	}
	if j < len(s) && s[j] == '.' {
		isFloat = true
		j++
		d := j
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == d {
			return j, true, jsonErrAt(j, "invalid number")
		}
	}
	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		isFloat = true
		j++
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		d := j
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == d {
			return j, true, jsonErrAt(j, "invalid number")
		}
	}
	return j, isFloat, nil
}

// jsonUnescape appends the decoded body of the string literal
// s[start:end] (quotes included) to dst.
func jsonUnescape(dst []byte, lit string) []byte {
	body := lit[1 : len(lit)-1]
	for i := 0; i < len(body); {
		c := body[i]
		if c != '\\' {
			dst = append(dst, c)
			i++
			continue
		}
		switch body[i+1] {
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r := jsonHex4(body[i+2 : i+6])
			i += 6
			if utf16.IsSurrogate(r) {
				if i+6 <= len(body) && body[i] == '\\' && body[i+1] == 'u' {
					r2 := jsonHex4(body[i+2 : i+6])
					if dec := utf16.DecodeRune(r, r2); dec != utf8.RuneError {
						dst = utf8.AppendRune(dst, dec)
						i += 6
						continue
					}
				}
				r = utf8.RuneError
			}
			dst = utf8.AppendRune(dst, r)
			continue
		default:
			dst = append(dst, body[i+1])
		}
		i += 2
	}
	return dst
}

func jsonHex4(s string) rune {
	var r rune
	for k := range 4 {
		r = r<<4 | rune(jsonHexVal(s[k]))
	}
	return r
}

// jsonEachMember iterates the members of the object starting at s[i].
// The key span includes its quotes. fn returning false stops early.
// The input must already be validated.
func jsonEachMember(s string, i int, fn func(ks, ke int, kesc bool, vs, ve int) bool) {
	i = jsonSkipWS(s, i+1)
	if s[i] == '}' {
		return
	}
	for {
		ke, kesc, _ := jsonSkipString(s, i)
		vs := jsonSkipWS(s, jsonSkipWS(s, ke)+1)
		ve, _ := jsonSkipValue(s, vs, 0)
		if !fn(i, ke, kesc, vs, ve) {
			return
		}
		i = jsonSkipWS(s, ve)
		if s[i] == '}' {
			return
		}
		i = jsonSkipWS(s, i+1)
	}
}

// jsonEachElem iterates the elements of the array starting at s[i].
// The input must already be validated.
func jsonEachElem(s string, i int, fn func(vs, ve int) bool) {
	i = jsonSkipWS(s, i+1)
	if s[i] == ']' {
		return
	}
	for {
		ve, _ := jsonSkipValue(s, i, 0)
		if !fn(i, ve) {
			return
		}
		i = jsonSkipWS(s, ve)
		if s[i] == ']' {
			return
		}
		i = jsonSkipWS(s, i+1)
	}
}

// jsonKeyEquals compares the decoded key literal s[ks:ke] with name.
func jsonKeyEquals(s string, ks, ke int, kesc bool, name string, scratch *[]byte) bool {
	if !kesc {
		return s[ks+1:ke-1] == name
	}
	*scratch = jsonUnescape((*scratch)[:0], s[ks:ke])
	return string(*scratch) == name
}

// jsonAppendEscaped appends v as a JSON string literal using the same
// escaping rules as serde_json (which polars uses): quote, backslash
// and control characters are escaped, everything else is written
// verbatim.
func jsonAppendEscaped(dst []byte, v string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, v[start:i]...)
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = i + 1
	}
	dst = append(dst, v[start:]...)
	return append(dst, '"')
}

// jsonAppendFloat formats f the way polars writes floats to JSON:
// the shortest round-trip digits, fixed notation when the decimal
// point falls within 16 digits left or 5 digits right of the first
// digit (always with a fractional part, so 2.0 stays "2.0"),
// scientific notation otherwise with an explicit exponent sign
// ("1e+20", "1e-7"). bits is 32 or 64. NaN and infinities are the
// caller's concern.
func jsonAppendFloat(dst []byte, f float64, bits int) []byte {
	var buf [32]byte
	sci := strconv.AppendFloat(buf[:0], f, 'e', -1, bits)
	neg := false
	if sci[0] == '-' {
		neg = true
		sci = sci[1:]
	}
	// sci is d[.ddd]e(+|-)XX
	epos := 0
	for epos < len(sci) && sci[epos] != 'e' {
		epos++
	}
	var digits [24]byte
	nd := 0
	for _, c := range sci[:epos] {
		if c != '.' {
			digits[nd] = c
			nd++
		}
	}
	exp, _ := strconv.Atoi(string(sci[epos+1:]))
	if neg {
		dst = append(dst, '-')
	}
	kk := exp + 1
	switch {
	case kk > 0 && kk <= 16:
		if nd <= kk {
			dst = append(dst, digits[:nd]...)
			for range kk - nd {
				dst = append(dst, '0')
			}
			return append(dst, '.', '0')
		}
		dst = append(dst, digits[:kk]...)
		dst = append(dst, '.')
		return append(dst, digits[kk:nd]...)
	case kk > -5 && kk <= 0:
		dst = append(dst, '0', '.')
		for range -kk {
			dst = append(dst, '0')
		}
		return append(dst, digits[:nd]...)
	default:
		dst = append(dst, digits[0])
		if nd > 1 {
			dst = append(dst, '.')
			dst = append(dst, digits[1:nd]...)
		}
		dst = append(dst, 'e')
		if exp >= 0 {
			dst = append(dst, '+')
		}
		return strconv.AppendInt(dst, int64(exp), 10)
	}
}

// jsonAppendNumber re-renders the JSON number literal num the way
// serde_json would after parsing it: integers that fit in i64/u64 are
// kept verbatim, everything else goes through jsonAppendFloat.
func jsonAppendNumber(dst []byte, num string, isFloat bool) []byte {
	if !isFloat {
		if num[0] == '-' {
			if v, err := strconv.ParseInt(num, 10, 64); err == nil {
				return strconv.AppendInt(dst, v, 10)
			}
		} else if v, err := strconv.ParseUint(num, 10, 64); err == nil {
			return strconv.AppendUint(dst, v, 10)
		}
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil && !math.IsInf(f, 0) {
		return append(dst, num...)
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return append(dst, "null"...)
	}
	return jsonAppendFloat(dst, f, 64)
}

// jsonAppendCompact re-serialises the validated value s[vs:ve] without
// insignificant whitespace, normalising numbers and string escapes.
func jsonAppendCompact(dst []byte, s string, vs, ve int, scratch *[]byte) []byte {
	switch s[vs] {
	case '{':
		dst = append(dst, '{')
		first := true
		jsonEachMember(s, vs, func(ks, ke int, kesc bool, cvs, cve int) bool {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = jsonAppendStringLit(dst, s[ks:ke], kesc, scratch)
			dst = append(dst, ':')
			dst = jsonAppendCompact(dst, s, cvs, cve, scratch)
			return true
		})
		return append(dst, '}')
	case '[':
		dst = append(dst, '[')
		first := true
		jsonEachElem(s, vs, func(cvs, cve int) bool {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = jsonAppendCompact(dst, s, cvs, cve, scratch)
			return true
		})
		return append(dst, ']')
	case '"':
		_, esc, _ := jsonSkipString(s, vs)
		return jsonAppendStringLit(dst, s[vs:ve], esc, scratch)
	case 't', 'f', 'n':
		return append(dst, s[vs:ve]...)
	default:
		_, isFloat, _ := jsonScanNumber(s, vs)
		return jsonAppendNumber(dst, s[vs:ve], isFloat)
	}
}

// jsonAppendStringLit re-escapes a string literal in canonical form.
func jsonAppendStringLit(dst []byte, lit string, esc bool, scratch *[]byte) []byte {
	if !esc {
		body := lit[1 : len(lit)-1]
		clean := true
		for i := 0; i < len(body); i++ {
			if body[i] < 0x20 {
				clean = false
				break
			}
		}
		if clean {
			return append(dst, lit...)
		}
		return jsonAppendEscaped(dst, body)
	}
	*scratch = jsonUnescape((*scratch)[:0], lit)
	return jsonAppendEscaped(dst, string(*scratch))
}
