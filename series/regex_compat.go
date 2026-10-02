package series

import (
	"regexp"
	"strings"
)

// Polars evaluates patterns with the Rust regex crate, where the Perl
// classes \w, \d and \s are Unicode-aware. Go's RE2 keeps them ASCII
// only, so "héllo" would not match \w+ as a whole. compileRegex
// rewrites those escapes into their Unicode property equivalents
// before compiling, which makes the common patterns behave the same
// in both engines. \b stays ASCII (RE2 has no Unicode word boundary).

const (
	reWordClass  = `\p{L}\p{Nl}\p{M}\p{Nd}\p{Pc}\x{200C}\x{200D}`
	reDigitClass = `\p{Nd}`
	reSpaceClass = `\t\n\v\f\r \x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}`
)

func compileRegex(pattern string) (*regexp.Regexp, error) {
	return regexp.Compile(unicodePerlClasses(pattern))
}

func unicodePerlClasses(p string) string {
	if !strings.ContainsRune(p, '\\') {
		return p
	}
	var b strings.Builder
	b.Grow(len(p) + 32)
	inClass := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '\\' && i+1 < len(p):
			next := p[i+1]
			var cls string
			neg := false
			switch next {
			case 'w':
				cls = reWordClass
			case 'W':
				cls, neg = reWordClass, true
			case 'd':
				cls = reDigitClass
			case 'D':
				cls, neg = reDigitClass, true
			case 's':
				cls = reSpaceClass
			case 'S':
				cls, neg = reSpaceClass, true
			}
			switch {
			case cls == "":
				b.WriteByte(c)
				b.WriteByte(next)
			case inClass && neg:
				// A negated class cannot be spliced into a bracket
				// expression; keep RE2's ASCII meaning.
				b.WriteByte(c)
				b.WriteByte(next)
			case inClass:
				b.WriteString(cls)
			case neg:
				b.WriteString("[^" + cls + "]")
			default:
				b.WriteString("[" + cls + "]")
			}
			i++
		case c == '[' && !inClass:
			inClass = true
			b.WriteByte(c)
			// A leading ']' (or '^]') is a literal member.
			if i+1 < len(p) && p[i+1] == '^' {
				b.WriteByte('^')
				i++
			}
			if i+1 < len(p) && p[i+1] == ']' {
				b.WriteByte(']')
				i++
			}
		case c == '[' && inClass && i+1 < len(p) && p[i+1] == ':':
			// POSIX class like [:alpha:] inside a bracket expression.
			end := strings.Index(p[i:], ":]")
			if end < 0 {
				b.WriteByte(c)
				continue
			}
			b.WriteString(p[i : i+end+2])
			i += end + 1
		case c == ']' && inClass:
			inClass = false
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
