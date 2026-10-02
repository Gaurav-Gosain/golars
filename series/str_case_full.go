package series

import (
	"strings"
	"unicode"
)

// upperSpecial holds the unconditional one-to-many uppercase mappings
// of Unicode SpecialCasing.txt that Rust's str::to_uppercase (and so
// polars) applies and Go's strings.ToUpper does not.
var upperSpecial = map[rune]string{
	'ß': "SS", 'ŉ': "ʼN", 'ǰ': "J̌", 'ΐ': "Ϊ́", 'ΰ': "Ϋ́", 'և': "ԵՒ",
	'ẖ': "H̱", 'ẗ': "T̈", 'ẘ': "W̊", 'ẙ': "Y̊", 'ẚ': "Aʾ",
	'ﬀ': "FF", 'ﬁ': "FI", 'ﬂ': "FL", 'ﬃ': "FFI", 'ﬄ': "FFL", 'ﬅ': "ST", 'ﬆ': "ST",
	'ﬓ': "ՄՆ", 'ﬔ': "ՄԵ", 'ﬕ': "ՄԻ", 'ﬖ': "ՎՆ", 'ﬗ': "ՄԽ",
}

// fullToUpper uppercases s like Rust's str::to_uppercase.
func fullToUpper(s string) string {
	special := false
	for _, r := range s {
		if _, ok := upperSpecial[r]; ok {
			special = true
			break
		}
	}
	if !special {
		return strings.ToUpper(s)
	}
	var b strings.Builder
	for _, r := range s {
		if m, ok := upperSpecial[r]; ok {
			b.WriteString(m)
			continue
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

// fullToLower lowercases s like Rust's str::to_lowercase: 'İ' becomes
// "i̇" and a capital sigma at the end of a word becomes 'ς'.
func fullToLower(s string) string {
	if !strings.ContainsAny(s, "Σİ") {
		return strings.ToLower(s)
	}
	rs := []rune(s)
	var b strings.Builder
	for i, r := range rs {
		switch r {
		case 'İ':
			b.WriteString("i̇")
		case 'Σ':
			if finalSigma(rs, i) {
				b.WriteRune('ς')
			} else {
				b.WriteRune('σ')
			}
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// finalSigma applies the Final_Sigma condition: a cased letter before
// position i (skipping case-ignorable runes) and none after.
func finalSigma(rs []rune, i int) bool {
	cased := func(r rune) bool { return unicode.IsUpper(r) || unicode.IsLower(r) || unicode.IsTitle(r) }
	before := false
	for j := i - 1; j >= 0; j-- {
		if isCaseIgnorable(rs[j]) {
			continue
		}
		before = cased(rs[j])
		break
	}
	if !before {
		return false
	}
	for j := i + 1; j < len(rs); j++ {
		if isCaseIgnorable(rs[j]) {
			continue
		}
		return !cased(rs[j])
	}
	return true
}

func isCaseIgnorable(r rune) bool {
	return unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk) ||
		r == '\'' || r == '.' || r == ':' || r == '·' || r == '’'
}
