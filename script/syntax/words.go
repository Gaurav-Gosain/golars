package syntax

import "github.com/Gaurav-Gosain/golars/script"

// GrammarWords lists the keyword and option words a command accepts,
// for completion.
func GrammarWords(spec *script.CommandSpec) []string {
	switch spec.Grammar {
	case "@sort":
		return []string{"asc", "desc"}
	case "@dynamic":
		return append(append([]string(nil), DynamicOptions...), "left", "right", "both", "none", "datapoint")
	case "@asof":
		return append([]string{"on", "by", "tolerance"}, AsofStrategies...)
	}
	return grammarWords(compileGrammar(spec.Grammar))
}

func grammarWords(items []gItem) []string {
	var out []string
	for _, it := range items {
		switch it.kind {
		case gKeyword, gEnum:
			out = append(out, it.words...)
		case gGroup:
			out = append(out, grammarWords(it.items)...)
		}
	}
	return out
}

// Snippet returns an LSP snippet for a command with placeholders for
// its required arguments, as in `join ${1:frame} on ${2:key}`.
func Snippet(spec *script.CommandSpec) string {
	if len(spec.Grammar) > 0 && spec.Grammar[0] == '@' {
		switch spec.Grammar {
		case "@with":
			return spec.Name + " ${1:name} = ${2:expr}"
		case "@filter":
			return spec.Name + " ${1:predicate}"
		case "@select":
			return spec.Name + " ${1:columns}"
		case "@sort":
			return spec.Name + " ${1:column}"
		case "@groupby":
			return spec.Name + " ${1:keys} ${2:col:sum}"
		case "@dynamic":
			return spec.Name + " ${1:time} every ${2:1h} ${3:col:sum}"
		case "@asof":
			return spec.Name + " ${1:frame} on ${2:key}"
		}
	}
	out := spec.Name
	n := 0
	for _, it := range compileGrammar(spec.Grammar) {
		switch it.kind {
		case gKeyword:
			out += " " + it.words[0]
		case gSlot:
			n++
			out += " ${" + itoa(n) + ":" + it.slot + "}"
		default:
			return withFinal(out, n)
		}
	}
	return withFinal(out, n)
}

func withFinal(s string, n int) string {
	if n == 0 {
		return s
	}
	return s + "$0"
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + string(rune('0'+n%10))
}
