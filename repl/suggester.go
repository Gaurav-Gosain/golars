package repl

import (
	"os"
	"path/filepath"
	"strings"
)

// Suggester computes an inline completion for the current value of
// the prompt. It returns two strings:
//
//   - ghost: appended to the value on screen in GhostStyle and
//     accepted on Tab or Right-arrow.
//   - hint: shown dim-italic to the right of the input. Informational
//     only; never committed into the value.
//
// Return ("", "") to suppress both.
//
// Suggesters are called on every keystroke, so implementations should
// be cheap. Don't do I/O outside the Suggester itself unless the I/O
// is already cached. Returning a ghost longer than the terminal width
// is fine; the widget clips.
type Suggester interface {
	Suggest(value string) (ghost, hint string)
}

// SuggesterFunc adapts a plain function as a Suggester.
type SuggesterFunc func(value string) (ghost, hint string)

// Suggest calls f.
func (f SuggesterFunc) Suggest(v string) (string, string) { return f(v) }

// CompletePrefix returns the portion of the first candidate in
// candidates that prefix-matches partial, beyond what's already
// typed. Returns "" if no candidate extends partial.
//
// Matching is case-sensitive. candidates is traversed in order;
// caller controls priority by ordering the slice.
func CompletePrefix(partial string, candidates []string) string {
	if partial == "" {
		return ""
	}
	for _, c := range candidates {
		if len(c) > len(partial) && strings.HasPrefix(c, partial) {
			return c[len(partial):]
		}
	}
	return ""
}

// CompleteFromList completes the last item in a separated list.
// For example with sep=',' and current="a,b,ag" and items including
// "age", it returns "e" because the last item "ag" prefix-matches
// "age".
//
// If sep is 0 the whole current is treated as the prefix.
func CompleteFromList(current string, items []string, sep rune) string {
	if current == "" {
		return ""
	}
	prefix := current
	if sep != 0 {
		if i := strings.LastIndex(current, string(sep)); i >= 0 {
			prefix = strings.TrimLeft(current[i+len(string(sep)):], " \t")
		}
	}
	if prefix == "" {
		return ""
	}
	for _, item := range items {
		if len(item) > len(prefix) && strings.HasPrefix(item, prefix) {
			return item[len(prefix):]
		}
	}
	return ""
}

// CompletePath completes a filesystem path against cwd. current may be
// absolute, relative (resolved against cwd), or bare. Returns the suffix
// to append to current to land on the first lexicographically-ordered
// directory entry that extends it. Directory matches get a trailing "/".
//
// I/O errors (unreadable parent dir, etc.) return "": completion is
// a best-effort UX feature, not a contract.
func CompletePath(current, cwd string) string {
	if current == "" {
		return ""
	}
	dir, base := filepath.Split(current)
	search := dir
	switch {
	case search == "":
		search = cwd
	case !filepath.IsAbs(search):
		search = filepath.Join(cwd, search)
	}
	entries, err := os.ReadDir(search)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base) && (len(name) > len(base) || e.IsDir()) {
			suffix := name[len(base):]
			if e.IsDir() {
				suffix += "/"
			}
			return suffix
		}
	}
	return ""
}

// SplitFields mirrors strings.Fields but preserves whether the input
// ended with whitespace. Useful for suggesters that need to tell the
// difference between "foo bar" (completing "bar") and "foo bar "
// (typed a space, expecting the next token).
func SplitFields(line string) (parts []string, trailingSpace bool) {
	parts = strings.Fields(line)
	if len(line) == 0 {
		return nil, false
	}
	last := line[len(line)-1]
	trailingSpace = last == ' ' || last == '\t'
	return parts, trailingSpace
}
