package expr

import "github.com/Gaurav-Gosain/golars/dtype"

// str_ns.go holds the rest of the polars `str` namespace. Each method
// lowers to a "str.*" FunctionNode whose Params carry the scalar
// arguments in the order the evaluator expects.

// Split splits each string on by and returns a List<String> column.
// An empty by splits into characters. Polars `str.split(by)`.
func (o StrOps) Split(by string) Expr { return fn1p("str.split", o.e, by, false) }

// SplitInclusive is Split that keeps the separator at the end of each
// piece. Polars `str.split(by, inclusive=True)`.
func (o StrOps) SplitInclusive(by string) Expr { return fn1p("str.split", o.e, by, true) }

// SplitN splits into at most n pieces (the last piece keeps the
// remainder) and returns a struct with fields field_0 .. field_{n-1}.
// Polars `str.splitn(by, n)`.
func (o StrOps) SplitN(by string, n int) Expr { return fn1p("str.splitn", o.e, by, n) }

// SplitExactN returns a struct with n+1 fields holding the first n+1
// pieces of the split; missing pieces are null. Polars
// `str.split_exact(by, n, inclusive)`. (SplitExact, which predates
// this method, returns the full List<String> split.)
func (o StrOps) SplitExactN(by string, n int, inclusive bool) Expr {
	return fn1p("str.split_exact_n", o.e, by, n, inclusive)
}

// Join concatenates all values of the column into a single string.
// With ignoreNulls, nulls are skipped; otherwise one null makes the
// result null. Polars `str.join(delimiter, ignore_nulls)`.
func (o StrOps) Join(delimiter string, ignoreNulls bool) Expr {
	return fn1p("str.join", o.e, delimiter, ignoreNulls)
}

// ContainsAny tests each string for any of the literal patterns.
// Polars `str.contains_any(patterns, ascii_case_insensitive)`.
func (o StrOps) ContainsAny(patterns []string, asciiCaseInsensitive bool) Expr {
	return fn1p("str.contains_any", o.e, patterns, asciiCaseInsensitive)
}

// EscapeRegex escapes regex meta characters. Polars `str.escape_regex`.
func (o StrOps) EscapeRegex() Expr { return fn1("str.escape_regex", o.e) }

// Explode turns every character into its own row. The output length
// differs from the input. Polars `str.explode`.
func (o StrOps) Explode() Expr { return fn1("str.explode", o.e) }

// Extract returns capture group `group` (0 is the whole match) of the
// first regex match, or null. Polars `str.extract(pattern, group_index)`.
func (o StrOps) Extract(pattern string, group int) Expr {
	return fn1p("str.extract", o.e, pattern, group)
}

// ExtractAll returns every regex match per row as List<String>.
// Polars `str.extract_all(pattern)`.
func (o StrOps) ExtractAll(pattern string) Expr { return fn1p("str.extract_all", o.e, pattern) }

// ExtractGroups returns a struct with one field per capture group.
// Polars `str.extract_groups(pattern)`.
func (o StrOps) ExtractGroups(pattern string) Expr {
	return fn1p("str.extract_groups", o.e, pattern)
}

// ExtractMany returns the literal pattern matches per row as
// List<String>. Polars `str.extract_many`.
func (o StrOps) ExtractMany(patterns []string, asciiCaseInsensitive, overlapping bool) Expr {
	return fn1p("str.extract_many", o.e, patterns, asciiCaseInsensitive, overlapping)
}

// FindMany returns the byte offsets where the literal patterns match
// as List<UInt32>. Polars `str.find_many`.
func (o StrOps) FindMany(patterns []string, asciiCaseInsensitive, overlapping bool) Expr {
	return fn1p("str.find_many", o.e, patterns, asciiCaseInsensitive, overlapping)
}

// ReplaceMany replaces each literal pattern with the replacement at
// the same index (a single replacement is broadcast). Polars
// `str.replace_many(patterns, replace_with, ascii_case_insensitive)`.
func (o StrOps) ReplaceMany(patterns, replaceWith []string, asciiCaseInsensitive bool) Expr {
	return fn1p("str.replace_many", o.e, patterns, replaceWith, asciiCaseInsensitive)
}

// Reverse reverses each string by grapheme cluster. Polars `str.reverse`.
func (o StrOps) Reverse() Expr { return fn1("str.reverse", o.e) }

// StripChars removes leading and trailing characters in chars (their
// union). With no argument it strips whitespace. Polars
// `str.strip_chars(characters)`.
func (o StrOps) StripChars(chars ...string) Expr {
	return fn1p("str.strip_chars", o.e, stripCharsParam(chars))
}

// StripCharsStart strips on the leading side only.
func (o StrOps) StripCharsStart(chars ...string) Expr {
	return fn1p("str.strip_chars_start", o.e, stripCharsParam(chars))
}

// StripCharsEnd strips on the trailing side only.
func (o StrOps) StripCharsEnd(chars ...string) Expr {
	return fn1p("str.strip_chars_end", o.e, stripCharsParam(chars))
}

// stripCharsParam keeps the difference between "no argument"
// (whitespace, a nil slice) and an explicit set.
func stripCharsParam(chars []string) []string {
	if chars == nil {
		return nil
	}
	return append([]string{}, chars...)
}

// ToInteger parses strings as integers in base (2..36) into dt (a zero
// DType means Int64). With strict, unparsable values are an error;
// otherwise they become null. Polars `str.to_integer(base, dtype, strict)`.
func (o StrOps) ToInteger(base int, dt dtype.DType, strict bool) Expr {
	return fn1p("str.to_integer", o.e, base, dt, strict)
}

// ToTitlecase capitalises the first letter of each word. Polars
// `str.to_titlecase`.
func (o StrOps) ToTitlecase() Expr { return fn1("str.to_titlecase", o.e) }

// PadStart left-pads with fill up to length characters. Polars
// `str.pad_start(length, fill_char)`.
func (o StrOps) PadStart(length int, fill rune) Expr {
	return fn1p("str.pad_start", o.e, length, fill)
}

// PadEnd right-pads with fill up to length characters. Polars
// `str.pad_end(length, fill_char)`.
func (o StrOps) PadEnd(length int, fill rune) Expr {
	return fn1p("str.pad_end", o.e, length, fill)
}

// ZFill left-pads with zeros up to length bytes, keeping a leading
// sign first. Polars `str.zfill(length)`.
func (o StrOps) ZFill(length int) Expr { return fn1p("str.zfill", o.e, length) }
