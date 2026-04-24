package expr

// StrOps is the expression-level mirror of series.StrOps: a namespace
// of string-column operations reachable from an Expr via .Str().
// Methods return new Exprs so they compose with the rest of the
// algebra; the concrete kernel runs when eval dispatches on the
// FunctionNode's Name.
//
// Every name here is prefixed with "str." so the eval switch can
// separate string kernels from the general function table. Polars
// uses the same mental model: `pl.col("x").str.contains(...)` is a
// namespaced op, not a global function.
type StrOps struct{ e Expr }

// Str returns a string-ops view over e. This mirrors polars'
// `col("x").str.*` namespace: every method returns a new [Expr]
// that plugs back into the normal expression algebra.
//
// Allocation-free wrapper; the actual work happens inside the
// kernel that evaluates the returned [Expr].
//
// # Examples
//
//	// Case fold:
//	expr.Col("name").Str().ToUpper()
//
//	// Regex predicate:
//	expr.Col("email").Str().ContainsRegex(`@example\.com$`)
//
//	// Chain with the list namespace:
//	expr.Col("csv").Str().SplitExact(",").List().Len()
func (e Expr) Str() StrOps { return StrOps{e: e} }

// SplitExact splits each cell by sep and returns a list column
// (dtype List<String>). Compose with the list.* namespace, e.g.
// `col("x").Str().SplitExact(",").List().Len()`.
func (o StrOps) SplitExact(sep string) Expr {
	return fn1p("str.split_exact", o.e, sep)
}

// ---- predicates ----

// Contains tests each row for a literal substring needle. Empty
// needle matches every non-null row; use [StrOps.ContainsRegex] for
// regex behaviour.
//
// # Examples
//
//	// "foo" anywhere in the value:
//	expr.Col("msg").Str().Contains("foo")
func (o StrOps) Contains(needle string) Expr {
	return fn1p("str.contains", o.e, needle)
}

// ContainsRegex runs a compiled [regexp] against each row and
// returns a boolean [Expr].
//
// # Examples
//
//	// Email addresses on example.com:
//	expr.Col("email").Str().ContainsRegex(`@example\.com$`)
func (o StrOps) ContainsRegex(pattern string) Expr {
	return fn1p("str.contains_regex", o.e, pattern)
}

// StartsWith is a byte-prefix predicate. Compare the row's raw
// bytes against prefix; nulls stay null.
//
// # Examples
//
//	expr.Col("sku").Str().StartsWith("US-")
func (o StrOps) StartsWith(prefix string) Expr {
	return fn1p("str.starts_with", o.e, prefix)
}

// EndsWith is a byte-suffix predicate. See [StrOps.StartsWith].
func (o StrOps) EndsWith(suffix string) Expr {
	return fn1p("str.ends_with", o.e, suffix)
}

// Like is a SQL-style wildcard predicate.
//
// # Wildcards
//
//   - `%` matches any sequence of characters (including empty)
//   - `_` matches exactly one character
//   - `\` escapes the following character literal
//
// # Examples
//
//	// Names starting with "a" followed by anything:
//	expr.Col("name").Str().Like("a%")
//
//	// Three-letter codes starting with "F":
//	expr.Col("code").Str().Like("F__")
func (o StrOps) Like(pattern string) Expr {
	return fn1p("str.like", o.e, pattern)
}

// NotLike is the logical negation of [StrOps.Like].
func (o StrOps) NotLike(pattern string) Expr {
	return fn1p("str.not_like", o.e, pattern)
}

// ---- transforms ----

// ToLower folds letters to lowercase (ASCII fast path, UTF-8 fallback).
func (o StrOps) ToLower() Expr { return fn1("str.to_lower", o.e) }

// ToUpper folds letters to uppercase.
func (o StrOps) ToUpper() Expr { return fn1("str.to_upper", o.e) }

// ---- measurement ----

// LenBytes returns the byte-length of each row as int64.
func (o StrOps) LenBytes() Expr { return fn1("str.len_bytes", o.e) }

// LenChars returns the rune-count of each row as int64.
func (o StrOps) LenChars() Expr { return fn1("str.len_chars", o.e) }

// ---- slicing ----

// Head returns the first n characters (runes) of each row. Negative n
// means "all but the last |n|".
func (o StrOps) Head(n int) Expr { return fn1p("str.head", o.e, n) }

// Tail returns the last n characters.
func (o StrOps) Tail(n int) Expr { return fn1p("str.tail", o.e, n) }

// Slice is a two-argument variant: start, length. Length -1 means "to
// end". Mirrors polars str.slice.
func (o StrOps) Slice(start, length int) Expr {
	return fn1p("str.slice", o.e, start, length)
}

// ---- search ----

// Find returns the byte offset of the first occurrence of needle, or
// -1 when missing.
func (o StrOps) Find(needle string) Expr {
	return fn1p("str.find", o.e, needle)
}

// CountMatches counts non-overlapping occurrences of needle per row.
func (o StrOps) CountMatches(needle string) Expr {
	return fn1p("str.count_matches", o.e, needle)
}

// ---- trim / pad ----

// Trim strips leading + trailing whitespace.
func (o StrOps) Trim() Expr { return fn1("str.trim", o.e) }

// StripPrefix drops one occurrence of prefix from the start of each
// row; unmatched rows pass through unchanged.
func (o StrOps) StripPrefix(prefix string) Expr {
	return fn1p("str.strip_prefix", o.e, prefix)
}

// StripSuffix is the symmetric counterpart.
func (o StrOps) StripSuffix(suffix string) Expr {
	return fn1p("str.strip_suffix", o.e, suffix)
}

// ---- replace ----

// Replace swaps the first occurrence of old with new in each row.
func (o StrOps) Replace(old, new string) Expr {
	return fn1p("str.replace", o.e, old, new)
}

// ReplaceAll swaps every occurrence of old.
func (o StrOps) ReplaceAll(old, new string) Expr {
	return fn1p("str.replace_all", o.e, old, new)
}
