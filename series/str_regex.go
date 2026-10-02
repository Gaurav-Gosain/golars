package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ContainsRegex returns a boolean Series: true where the value
// matches the compiled regex pattern. Nulls stay null.
func (o StrOps) ContainsRegex(pattern string, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.Str.ContainsRegex: %w", err)
	}
	return o.mapBool("ContainsRegex", re.MatchString, opts)
}

// Extract returns the capture group `group` (0 = entire match) from
// the first regex match per cell. Cells that don't match become null.
// Mirrors polars' str.extract(pattern, group_index).
func (o StrOps) Extract(pattern string, group int, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.Str.Extract: %w", err)
	}
	cfg := resolve(opts)
	a, err := o.stringArr("Extract")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	out := make([]string, n)
	valid := make([]bool, n)
	for i := range n {
		if !a.IsValid(i) {
			continue
		}
		m := re.FindStringSubmatch(a.Value(i))
		if m == nil || group < 0 || group >= len(m) {
			continue
		}
		out[i] = m[group]
		valid[i] = true
	}
	return FromString(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// CountMatchesRegex returns the number of non-overlapping matches of
// pattern in each string.
func (o StrOps) CountMatchesRegex(pattern string, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.Str.CountMatchesRegex: %w", err)
	}
	cfg := resolve(opts)
	a, err := o.stringArr("CountMatchesRegex")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	out := make([]uint32, n) // polars: u32
	valid := make([]bool, n)
	for i := range n {
		if !a.IsValid(i) {
			continue
		}
		out[i] = uint32(len(re.FindAllStringIndex(a.Value(i), -1)))
		valid[i] = true
	}
	return FromUint32(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// ReplaceRegex replaces every regex-matching substring with repl.
func (o StrOps) ReplaceRegex(pattern, repl string, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.Str.ReplaceRegex: %w", err)
	}
	return o.mapString("ReplaceRegex", func(s string) string {
		return re.ReplaceAllString(s, repl)
	}, opts)
}

// SplitN returns the n-th (0-indexed) split segment of each string,
// using sep as the separator. When a cell doesn't have enough
// segments the output is null. Equivalent to polars'
// `str.split_exact(sep, n).struct.field("field_n")`.
func (o StrOps) SplitN(sep string, idx int, opts ...Option) (*Series, error) {
	return o.mapString("SplitN", func(s string) string {
		parts := splitOrEmpty(s, sep)
		if idx < 0 || idx >= len(parts) {
			return ""
		}
		return parts[idx]
	}, opts)
}

// SplitExact splits each cell by sep and returns a List&lt;String&gt;
// Series where each row is the full slice of components for that
// row. Null input rows produce null output rows. Empty strings
// produce a single-element list of "".
//
// Mirrors polars' `s.str.split(sep)`. For the positional-pluck
// variant use SplitN or SplitExactNullShort.
func (o StrOps) SplitExact(sep string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("SplitExact")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	lb := array.NewListBuilder(cfg.alloc, arrow.BinaryTypes.String)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.StringBuilder)
	for i := 0; i < n; i++ {
		if !a.IsValid(i) {
			lb.AppendNull()
			continue
		}
		parts := splitOrEmpty(a.Value(i), sep)
		lb.Append(true)
		for _, p := range parts {
			vb.Append(p)
		}
	}
	return New(o.s.Name()+"_split", lb.NewListArray())
}

// SplitExactNullShort returns the n-th split component, or null if
// the cell has fewer than n+1 components. Use SplitN for the
// empty-string fallback variant.
func (o StrOps) SplitExactNullShort(sep string, idx int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("SplitExactNullShort")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	out := make([]string, n)
	valid := make([]bool, n)
	for i := range n {
		if !a.IsValid(i) {
			continue
		}
		parts := splitOrEmpty(a.Value(i), sep)
		if idx < 0 || idx >= len(parts) {
			continue
		}
		out[i] = parts[idx]
		valid[i] = true
	}
	return FromString(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

func splitOrEmpty(s, sep string) []string {
	if sep == "" {
		return []string{s}
	}
	var out []string
	for {
		i := indexOf(s, sep)
		if i < 0 {
			out = append(out, s)
			return out
		}
		out = append(out, s[:i])
		s = s[i+len(sep):]
	}
}

// indexOf is strings.Index without the pkg import; keeps this file's
// import list tight for callers that only use regex-flavoured ops.
func indexOf(s, substr string) int {
	// Fallback to the standard library's well-tested matcher.
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
