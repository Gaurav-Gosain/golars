package series

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// str_ns.go completes the polars `str` namespace: splitting into lists
// and structs, multi-pattern search, regex extraction, character-set
// stripping, integer parsing and title casing. Every kernel keeps the
// input name, propagates nulls row by row, and writes its output
// through the direct builders in nsbuild.go.

// Split splits each string on by and returns a List<String> column.
//
// Semantics follow polars `str.split(by, inclusive)`:
//   - an empty by splits into characters, and "" becomes [];
//   - with a non-empty by, "" becomes [""];
//   - inclusive keeps the separator at the end of each piece, drops a
//     trailing empty piece, and turns "" into [].
func (o StrOps) Split(by string, inclusive bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "split")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	if len(by) == 1 && !inclusive {
		return splitByteWholeBuffer(o.s.Name(), a, by[0], cfg.alloc)
	}
	n := a.Len()
	lb := newListOffsetsBuilder(cfg.alloc, n)
	child := newStrColBuilder(cfg.alloc, n*2, len(a.ValueBytes()))
	for i := range n {
		if a.IsNull(i) {
			lb.closeRow(child.length, false)
			continue
		}
		strSplitEach(a.Value(i), by, inclusive, -1, func(piece string) bool {
			child.appendString(piece)
			return true
		})
		lb.closeRow(child.length, true)
	}
	return New(o.s.Name(), lb.newListArray(child.newArray()))
}

// strSplitEach yields the pieces of s split on by. limit > 0 caps the
// number of pieces; the last piece then holds the remainder (Rust
// splitn). fn returns false to stop early.
func strSplitEach(s, by string, inclusive bool, limit int, fn func(string) bool) {
	if limit == 0 {
		return
	}
	if by == "" {
		count := 0
		for i := 0; i < len(s); {
			count++
			if limit > 0 && count == limit {
				fn(s[i:])
				return
			}
			_, size := utf8.DecodeRuneInString(s[i:])
			if !fn(s[i : i+size]) {
				return
			}
			i += size
		}
		return
	}
	if inclusive && s == "" {
		return
	}
	count := 0
	for {
		count++
		if limit > 0 && count == limit {
			fn(s)
			return
		}
		j := strings.Index(s, by)
		if j < 0 {
			if inclusive && s == "" {
				return
			}
			fn(s)
			return
		}
		piece := s[:j]
		if inclusive {
			piece = s[:j+len(by)]
		}
		if !fn(piece) {
			return
		}
		s = s[j+len(by):]
		if inclusive && s == "" {
			return
		}
	}
}

// SplitNStruct mirrors polars `str.splitn(by, n)`: split into at most
// n pieces (the last keeps the remainder) and return a struct with
// fields field_0 .. field_{n-1}. Missing pieces are null; a null
// input gives a row whose fields are all null.
func (o StrOps) SplitNStruct(by string, n int, opts ...Option) (*Series, error) {
	return o.splitToStruct("splitn", by, n, n, false, opts)
}

// SplitExactStruct mirrors polars `str.split_exact(by, n, inclusive)`:
// a struct with n+1 fields holding the first n+1 pieces of a full
// split. Extra pieces are dropped; missing pieces are null.
func (o StrOps) SplitExactStruct(by string, n int, inclusive bool, opts ...Option) (*Series, error) {
	return o.splitToStruct("split_exact", by, n+1, -1, inclusive, opts)
}

func (o StrOps) splitToStruct(op, by string, fields, limit int, inclusive bool, opts []Option) (*Series, error) {
	if fields < 0 {
		return nil, fmt.Errorf("series.str.%s: n must be non-negative", op)
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, op)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	rows := a.Len()
	if fields == 0 {
		return emptyStructSeries(o.s.Name(), rows)
	}
	builders := make([]*strColBuilder, fields)
	for f := range builders {
		builders[f] = newStrColBuilder(cfg.alloc, rows, 0)
	}
	for i := range rows {
		k := 0
		if !a.IsNull(i) {
			strSplitEach(a.Value(i), by, inclusive, limit, func(piece string) bool {
				if k >= fields {
					return false
				}
				builders[k].appendString(piece)
				k++
				return true
			})
		}
		for ; k < fields; k++ {
			builders[k].appendNull()
		}
	}
	cols := make([]arrow.Array, fields)
	names := make([]string, fields)
	for f, b := range builders {
		cols[f] = b.newArray()
		names[f] = "field_" + strconv.Itoa(f)
	}
	arr, err := structFromChildren(cols, names, rows, nil, 0)
	if err != nil {
		return nil, err
	}
	return New(o.s.Name(), arr)
}

// emptyStructSeries returns a zero-field struct column of the given
// length. Polars produces these for splitn(by, 0) and for
// extract_groups on a pattern without groups.
func emptyStructSeries(name string, rows int) (*Series, error) {
	data := array.NewData(arrow.StructOf(), rows, []*memory.Buffer{nil}, nil, 0, 0)
	arr := array.NewStructData(data)
	data.Release()
	return New(name, arr)
}

// Join concatenates every string of the column into one value with
// delimiter between them. With ignoreNulls the nulls are skipped;
// otherwise any null makes the result null. Polars `str.join`.
func (o StrOps) Join(delimiter string, ignoreNulls bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "join")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	b := newStrColBuilder(cfg.alloc, 1, len(a.ValueBytes())+a.Len()*len(delimiter))
	if !ignoreNulls && a.NullN() > 0 {
		b.appendNull()
		return b.finish(o.s.Name())
	}
	first := true
	for i := range a.Len() {
		if a.IsNull(i) {
			continue
		}
		if !first {
			b.extend(delimiter)
		}
		b.extend(a.Value(i))
		first = false
	}
	b.closeValue()
	return b.finish(o.s.Name())
}

// ContainsAny reports whether each string contains at least one of
// patterns (literal). Polars `str.contains_any`.
func (o StrOps) ContainsAny(patterns []string, asciiCaseInsensitive bool, opts ...Option) (*Series, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("series.str.contains_any: at least one pattern is required")
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "contains_any")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	ac := newAhoCorasick(patterns, asciiCaseInsensitive)
	return boolResultFromStr(o.s.Name(), a, cfg.alloc, ac.isMatch)
}

// ExtractMany returns, per row, the list of pattern matches found in
// the string (standard Aho-Corasick semantics, see ahocorasick.go).
// Polars `str.extract_many`.
func (o StrOps) ExtractMany(patterns []string, asciiCaseInsensitive, overlapping bool, opts ...Option) (*Series, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("series.str.extract_many: at least one pattern is required")
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "extract_many")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	ac := newAhoCorasick(patterns, asciiCaseInsensitive)
	n := a.Len()
	lb := newListOffsetsBuilder(cfg.alloc, n)
	child := newStrColBuilder(cfg.alloc, n, 0)
	for i := range n {
		if a.IsNull(i) {
			lb.closeRow(child.length, false)
			continue
		}
		v := a.Value(i)
		ac.each(v, overlapping, func(m acMatch) { child.appendString(v[m.start:m.end]) })
		lb.closeRow(child.length, true)
	}
	return New(o.s.Name(), lb.newListArray(child.newArray()))
}

// FindMany returns, per row, the byte offsets (UInt32) where pattern
// matches start. Polars `str.find_many`.
func (o StrOps) FindMany(patterns []string, asciiCaseInsensitive, overlapping bool, opts ...Option) (*Series, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("series.str.find_many: at least one pattern is required")
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "find_many")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	ac := newAhoCorasick(patterns, asciiCaseInsensitive)
	n := a.Len()
	lb := newListOffsetsBuilder(cfg.alloc, n)
	child := array.NewUint32Builder(cfg.alloc)
	defer child.Release()
	for i := range n {
		if a.IsNull(i) {
			lb.closeRow(child.Len(), false)
			continue
		}
		ac.each(a.Value(i), overlapping, func(m acMatch) { child.Append(uint32(m.start)) })
		lb.closeRow(child.Len(), true)
	}
	return New(o.s.Name(), lb.newListArray(child.NewArray()))
}

// ReplaceMany replaces every non-overlapping match of patterns with
// the replacement at the same index. A single replacement is broadcast
// to every pattern. Polars `str.replace_many`.
func (o StrOps) ReplaceMany(patterns, replaceWith []string, asciiCaseInsensitive bool, opts ...Option) (*Series, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("series.str.replace_many: at least one pattern is required")
	}
	if len(replaceWith) != 1 && len(replaceWith) != len(patterns) {
		return nil, fmt.Errorf("series.str.replace_many: expected the same amount of patterns as replacement strings")
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "replace_many")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	ac := newAhoCorasick(patterns, asciiCaseInsensitive)
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes()))
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		last := 0
		ac.each(v, false, func(m acMatch) {
			b.extend(v[last:m.start])
			if len(replaceWith) == 1 {
				b.extend(replaceWith[0])
			} else {
				b.extend(replaceWith[m.pattern])
			}
			last = m.end
		})
		b.extend(v[last:])
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

// EscapeRegex escapes every regex meta character so the value can be
// embedded in a pattern as a literal. Matches the Rust regex crate's
// escape set, which is wider than regexp.QuoteMeta (# & - ~ too).
func (o StrOps) EscapeRegex(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "escape_regex")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes())+n)
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		start := 0
		for j := 0; j < len(v); j++ {
			if isRegexMeta(v[j]) {
				b.extend(v[start:j])
				b.extend(`\`)
				start = j
			}
		}
		b.extend(v[start:])
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

func isRegexMeta(c byte) bool {
	switch c {
	case '\\', '.', '+', '*', '?', '(', ')', '|', '[', ']', '{', '}', '^', '$', '#', '&', '-', '~':
		return true
	}
	return false
}

// Explode turns every character of every string into its own row.
// Empty strings stay as one "" row and nulls stay as one null row, as
// in polars' (deprecated) `str.explode`.
func (o StrOps) Explode(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "explode")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, len(a.ValueBytes())+n, len(a.ValueBytes()))
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		if v == "" {
			b.appendString("")
			continue
		}
		for j := 0; j < len(v); {
			_, size := utf8.DecodeRuneInString(v[j:])
			b.appendString(v[j : j+size])
			j += size
		}
	}
	return b.finish(o.s.Name())
}

// ExtractAll returns every non-overlapping regex match per row as a
// List<String>. Polars `str.extract_all`.
func (o StrOps) ExtractAll(pattern string, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.str.extract_all: %w", err)
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "extract_all")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	lb := newListOffsetsBuilder(cfg.alloc, n)
	child := newStrColBuilder(cfg.alloc, n, 0)
	for i := range n {
		if a.IsNull(i) {
			lb.closeRow(child.length, false)
			continue
		}
		v := a.Value(i)
		for _, loc := range re.FindAllStringIndex(v, -1) {
			child.appendString(v[loc[0]:loc[1]])
		}
		lb.closeRow(child.length, true)
	}
	return New(o.s.Name(), lb.newListArray(child.newArray()))
}

// ExtractGroups returns a struct column with one String field per
// capture group of pattern. Named groups use their name, unnamed ones
// their 1-based index. Rows without a match get null fields; null rows
// stay null. Polars `str.extract_groups`.
func (o StrOps) ExtractGroups(pattern string, opts ...Option) (*Series, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("series.str.extract_groups: %w", err)
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "extract_groups")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	rows := a.Len()
	groups := re.NumSubexp()
	if groups == 0 {
		return emptyStructSeries(o.s.Name(), rows)
	}
	names := make([]string, groups)
	for g, nm := range re.SubexpNames()[1:] {
		if nm == "" {
			nm = strconv.Itoa(g + 1)
		}
		names[g] = nm
	}
	builders := make([]*strColBuilder, groups)
	for g := range builders {
		builders[g] = newStrColBuilder(cfg.alloc, rows, 0)
	}
	var idx []int
	for i := range rows {
		if a.IsNull(i) {
			for _, b := range builders {
				b.appendNull()
			}
			continue
		}
		v := a.Value(i)
		idx = re.FindStringSubmatchIndex(v)
		for g, b := range builders {
			if idx == nil || idx[2*(g+1)] < 0 {
				b.appendNull()
				continue
			}
			b.appendString(v[idx[2*(g+1)]:idx[2*(g+1)+1]])
		}
	}
	cols := make([]arrow.Array, groups)
	for g, b := range builders {
		cols[g] = b.newArray()
	}
	validity := CopyValidityBitmap(a, cfg.alloc)
	arr, err := structFromChildren(cols, names, rows, validity, a.NullN())
	if validity != nil {
		validity.Release()
	}
	if err != nil {
		return nil, err
	}
	return New(o.s.Name(), arr)
}

// StripChars removes leading and trailing characters found in chars.
// With no argument it strips Unicode whitespace; an explicit empty set
// strips nothing. Polars `str.strip_chars`.
func (o StrOps) StripChars(chars []string, opts ...Option) (*Series, error) {
	return o.stripCharsImpl("strip_chars", chars, true, true, opts)
}

// StripCharsStart is StripChars on the leading side only.
func (o StrOps) StripCharsStart(chars []string, opts ...Option) (*Series, error) {
	return o.stripCharsImpl("strip_chars_start", chars, true, false, opts)
}

// StripCharsEnd is StripChars on the trailing side only.
func (o StrOps) StripCharsEnd(chars []string, opts ...Option) (*Series, error) {
	return o.stripCharsImpl("strip_chars_end", chars, false, true, opts)
}

func (o StrOps) stripCharsImpl(op string, chars []string, left, right bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, op)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	var pred func(rune) bool
	if chars == nil {
		pred = unicode.IsSpace
	} else {
		set := strings.Join(chars, "")
		pred = func(r rune) bool { return strings.ContainsRune(set, r) }
	}
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes()))
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		if left {
			v = strings.TrimLeftFunc(v, pred)
		}
		if right {
			v = strings.TrimRightFunc(v, pred)
		}
		b.appendString(v)
	}
	return b.finish(o.s.Name())
}

// ToInteger parses each string as an integer in base (2..36) into the
// integer dtype dt (Int64 when dt is invalid). With strict, any
// unparsable non-null value is an error; otherwise it becomes null.
// Polars `str.to_integer(base, dtype, strict)`.
func (o StrOps) ToInteger(base int, dt dtype.DType, strict bool, opts ...Option) (*Series, error) {
	if base < 2 || base > 36 {
		return nil, fmt.Errorf("series.str.to_integer: `to_integer` called with invalid base '%d'", base)
	}
	if !dt.IsValid() {
		dt = dtype.Int64()
	}
	if !dt.IsInteger() {
		return nil, fmt.Errorf("series.str.to_integer: Invalid dtype %s", dt)
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "to_integer")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	bits := arrowIntBits(dt.ID())
	signed := arrow.IsSignedInteger(dt.ID())
	n := a.Len()
	valid := make([]bool, n)
	ivals := make([]int64, n)
	uvals := make([]uint64, n)
	var failed []string
	var firstErr string
	for i := range n {
		if a.IsNull(i) {
			continue
		}
		v := a.Value(i)
		var perr error
		if signed {
			ivals[i], perr = strconv.ParseInt(v, base, bits)
		} else {
			// Rust accepts a leading '+' for unsigned targets too.
			uvals[i], perr = strconv.ParseUint(strings.TrimPrefix(v, "+"), base, bits)
		}
		if perr != nil {
			if strict {
				if firstErr == "" {
					firstErr = rustIntErrMessage(v, perr)
				}
				failed = append(failed, v)
			}
			continue
		}
		valid[i] = true
	}
	if len(failed) > 0 {
		shown := make([]string, 0, min(len(failed), 10)+1)
		for j, f := range failed {
			if j == 10 {
				shown = append(shown, "…")
				break
			}
			shown = append(shown, strconv.Quote(f))
		}
		return nil, fmt.Errorf("strict integer parsing failed for %d value(s): [%s]; error message for the first shown value: '%s' (consider non-strict parsing)",
			len(failed), strings.Join(shown, ", "), firstErr)
	}
	return buildIntColumn(o.s.Name(), dt, ivals, uvals, valid, cfg.alloc)
}

func arrowIntBits(id arrow.Type) int {
	switch id {
	case arrow.INT8, arrow.UINT8:
		return 8
	case arrow.INT16, arrow.UINT16:
		return 16
	case arrow.INT32, arrow.UINT32:
		return 32
	}
	return 64
}

// rustIntErrMessage maps a strconv failure to the message Rust's
// from_str_radix gives, which polars surfaces verbatim.
func rustIntErrMessage(v string, err error) string {
	if v == "" || v == "+" || v == "-" {
		if v == "" {
			return "cannot parse integer from empty string"
		}
		return "invalid digit found in string"
	}
	if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
		if strings.HasPrefix(v, "-") {
			return "number too small to fit in target type"
		}
		return "number too large to fit in target type"
	}
	return "invalid digit found in string"
}

// buildIntColumn packs parsed integers into a column of dtype dt.
func buildIntColumn(name string, dt dtype.DType, ivals []int64, uvals []uint64, valid []bool, mem memory.Allocator) (*Series, error) {
	n := len(valid)
	allValid := true
	for _, ok := range valid {
		if !ok {
			allValid = false
			break
		}
	}
	var validity []bool
	if !allValid {
		validity = valid
	}
	switch dt.ID() {
	case arrow.INT8:
		b := array.NewInt8Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := range n {
			b.UnsafeAppend(int8(ivals[i]))
		}
		return newWithValidity(name, b.NewArray(), validity, mem)
	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := range n {
			b.UnsafeAppend(int16(ivals[i]))
		}
		return newWithValidity(name, b.NewArray(), validity, mem)
	case arrow.INT32:
		out := make([]int32, n)
		for i := range n {
			out[i] = int32(ivals[i])
		}
		return FromInt32(name, out, validity, WithAllocator(mem))
	case arrow.INT64:
		return FromInt64(name, ivals, validity, WithAllocator(mem))
	case arrow.UINT8:
		b := array.NewUint8Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := range n {
			b.UnsafeAppend(uint8(uvals[i]))
		}
		return newWithValidity(name, b.NewArray(), validity, mem)
	case arrow.UINT16:
		b := array.NewUint16Builder(mem)
		defer b.Release()
		b.Reserve(n)
		for i := range n {
			b.UnsafeAppend(uint16(uvals[i]))
		}
		return newWithValidity(name, b.NewArray(), validity, mem)
	case arrow.UINT32:
		out := make([]uint32, n)
		for i := range n {
			out[i] = uint32(uvals[i])
		}
		return FromUint32(name, out, validity, WithAllocator(mem))
	case arrow.UINT64:
		return FromUint64(name, uvals, validity, WithAllocator(mem))
	}
	return nil, fmt.Errorf("series.str.to_integer: unsupported dtype %s", dt)
}

// newWithValidity attaches a []bool validity to a freshly built
// primitive array (whose own validity is all-valid).
func newWithValidity(name string, arr arrow.Array, valid []bool, mem memory.Allocator) (*Series, error) {
	if valid == nil {
		return New(name, arr)
	}
	vbuf, nulls := packBoolsCounted(valid, mem)
	d := arr.Data()
	bufs := append([]*memory.Buffer(nil), d.Buffers()...)
	bufs[0] = vbuf
	data := array.NewData(d.DataType(), d.Len(), bufs, nil, nulls, d.Offset())
	out := array.MakeFromData(data)
	data.Release()
	arr.Release()
	if vbuf != nil {
		vbuf.Release()
	}
	return New(name, out)
}

// ToTitlecase uppercases the first character of every word and
// lowercases the rest. A word starts after any non-alphabetic
// character, as in polars `str.to_titlecase` (so "it's" becomes
// "It'S" and "x2y" becomes "X2Y").
func (o StrOps) ToTitlecase(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "to_titlecase")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes()))
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		upperNext := true
		for _, r := range a.Value(i) {
			if upperNext {
				if full, ok := fullUpperMapping[r]; ok {
					b.extend(full)
				} else {
					b.extendRune(unicode.ToUpper(r))
				}
			} else {
				if full, ok := fullLowerMapping[r]; ok {
					b.extend(full)
				} else {
					b.extendRune(unicode.ToLower(r))
				}
			}
			upperNext = !unicode.IsLetter(r)
		}
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

// fullUpperMapping holds the unconditional one-to-many uppercase
// mappings from Unicode SpecialCasing.txt that Rust applies and Go's
// simple case mapping does not.
var fullUpperMapping = map[rune]string{
	'ß': "SS", 'ŉ': "ʼN", 'ǰ': "J̌", 'ΐ': "Ϊ́", 'ΰ': "Ϋ́", 'և': "ԵՒ",
	'ẖ': "H̱", 'ẗ': "T̈", 'ẘ': "W̊", 'ẙ': "Y̊", 'ẚ': "Aʾ",
	'ﬀ': "FF", 'ﬁ': "FI", 'ﬂ': "FL", 'ﬃ': "FFI", 'ﬄ': "FFL", 'ﬅ': "ST", 'ﬆ': "ST",
	'ﬓ': "ՄՆ", 'ﬔ': "ՄԵ", 'ﬕ': "ՄԻ", 'ﬖ': "ՎՆ", 'ﬗ': "ՄԽ",
}

// fullLowerMapping is the lowercase counterpart.
var fullLowerMapping = map[rune]string{
	'İ': "i̇",
}

// ZFill left-pads with '0' up to length bytes, keeping a leading
// sign in front. Length is measured in bytes, as in polars
// `str.zfill`.
func (o StrOps) ZFill(length int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "zfill")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes())+n*max(length, 0))
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		pad := length - len(v)
		if pad <= 0 {
			b.appendString(v)
			continue
		}
		body := v
		if strings.HasPrefix(v, "-") || strings.HasPrefix(v, "+") {
			b.extend(v[:1])
			body = v[1:]
		}
		for range pad {
			b.extend("0")
		}
		b.extend(body)
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

// PadStart pads each string on the left with fill up to length
// characters. Polars `str.pad_start`.
func (o StrOps) PadStart(length int, fill rune, opts ...Option) (*Series, error) {
	return o.padImpl("pad_start", length, fill, true, opts)
}

// PadEnd pads on the right. Polars `str.pad_end`.
func (o StrOps) PadEnd(length int, fill rune, opts ...Option) (*Series, error) {
	return o.padImpl("pad_end", length, fill, false, opts)
}

func (o StrOps) padImpl(op string, length int, fill rune, start bool, opts []Option) (*Series, error) {
	if length < 0 {
		return nil, fmt.Errorf("series.str.%s: length must be non-negative, got %d", op, length)
	}
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, op)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	var fillBuf [4]byte
	fillStr := string(fillBuf[:encodeRune(fillBuf[:], fill)])
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes())+n*length)
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		pad := length - utf8.RuneCountInString(v)
		if !start {
			b.extend(v)
		}
		for range pad {
			b.extend(fillStr)
		}
		if start {
			b.extend(v)
		}
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

// Reverse reverses each string by extended grapheme cluster
// (approximated: combining marks, variation selectors, emoji
// modifiers, ZWJ sequences, regional-indicator pairs and CRLF stay
// together), which is what polars `str.reverse` does.
func (o StrOps) Reverse(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := stringArrayOf(o.s, "reverse")
	if err != nil {
		return nil, err
	}
	defer a.Release()
	n := a.Len()
	b := newStrColBuilder(cfg.alloc, n, len(a.ValueBytes()))
	var bounds []int
	for i := range n {
		if a.IsNull(i) {
			b.appendNull()
			continue
		}
		v := a.Value(i)
		if isASCIIString(v) && !strings.Contains(v, "\r\n") {
			b.data.reserve(b.mem, len(v))
			dst := b.data.b[b.data.n : b.data.n+len(v)]
			for j := range len(v) {
				dst[len(v)-1-j] = v[j]
			}
			b.data.n += len(v)
			b.closeValue()
			continue
		}
		bounds = graphemeBounds(v, bounds[:0])
		for k := len(bounds) - 1; k > 0; k-- {
			b.extend(v[bounds[k-1]:bounds[k]])
		}
		b.closeValue()
	}
	return b.finish(o.s.Name())
}

func isASCIIString(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// graphemeBounds appends cluster boundaries of s (starting with 0 and
// ending with len(s)) to dst.
func graphemeBounds(s string, dst []int) []int {
	dst = append(dst, 0)
	prevPictographic := false
	prevZWJ := false
	riRun := 0
	prevCR := false
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if i > 0 {
			join := false
			switch {
			case prevCR && r == '\n':
				join = true
			case isGraphemeExtend(r):
				join = true
			case prevZWJ && prevPictographic && isPictographic(r):
				join = true
			case isRegionalIndicator(r) && riRun%2 == 1:
				join = true
			}
			if !join {
				dst = append(dst, i)
				prevPictographic = false
			}
		}
		if isRegionalIndicator(r) {
			riRun++
		} else {
			riRun = 0
		}
		if isPictographic(r) {
			prevPictographic = true
		}
		prevZWJ = r == 0x200D
		prevCR = r == '\r'
		i += size
	}
	return append(dst, len(s))
}

func isGraphemeExtend(r rune) bool {
	if r < 0x300 {
		return false
	}
	return r == 0x200D || (r >= 0xFE00 && r <= 0xFE0F) || (r >= 0xE0100 && r <= 0xE01EF) ||
		(r >= 0x1F3FB && r <= 0x1F3FF) || (r >= 0xE0020 && r <= 0xE007F) ||
		unicode.In(r, unicode.Mn, unicode.Me, unicode.Mc)
}

func isRegionalIndicator(r rune) bool { return r >= 0x1F1E6 && r <= 0x1F1FF }

func isPictographic(r rune) bool {
	return (r >= 0x1F000 && r <= 0x1FAFF && !isRegionalIndicator(r) && !(r >= 0x1F3FB && r <= 0x1F3FF)) ||
		(r >= 0x2600 && r <= 0x27BF) || (r >= 0x2300 && r <= 0x23FF) || r == 0x00A9 || r == 0x00AE ||
		(r >= 0x2190 && r <= 0x21FF) || (r >= 0x2B00 && r <= 0x2BFF)
}
