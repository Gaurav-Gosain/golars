package series

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// StrOps exposes string-specific operations on a utf8 Series. Access
// via Series.Str(). A Series.Str() on a non-string dtype returns an
// ops handle whose methods return errors.
//
// Mirrors polars' `s.str.*` namespace. Each method returns a fresh
// Series and does not mutate the receiver. The underlying string
// buffer is not shared: results are freshly allocated so the caller
// owns their buffers (Release when done).
type StrOps struct {
	s *Series
}

// Str returns the string-ops view over s. Call Str() once per
// pipeline step; the returned value is cheap (no allocation).
func (s *Series) Str() StrOps { return StrOps{s: s} }

func (o StrOps) unsupported(op string) error {
	return fmt.Errorf("series.Str.%s: requires string dtype, got %s", op, o.s.DType())
}

// stringArr returns the underlying array.String or an error when the
// Series dtype is not string.
func (o StrOps) stringArr(op string) (*array.String, error) {
	a, ok := o.s.Chunk(0).(*array.String)
	if !ok {
		return nil, o.unsupported(op)
	}
	return a, nil
}

// mapString applies fn to every non-null string and builds a new
// Series with the same null pattern. Returns (nil, unsupported-error)
// for non-string dtypes.
func (o StrOps) mapString(op string, fn func(string) string, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr(op)
	if err != nil {
		return nil, err
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]string, n)
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = fn(a.Value(i))
	}
	return FromString(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// mapBool applies fn to every non-null string and builds a boolean
// Series. Null inputs yield null outputs.
func (o StrOps) mapBool(op string, fn func(string) bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr(op)
	if err != nil {
		return nil, err
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]bool, n)
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = fn(a.Value(i))
	}
	return FromBool(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// ToUppercase uppercases every character.
//
// Pure-ASCII columns take caseFoldAscii, which folds the flat values
// buffer eight bytes at a time (in parallel for large inputs) and
// copies the offsets unchanged. Other columns fall back to
// strings.ToUpper per row.
func (o StrOps) ToUppercase(opts ...Option) (*Series, error) {
	return o.caseFold("ToUppercase", true, strings.ToUpper, opts)
}

// ToLowercase is the symmetric counterpart of ToUppercase.
func (o StrOps) ToLowercase(opts ...Option) (*Series, error) {
	return o.caseFold("ToLowercase", false, strings.ToLower, opts)
}

func (o StrOps) caseFold(op string, upper bool, slow func(string) string, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr(op)
	if err != nil {
		return nil, err
	}
	out, ok, err := caseFoldAscii(o.s.Name(), a, cfg.alloc, upper)
	if ok || err != nil {
		return out, err
	}
	return o.mapString(op, slow, opts)
}

// Upper is the polars-style alias for ToUppercase.
func (o StrOps) Upper(opts ...Option) (*Series, error) { return o.ToUppercase(opts...) }

// Lower is the polars-style alias for ToLowercase.
func (o StrOps) Lower(opts ...Option) (*Series, error) { return o.ToLowercase(opts...) }

// Title capitalises the first letter of each word. Go's stdlib has no
// direct equivalent of Python's str.title; we roll a simple version
// that splits on whitespace.
func (o StrOps) Title(opts ...Option) (*Series, error) {
	return o.mapString("Title", func(s string) string {
		fields := strings.Fields(s)
		for i, f := range fields {
			if f == "" {
				continue
			}
			fields[i] = strings.ToUpper(f[:1]) + strings.ToLower(f[1:])
		}
		return strings.Join(fields, " ")
	}, opts)
}

// LenBytes returns the byte-length of each string as an int64 Series.
// Nulls propagate to null outputs.
//
// Zero-read implementation: each row's byte length is the difference
// of adjacent offsets. We never touch the values buffer, so the hot
// loop is two int32 loads + subtract + store, trivially SIMD-friendly
// and predictable for the branch predictor.
func (o StrOps) LenBytes(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("LenBytes")
	if err != nil {
		return nil, err
	}
	return int64FromOffsets(o.s.Name(), a, cfg.alloc, lenBytesFill(a.ValueOffsets()))
}

// LenChars returns the rune-count of each string as an int64 Series.
// For ASCII-only input this equals LenBytes, and the kernel reads only
// the offsets.
func (o StrOps) LenChars(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("LenChars")
	if err != nil {
		return nil, err
	}
	return lenCharsKernel(o.s.Name(), a, cfg.alloc)
}

// Contains reports whether each string contains needle (plain substring,
// not regex). For regex patterns, use ContainsRegex. Null inputs yield
// null outputs.
//
// A non-empty needle is searched over the flat values buffer in one
// pass (see containsLiteralKernel). An empty needle matches every
// non-null row.
func (o StrOps) Contains(needle string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("Contains")
	if err != nil {
		return nil, err
	}
	if needle == "" {
		return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(string) bool { return true })
	}
	return containsLiteralKernel(o.s.Name(), a, cfg.alloc, []byte(needle))
}

// StartsWith reports whether each string begins with prefix. Direct
// byte comparison via strings.HasPrefix; no allocation per row.
func (o StrOps) StartsWith(prefix string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("StartsWith")
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(string) bool { return true })
	}
	return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(s string) bool {
		return strings.HasPrefix(s, prefix)
	})
}

// EndsWith reports whether each string ends with suffix.
func (o StrOps) EndsWith(suffix string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("EndsWith")
	if err != nil {
		return nil, err
	}
	if suffix == "" {
		return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(string) bool { return true })
	}
	return boolResultFromStr(o.s.Name(), a, cfg.alloc, func(s string) bool {
		return strings.HasSuffix(s, suffix)
	})
}

// Replace replaces the first occurrence of old with new in each
// string. Use ReplaceAll for global replacement.
func (o StrOps) Replace(old, new string, opts ...Option) (*Series, error) {
	return o.mapString("Replace", func(s string) string {
		return strings.Replace(s, old, new, 1)
	}, opts)
}

// ReplaceAll replaces every occurrence of old.
func (o StrOps) ReplaceAll(old, new string, opts ...Option) (*Series, error) {
	return o.mapString("ReplaceAll", func(s string) string {
		return strings.ReplaceAll(s, old, new)
	}, opts)
}

// Trim removes leading and trailing whitespace.
func (o StrOps) Trim(opts ...Option) (*Series, error) {
	return o.mapString("Trim", strings.TrimSpace, opts)
}

// LStrip removes leading characters matching cutset. When cutset is
// empty, removes whitespace.
func (o StrOps) LStrip(cutset string, opts ...Option) (*Series, error) {
	return o.mapString("LStrip", func(s string) string {
		if cutset == "" {
			return strings.TrimLeft(s, " \t\n\r")
		}
		return strings.TrimLeft(s, cutset)
	}, opts)
}

// RStrip removes trailing characters matching cutset.
func (o StrOps) RStrip(cutset string, opts ...Option) (*Series, error) {
	return o.mapString("RStrip", func(s string) string {
		if cutset == "" {
			return strings.TrimRight(s, " \t\n\r")
		}
		return strings.TrimRight(s, cutset)
	}, opts)
}

// StripPrefix removes prefix from each string that starts with it;
// strings that don't are left unchanged.
func (o StrOps) StripPrefix(prefix string, opts ...Option) (*Series, error) {
	return o.mapString("StripPrefix", func(s string) string {
		return strings.TrimPrefix(s, prefix)
	}, opts)
}

// StripSuffix is the symmetric counterpart.
func (o StrOps) StripSuffix(suffix string, opts ...Option) (*Series, error) {
	return o.mapString("StripSuffix", func(s string) string {
		return strings.TrimSuffix(s, suffix)
	}, opts)
}

// Slice returns runes[start:start+length] of each string. length=-1
// means "to the end". start may be negative (polars convention: count
// from end).
func (o StrOps) Slice(start, length int, opts ...Option) (*Series, error) {
	return o.mapString("Slice", func(s string) string {
		runes := []rune(s)
		n := len(runes)
		// polars computes the stop from the unclamped start, so a
		// negative offset past the beginning shortens the window:
		// "c0".slice(-3, 2) is "c".
		a := start
		if a < 0 {
			a = n + a
		}
		b := n
		if length >= 0 {
			b = a + length
		}
		a = max(0, min(a, n))
		b = max(0, min(b, n))
		if b <= a {
			return ""
		}
		return string(runes[a:b])
	}, opts)
}

// CountMatches counts the number of (non-overlapping) occurrences of
// needle in each string, as u32 like polars.
func (o StrOps) CountMatches(needle string, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	a, err := o.stringArr("CountMatches")
	if err != nil {
		return nil, err
	}
	n := a.Len()
	valid := validFromChunk(a)
	out := make([]uint32, n) // polars: u32
	for i := range n {
		if valid != nil && !valid[i] {
			continue
		}
		out[i] = uint32(strings.Count(a.Value(i), needle))
	}
	return FromUint32(o.s.Name(), out, valid, WithAllocator(cfg.alloc))
}

// Concat appends suffix to every non-null string. Name-level:
// Series.Str().Concat(" suffix") returns `value + " suffix"`.
func (o StrOps) Concat(suffix string, opts ...Option) (*Series, error) {
	return o.mapString("Concat", func(s string) string { return s + suffix }, opts)
}

// Prefix prepends prefix to every non-null string.
func (o StrOps) Prefix(prefix string, opts ...Option) (*Series, error) {
	return o.mapString("Prefix", func(s string) string { return prefix + s }, opts)
}
