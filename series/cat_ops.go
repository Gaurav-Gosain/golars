package series

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// CatOps is the namespace for Categorical and Enum Series. Reach it via
// s.Cat(). Mirrors polars' `s.cat.*`. Kernels evaluate once per
// category and then map the result through the codes, so their cost is
// one table lookup per row regardless of string length.
type CatOps struct{ s *Series }

// Cat returns the categorical namespace. Methods fail with an error
// when the Series is not Categorical or Enum.
func (s *Series) Cat() CatOps { return CatOps{s: s} }

// NewCategorical builds a Categorical Series from Go strings. valid may
// be nil (all valid). Codes follow first appearance.
func NewCategorical(name string, values []string, valid []bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	str, err := FromString(name, values, valid, opts...)
	if err != nil {
		return nil, err
	}
	defer str.Release()
	a := str.Chunk(0).(*array.String)
	return New(name, CatEncodeStrings(a, cfg.alloc))
}

// NewEnum builds an Enum Series with the categories of dt from Go
// strings. It errors when a value is not one of the categories.
func NewEnum(name string, values []string, valid []bool, dt dtype.DType, opts ...Option) (*Series, error) {
	cats, ok := dt.EnumCategories()
	if !ok {
		return nil, fmt.Errorf("series.NewEnum: %s is not an Enum dtype with known categories", dt)
	}
	cfg := resolve(opts)
	str, err := FromString(name, values, valid, opts...)
	if err != nil {
		return nil, err
	}
	defer str.Release()
	out, unknown, nUnknown := CatEncodeEnum(str.Chunk(0).(*array.String), cats, cfg.alloc)
	if out == nil {
		return nil, CatEnumError("str", name, nUnknown, len(values), unknown)
	}
	return New(name, out)
}

// CatEnumError builds the polars-style error returned when a value is
// missing from an Enum's categories.
func CatEnumError(from, name string, nUnknown, total int, unknown []string) error {
	quoted := make([]string, 0, min(len(unknown), 10))
	for _, u := range unknown[:min(len(unknown), 10)] {
		quoted = append(quoted, fmt.Sprintf("%q", u))
	}
	return fmt.Errorf("conversion from `%s` to `enum` failed in column '%s' for %d out of %d values: [%s]; ensure that all values in the input column are present in the categories of the enum datatype",
		from, name, nUnknown, total, strings.Join(quoted, ", "))
}

// dictArray returns the Series as one dictionary array. The caller
// releases it.
func (o CatOps) dictArray(op string) (*array.Dictionary, error) {
	if !o.s.DType().IsDictionary() {
		return nil, fmt.Errorf("series.cat.%s: %q has dtype %s (need cat or enum)", op, o.s.Name(), o.s.DType())
	}
	if o.s.NumChunks() == 0 {
		idx := array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Uint32, 0)
		defer idx.Release()
		dict := catStringArray(nil, memory.DefaultAllocator)
		defer dict.Release()
		return array.NewDictionaryArray(o.s.DType().Arrow(), idx, dict), nil
	}
	arr, err := o.s.Consolidated()
	if err != nil {
		return nil, err
	}
	d, ok := arr.(*array.Dictionary)
	if !ok {
		arr.Release()
		return nil, fmt.Errorf("series.cat.%s: unexpected array type %T", op, arr)
	}
	return d, nil
}

// catDictStrings returns the dictionary values and their validity.
func catDictStrings(d *array.Dictionary) ([]string, []bool, error) {
	type stringer interface {
		Len() int
		Value(int) string
		IsValid(int) bool
	}
	dict, ok := d.Dictionary().(stringer)
	if !ok {
		return nil, nil, fmt.Errorf("series.cat: dictionary values must be strings, got %s", d.Dictionary().DataType())
	}
	n := dict.Len()
	vals := make([]string, n)
	valid := make([]bool, n)
	for i := range n {
		if dict.IsValid(i) {
			vals[i] = dict.Value(i)
			valid[i] = true
		}
	}
	return vals, valid, nil
}

// GetCategories returns the categories of the column as a String
// Series named after the column. For Enum this is the fixed category
// list; for Categorical it is the column's own dictionary in order of
// first appearance (polars returns its global string cache instead).
func (o CatOps) GetCategories(opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	d, err := o.dictArray("get_categories")
	if err != nil {
		return nil, err
	}
	defer d.Release()
	vals, valid, err := catDictStrings(d)
	if err != nil {
		return nil, err
	}
	allValid := true
	for _, v := range valid {
		allValid = allValid && v
	}
	if allValid {
		valid = nil
	}
	return FromString(o.s.Name(), vals, valid, WithAllocator(cfg.alloc))
}

// LenBytes returns the byte length of each value as UInt32.
func (o CatOps) LenBytes(opts ...Option) (*Series, error) {
	return o.mapUint32("len_bytes", func(s string) uint32 { return uint32(len(s)) }, opts)
}

// LenChars returns the character (rune) count of each value as UInt32.
func (o CatOps) LenChars(opts ...Option) (*Series, error) {
	return o.mapUint32("len_chars", func(s string) uint32 { return uint32(utf8.RuneCountInString(s)) }, opts)
}

// StartsWith reports whether each value begins with prefix.
func (o CatOps) StartsWith(prefix string, opts ...Option) (*Series, error) {
	return o.mapBool("starts_with", func(s string) bool { return strings.HasPrefix(s, prefix) }, opts)
}

// EndsWith reports whether each value ends with suffix.
func (o CatOps) EndsWith(suffix string, opts ...Option) (*Series, error) {
	return o.mapBool("ends_with", func(s string) bool { return strings.HasSuffix(s, suffix) }, opts)
}

// Slice returns a character slice of each value as a String Series.
// offset counts from the end when negative; length < 0 means to the
// end of the string. Mirrors polars' cat.slice(offset, length=None).
func (o CatOps) Slice(offset, length int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	d, err := o.dictArray("slice")
	if err != nil {
		return nil, err
	}
	defer d.Release()
	vals, valid, err := catDictStrings(d)
	if err != nil {
		return nil, err
	}
	sliced := make([]string, len(vals))
	for i, v := range vals {
		if valid[i] {
			sliced[i] = catSliceRunes(v, offset, length)
		}
	}
	b := array.NewStringBuilder(cfg.alloc)
	defer b.Release()
	b.AppendValues(sliced, valid)
	dict := b.NewStringArray()
	defer dict.Release()
	re := CatWithDictionary(d, d.DataType(), dict)
	defer re.Release()
	out, err := CatDecode(re, cfg.alloc)
	if err != nil {
		return nil, err
	}
	return New(o.s.Name(), out)
}

// catSliceRunes slices s by runes with polars' offset/length rules.
func catSliceRunes(s string, offset, length int) string {
	n := utf8.RuneCountInString(s)
	start := offset
	if start < 0 {
		start += n
	}
	// The end is computed from the unclamped start, so a far negative
	// offset with a short length yields "" like polars.
	end := n
	if length >= 0 {
		end = start + length
	}
	start = min(max(start, 0), n)
	end = min(max(end, 0), n)
	if start >= end {
		return ""
	}
	// Walk runes once to find byte positions.
	bs, be := len(s), len(s)
	r := 0
	for i := range s {
		if r == start {
			bs = i
		}
		if r == end {
			be = i
			break
		}
		r++
	}
	return s[bs:be]
}

func (o CatOps) mapUint32(op string, fn func(string) uint32, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	d, err := o.dictArray(op)
	if err != nil {
		return nil, err
	}
	defer d.Release()
	vals, dvalid, err := catDictStrings(d)
	if err != nil {
		return nil, err
	}
	lut := make([]uint32, len(vals))
	for i, v := range vals {
		if dvalid[i] {
			lut[i] = fn(v)
		}
	}
	codes, err := catCodes(d)
	if err != nil {
		return nil, err
	}
	n := d.Len()
	buf := memory.NewResizableBuffer(cfg.alloc)
	buf.Resize(n * 4)
	out := arrow.Uint32Traits.CastFromBytes(buf.Bytes())
	nullBuf, nulls := catRowValidity(d, codes, dvalid, cfg.alloc)
	for i := range n {
		if !d.IsNull(i) {
			out[i] = lut[codes[i]]
		} else {
			out[i] = 0
		}
	}
	data := array.NewData(arrow.PrimitiveTypes.Uint32, n, []*memory.Buffer{nullBuf, buf}, nil, nulls, 0)
	arr := array.NewUint32Data(data)
	data.Release()
	buf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(o.s.Name(), arr)
}

func (o CatOps) mapBool(op string, fn func(string) bool, opts []Option) (*Series, error) {
	cfg := resolve(opts)
	d, err := o.dictArray(op)
	if err != nil {
		return nil, err
	}
	defer d.Release()
	vals, dvalid, err := catDictStrings(d)
	if err != nil {
		return nil, err
	}
	lut := make([]bool, len(vals))
	for i, v := range vals {
		if dvalid[i] {
			lut[i] = fn(v)
		}
	}
	codes, err := catCodes(d)
	if err != nil {
		return nil, err
	}
	n := d.Len()
	buf := memory.NewResizableBuffer(cfg.alloc)
	buf.Resize((n + 7) / 8)
	bits := buf.Bytes()
	clear(bits)
	for i := range n {
		if !d.IsNull(i) && lut[codes[i]] {
			bits[i>>3] |= 1 << uint(i&7)
		}
	}
	nullBuf, nulls := catRowValidity(d, codes, dvalid, cfg.alloc)
	arr := array.NewBoolean(n, buf, nullBuf, nulls)
	buf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return New(o.s.Name(), arr)
}

// catRowValidity builds the output validity bitmap: a row is valid when
// its code is valid and the category it points to is valid.
func catRowValidity(d *array.Dictionary, codes []uint32, dvalid []bool, mem memory.Allocator) (*memory.Buffer, int) {
	dictNulls := false
	for _, v := range dvalid {
		if !v {
			dictNulls = true
			break
		}
	}
	if !dictNulls {
		return CopyValidityBitmap(d, mem), d.NullN()
	}
	n := d.Len()
	buf := memory.NewResizableBuffer(mem)
	buf.Resize((n + 7) / 8)
	bits := buf.Bytes()
	clear(bits)
	nulls := 0
	for i := range n {
		if d.IsNull(i) || !dvalid[codes[i]] {
			nulls++
			continue
		}
		bits[i>>3] |= 1 << uint(i&7)
	}
	return buf, nulls
}
