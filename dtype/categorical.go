package dtype

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

// Categorical and Enum are both stored as arrow dictionary arrays with
// uint32 codes and utf8 values. The two are told apart by the
// dictionary's Ordered flag: Categorical is unordered, Enum is ordered.
// This survives an IPC round trip because arrow carries the flag in the
// schema.
//
// A Categorical column's dictionary holds the categories in order of
// first appearance. An Enum column's dictionary always holds the full,
// fixed category list, so codes are stable across columns that share
// the same Enum dtype.

type enumInfo struct {
	cats []string
}

func (e *enumInfo) equal(o *enumInfo) bool { return slices.Equal(e.cats, o.cats) }

var (
	categoricalArrow = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint32,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}
	enumArrow = &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint32,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   true,
	}
)

// Categorical returns the categorical string dtype. Mirrors
// polars' pl.Categorical.
func Categorical() DType { return DType{inner: categoricalArrow} }

// Enum returns an enum dtype with a fixed, ordered list of categories.
// Mirrors polars' pl.Enum(categories). It panics when categories hold
// a duplicate; use NewEnum for an error instead.
func Enum(categories []string) DType {
	d, err := NewEnum(categories)
	if err != nil {
		panic(err.Error())
	}
	return d
}

// NewEnum is the error-returning form of Enum. Categories must be
// unique; the slice is copied.
func NewEnum(categories []string) (DType, error) {
	seen := make(map[string]struct{}, len(categories))
	for _, c := range categories {
		if _, dup := seen[c]; dup {
			return DType{}, fmt.Errorf("dtype: Enum categories must be unique; found duplicate %q", c)
		}
		seen[c] = struct{}{}
	}
	return DType{inner: enumArrow, enum: &enumInfo{cats: slices.Clone(categories)}}, nil
}

// IsDictionary reports whether the dtype is backed by an arrow
// dictionary array (Categorical or Enum).
func (d DType) IsDictionary() bool { return d.ID() == arrow.DICTIONARY }

// IsCategorical reports whether d is the Categorical dtype.
func (d DType) IsCategorical() bool {
	t, ok := d.inner.(*arrow.DictionaryType)
	return ok && !t.Ordered
}

// IsEnum reports whether d is an Enum dtype.
func (d DType) IsEnum() bool {
	t, ok := d.inner.(*arrow.DictionaryType)
	return ok && t.Ordered
}

// EnumCategories returns the fixed categories of an Enum dtype built
// with Enum or NewEnum. It returns (nil, false) for other dtypes and
// for Enum dtypes recovered from an arrow array, where the categories
// are the array's dictionary instead.
func (d DType) EnumCategories() ([]string, bool) {
	if d.enum == nil {
		return nil, false
	}
	return slices.Clone(d.enum.cats), true
}

func reprDictionary(t *arrow.DictionaryType) string {
	if !arrow.IsBaseBinary(t.ValueType.ID()) {
		return t.String()
	}
	if t.Ordered {
		return "enum"
	}
	return "cat"
}
