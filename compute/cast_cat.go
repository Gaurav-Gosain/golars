package compute

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// castCategorical handles every cast where the source or the target is
// a dictionary dtype (Categorical or Enum). Rules follow polars:
//
//   - String -> Categorical assigns codes by first appearance.
//   - String / Categorical / Enum -> Enum maps values onto the fixed
//     categories and fails when a value is not a category.
//   - Categorical / Enum -> String decodes. -> integer returns the
//     physical codes. Other targets decode first, then cast the strings.
//   - Other sources -> Categorical / Enum go through String first.
func castCategorical(ctx context.Context, s *series.Series, to dtype.DType, cfg config) (*series.Series, error) {
	name := cfg.outName(s.Name())
	from := s.DType()

	if !from.IsDictionary() {
		src := s
		if !from.IsString() {
			str, err := Cast(ctx, s, dtype.String(), WithAllocator(cfg.alloc))
			if err != nil {
				return nil, err
			}
			defer str.Release()
			src = str
		}
		arr, err := extractChunk(src, cfg.alloc)
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		sa, ok := arr.(*array.String)
		if !ok {
			return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, from, to)
		}
		if to.IsCategorical() {
			return series.New(name, series.CatEncodeStrings(sa, cfg.alloc))
		}
		cats, ok := to.EnumCategories()
		if !ok {
			return nil, fmt.Errorf("%w: cast to enum requires an Enum dtype with categories", ErrUnsupportedDType)
		}
		out, unknown, nUnknown := series.CatEncodeEnum(sa, cats, cfg.alloc)
		if out == nil {
			return nil, series.CatEnumError(from.String(), s.Name(), nUnknown, sa.Len(), unknown)
		}
		return series.New(name, out)
	}

	arr, err := extractChunk(s, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	d, ok := arr.(*array.Dictionary)
	if !ok {
		return nil, fmt.Errorf("%w: cast %s to %s", ErrUnsupportedDType, from, to)
	}

	switch {
	case to.IsCategorical():
		if from.IsCategorical() {
			return s.Rename(name), nil
		}
		dict := d.Dictionary()
		return series.New(name, series.CatWithDictionary(d, to.Arrow(), dict))
	case to.IsEnum():
		cats, ok := to.EnumCategories()
		if !ok {
			if from.IsEnum() {
				return s.Rename(name), nil
			}
			return nil, fmt.Errorf("%w: cast to enum requires an Enum dtype with categories", ErrUnsupportedDType)
		}
		return castDictToEnum(s.Name(), name, from, d, to, cats, cfg)
	case to.IsString():
		out, err := series.CatDecode(d, cfg.alloc)
		if err != nil {
			return nil, err
		}
		return series.New(name, out)
	case to.IsInteger():
		idx := d.Indices()
		idx.Retain()
		codes, err := series.New(name, idx)
		if err != nil {
			idx.Release()
			return nil, err
		}
		defer codes.Release()
		return Cast(ctx, codes, to, WithAllocator(cfg.alloc), WithName(name))
	}
	decoded, err := series.CatDecode(d, cfg.alloc)
	if err != nil {
		return nil, err
	}
	str, err := series.New(name, decoded)
	if err != nil {
		decoded.Release()
		return nil, err
	}
	defer str.Release()
	return Cast(ctx, str, to, WithAllocator(cfg.alloc), WithName(name))
}

// castDictToEnum remaps a Categorical or Enum array onto the target
// Enum's categories through a per-category lookup table.
func castDictToEnum(srcName, name string, from dtype.DType, d *array.Dictionary, to dtype.DType, cats []string, cfg config) (*series.Series, error) {
	pos := make(map[string]int64, len(cats))
	for i, c := range cats {
		pos[c] = int64(i)
	}
	type stringer interface {
		Len() int
		Value(int) string
		IsValid(int) bool
	}
	dict, ok := d.Dictionary().(stringer)
	if !ok {
		return nil, fmt.Errorf("%w: dictionary values %s", ErrUnsupportedDType, d.Dictionary().DataType())
	}
	lut := make([]int64, dict.Len())
	missing := make([]bool, dict.Len())
	for i := range lut {
		if !dict.IsValid(i) {
			lut[i] = -1
			continue
		}
		p, ok := pos[dict.Value(i)]
		if !ok {
			lut[i] = -1
			missing[i] = true
			continue
		}
		lut[i] = p
	}
	// Only categories that rows actually use can fail the cast.
	nUnknown := 0
	var unknown []string
	reported := map[int]bool{}
	for i := range d.Len() {
		if d.IsNull(i) {
			continue
		}
		c := d.GetValueIndex(i)
		if missing[c] {
			nUnknown++
			if !reported[c] {
				reported[c] = true
				unknown = append(unknown, dict.Value(c))
			}
		}
	}
	if nUnknown > 0 {
		return nil, series.CatEnumError(from.String(), srcName, nUnknown, d.Len(), unknown)
	}
	identity := len(lut) == len(cats)
	for i, p := range lut {
		if !identity {
			break
		}
		identity = p == int64(i)
	}
	if identity {
		// Same categories in the same order: share codes and dictionary.
		return series.New(name, series.CatWithDictionary(d, to.Arrow(), d.Dictionary()))
	}
	newDict := catEnumDict(cats, cfg)
	defer newDict.Release()
	out, err := series.CatRemap(d, lut, to.Arrow(), newDict, cfg.alloc)
	if err != nil {
		return nil, err
	}
	return series.New(name, out)
}

func catEnumDict(cats []string, cfg config) *array.String {
	b := array.NewStringBuilder(cfg.alloc)
	defer b.Release()
	b.AppendValues(cats, nil)
	return b.NewStringArray()
}
