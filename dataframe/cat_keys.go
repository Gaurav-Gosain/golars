package dataframe

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Group-by and join on Categorical or Enum keys work by decoding the
// keys to strings, running the string path, and restoring the key
// dtype on the output. This is correct but pays a decode per key; a
// code-based fast path is future work.

// catGroupByAgg runs g.Agg with dictionary keys decoded to strings. ok
// is false when no key is a dictionary column.
func catGroupByAgg(ctx context.Context, g *GroupBy, aggs []expr.Expr, opts []GroupByOption) (*DataFrame, bool, error) {
	var dictKeys []string
	for _, k := range g.keys {
		col, err := g.df.Column(k)
		if err != nil {
			return nil, false, nil // let the normal path report it
		}
		if col.DType().IsDictionary() {
			dictKeys = append(dictKeys, k)
		}
	}
	if len(dictKeys) == 0 {
		return nil, false, nil
	}
	cfg := resolveGroupBy(opts)
	decoded := g.df.Clone()
	origTypes := make(map[string]*series.Series, len(dictKeys))
	for _, k := range dictKeys {
		col, _ := g.df.Column(k)
		origTypes[k] = col
		str, err := compute.Cast(ctx, col, dtype.String(), compute.WithAllocator(cfg.alloc))
		if err != nil {
			decoded.Release()
			return nil, true, err
		}
		next, err := decoded.WithColumn(str)
		decoded.Release()
		if err != nil {
			str.Release()
			return nil, true, err
		}
		decoded = next
	}
	defer decoded.Release()
	out, err := decoded.GroupBy(g.keys...).Agg(ctx, aggs, opts...)
	if err != nil {
		return nil, true, err
	}
	for _, k := range dictKeys {
		col, err := out.Column(k)
		if err != nil {
			out.Release()
			return nil, true, err
		}
		restored, err := catRestore(ctx, col, origTypes[k], cfg.alloc)
		if err != nil {
			out.Release()
			return nil, true, err
		}
		next, err := out.WithColumn(restored)
		out.Release()
		if err != nil {
			restored.Release()
			return nil, true, err
		}
		out = next
	}
	return out, true, nil
}

// catRestore casts a decoded string key back to the dtype of orig. An
// Enum keeps orig's dictionary as its categories so codes stay stable.
func catRestore(ctx context.Context, str, orig *series.Series, alloc memory.Allocator) (*series.Series, error) {
	target := dtype.Categorical()
	if orig.DType().IsEnum() {
		cats, err := catDictionaryValues(orig)
		if err != nil {
			return nil, err
		}
		target, err = dtype.NewEnum(cats)
		if err != nil {
			return nil, err
		}
	}
	return compute.Cast(ctx, str, target, compute.WithAllocator(alloc))
}

func catDictionaryValues(s *series.Series) ([]string, error) {
	if s.NumChunks() == 0 {
		return nil, nil
	}
	d, ok := s.Chunk(0).(*array.Dictionary)
	if !ok {
		return nil, fmt.Errorf("dataframe: %q is not a dictionary column", s.Name())
	}
	type stringer interface {
		Len() int
		Value(int) string
	}
	dict, ok := d.Dictionary().(stringer)
	if !ok {
		return nil, fmt.Errorf("dataframe: %q dictionary is not utf8", s.Name())
	}
	out := make([]string, dict.Len())
	for i := range out {
		out[i] = dict.Value(i)
	}
	return out, nil
}

// catJoinIndices hash-joins two dictionary key arrays through their
// decoded strings.
func catJoinIndices(la, ra arrow.Array, how JoinType) ([]int, []int, error) {
	ls, err := catDecodeArray(la)
	if err != nil {
		return nil, nil, err
	}
	defer ls.Release()
	rs, err := catDecodeArray(ra)
	if err != nil {
		return nil, nil, err
	}
	defer rs.Release()
	return hashJoinString(ls, rs, how)
}

func catDecodeArray(a arrow.Array) (arrow.Array, error) {
	d, ok := a.(*array.Dictionary)
	if !ok {
		a.Retain()
		return a, nil
	}
	return series.CatDecode(d, memory.DefaultAllocator)
}

// catGatherNullable gathers dictionary rows where index -1 means null.
// Codes move; the dictionary is shared with the source.
func catGatherNullable(src *series.Series, indices []int, alloc memory.Allocator) (*series.Series, error) {
	d, ok := src.Chunk(0).(*array.Dictionary)
	if !ok {
		return nil, fmt.Errorf("dataframe.Join: %q is not a dictionary column", src.Name())
	}
	b := array.NewUint32Builder(alloc)
	defer b.Release()
	b.Reserve(len(indices))
	for _, i := range indices {
		if i < 0 || d.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.UnsafeAppend(uint32(d.GetValueIndex(i)))
	}
	codes := b.NewUint32Array()
	defer codes.Release()
	dt := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint32,
		ValueType: d.Dictionary().DataType(),
		Ordered:   d.DataType().(*arrow.DictionaryType).Ordered,
	}
	return series.New(src.Name(), array.NewDictionaryArray(dt, codes, d.Dictionary()))
}
