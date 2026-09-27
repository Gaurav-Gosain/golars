package compute

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// catCompare compares when at least one side is Categorical or Enum.
// Dictionary sides are decoded to strings and compared lexically, which
// matches polars for equality and for its lexical ordering.
func catCompare(ctx context.Context, a, b *series.Series, opts []Option, kernel string, op compareOp) (*series.Series, error) {
	cfg := resolve(opts)
	name := cfg.outName(a.Name())
	as, err := catAsString(ctx, a, cfg)
	if err != nil {
		return nil, err
	}
	defer as.Release()
	bs, err := catAsString(ctx, b, cfg)
	if err != nil {
		return nil, err
	}
	defer bs.Release()
	return runCompare(ctx, as, bs, append(opts, WithName(name)), kernel, op)
}

func catAsString(ctx context.Context, s *series.Series, cfg config) (*series.Series, error) {
	if !s.DType().IsDictionary() {
		return s.Clone(), nil
	}
	return Cast(ctx, s, dtype.String(), WithAllocator(cfg.alloc))
}

// catCompareLit compares a dictionary column against a string literal
// by evaluating the predicate once per category and mapping the result
// through the codes.
func catCompareLit(a *series.Series, lit string, opts []Option, op compareOp) (*series.Series, error) {
	cfg := resolve(opts)
	arr, err := extractChunk(a, cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer arr.Release()
	d := arr.(*array.Dictionary)
	type stringer interface {
		Len() int
		Value(int) string
		IsValid(int) bool
	}
	dict, ok := d.Dictionary().(stringer)
	if !ok {
		return nil, isUnsupported("CompareLit", a.DType())
	}
	pred := stringPredicate(op)
	lut := make([]int8, dict.Len()) // -1 null, 0 false, 1 true
	for i := range lut {
		switch {
		case !dict.IsValid(i):
			lut[i] = -1
		case pred(dict.Value(i), lit):
			lut[i] = 1
		}
	}
	n := d.Len()
	valBuf := memory.NewResizableBuffer(cfg.alloc)
	valBuf.Resize((n + 7) / 8)
	vals := valBuf.Bytes()
	clear(vals)
	nullBuf := memory.NewResizableBuffer(cfg.alloc)
	nullBuf.Resize((n + 7) / 8)
	valid := nullBuf.Bytes()
	clear(valid)
	nulls := 0
	for i := range n {
		if d.IsNull(i) {
			nulls++
			continue
		}
		switch lut[d.GetValueIndex(i)] {
		case -1:
			nulls++
			continue
		case 1:
			vals[i>>3] |= 1 << uint(i&7)
		}
		valid[i>>3] |= 1 << uint(i&7)
	}
	if nulls == 0 {
		nullBuf.Release()
		nullBuf = nil
	}
	out := array.NewBoolean(n, valBuf, nullBuf, nulls)
	valBuf.Release()
	if nullBuf != nil {
		nullBuf.Release()
	}
	return series.New(cfg.outName(a.Name()), out)
}
