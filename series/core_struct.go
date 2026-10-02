package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StructFromSeries combines equal-length Series into one struct Series
// whose fields are named after each input. valid may be nil (every row
// valid). Mirrors the construction behind polars' pl.struct.
func StructFromSeries(name string, fields []*Series, valid []bool, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if len(fields) == 0 {
		return nil, fmt.Errorf("series: struct needs at least one field")
	}
	n := fields[0].Len()
	arrowFields := make([]arrow.Field, len(fields))
	children := make([]arrow.ArrayData, len(fields))
	arrs := make([]arrow.Array, len(fields))
	defer func() {
		for _, a := range arrs {
			if a != nil {
				a.Release()
			}
		}
	}()
	for i, f := range fields {
		if f.Len() != n {
			return nil, fmt.Errorf("series: struct field %q has length %d, want %d", f.Name(), f.Len(), n)
		}
		a, err := f.single(cfg.alloc)
		if err != nil {
			return nil, err
		}
		arrs[i] = a
		arrowFields[i] = arrow.Field{Name: f.Name(), Type: a.DataType(), Nullable: true}
		children[i] = a.Data()
	}
	var validBuf *memory.Buffer
	nulls := 0
	if valid != nil {
		validBuf, nulls = packValidity(valid, cfg.alloc)
		defer validBuf.Release()
		if nulls == 0 {
			validBuf = nil
		}
	}
	data := array.NewData(arrow.StructOf(arrowFields...), n, []*memory.Buffer{validBuf}, children, nulls, 0)
	defer data.Release()
	return New(name, array.MakeFromData(data))
}

// FixedListFromValues wraps values as a fixed-size list Series with the
// given width (polars' Array dtype). values.Len() must be a multiple of
// width.
func FixedListFromValues(name string, values *Series, width int, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	if width <= 0 || values.Len()%width != 0 {
		return nil, fmt.Errorf("series: cannot split %d values into rows of %d", values.Len(), width)
	}
	child, err := values.single(cfg.alloc)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	dt := arrow.FixedSizeListOf(int32(width), child.DataType())
	data := array.NewData(dt, values.Len()/width, []*memory.Buffer{nil}, []arrow.ArrayData{child.Data()}, 0, 0)
	defer data.Release()
	return New(name, array.MakeFromData(data))
}
