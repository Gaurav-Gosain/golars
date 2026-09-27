package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StructOps is the namespace for operations over struct-typed Series.
// Reach it via `s.Struct()`. Mirrors polars' `s.struct.*` surface.
type StructOps struct{ s *Series }

// Struct returns the struct namespace. Every method fails with a
// clear error when the underlying Series is not struct-typed.
func (s *Series) Struct() StructOps { return StructOps{s: s} }

// structArray returns the consolidated struct array (caller releases).
func (o StructOps) structArray(op string) (*array.Struct, error) {
	if !o.s.DType().IsStruct() {
		return nil, fmt.Errorf("series.struct.%s: %q has dtype %s (need struct)",
			op, o.s.Name(), o.s.DType())
	}
	a, err := o.s.Consolidated()
	if err != nil {
		return nil, err
	}
	sa, ok := a.(*array.Struct)
	if !ok {
		a.Release()
		return nil, fmt.Errorf("series.struct.%s: chunk is not a struct array", op)
	}
	return sa, nil
}

// Field returns the named field of the struct as a new top-level
// Series named after the field. Struct-level nulls propagate into the
// child's validity so downstream kernels see consistent null rows.
func (o StructOps) Field(name string, opts ...Option) (*Series, error) {
	sa, err := o.structArray("field")
	if err != nil {
		return nil, err
	}
	defer sa.Release()
	st := sa.DataType().(*arrow.StructType)
	idx, ok := st.FieldIdx(name)
	if !ok {
		return nil, fmt.Errorf("series.struct.field: no field %q (struct has %s)", name, dtypeName(st))
	}
	folded := foldStructNullsInto(sa, sa.Field(idx), resolve(opts).alloc)
	return New(name, folded)
}

// FieldNames returns the struct's field names in declaration order.
func (o StructOps) FieldNames() ([]string, error) {
	if !o.s.DType().IsStruct() {
		return nil, fmt.Errorf("series.struct.field_names: %q has dtype %s (need struct)",
			o.s.Name(), o.s.DType())
	}
	st, ok := o.s.DType().Arrow().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("series.struct.field_names: %q has no struct metadata", o.s.Name())
	}
	out := make([]string, st.NumFields())
	for i, f := range st.Fields() {
		out[i] = f.Name
	}
	return out, nil
}

// NumFields returns the field count of the struct.
func (o StructOps) NumFields() (int, error) {
	names, err := o.FieldNames()
	if err != nil {
		return 0, err
	}
	return len(names), nil
}

// Unnest returns every field as its own Series (named after the
// field), with struct-level nulls folded into each field. Polars
// `struct.unnest`.
func (o StructOps) Unnest(opts ...Option) ([]*Series, error) {
	sa, err := o.structArray("unnest")
	if err != nil {
		return nil, err
	}
	defer sa.Release()
	mem := resolve(opts).alloc
	st := sa.DataType().(*arrow.StructType)
	out := make([]*Series, 0, st.NumFields())
	for f := range st.NumFields() {
		s, err := New(st.Field(f).Name, foldStructNullsInto(sa, sa.Field(f), mem))
		if err != nil {
			for _, p := range out {
				p.Release()
			}
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// RenameFields renames the struct fields positionally. Extra names are
// ignored; with fewer names the trailing fields are dropped, as in
// polars `struct.rename_fields`.
func (o StructOps) RenameFields(names []string) (*Series, error) {
	sa, err := o.structArray("rename_fields")
	if err != nil {
		return nil, err
	}
	defer sa.Release()
	st := sa.DataType().(*arrow.StructType)
	k := min(len(names), st.NumFields())
	fields := make([]arrow.Field, k)
	children := make([]arrow.ArrayData, k)
	for f := range k {
		fields[f] = st.Field(f)
		fields[f].Name = names[f]
		children[f] = sa.Field(f).Data()
	}
	return rebuildStruct(o.s.Name(), sa, fields, children)
}

// MapFieldNames renames every field with fn (the name.*_fields
// family).
func (o StructOps) MapFieldNames(fn func(string) string) (*Series, error) {
	names, err := o.FieldNames()
	if err != nil {
		return nil, err
	}
	for i, n := range names {
		names[i] = fn(n)
	}
	return o.RenameFields(names)
}

// WithFields returns the struct with the given Series added as fields
// (named after each Series). A Series whose name matches an existing
// field replaces it in place. Length-1 Series broadcast. The struct's
// own null rows are kept.
func (o StructOps) WithFields(cols []*Series, opts ...Option) (*Series, error) {
	sa, err := o.structArray("with_fields")
	if err != nil {
		return nil, err
	}
	defer sa.Release()
	mem := resolve(opts).alloc
	n := sa.Len()
	st := sa.DataType().(*arrow.StructType)
	fields := append([]arrow.Field(nil), st.Fields()...)
	children := make([]arrow.Array, len(fields))
	for f := range fields {
		children[f] = sa.Field(f)
		children[f].Retain()
	}
	defer func() {
		for _, c := range children {
			c.Release()
		}
	}()
	for _, c := range cols {
		a, err := c.Consolidated()
		if err != nil {
			return nil, err
		}
		if a.Len() != n {
			if a.Len() != 1 {
				a.Release()
				return nil, fmt.Errorf("series.struct.with_fields: field %q has length %d, struct has %d", c.Name(), a.Len(), n)
			}
			idx := make([]int, n)
			b, err := takeArrow(a, idx, mem)
			a.Release()
			if err != nil {
				return nil, err
			}
			a = b
		}
		field := arrow.Field{Name: c.Name(), Type: a.DataType(), Nullable: true}
		replaced := false
		for f := range fields {
			if fields[f].Name == c.Name() {
				fields[f] = field
				children[f].Release()
				children[f] = a
				replaced = true
				break
			}
		}
		if !replaced {
			fields = append(fields, field)
			children = append(children, a)
		}
	}
	datas := make([]arrow.ArrayData, len(children))
	for f, c := range children {
		datas[f] = c.Data()
	}
	return rebuildStruct(o.s.Name(), sa, fields, datas)
}

// rebuildStruct makes a struct array with parent's length and validity
// around new fields and children. Children taken from Struct.Field are
// already sliced to the parent's rows, so the result uses offset 0 and
// a re-based validity bitmap.
func rebuildStruct(name string, parent *array.Struct, fields []arrow.Field, children []arrow.ArrayData) (*Series, error) {
	var validity *memory.Buffer
	owned := false
	if parent.NullN() > 0 {
		if parent.Data().Offset() == 0 {
			validity = parent.Data().Buffers()[0]
		} else {
			validity = CopyValidityBitmap(parent, memory.DefaultAllocator)
			owned = true
		}
	}
	data := array.NewData(arrow.StructOf(fields...), parent.Len(), []*memory.Buffer{validity}, children, parent.NullN(), 0)
	if owned {
		validity.Release()
	}
	arr := array.MakeFromData(data)
	data.Release()
	return New(name, arr)
}

// foldStructNullsInto returns a child array whose validity also marks
// the parent struct's null rows as null. Retain-only when the parent
// has no null rows.
func foldStructNullsInto(parent *array.Struct, child arrow.Array, mem memory.Allocator) arrow.Array {
	if parent.NullN() == 0 {
		child.Retain()
		return child
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	n := parent.Len()
	cd := child.Data()
	cOff := cd.Offset()
	// The new bitmap lives in the child's coordinate space (bit
	// cOff+i is row i) so the child's own offset stays valid.
	merged := memory.NewResizableBuffer(mem)
	merged.Resize(int(bitutil.BytesForBits(int64(cOff + n))))
	dst := merged.Bytes()
	clear(dst)
	nulls := 0
	for i := range n {
		if parent.IsValid(i) && child.IsValid(i) {
			bitutil.SetBit(dst, cOff+i)
		} else {
			nulls++
		}
	}
	bufs := append([]*memory.Buffer(nil), cd.Buffers()...)
	if len(bufs) == 0 {
		bufs = []*memory.Buffer{merged}
	} else {
		bufs[0] = merged
	}
	newData := array.NewData(child.DataType(), n, bufs, cd.Children(), nulls, cOff)
	merged.Release()
	out := array.MakeFromData(newData)
	newData.Release()
	return out
}
