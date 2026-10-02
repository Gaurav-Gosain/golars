package series

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// nested_view.go is the shared plumbing behind the list and arr
// namespaces. A nestedView gives uniform row ranges over List,
// LargeList and FixedSizeList arrays; numChild exposes the flat child
// values as one of three working representations so the per-row
// kernels are written once for every numeric dtype.

type nestedView struct {
	name   string
	arr    array.ListLike // retained; release with release()
	values arrow.Array
	n      int
	width  int // > 0 for fixed-size lists
}

func newNestedView(s *Series, op string, wantFixed bool) (nestedView, error) {
	dt := s.DType()
	if wantFixed && !dt.IsFixedList() {
		return nestedView{}, fmt.Errorf("series.arr.%s: %q has dtype %s (need array)", op, s.Name(), dt)
	}
	if !wantFixed && !dt.IsList() {
		return nestedView{}, fmt.Errorf("series.list.%s: %q has dtype %s (need list)", op, s.Name(), dt)
	}
	a, err := s.Consolidated()
	if err != nil {
		return nestedView{}, err
	}
	ll, ok := a.(array.ListLike)
	if !ok {
		a.Release()
		return nestedView{}, fmt.Errorf("series: %s: unsupported array %T", op, a)
	}
	v := nestedView{name: s.Name(), arr: ll, values: ll.ListValues(), n: ll.Len()}
	if fl, ok := dt.Arrow().(*arrow.FixedSizeListType); ok {
		v.width = int(fl.Len())
	}
	return v, nil
}

func (v nestedView) release() { v.arr.Release() }

func (v nestedView) valid(i int) bool { return v.arr.IsValid(i) }

func (v nestedView) rng(i int) (int, int) {
	s, e := v.arr.ValueOffsets(i)
	return int(s), int(e)
}

func (v nestedView) childType() arrow.DataType { return v.values.DataType() }

// validityBuf copies the outer validity (nil when no nulls).
func (v nestedView) validityBuf(mem memory.Allocator) (*memory.Buffer, int) {
	if v.arr.NullN() == 0 {
		return nil, 0
	}
	return CopyValidityBitmap(v.arr, mem), v.arr.NullN()
}

// selectRows builds a List<child> column. For each valid input row fn
// appends child indices (-1 for a null element) to idx and reports
// whether the output row is valid. Null input rows become null.
func (v nestedView) selectRows(mem memory.Allocator, fn func(i, start, end int, idx []int) ([]int, bool)) (*Series, error) {
	return v.selectRowsFrom(v.values, mem, fn)
}

// selectRowsFrom is selectRows gathering from an explicit child array,
// which lets two-input kernels (concat, set ops) index into a combined
// child.
func (v nestedView) selectRowsFrom(child arrow.Array, mem memory.Allocator, fn func(i, start, end int, idx []int) ([]int, bool)) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	lb := newListOffsetsBuilder(mem, v.n)
	idx := make([]int, 0, child.Len())
	for i := range v.n {
		if !v.valid(i) {
			lb.closeRow(len(idx), false)
			continue
		}
		start, end := v.rng(i)
		var ok bool
		before := len(idx)
		idx, ok = fn(i, start, end, idx)
		if !ok {
			idx = idx[:before]
		}
		lb.closeRow(len(idx), ok)
	}
	out, err := takeArrow(child, idx, mem)
	if err != nil {
		lb.release()
		return nil, err
	}
	return New(v.name, lb.newListArray(out))
}

// numKind classifies the working representation of a numeric child.
type numKind uint8

const (
	numNone numKind = iota
	numInt
	numUint
	numFloat
)

// numChild views the child values of a nested column as int64, uint64
// or float64. Narrow dtypes are widened once (one allocation for the
// whole child, never per row).
type numChild struct {
	kind  numKind
	i     []int64
	u     []uint64
	f     []float64
	arr   arrow.Array
	dtype arrow.DataType
}

func (c numChild) isValid(j int) bool { return c.arr.IsValid(j) }

func (c numChild) float(j int) float64 {
	switch c.kind {
	case numInt:
		return float64(c.i[j])
	case numUint:
		return float64(c.u[j])
	}
	return c.f[j]
}

func rawValues[T any](a arrow.Array) []T {
	d := a.Data()
	bufs := d.Buffers()
	if a.Len() == 0 || len(bufs) < 2 || bufs[1] == nil || bufs[1].Len() == 0 {
		return nil
	}
	b := bufs[1].Bytes()
	var zero T
	size := int(unsafe.Sizeof(zero))
	all := unsafe.Slice((*T)(unsafe.Pointer(&b[0])), len(b)/size)
	return all[d.Offset() : d.Offset()+a.Len()]
}

func widen[S int8 | int16 | int32 | uint8 | uint16 | uint32 | float32, D int64 | uint64 | float64](src []S) []D {
	out := make([]D, len(src))
	for j, x := range src {
		out[j] = D(x)
	}
	return out
}

func newNumChild(a arrow.Array) numChild {
	c := numChild{arr: a, dtype: a.DataType()}
	switch a.DataType().ID() {
	case arrow.INT8:
		c.kind, c.i = numInt, widen[int8, int64](rawValues[int8](a))
	case arrow.INT16:
		c.kind, c.i = numInt, widen[int16, int64](rawValues[int16](a))
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		c.kind, c.i = numInt, widen[int32, int64](rawValues[int32](a))
	case arrow.INT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		c.kind, c.i = numInt, rawValues[int64](a)
	case arrow.UINT8:
		c.kind, c.u = numUint, widen[uint8, uint64](rawValues[uint8](a))
	case arrow.UINT16:
		c.kind, c.u = numUint, widen[uint16, uint64](rawValues[uint16](a))
	case arrow.UINT32:
		c.kind, c.u = numUint, widen[uint32, uint64](rawValues[uint32](a))
	case arrow.UINT64:
		c.kind, c.u = numUint, rawValues[uint64](a)
	case arrow.FLOAT32:
		c.kind, c.f = numFloat, widen[float32, float64](rawValues[float32](a))
	case arrow.FLOAT64:
		c.kind, c.f = numFloat, rawValues[float64](a)
	}
	return c
}

// numOut accumulates per-row results in the working representation and
// packs them into the requested arrow dtype.
type numOut struct {
	kind  numKind
	i     []int64
	u     []uint64
	f     []float64
	valid []bool
	nulls int
}

func newNumOut(kind numKind, n int) *numOut {
	o := &numOut{kind: kind, valid: make([]bool, n)}
	switch kind {
	case numInt:
		o.i = make([]int64, n)
	case numUint:
		o.u = make([]uint64, n)
	default:
		o.kind = numFloat
		o.f = make([]float64, n)
	}
	return o
}

func (o *numOut) setNull(i int) { o.nulls++ }

func (o *numOut) finish(name string, dt arrow.DataType, mem memory.Allocator) (*Series, error) {
	n := len(o.valid)
	var vbuf *memory.Buffer
	if o.nulls > 0 {
		vbuf, _ = packValidity(o.valid, mem)
	}
	var arr arrow.Array
	switch dt.ID() {
	case arrow.BOOL:
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(int(bitutil.BytesForBits(int64(n))))
		bits := buf.Bytes()
		clear(bits)
		for j := range n {
			if o.getInt(j) != 0 {
				bitutil.SetBit(bits, j)
			}
		}
		data := array.NewData(dt, n, []*memory.Buffer{vbuf, buf}, nil, o.nulls, 0)
		arr = array.MakeFromData(data)
		data.Release()
		buf.Release()
	default:
		fw, ok := dt.(arrow.FixedWidthDataType)
		if !ok {
			if vbuf != nil {
				vbuf.Release()
			}
			return nil, fmt.Errorf("series: cannot build numeric output of dtype %s", dt)
		}
		width := fw.BitWidth() / 8
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(n * width)
		if n > 0 {
			o.write(dt.ID(), buf.Bytes())
		}
		data := array.NewData(dt, n, []*memory.Buffer{vbuf, buf}, nil, o.nulls, 0)
		arr = array.MakeFromData(data)
		data.Release()
		buf.Release()
	}
	if vbuf != nil {
		vbuf.Release()
	}
	return New(name, arr)
}

func (o *numOut) getInt(j int) int64 {
	switch o.kind {
	case numInt:
		return o.i[j]
	case numUint:
		return int64(o.u[j])
	}
	return int64(o.f[j])
}

func writeAs[D int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64](o *numOut, dst []byte) {
	n := len(o.valid)
	out := unsafe.Slice((*D)(unsafe.Pointer(&dst[0])), n)
	switch o.kind {
	case numInt:
		for j, x := range o.i {
			out[j] = D(x)
		}
	case numUint:
		for j, x := range o.u {
			out[j] = D(x)
		}
	default:
		for j, x := range o.f {
			out[j] = D(x)
		}
	}
}

func (o *numOut) write(id arrow.Type, dst []byte) {
	switch id {
	case arrow.INT8:
		writeAs[int8](o, dst)
	case arrow.INT16:
		writeAs[int16](o, dst)
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		writeAs[int32](o, dst)
	case arrow.INT64, arrow.DATE64, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		writeAs[int64](o, dst)
	case arrow.UINT8:
		writeAs[uint8](o, dst)
	case arrow.UINT16:
		writeAs[uint16](o, dst)
	case arrow.UINT32:
		writeAs[uint32](o, dst)
	case arrow.UINT64:
		writeAs[uint64](o, dst)
	case arrow.FLOAT32:
		writeAs[float32](o, dst)
	default:
		writeAs[float64](o, dst)
	}
}

// elemKey is a comparable identity for one child element, used by the
// hash-based kernels (unique, n_unique, set ops, count_matches). NaNs
// compare equal to each other and -0.0 equals 0.0, as in polars.
type elemKey struct {
	u    uint64
	s    string
	null bool
}

type elemKeyer struct {
	num  numChild
	strs interface{ Value(int) string }
	bins *array.Binary
	bool *array.Boolean
	arr  arrow.Array
}

func newElemKeyer(a arrow.Array) elemKeyer {
	k := elemKeyer{arr: a}
	switch x := a.(type) {
	case *array.String:
		k.strs = x
	case *array.LargeString:
		k.strs = x
	case *array.Binary:
		k.bins = x
	case *array.Boolean:
		k.bool = x
	default:
		k.num = newNumChild(a)
	}
	return k
}

func (k elemKeyer) key(j int) elemKey {
	if k.arr.IsNull(j) {
		return elemKey{null: true}
	}
	switch {
	case k.strs != nil:
		return elemKey{s: k.strs.Value(j)}
	case k.bins != nil:
		return elemKey{s: string(k.bins.Value(j))}
	case k.bool != nil:
		if k.bool.Value(j) {
			return elemKey{u: 1}
		}
		return elemKey{}
	}
	switch k.num.kind {
	case numInt:
		return elemKey{u: uint64(k.num.i[j])}
	case numUint:
		return elemKey{u: k.num.u[j]}
	case numFloat:
		f := k.num.f[j]
		if f != f {
			return elemKey{u: 0x7ff8000000000001}
		}
		if f == 0 {
			f = 0
		}
		return elemKey{u: math.Float64bits(f)}
	}
	// Nested or exotic child: fall back to the textual form.
	return elemKey{s: k.arr.ValueStr(j)}
}

// scalarKey converts a Go needle to the elemKey space of the child
// type, reporting false when the needle can never match.
func (k elemKeyer) scalarKey(v any) (elemKey, bool) {
	if v == nil {
		return elemKey{null: true}, true
	}
	switch {
	case k.strs != nil:
		s, ok := v.(string)
		return elemKey{s: s}, ok
	case k.bins != nil:
		switch b := v.(type) {
		case []byte:
			return elemKey{s: string(b)}, true
		case string:
			return elemKey{s: b}, true
		}
		return elemKey{}, false
	case k.bool != nil:
		b, ok := v.(bool)
		if !ok {
			return elemKey{}, false
		}
		if b {
			return elemKey{u: 1}, true
		}
		return elemKey{}, true
	}
	switch k.num.kind {
	case numInt:
		if i, ok := toInt64(v); ok {
			return elemKey{u: uint64(i)}, true
		}
		if f, ok := toFloat64(v); ok && f == math.Trunc(f) {
			return elemKey{u: uint64(int64(f))}, true
		}
	case numUint:
		if i, ok := toInt64(v); ok && i >= 0 {
			return elemKey{u: uint64(i)}, true
		}
		if u, ok := v.(uint64); ok {
			return elemKey{u: u}, true
		}
	case numFloat:
		if f, ok := toFloat64(v); ok {
			if f != f {
				return elemKey{u: 0x7ff8000000000001}, true
			}
			if f == 0 {
				f = 0
			}
			return elemKey{u: math.Float64bits(f)}, true
		}
	}
	return elemKey{}, false
}
