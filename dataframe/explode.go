package dataframe

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// ErrNotList is returned when Explode is called on a non-list column.
var ErrNotList = errors.New("dataframe: column is not a list")

// Explode turns each element of a list-typed column into its own row.
// Mirrors polars' df.explode.
//
// Null and empty lists each become a single null row in the output
// (polars' default). Surrounding columns are repeated as needed so
// the result remains rectangular.
//
// Only variable-length list columns (List<T>, LargeList<T>) are
// supported for now. FixedSizeList will fall through the same path
// once a kernel lands for it.
func (df *DataFrame) Explode(ctx context.Context, col string) (*DataFrame, error) {
	idx, ok := df.sch.Index(col)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, col)
	}
	s := df.cols[idx]
	if !s.DType().IsList() {
		return nil, fmt.Errorf("%w: %q has dtype %s", ErrNotList, col, s.DType())
	}

	// Flatten to a single chunk for simplicity. Multi-chunk list
	// arrays are rare in practice (scans typically produce one
	// chunk per file) and re-chunking here keeps the kernel small.
	ch := s.ToArrowChunked()
	defer ch.Release()
	listArr, err := concatListChunks(ch)
	if err != nil {
		return nil, err
	}
	defer listArr.Release()

	takeIdx, nullMask, totalLen := explodePlan(listArr)
	values := explodeValues(listArr, nullMask, totalLen)

	// Apply Take to every non-list column; insert the exploded list
	// values where the original list column sat.
	out := make([]*series.Series, 0, df.Width())
	for i, c := range df.cols {
		if i == idx {
			ser, err := series.New(c.Name(), values)
			if err != nil {
				values.Release()
				for _, o := range out {
					o.Release()
				}
				return nil, err
			}
			out = append(out, ser)
			continue
		}
		taken, err := takeOptional(ctx, c, takeIdx, memory.DefaultAllocator)
		if err != nil {
			values.Release()
			for _, o := range out {
				o.Release()
			}
			return nil, fmt.Errorf("dataframe.Explode: take on %q: %w", c.Name(), err)
		}
		out = append(out, taken)
	}
	res, err := New(out...)
	if err != nil {
		for _, o := range out {
			o.Release()
		}
		return nil, err
	}
	return res, nil
}

// explodePlan walks the list array once and returns:
//   - takeIdx: the row index in the source for each output row,
//   - nullMask: true where the output row is a sentinel null
//     (produced by a null or empty source list),
//   - totalLen: the length of takeIdx / nullMask.
func explodePlan(la arrayList) ([]int, []bool, int) {
	n := la.Len()
	total := 0
	for i := 0; i < n; i++ {
		if la.IsNull(i) {
			total++
			continue
		}
		start, end := la.Range(i)
		length := end - start
		if length == 0 {
			total++
			continue
		}
		total += length
	}
	idx := make([]int, 0, total)
	mask := make([]bool, 0, total)
	for i := 0; i < n; i++ {
		if la.IsNull(i) {
			idx = append(idx, i)
			mask = append(mask, true)
			continue
		}
		start, end := la.Range(i)
		length := end - start
		if length == 0 {
			idx = append(idx, i)
			mask = append(mask, true)
			continue
		}
		for j := 0; j < length; j++ {
			idx = append(idx, i)
			mask = append(mask, false)
		}
	}
	return idx, mask, total
}

// explodeValues builds the output value array for the list column.
// For each source list row, its child slice [start, end) is copied
// into the output; for null / empty source rows, a single null slot
// is appended instead. Called after explodePlan so the length lines
// up with takeIdx.
func explodeValues(la arrayList, nullMask []bool, totalLen int) arrow.Array {
	child := la.Values()
	mem := memory.DefaultAllocator
	// Build output by pulling row indices into a []int that Take
	// can consume. For null / empty source rows the corresponding
	// output row is a "don't care" index; we will override it with
	// a null via BuildWithNulls below. Pick index 0 when child is
	// non-empty (safe), fall back to 0 (will be masked) when it's
	// empty; Take treats OOB as error, so for an empty child we
	// can't call Take. Handle the all-empty-child case by emitting
	// an all-null array of the child's dtype.
	if child.Len() == 0 {
		return makeAllNull(mem, child.DataType(), totalLen)
	}
	// -1 marks the null slot of a null or empty source list.
	idxs := make([]int, 0, totalLen)
	hasNull := false
	n := la.Len()
	for i := 0; i < n; i++ {
		start, end := 0, 0
		if !la.IsNull(i) {
			start, end = la.Range(i)
		}
		if end == start {
			idxs = append(idxs, -1)
			hasNull = true
			continue
		}
		for j := start; j < end; j++ {
			idxs = append(idxs, j)
		}
	}
	// child is borrowed from la; FromArrowArray consumes a reference.
	child.Retain()
	childSer, err := series.FromArrowArray("", child)
	if err != nil {
		child.Release()
		return makeAllNull(mem, child.DataType(), totalLen)
	}
	defer childSer.Release()
	var taken *series.Series
	if !hasNull {
		taken, err = compute.Take(context.Background(), childSer, idxs)
	}
	if hasNull || err != nil {
		// Gather takes -1 as null and covers every dtype, including a
		// nested child (a list of lists).
		taken, err = childSer.Gather(idxs)
	}
	if err != nil {
		return makeAllNull(mem, child.DataType(), totalLen)
	}
	defer taken.Release()
	return taken.ToArrow()
}

// arrayList is the slice of the arrow.ListLike surface we need
// without pulling in each concrete list type. Both *array.List and
// *array.LargeList satisfy it.
type arrayList interface {
	arrow.Array
	Values() arrow.Array
	Range(int) (int, int)
}

type listView struct {
	*array.List
}

// Range uses ValueOffsets, which honours the array's slice offset.
// Offsets() is the raw buffer and is wrong for sliced arrays.
func (l listView) Range(i int) (int, int) {
	start, end := l.List.ValueOffsets(i)
	return int(start), int(end)
}
func (l listView) Values() arrow.Array { return l.List.ListValues() }

type largeListView struct {
	*array.LargeList
}

func (l largeListView) Range(i int) (int, int) {
	start, end := l.LargeList.ValueOffsets(i)
	return int(start), int(end)
}
func (l largeListView) Values() arrow.Array { return l.LargeList.ListValues() }

// concatListChunks reduces a chunked list array to a single arrayList
// view. When already one chunk, it is returned with its Retain
// bumped. Otherwise the chunks are concatenated by walking each
// chunk's offsets and rebuilding a single list.
func concatListChunks(ch *arrow.Chunked) (arrayList, error) {
	chunks := ch.Chunks()
	switch len(chunks) {
	case 0:
		// A zero-row column may carry no chunks at all.
		empty := array.MakeArrayOfNull(memory.DefaultAllocator, ch.DataType(), 0)
		defer empty.Release()
		return wrapListArray(empty)
	case 1:
		return wrapListArray(chunks[0])
	default:
		// Fall back to a straight concat via arrow-go. The compute
		// concat helpers aren't exposed on arrow.Chunked directly,
		// so walk chunks and append through a fresh builder.
		mem := memory.DefaultAllocator
		typed := ch.DataType()
		builder := array.NewBuilder(mem, typed).(*array.ListBuilder)
		defer builder.Release()
		for _, c := range ch.Chunks() {
			lv, err := wrapListArray(c)
			if err != nil {
				return nil, err
			}
			appendListInto(builder, lv)
			lv.Release()
		}
		merged := builder.NewArray()
		return wrapListArray(merged)
	}
}

func wrapListArray(a arrow.Array) (arrayList, error) {
	switch v := a.(type) {
	case *array.List:
		v.Retain()
		return listView{v}, nil
	case *array.LargeList:
		v.Retain()
		return largeListView{v}, nil
	}
	return nil, fmt.Errorf("dataframe.Explode: unsupported list type %s", a.DataType())
}

// appendListInto pushes every row of src into dst. Uses the generic
// append-by-value path which works across scalar element dtypes.
// Rarely hit (only when a list column actually has more than one
// chunk), so we don't need a specialised copy.
func appendListInto(dst *array.ListBuilder, src arrayList) {
	child := src.Values()
	for i := 0; i < src.Len(); i++ {
		if src.IsNull(i) {
			dst.AppendNull()
			continue
		}
		start, end := src.Range(i)
		dst.Append(true)
		for j := start; j < end; j++ {
			appendScalar(dst.ValueBuilder(), child, j)
		}
	}
}

// appendScalar copies the i-th element of src into dst. Handles
// the dtypes commonly used as list elements. Extend as needed.
func appendScalar(dst array.Builder, src arrow.Array, i int) {
	if src.IsNull(i) {
		dst.AppendNull()
		return
	}
	switch s := src.(type) {
	case *array.Int64:
		dst.(*array.Int64Builder).Append(s.Value(i))
	case *array.Int32:
		dst.(*array.Int32Builder).Append(s.Value(i))
	case *array.Float64:
		dst.(*array.Float64Builder).Append(s.Value(i))
	case *array.Float32:
		dst.(*array.Float32Builder).Append(s.Value(i))
	case *array.Boolean:
		dst.(*array.BooleanBuilder).Append(s.Value(i))
	case *array.String:
		dst.(*array.StringBuilder).Append(s.Value(i))
	case *array.Uint64:
		dst.(*array.Uint64Builder).Append(s.Value(i))
	case *array.Uint32:
		dst.(*array.Uint32Builder).Append(s.Value(i))
	default:
		dst.AppendNull()
	}
}

// makeAllNull returns an arrow.Array of the requested dtype and
// length where every slot is null. Used as a shortcut when every
// source row is null or empty.
func makeAllNull(mem memory.Allocator, dt arrow.DataType, n int) arrow.Array {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
	return b.NewArray()
}

