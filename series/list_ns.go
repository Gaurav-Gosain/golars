package series

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// list_ns.go holds the list kernels that reshape rows: selections
// (head, tail, slice, reverse, gather, shift, sort, unique, ...),
// structural conversions (explode, to_struct, to_array) and the
// two-input kernels (concat and the set operations).

// sliceBounds mirrors polars' slice_offsets: a negative offset counts
// from the end, and both ends are clamped into [0, n]. length < 0
// means "to the end".
func sliceBounds(offset, length, n int) (int, int) {
	start := offset
	if offset < 0 {
		start = n + offset
	}
	stop := n
	if length >= 0 {
		stop = start + length
	}
	start = min(max(start, 0), n)
	stop = min(max(stop, 0), n)
	if stop < start {
		stop = start
	}
	return start, stop
}

func nsSlice(v nestedView, offset, length int, mem memory.Allocator) (*Series, error) {
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		a, b := sliceBounds(offset, length, e-s)
		for j := s + a; j < s+b; j++ {
			idx = append(idx, j)
		}
		return idx, true
	})
}

// Slice returns elements [offset, offset+length) of each list. A
// negative offset counts from the end; length < 0 takes the rest.
func (o ListOps) Slice(offset, length int, opts ...Option) (*Series, error) {
	return withView(o, "slice", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsSlice(v, offset, length, mem)
	})
}

// Head returns the first n elements of each list. A negative n keeps
// the whole list, as polars does.
func (o ListOps) Head(n int, opts ...Option) (*Series, error) { return o.Slice(0, n, opts...) }

// Tail returns the last n elements of each list. A negative n drops the
// first |n| elements.
func (o ListOps) Tail(n int, opts ...Option) (*Series, error) {
	if n < 0 {
		return o.Slice(-n, -1, opts...)
	}
	return o.Slice(-n, n, opts...)
}

func nsReverse(v nestedView, mem memory.Allocator) (*Series, error) {
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		for j := e - 1; j >= s; j-- {
			idx = append(idx, j)
		}
		return idx, true
	})
}

// Reverse reverses the order of elements in each list.
func (o ListOps) Reverse(opts ...Option) (*Series, error) {
	return withView(o, "reverse", opts, nsReverse)
}

func nsDropNulls(v nestedView, mem memory.Allocator) (*Series, error) {
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		for j := s; j < e; j++ {
			if v.values.IsValid(j) {
				idx = append(idx, j)
			}
		}
		return idx, true
	})
}

// DropNulls removes null elements from each list.
func (o ListOps) DropNulls(opts ...Option) (*Series, error) {
	return withView(o, "drop_nulls", opts, nsDropNulls)
}

func nsShift(v nestedView, n int, mem memory.Allocator) (*Series, error) {
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		for k := range e - s {
			src := k - n
			if src < 0 || src >= e-s {
				idx = append(idx, -1)
			} else {
				idx = append(idx, s+src)
			}
		}
		return idx, true
	})
}

// Shift moves the elements of each list by n positions, filling the
// vacated slots with null. Negative n shifts towards the front.
func (o ListOps) Shift(n int, opts ...Option) (*Series, error) {
	return withView(o, "shift", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsShift(v, n, mem)
	})
}

func nsGather(v nestedView, indices []int, nullOnOOB bool, mem memory.Allocator) (*Series, error) {
	var oob bool
	out, err := v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		for _, k := range indices {
			if k < 0 {
				k += e - s
			}
			if k < 0 || k >= e-s {
				oob = true
				idx = append(idx, -1)
				continue
			}
			idx = append(idx, s+k)
		}
		return idx, true
	})
	if err != nil {
		return nil, err
	}
	if oob && !nullOnOOB {
		out.Release()
		return nil, fmt.Errorf("gather indices are out of bounds")
	}
	return out, nil
}

// Gather picks the given positions (negative counts from the end) out
// of every list. Out-of-range positions are null with nullOnOOB and an
// error otherwise.
func (o ListOps) Gather(indices []int, nullOnOOB bool, opts ...Option) (*Series, error) {
	return withView(o, "gather", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsGather(v, indices, nullOnOOB, mem)
	})
}

func nsGatherEvery(v nestedView, n, offset int, mem memory.Allocator) (*Series, error) {
	if n <= 0 {
		return nil, fmt.Errorf("cannot perform gather every for `n=%d`", n)
	}
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		for j := s + offset; j < e; j += n {
			idx = append(idx, j)
		}
		return idx, true
	})
}

// GatherEvery takes every n-th element of each list starting at
// offset.
func (o ListOps) GatherEvery(n, offset int, opts ...Option) (*Series, error) {
	return withView(o, "gather_every", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsGatherEvery(v, n, offset, mem)
	})
}

// sortRowIdx appends the positions [s, e) ordered for a sort: nulls
// first unless nullsLast, the rest ascending (or descending), stable.
func sortRowIdx(values arrow.Array, less elemCmp, s, e int, descending, nullsLast bool, idx []int) []int {
	if !nullsLast {
		for j := s; j < e; j++ {
			if values.IsNull(j) {
				idx = append(idx, j)
			}
		}
	}
	vstart := len(idx)
	for j := s; j < e; j++ {
		if values.IsValid(j) {
			idx = append(idx, j)
		}
	}
	body := idx[vstart:]
	if descending {
		slices.SortStableFunc(body, func(a, b int) int { return less(b, a) })
	} else {
		slices.SortStableFunc(body, func(a, b int) int { return less(a, b) })
	}
	if nullsLast {
		for j := s; j < e; j++ {
			if values.IsNull(j) {
				idx = append(idx, j)
			}
		}
	}
	return idx
}

func nsSort(v nestedView, descending, nullsLast bool, mem memory.Allocator) (*Series, error) {
	less, ok := newElemCmp(v.values)
	if !ok {
		return nil, fmt.Errorf("sort not supported for dtype `%s`", dtypeName(v.childType()))
	}
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		return sortRowIdx(v.values, less, s, e, descending, nullsLast, idx), true
	})
}

// Sort sorts each list. Nulls go first unless nullsLast; NaN sorts
// above every number.
func (o ListOps) Sort(descending, nullsLast bool, opts ...Option) (*Series, error) {
	return withView(o, "sort", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsSort(v, descending, nullsLast, mem)
	})
}

func nsUnique(v nestedView, maintainOrder bool, mem memory.Allocator) (*Series, error) {
	k := newElemKeyer(v.values)
	less, sortable := newElemCmp(v.values)
	// Polars returns numeric uniques sorted and keeps first-occurrence
	// order for strings and booleans.
	// Booleans come back sorted too, with nulls last.
	_, isBool := v.values.(*array.Boolean)
	sorted := !maintainOrder && sortable && (k.num.kind != numNone || isBool)
	seen := map[elemKey]struct{}{}
	return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
		clear(seen)
		if sorted {
			start := len(idx)
			idx = sortRowIdx(v.values, less, s, e, false, isBool, idx)
			w := start
			for r := start; r < len(idx); r++ {
				key := k.key(idx[r])
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				idx[w] = idx[r]
				w++
			}
			return idx[:w], true
		}
		for j := s; j < e; j++ {
			key := k.key(j)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			idx = append(idx, j)
		}
		return idx, true
	})
}

// Unique removes duplicates from each list. Numeric lists come back
// sorted (as in polars); pass maintainOrder to keep first-occurrence
// order for every dtype.
func (o ListOps) Unique(maintainOrder bool, opts ...Option) (*Series, error) {
	return withView(o, "unique", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsUnique(v, maintainOrder, mem)
	})
}

// diffDType is the dtype polars gives list.diff for a child dtype.
func diffDType(dt arrow.DataType) (arrow.DataType, bool) {
	switch dt.ID() {
	case arrow.UINT8:
		return arrow.PrimitiveTypes.Int16, true
	case arrow.UINT16:
		return arrow.PrimitiveTypes.Int32, true
	case arrow.UINT32, arrow.UINT64:
		return arrow.PrimitiveTypes.Int64, true
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
		return dt, true
	}
	return nil, false
}

func nsDiff(v nestedView, n int, mem memory.Allocator) (*Series, error) {
	outDT, ok := diffDType(v.childType())
	if !ok {
		name := dtypeName(v.childType())
		if v.childType().ID() == arrow.STRING {
			return nil, fmt.Errorf("sub operation not supported for dtypes `%s` and `%s`", name, name)
		}
		return nil, fmt.Errorf("`subtract` operation not supported for dtype `%s`", name)
	}
	c := newNumChild(v.values)
	kind := numInt
	if c.kind == numFloat {
		kind = numFloat
	}
	total := v.values.Len()
	res := newNumOut(kind, total)
	for i := range v.n {
		if !v.valid(i) {
			continue
		}
		s, e := v.rng(i)
		for j := s; j < e; j++ {
			p := j - n
			if p < s || p >= e || !c.isValid(j) || !c.isValid(p) {
				continue
			}
			switch c.kind {
			case numInt:
				res.i[j] = c.i[j] - c.i[p]
			case numUint:
				res.i[j] = int64(c.u[j]) - int64(c.u[p])
			default:
				res.f[j] = c.f[j] - c.f[p]
			}
			res.valid[j] = true
		}
	}
	for j := range total {
		if !res.valid[j] {
			res.nulls++
		}
	}
	child, err := res.finish("", outDT, mem)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	return v.rewrapChild(child.Chunk(0), mem)
}

// rewrapChild builds a list column with v's offsets and validity around
// a new child of the same length as v.values.
func (v nestedView) rewrapChild(child arrow.Array, mem memory.Allocator) (*Series, error) {
	return v.selectRowsFrom(child, mem, func(_, s, e int, idx []int) ([]int, bool) {
		for j := s; j < e; j++ {
			idx = append(idx, j)
		}
		return idx, true
	})
}

// Diff returns the element-wise difference x[i] - x[i-n] inside each
// list (null where the partner is out of the list or null).
func (o ListOps) Diff(n int, opts ...Option) (*Series, error) {
	return withView(o, "diff", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsDiff(v, n, mem)
	})
}

func nsExplode(v nestedView, emptyAsNull, keepNulls bool, mem memory.Allocator) (*Series, error) {
	idx := make([]int, 0, v.values.Len()+v.n)
	for i := range v.n {
		if !v.valid(i) {
			if keepNulls {
				idx = append(idx, -1)
			}
			continue
		}
		s, e := v.rng(i)
		if s == e {
			if emptyAsNull {
				idx = append(idx, -1)
			}
			continue
		}
		for j := s; j < e; j++ {
			idx = append(idx, j)
		}
	}
	arr, err := takeArrow(v.values, idx, mem)
	if err != nil {
		return nil, err
	}
	return New(v.name, arr)
}

// Explode flattens the lists into one row per element. Empty and null
// lists each produce a single null row (polars' defaults).
func (o ListOps) Explode(opts ...Option) (*Series, error) {
	return o.ExplodeWith(true, true, opts...)
}

// ExplodeWith is Explode with polars' empty_as_null and keep_nulls
// switches.
func (o ListOps) ExplodeWith(emptyAsNull, keepNulls bool, opts ...Option) (*Series, error) {
	return withView(o, "explode", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsExplode(v, emptyAsNull, keepNulls, mem)
	})
}

func nsToStruct(v nestedView, fields []string, maxWidth bool, mem memory.Allocator) (*Series, error) {
	width := len(fields)
	if fields == nil {
		for i := range v.n {
			if !v.valid(i) {
				continue
			}
			s, e := v.rng(i)
			if !maxWidth {
				width = e - s
				break
			}
			width = max(width, e-s)
		}
	}
	names := fields
	if names == nil {
		names = make([]string, width)
		for f := range width {
			names[f] = "field_" + strconv.Itoa(f)
		}
	}
	if width == 0 {
		return emptyStructSeries(v.name, v.n)
	}
	cols := make([]arrow.Array, width)
	idx := make([]int, v.n)
	for f := range width {
		for i := range v.n {
			idx[i] = -1
			if !v.valid(i) {
				continue
			}
			s, e := v.rng(i)
			if f < e-s {
				idx[i] = s + f
			}
		}
		c, err := takeArrow(v.values, idx, mem)
		if err != nil {
			for _, p := range cols[:f] {
				p.Release()
			}
			return nil, err
		}
		cols[f] = c
	}
	arr, err := structFromChildren(cols, names, v.n, nil, 0)
	if err != nil {
		return nil, err
	}
	return New(v.name, arr)
}

// ToStruct converts each list into a struct. With fields the struct
// has exactly those fields; otherwise it has field_0 .. field_{k-1}
// where k is the length of the first non-null list, or of the longest
// list with maxWidth. Missing positions are null.
func (o ListOps) ToStruct(fields []string, maxWidth bool, opts ...Option) (*Series, error) {
	return withView(o, "to_struct", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsToStruct(v, fields, maxWidth, mem)
	})
}

// ToArray converts each list into a fixed-size array of width. Every
// non-null list must have exactly width elements.
func (o ListOps) ToArray(width int, opts ...Option) (*Series, error) {
	return withView(o, "to_array", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if width <= 0 {
			return nil, fmt.Errorf("to_array: width must be positive, got %d", width)
		}
		idx := make([]int, 0, v.n*width)
		for i := range v.n {
			if !v.valid(i) {
				for range width {
					idx = append(idx, -1)
				}
				continue
			}
			s, e := v.rng(i)
			if e-s != width {
				return nil, fmt.Errorf("not all elements have the specified width %d", width)
			}
			for j := s; j < e; j++ {
				idx = append(idx, j)
			}
		}
		child, err := takeArrow(v.values, idx, mem)
		if err != nil {
			return nil, err
		}
		defer child.Release()
		validBuf, nulls := v.validityBuf(mem)
		dt := arrow.FixedSizeListOfField(int32(width), arrow.Field{Name: "item", Type: child.DataType(), Nullable: true})
		data := array.NewData(dt, v.n, []*memory.Buffer{validBuf}, []arrow.ArrayData{child.Data()}, nulls, 0)
		arr := array.MakeFromData(data)
		data.Release()
		if validBuf != nil {
			validBuf.Release()
		}
		return New(v.name, arr)
	})
}

// listOperand is one input of concat or a set operation, viewed as a
// list per row. Non-list inputs act as single-element lists and
// length-1 inputs broadcast.
type listOperand struct {
	view   nestedView
	scalar arrow.Array // non-list input
	base   int         // offset of this operand's child in the combined child
	n      int
}

func (op listOperand) row(i int) (start, end int, valid bool) {
	if op.n == 1 {
		i = 0
	}
	if op.scalar != nil {
		return op.base + i, op.base + i + 1, true
	}
	if !op.view.valid(i) {
		return 0, 0, false
	}
	s, e := op.view.rng(i)
	return op.base + s, op.base + e, true
}

// combineOperands concatenates the children of every operand so a
// single gather can pick from any of them.
func combineOperands(first nestedView, others []*Series, op string, mem memory.Allocator) ([]listOperand, arrow.Array, func(), error) {
	ops := []listOperand{{view: first, n: first.n}}
	children := []arrow.Array{first.values}
	var cleanup []func()
	done := func() {
		for _, c := range cleanup {
			c()
		}
	}
	base := first.values.Len()
	for _, o := range others {
		if o.Len() != first.n && o.Len() != 1 {
			done()
			return nil, nil, nil, fmt.Errorf("series.list.%s: length %d does not match %d", op, o.Len(), first.n)
		}
		lo := listOperand{base: base, n: o.Len()}
		if o.DType().IsList() {
			v, err := newNestedView(o, op, false)
			if err != nil {
				done()
				return nil, nil, nil, err
			}
			cleanup = append(cleanup, v.release)
			lo.view = v
			children = append(children, v.values)
			base += v.values.Len()
		} else {
			a, err := o.Consolidated()
			if err != nil {
				done()
				return nil, nil, nil, err
			}
			cleanup = append(cleanup, a.Release)
			lo.scalar = a
			children = append(children, a)
			base += a.Len()
		}
		ops = append(ops, lo)
	}
	for _, c := range children[1:] {
		if !arrow.TypeEqual(c.DataType(), children[0].DataType()) {
			done()
			return nil, nil, nil, fmt.Errorf("series.list.%s: inner dtypes differ: %s vs %s", op,
				dtypeName(children[0].DataType()), dtypeName(c.DataType()))
		}
	}
	if len(children) == 1 {
		children[0].Retain()
		return ops, children[0], done, nil
	}
	combined, err := array.Concatenate(children, mem)
	if err != nil {
		done()
		return nil, nil, nil, err
	}
	return ops, combined, done, nil
}

// Concat appends the lists (or scalars) of others to each list,
// row by row. A null in any input makes the row null.
func (o ListOps) Concat(others []*Series, opts ...Option) (*Series, error) {
	return withView(o, "concat", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		ops, combined, done, err := combineOperands(v, others, "concat", mem)
		if err != nil {
			return nil, err
		}
		defer done()
		defer combined.Release()
		return v.selectRowsFrom(combined, mem, func(i, _, _ int, idx []int) ([]int, bool) {
			for _, op := range ops {
				s, e, ok := op.row(i)
				if !ok {
					return idx, false
				}
				for j := s; j < e; j++ {
					idx = append(idx, j)
				}
			}
			return idx, true
		})
	})
}

// SetOp names a list set operation.
type SetOp uint8

const (
	SetUnion SetOp = iota
	SetIntersection
	SetDifference
	SetSymmetricDifference
)

// SetOperation computes a per-row set operation between each list and
// the list at the same row of other. Results keep first-occurrence
// order (the left operand first); nulls count as a value. A null list
// on either side gives a null row.
func (o ListOps) SetOperation(other *Series, op SetOp, opts ...Option) (*Series, error) {
	return withView(o, "set_operation", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if !other.DType().IsList() {
			return nil, fmt.Errorf("series.list.set_operation: other must be a list, got %s", other.DType())
		}
		ops, combined, done, err := combineOperands(v, []*Series{other}, "set_operation", mem)
		if err != nil {
			return nil, err
		}
		defer done()
		defer combined.Release()
		k := newElemKeyer(combined)
		inA := map[elemKey]struct{}{}
		inB := map[elemKey]struct{}{}
		emitted := map[elemKey]struct{}{}
		return v.selectRowsFrom(combined, mem, func(i, _, _ int, idx []int) ([]int, bool) {
			as, ae, aok := ops[0].row(i)
			bs, be, bok := ops[1].row(i)
			if !aok || !bok {
				return idx, false
			}
			clear(inA)
			clear(inB)
			clear(emitted)
			for j := as; j < ae; j++ {
				inA[k.key(j)] = struct{}{}
			}
			for j := bs; j < be; j++ {
				inB[k.key(j)] = struct{}{}
			}
			emit := func(j int, keep func(elemKey) bool) {
				key := k.key(j)
				if _, dup := emitted[key]; dup || !keep(key) {
					return
				}
				emitted[key] = struct{}{}
				idx = append(idx, j)
			}
			always := func(elemKey) bool { return true }
			notInB := func(key elemKey) bool { _, ok := inB[key]; return !ok }
			notInA := func(key elemKey) bool { _, ok := inA[key]; return !ok }
			inBoth := func(key elemKey) bool { _, ok := inB[key]; return ok }
			switch op {
			case SetUnion:
				for j := as; j < ae; j++ {
					emit(j, always)
				}
				for j := bs; j < be; j++ {
					emit(j, always)
				}
			case SetIntersection:
				for j := as; j < ae; j++ {
					emit(j, inBoth)
				}
			case SetDifference:
				for j := as; j < ae; j++ {
					emit(j, notInB)
				}
			case SetSymmetricDifference:
				for j := as; j < ae; j++ {
					emit(j, notInB)
				}
				for j := bs; j < be; j++ {
					emit(j, notInA)
				}
			}
			return idx, true
		})
	})
}

// Sample draws n elements from each list (with or without
// replacement). Without replacement n may not exceed a list's length.
// The order of the draw is random; seed makes it reproducible.
func (o ListOps) Sample(n int, withReplacement, shuffle bool, seed uint64, opts ...Option) (*Series, error) {
	return o.sampleImpl(func(int) int { return n }, withReplacement, shuffle, seed, opts)
}

// SampleFraction draws fraction * len elements from each list.
func (o ListOps) SampleFraction(fraction float64, withReplacement, shuffle bool, seed uint64, opts ...Option) (*Series, error) {
	return o.sampleImpl(func(l int) int { return int(fraction * float64(l)) }, withReplacement, shuffle, seed, opts)
}

func (o ListOps) sampleImpl(count func(int) int, withReplacement, shuffle bool, seed uint64, opts []Option) (*Series, error) {
	return withView(o, "sample", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		var sampleErr error
		out, err := v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
			l := e - s
			k := count(l)
			if k < 0 {
				k = 0
			}
			if !withReplacement && k > l {
				sampleErr = fmt.Errorf("cannot take a larger sample than the total population when `with_replacement=false`")
				return idx, true
			}
			if withReplacement {
				for range k {
					idx = append(idx, s+rng.IntN(max(l, 1)))
				}
				return idx, true
			}
			start := len(idx)
			for j := s; j < e; j++ {
				idx = append(idx, j)
			}
			part := idx[start:]
			// Partial Fisher-Yates: the first k slots become the draw.
			for r := range k {
				p := r + rng.IntN(l-r)
				part[r], part[p] = part[p], part[r]
			}
			if !shuffle {
				slices.Sort(part[:k])
			}
			return idx[:start+k], true
		})
		if err != nil {
			return nil, err
		}
		if sampleErr != nil {
			out.Release()
			return nil, sampleErr
		}
		return out, nil
	})
}

// Filter keeps, in every list, the elements where mask is true. mask
// is aligned with the exploded child values (one entry per element);
// null mask entries drop the element.
func (o ListOps) Filter(mask *Series, opts ...Option) (*Series, error) {
	return withView(o, "filter", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		m, err := mask.Consolidated()
		if err != nil {
			return nil, err
		}
		defer m.Release()
		b, ok := m.(*array.Boolean)
		if !ok {
			return nil, fmt.Errorf("series.list.filter: mask must be boolean, got %s", mask.DType())
		}
		if b.Len() != v.values.Len() {
			return nil, fmt.Errorf("series.list.filter: mask has %d entries, list holds %d elements", b.Len(), v.values.Len())
		}
		return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
			for j := s; j < e; j++ {
				if b.IsValid(j) && b.Value(j) {
					idx = append(idx, j)
				}
			}
			return idx, true
		})
	})
}
