package series

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ListOps is the namespace for operations over list-typed Series.
// Reach it via `s.List()`. Mirrors polars' `s.list.*` surface: output
// dtypes, null propagation and output names follow polars 1.39.
//
// Every kernel works on the flat child array plus row offsets and
// allocates its outputs once per call, never per row. Kernels that
// produce lists compute child indices and gather them in one pass
// (see nstake.go).
type ListOps struct{ s *Series }

// List returns the list namespace. Every method on the returned
// ListOps fails with a clear error when the underlying Series is
// not list-typed.
func (s *Series) List() ListOps { return ListOps{s: s} }

func (o ListOps) view(op string) (nestedView, error) { return newNestedView(o.s, op, false) }

// nsLen, nsSum and the other ns* functions implement the per-row
// kernels once for both the list and the arr namespaces.

func nsLen(v nestedView, mem memory.Allocator) (*Series, error) {
	out := newNumOut(numUint, v.n)
	for i := range v.n {
		if !v.valid(i) {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		out.u[i] = uint64(e - s)
		out.valid[i] = true
	}
	return out.finish(v.name, arrow.PrimitiveTypes.Uint32, mem)
}

// Len returns the length of each list as UInt32. Null lists stay null.
func (o ListOps) Len(opts ...Option) (*Series, error) {
	return withView(o, "len", opts, nsLen)
}

func withView(o ListOps, op string, opts []Option, fn func(nestedView, memory.Allocator) (*Series, error)) (*Series, error) {
	v, err := o.view(op)
	if err != nil {
		return nil, err
	}
	defer v.release()
	return fn(v, resolve(opts).alloc)
}

// sumDType maps a child dtype to the dtype polars uses for list.sum.
func sumDType(dt arrow.DataType) (arrow.DataType, bool) {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.UINT8, arrow.UINT16:
		return arrow.PrimitiveTypes.Int64, true
	case arrow.INT32, arrow.INT64, arrow.UINT32, arrow.UINT64, arrow.FLOAT32, arrow.FLOAT64:
		return dt, true
	case arrow.BOOL:
		return arrow.PrimitiveTypes.Uint32, true
	}
	return nil, false
}

func nsSum(v nestedView, mem memory.Allocator) (*Series, error) {
	outDT, ok := sumDType(v.childType())
	if !ok {
		return nil, fmt.Errorf("`sum` operation not supported for dtype `%s`", dtypeName(v.childType()))
	}
	if b, isBool := v.values.(*array.Boolean); isBool {
		out := newNumOut(numUint, v.n)
		for i := range v.n {
			if !v.valid(i) {
				out.setNull(i)
				continue
			}
			s, e := v.rng(i)
			for j := s; j < e; j++ {
				if b.IsValid(j) && b.Value(j) {
					out.u[i]++
				}
			}
			out.valid[i] = true
		}
		return out.finish(v.name, outDT, mem)
	}
	c := newNumChild(v.values)
	out := newNumOut(c.kind, v.n)
	for i := range v.n {
		if !v.valid(i) {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		switch c.kind {
		case numInt:
			var acc int64
			for j := s; j < e; j++ {
				if c.isValid(j) {
					acc += c.i[j]
				}
			}
			out.i[i] = acc
		case numUint:
			var acc uint64
			for j := s; j < e; j++ {
				if c.isValid(j) {
					acc += c.u[j]
				}
			}
			out.u[i] = acc
		default:
			var acc float64
			for j := s; j < e; j++ {
				if c.isValid(j) {
					acc += c.f[j]
				}
			}
			out.f[i] = acc
		}
		out.valid[i] = true
	}
	return out.finish(v.name, outDT, mem)
}

// Sum returns the sum of each list. Small integers widen to Int64,
// booleans count trues as UInt32, and an empty or all-null list sums
// to 0.
func (o ListOps) Sum(opts ...Option) (*Series, error) { return withView(o, "sum", opts, nsSum) }

// floatStatDType is Float32 for Float32 children and Float64 otherwise,
// the rule polars applies to mean, median, std and var.
func floatStatDType(dt arrow.DataType) arrow.DataType {
	if dt.ID() == arrow.FLOAT32 {
		return arrow.PrimitiveTypes.Float32
	}
	return arrow.PrimitiveTypes.Float64
}

func nsMean(v nestedView, mem memory.Allocator) (*Series, error) {
	outDT := floatStatDType(v.childType())
	out := newNumOut(numFloat, v.n)
	b, isBool := v.values.(*array.Boolean)
	c := newNumChild(v.values)
	for i := range v.n {
		s, e := v.rng(i)
		if !v.valid(i) || (!isBool && c.kind == numNone) {
			out.setNull(i)
			continue
		}
		var sum float64
		cnt := 0
		for j := s; j < e; j++ {
			if !v.values.IsValid(j) {
				continue
			}
			if isBool {
				if b.Value(j) {
					sum++
				}
			} else {
				sum += c.float(j)
			}
			cnt++
		}
		if cnt == 0 {
			out.setNull(i)
			continue
		}
		out.f[i] = sum / float64(cnt)
		out.valid[i] = true
	}
	return out.finish(v.name, outDT, mem)
}

// Mean returns the arithmetic mean of each list (Float32 for Float32
// lists, Float64 otherwise). Empty and all-null lists yield null.
func (o ListOps) Mean(opts ...Option) (*Series, error) { return withView(o, "mean", opts, nsMean) }

// collectFloats gathers the valid numeric values of a row into buf.
func collectFloats(c numChild, s, e int, buf []float64) []float64 {
	buf = buf[:0]
	for j := s; j < e; j++ {
		if c.isValid(j) {
			buf = append(buf, c.float(j))
		}
	}
	return buf
}

// floatLess orders NaN after every number, as polars sorts floats.
func floatCmp(a, b float64) int {
	an, bn := a != a, b != b
	switch {
	case an && bn:
		return 0
	case an:
		return 1
	case bn:
		return -1
	}
	return cmp.Compare(a, b)
}

func nsMedian(v nestedView, mem memory.Allocator) (*Series, error) {
	outDT := floatStatDType(v.childType())
	c := newNumChild(v.values)
	out := newNumOut(numFloat, v.n)
	var buf []float64
	for i := range v.n {
		if !v.valid(i) || c.kind == numNone {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		buf = collectFloats(c, s, e, buf)
		if len(buf) == 0 {
			out.setNull(i)
			continue
		}
		slices.SortFunc(buf, floatCmp)
		m := len(buf) / 2
		if len(buf)%2 == 1 {
			out.f[i] = buf[m]
		} else {
			out.f[i] = (buf[m-1] + buf[m]) / 2
		}
		out.valid[i] = true
	}
	return out.finish(v.name, outDT, mem)
}

// Median returns the median of each list.
func (o ListOps) Median(opts ...Option) (*Series, error) {
	return withView(o, "median", opts, nsMedian)
}

func nsVar(v nestedView, ddof int, std bool, mem memory.Allocator) (*Series, error) {
	outDT := floatStatDType(v.childType())
	c := newNumChild(v.values)
	out := newNumOut(numFloat, v.n)
	for i := range v.n {
		if !v.valid(i) || c.kind == numNone {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		// Two passes (mean, then squared deviations), which is what
		// polars does and keeps results bit-identical.
		var sum, m2 float64
		cnt := 0
		for j := s; j < e; j++ {
			if c.isValid(j) {
				sum += c.float(j)
				cnt++
			}
		}
		mean := sum / float64(cnt)
		for j := s; j < e; j++ {
			if c.isValid(j) {
				d := c.float(j) - mean
				m2 += d * d
			}
		}
		if cnt-ddof <= 0 {
			out.setNull(i)
			continue
		}
		r := m2 / float64(cnt-ddof)
		if std {
			r = math.Sqrt(r)
		}
		out.f[i] = r
		out.valid[i] = true
	}
	return out.finish(v.name, outDT, mem)
}

// Std returns the standard deviation of each list with ddof delta
// degrees of freedom (polars' default is 1).
func (o ListOps) Std(ddof int, opts ...Option) (*Series, error) {
	return withView(o, "std", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsVar(v, ddof, true, mem)
	})
}

// Var returns the variance of each list with ddof delta degrees of
// freedom.
func (o ListOps) Var(ddof int, opts ...Option) (*Series, error) {
	return withView(o, "var", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsVar(v, ddof, false, mem)
	})
}

// elemCmp compares two child elements of the same array (both valid).
type elemCmp func(a, b int) int

func newElemCmp(values arrow.Array) (elemCmp, bool) {
	switch x := values.(type) {
	case *array.String:
		return func(a, b int) int { return cmp.Compare(x.Value(a), x.Value(b)) }, true
	case *array.LargeString:
		return func(a, b int) int { return cmp.Compare(x.Value(a), x.Value(b)) }, true
	case *array.Binary:
		return func(a, b int) int { return cmp.Compare(string(x.Value(a)), string(x.Value(b))) }, true
	case *array.Boolean:
		return func(a, b int) int {
			av, bv := x.Value(a), x.Value(b)
			switch {
			case av == bv:
				return 0
			case !av:
				return -1
			}
			return 1
		}, true
	}
	c := newNumChild(values)
	switch c.kind {
	case numInt:
		return func(a, b int) int { return cmp.Compare(c.i[a], c.i[b]) }, true
	case numUint:
		return func(a, b int) int { return cmp.Compare(c.u[a], c.u[b]) }, true
	case numFloat:
		return func(a, b int) int { return floatCmp(c.f[a], c.f[b]) }, true
	}
	return nil, false
}

// isNaNAt reports whether child element j is a float NaN.
func isNaNAt(c numChild, j int) bool { return c.kind == numFloat && c.f[j] != c.f[j] }

// nsArgExtreme finds the position of the min (or max) valid element in
// each row. NaNs are skipped unless every valid element is NaN.
func nsArgExtreme(v nestedView, wantMax bool) (pos []int, ok []bool, err error) {
	less, supported := newElemCmp(v.values)
	if !supported {
		return nil, nil, fmt.Errorf("arg_min/arg_max not supported for dtype `%s`", dtypeName(v.childType()))
	}
	c := newNumChild(v.values)
	// Polars' string arg_max reports the last of equal maxima; every
	// other case reports the first.
	_, lastOnTie := v.values.(*array.String)
	pos = make([]int, v.n)
	ok = make([]bool, v.n)
	for i := range v.n {
		if !v.valid(i) {
			continue
		}
		s, e := v.rng(i)
		best, firstNaN := -1, -1
		for j := s; j < e; j++ {
			if !v.values.IsValid(j) {
				continue
			}
			if isNaNAt(c, j) {
				if firstNaN < 0 {
					firstNaN = j
				}
				continue
			}
			if best < 0 {
				best = j
				continue
			}
			r := less(j, best)
			if (wantMax && (r > 0 || (r == 0 && lastOnTie))) || (!wantMax && r < 0) {
				best = j
			}
		}
		if best < 0 {
			best = firstNaN
		}
		if best >= 0 {
			pos[i] = best - s
			ok[i] = true
		}
	}
	return pos, ok, nil
}

func nsArg(v nestedView, wantMax bool, mem memory.Allocator) (*Series, error) {
	pos, ok, err := nsArgExtreme(v, wantMax)
	if err != nil {
		return nil, err
	}
	out := newNumOut(numUint, v.n)
	for i := range v.n {
		if !ok[i] {
			out.setNull(i)
			continue
		}
		out.u[i] = uint64(pos[i])
		out.valid[i] = true
	}
	return out.finish(v.name, arrow.PrimitiveTypes.Uint32, mem)
}

// ArgMin returns the index of the minimum of each list as UInt32.
func (o ListOps) ArgMin(opts ...Option) (*Series, error) {
	return withView(o, "arg_min", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsArg(v, false, mem)
	})
}

// ArgMax returns the index of the maximum of each list as UInt32.
func (o ListOps) ArgMax(opts ...Option) (*Series, error) {
	return withView(o, "arg_max", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsArg(v, true, mem)
	})
}

func nsMinMax(v nestedView, wantMax bool, mem memory.Allocator) (*Series, error) {
	pos, ok, err := nsArgExtreme(v, wantMax)
	if err != nil {
		return nil, err
	}
	idx := make([]int, v.n)
	for i := range v.n {
		idx[i] = -1
		if ok[i] {
			s, _ := v.rng(i)
			idx[i] = s + pos[i]
		}
	}
	arr, err := takeArrow(v.values, idx, mem)
	if err != nil {
		return nil, err
	}
	return New(v.name, arr)
}

// Min returns the minimum of each list, in the child dtype. NaNs are
// ignored unless the list holds only NaNs.
func (o ListOps) Min(opts ...Option) (*Series, error) {
	return withView(o, "min", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsMinMax(v, false, mem)
	})
}

// Max returns the maximum of each list, in the child dtype.
func (o ListOps) Max(opts ...Option) (*Series, error) {
	return withView(o, "max", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsMinMax(v, true, mem)
	})
}

func nsNUnique(v nestedView, mem memory.Allocator) (*Series, error) {
	k := newElemKeyer(v.values)
	seen := map[elemKey]struct{}{}
	out := newNumOut(numUint, v.n)
	for i := range v.n {
		if !v.valid(i) {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		clear(seen)
		for j := s; j < e; j++ {
			seen[k.key(j)] = struct{}{}
		}
		out.u[i] = uint64(len(seen))
		out.valid[i] = true
	}
	return out.finish(v.name, arrow.PrimitiveTypes.Uint32, mem)
}

// NUnique counts the distinct values of each list (null counts as a
// value) as UInt32.
func (o ListOps) NUnique(opts ...Option) (*Series, error) {
	return withView(o, "n_unique", opts, nsNUnique)
}

func nsAnyAll(v nestedView, all bool, mem memory.Allocator) (*Series, error) {
	b, ok := v.values.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected boolean elements in list")
	}
	out := newNumOut(numUint, v.n)
	for i := range v.n {
		if !v.valid(i) {
			out.setNull(i)
			continue
		}
		s, e := v.rng(i)
		r := all
		for j := s; j < e; j++ {
			if !b.IsValid(j) {
				continue
			}
			if all && !b.Value(j) {
				r = false
				break
			}
			if !all && b.Value(j) {
				r = true
				break
			}
		}
		if r {
			out.u[i] = 1
		}
		out.valid[i] = true
	}
	return out.finish(v.name, arrow.FixedWidthTypes.Boolean, mem)
}

// Any reports whether any element of each boolean list is true.
func (o ListOps) Any(opts ...Option) (*Series, error) {
	return withView(o, "any", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsAnyAll(v, false, mem)
	})
}

// All reports whether every non-null element of each boolean list is
// true (true for an empty list).
func (o ListOps) All(opts ...Option) (*Series, error) {
	return withView(o, "all", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsAnyAll(v, true, mem)
	})
}

// nsGet picks element idx (negative counts from the end) of each row.
func nsGet(v nestedView, idx int, nullOnOOB bool, mem memory.Allocator) (*Series, error) {
	pick := make([]int, v.n)
	for i := range v.n {
		pick[i] = -1
		if !v.valid(i) {
			continue
		}
		s, e := v.rng(i)
		j := idx
		if j < 0 {
			j += e - s
		}
		if j < 0 || j >= e-s {
			if !nullOnOOB {
				return nil, fmt.Errorf("get index is out of bounds")
			}
			continue
		}
		pick[i] = s + j
	}
	arr, err := takeArrow(v.values, pick, mem)
	if err != nil {
		return nil, err
	}
	return New(v.name, arr)
}

// Get returns element idx of each list (negative counts from the
// back). Out-of-range positions yield null.
func (o ListOps) Get(idx int, opts ...Option) (*Series, error) {
	return o.GetChecked(idx, true, opts...)
}

// GetChecked is Get with polars' null_on_oob switch: with nullOnOOB
// false an out-of-range index on a non-null list is an error.
func (o ListOps) GetChecked(idx int, nullOnOOB bool, opts ...Option) (*Series, error) {
	return withView(o, "get", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsGet(v, idx, nullOnOOB, mem)
	})
}

// First returns the first element of each list (null when empty).
func (o ListOps) First(opts ...Option) (*Series, error) { return o.Get(0, opts...) }

// Last returns the last element of each list (null when empty).
func (o ListOps) Last(opts ...Option) (*Series, error) { return o.Get(-1, opts...) }

func nsItem(v nestedView, mem memory.Allocator) (*Series, error) {
	for i := range v.n {
		if !v.valid(i) {
			continue
		}
		s, e := v.rng(i)
		switch {
		case e-s == 0:
			return nil, fmt.Errorf("aggregation 'item' expected a single value, got none")
		case e-s > 1:
			return nil, fmt.Errorf("aggregation 'item' expected a single value, got %d values", e-s)
		}
	}
	return nsGet(v, 0, true, mem)
}

// Item returns the single element of each list and errors when a list
// does not hold exactly one element. Polars `list.item`.
func (o ListOps) Item(opts ...Option) (*Series, error) { return withView(o, "item", opts, nsItem) }

func nsCountMatches(v nestedView, needle any, contains bool, mem memory.Allocator) (*Series, error) {
	k := newElemKeyer(v.values)
	target, possible := k.scalarKey(needle)
	out := newNumOut(numUint, v.n)
	for i := range v.n {
		if !v.valid(i) {
			out.setNull(i)
			continue
		}
		out.valid[i] = true
		if !possible {
			continue
		}
		s, e := v.rng(i)
		for j := s; j < e; j++ {
			if k.key(j) == target {
				out.u[i]++
				if contains {
					break
				}
			}
		}
	}
	dt := arrow.DataType(arrow.PrimitiveTypes.Uint32)
	if contains {
		dt = arrow.FixedWidthTypes.Boolean
	}
	return out.finish(v.name, dt, mem)
}

// Contains reports whether each list holds v (nil looks for a null).
// A needle whose Go type cannot match the child dtype is never found.
func (o ListOps) Contains(v any, opts ...Option) (*Series, error) {
	return withView(o, "contains", opts, func(nv nestedView, mem memory.Allocator) (*Series, error) {
		return nsCountMatches(nv, v, true, mem)
	})
}

// CountMatches counts how often v occurs in each list, as UInt32.
func (o ListOps) CountMatches(v any, opts ...Option) (*Series, error) {
	return withView(o, "count_matches", opts, func(nv nestedView, mem memory.Allocator) (*Series, error) {
		return nsCountMatches(nv, v, false, mem)
	})
}

func nsJoin(v nestedView, sep string, ignoreNulls bool, mem memory.Allocator) (*Series, error) {
	sa, ok := v.values.(*array.String)
	if !ok {
		return nil, fmt.Errorf("attempted list join with non-string dtype: %s", dtypeName(v.arr.DataType()))
	}
	b := newStrColBuilder(mem, v.n, len(sa.ValueBytes()))
rows:
	for i := range v.n {
		if !v.valid(i) {
			b.appendNull()
			continue
		}
		s, e := v.rng(i)
		first := true
		for j := s; j < e; j++ {
			if sa.IsNull(j) {
				if !ignoreNulls {
					b.data.n = int(b.lastOffset())
					b.appendNull()
					continue rows
				}
				continue
			}
			if !first {
				b.extend(sep)
			}
			b.extend(sa.Value(j))
			first = false
		}
		b.closeValue()
	}
	return b.finish(v.name)
}

// Join concatenates the strings of each list with sep. Nulls inside a
// list are skipped when ignoreNulls is true; otherwise they make the
// row null. Null lists stay null and empty lists give "".
func (o ListOps) Join(sep string, ignoreNulls bool, opts ...Option) (*Series, error) {
	return withView(o, "join", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsJoin(v, sep, ignoreNulls, mem)
	})
}

// dtypeName renders a dtype the way polars error messages do.
func dtypeName(dt arrow.DataType) string { return dtype.FromArrow(dt).String() }
