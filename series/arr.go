package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrOps is the namespace for fixed-size list (polars Array) columns.
// Reach it via `s.Arr()`. It mirrors polars' `s.arr.*` and shares the
// per-row kernels of the list namespace; kernels that keep the width
// (reverse, sort, shift) return an Array, the others follow polars'
// output types (head, tail, slice and unique return a List).
type ArrOps struct{ s *Series }

// Arr returns the array namespace.
func (s *Series) Arr() ArrOps { return ArrOps{s: s} }

func withArr(o ArrOps, op string, opts []Option, fn func(nestedView, memory.Allocator) (*Series, error)) (*Series, error) {
	v, err := newNestedView(o.s, op, true)
	if err != nil {
		return nil, err
	}
	defer v.release()
	return fn(v, resolve(opts).alloc)
}

// selectFixedRows is selectRows for kernels that keep the array width:
// fn must append exactly width indices for every valid row. The result
// is a FixedSizeList with the input's validity.
func (v nestedView) selectFixedRows(mem memory.Allocator, fn func(i, start, end int, idx []int) []int) (*Series, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	idx := make([]int, 0, v.values.Len())
	for i := range v.n {
		if !v.valid(i) {
			for range v.width {
				idx = append(idx, -1)
			}
			continue
		}
		s, e := v.rng(i)
		idx = fn(i, s, e, idx)
	}
	child, err := takeArrow(v.values, idx, mem)
	if err != nil {
		return nil, err
	}
	defer child.Release()
	validBuf, nulls := v.validityBuf(mem)
	dt := arrow.FixedSizeListOfField(int32(v.width), arrow.Field{Name: "item", Type: child.DataType(), Nullable: true})
	data := array.NewData(dt, v.n, []*memory.Buffer{validBuf}, []arrow.ArrayData{child.Data()}, nulls, 0)
	arr := array.MakeFromData(data)
	data.Release()
	if validBuf != nil {
		validBuf.Release()
	}
	return New(v.name, arr)
}

// Len returns the width of each non-null array as UInt32.
func (o ArrOps) Len(opts ...Option) (*Series, error) { return withArr(o, "len", opts, nsLen) }

// Sum returns the sum of each array (0 when every element is null).
func (o ArrOps) Sum(opts ...Option) (*Series, error) {
	return withArr(o, "sum", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if _, ok := sumDType(v.childType()); !ok {
			return nil, fmt.Errorf("summing array with dtype: %s not yet supported", arrDTypeName(v))
		}
		return nsSum(v, mem)
	})
}

// arrDTypeName renders the array dtype as polars does in messages
// (array[str, 2]).
func arrDTypeName(v nestedView) string {
	return fmt.Sprintf("array[%s, %d]", dtypeName(v.childType()), v.width)
}

// Mean returns the mean of each array.
func (o ArrOps) Mean(opts ...Option) (*Series, error) { return withArr(o, "mean", opts, nsMean) }

// Median returns the median of each array.
func (o ArrOps) Median(opts ...Option) (*Series, error) {
	return withArr(o, "median", opts, nsMedian)
}

// Std returns the standard deviation of each array.
func (o ArrOps) Std(ddof int, opts ...Option) (*Series, error) {
	return withArr(o, "std", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsVar(v, ddof, true, mem)
	})
}

// Var returns the variance of each array.
func (o ArrOps) Var(ddof int, opts ...Option) (*Series, error) {
	return withArr(o, "var", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsVar(v, ddof, false, mem)
	})
}

func arrNumericOnly(v nestedView) error {
	switch v.childType().ID() {
	case arrow.BOOL:
		return fmt.Errorf("not implemented for dtype Boolean")
	case arrow.STRING, arrow.LARGE_STRING:
		return fmt.Errorf("not implemented for dtype String")
	}
	return nil
}

// Min returns the minimum of each numeric array.
func (o ArrOps) Min(opts ...Option) (*Series, error) {
	return withArr(o, "min", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if err := arrNumericOnly(v); err != nil {
			return nil, err
		}
		return nsMinMax(v, false, mem)
	})
}

// Max returns the maximum of each numeric array.
func (o ArrOps) Max(opts ...Option) (*Series, error) {
	return withArr(o, "max", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if err := arrNumericOnly(v); err != nil {
			return nil, err
		}
		return nsMinMax(v, true, mem)
	})
}

// ArgMin returns the index of each array's minimum as UInt32.
func (o ArrOps) ArgMin(opts ...Option) (*Series, error) {
	return withArr(o, "arg_min", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsArg(v, false, mem)
	})
}

// ArgMax returns the index of each array's maximum as UInt32.
func (o ArrOps) ArgMax(opts ...Option) (*Series, error) {
	return withArr(o, "arg_max", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsArg(v, true, mem)
	})
}

// NUnique counts distinct values per array (null counts as a value).
func (o ArrOps) NUnique(opts ...Option) (*Series, error) {
	return withArr(o, "n_unique", opts, nsNUnique)
}

func arrAnyAll(o ArrOps, op string, all bool, opts []Option) (*Series, error) {
	return withArr(o, op, opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if v.childType().ID() != arrow.BOOL {
			return nil, fmt.Errorf("expected boolean elements in array")
		}
		return nsAnyAll(v, all, mem)
	})
}

// Any reports whether any element of each boolean array is true.
func (o ArrOps) Any(opts ...Option) (*Series, error) { return arrAnyAll(o, "any", false, opts) }

// All reports whether every non-null element of each boolean array is
// true.
func (o ArrOps) All(opts ...Option) (*Series, error) { return arrAnyAll(o, "all", true, opts) }

// Get returns element idx of each array (negative counts from the
// back). Out-of-range indices are null with nullOnOOB and an error
// otherwise.
func (o ArrOps) Get(idx int, nullOnOOB bool, opts ...Option) (*Series, error) {
	return withArr(o, "get", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		j := idx
		if j < 0 {
			j += v.width
		}
		if !nullOnOOB && (j < 0 || j >= v.width) && v.n > v.arr.NullN() {
			return nil, fmt.Errorf("get index %d is out of bounds for array of width %d", idx, v.width)
		}
		return nsGet(v, idx, true, mem)
	})
}

// First returns the first element of each array.
func (o ArrOps) First(opts ...Option) (*Series, error) { return o.Get(0, true, opts...) }

// Last returns the last element of each array.
func (o ArrOps) Last(opts ...Option) (*Series, error) { return o.Get(-1, true, opts...) }

// Contains reports whether each array holds v (nil looks for a null).
func (o ArrOps) Contains(v any, opts ...Option) (*Series, error) {
	return withArr(o, "contains", opts, func(nv nestedView, mem memory.Allocator) (*Series, error) {
		return nsCountMatches(nv, v, true, mem)
	})
}

// CountMatches counts occurrences of v per array as UInt32.
func (o ArrOps) CountMatches(v any, opts ...Option) (*Series, error) {
	return withArr(o, "count_matches", opts, func(nv nestedView, mem memory.Allocator) (*Series, error) {
		return nsCountMatches(nv, v, false, mem)
	})
}

// Join concatenates the strings of each array with sep.
func (o ArrOps) Join(sep string, ignoreNulls bool, opts ...Option) (*Series, error) {
	return withArr(o, "join", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if v.childType().ID() != arrow.STRING {
			return nil, fmt.Errorf("`array.join` operation not supported for dtype `%s` (expected: String)", dtypeName(v.childType()))
		}
		return nsJoin(v, sep, ignoreNulls, mem)
	})
}

// Reverse reverses each array (the result is still an Array).
func (o ArrOps) Reverse(opts ...Option) (*Series, error) {
	return withArr(o, "reverse", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return v.selectFixedRows(mem, func(_, s, e int, idx []int) []int {
			for j := e - 1; j >= s; j-- {
				idx = append(idx, j)
			}
			return idx
		})
	})
}

// Sort sorts each array (nulls first unless nullsLast).
func (o ArrOps) Sort(descending, nullsLast bool, opts ...Option) (*Series, error) {
	return withArr(o, "sort", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		less, ok := newElemCmp(v.values)
		if !ok {
			return nil, fmt.Errorf("sort not supported for dtype `%s`", dtypeName(v.childType()))
		}
		return v.selectFixedRows(mem, func(_, s, e int, idx []int) []int {
			return sortRowIdx(v.values, less, s, e, descending, nullsLast, idx)
		})
	})
}

// Shift shifts the elements of each array by n, filling with null.
func (o ArrOps) Shift(n int, opts ...Option) (*Series, error) {
	return withArr(o, "shift", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return v.selectFixedRows(mem, func(_, s, e int, idx []int) []int {
			for k := range e - s {
				src := k - n
				if src < 0 || src >= e-s {
					idx = append(idx, -1)
				} else {
					idx = append(idx, s+src)
				}
			}
			return idx
		})
	})
}

// Unique returns the distinct values of each array as a List.
func (o ArrOps) Unique(maintainOrder bool, opts ...Option) (*Series, error) {
	return withArr(o, "unique", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsUnique(v, maintainOrder, mem)
	})
}

// Slice returns elements [offset, offset+length) of each array as a
// List. length < 0 takes the rest.
func (o ArrOps) Slice(offset, length int, opts ...Option) (*Series, error) {
	return withArr(o, "slice", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsSlice(v, offset, length, mem)
	})
}

// Head returns the first n elements of each array as a List.
func (o ArrOps) Head(n int, opts ...Option) (*Series, error) { return o.Slice(0, n, opts...) }

// Tail returns the last n elements of each array as a List.
func (o ArrOps) Tail(n int, opts ...Option) (*Series, error) {
	if n < 0 {
		return o.Slice(-n, -1, opts...)
	}
	return o.Slice(-n, n, opts...)
}

// Explode flattens the arrays into rows; a null array gives one null
// row.
func (o ArrOps) Explode(opts ...Option) (*Series, error) {
	return withArr(o, "explode", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return nsExplode(v, true, true, mem)
	})
}

// ToList converts the column to a variable-length List.
func (o ArrOps) ToList(opts ...Option) (*Series, error) {
	return withArr(o, "to_list", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		return v.selectRows(mem, func(_, s, e int, idx []int) ([]int, bool) {
			for j := s; j < e; j++ {
				idx = append(idx, j)
			}
			return idx, true
		})
	})
}

// ToStruct converts each array to a struct with field_0 ..
// field_{width-1} (or the given names).
func (o ArrOps) ToStruct(fields []string, opts ...Option) (*Series, error) {
	return withArr(o, "to_struct", opts, func(v nestedView, mem memory.Allocator) (*Series, error) {
		if fields == nil {
			fields = make([]string, v.width)
			for f := range v.width {
				fields[f] = fmt.Sprintf("field_%d", f)
			}
		}
		return nsToStruct(v, fields, true, mem)
	})
}
