package series

import (
	"fmt"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dtype"
)

// RangeAggOptions configures AggRanges.
type RangeAggOptions struct {
	// MinPeriods is the minimum number of rows (nulls included) a
	// window needs to produce a value. 0 disables the check.
	MinPeriods int
	// Ddof is the delta degrees of freedom of std and var.
	Ddof int
	// Quantile is the q of the "quantile" aggregation.
	Quantile float64
	// Interpolation of "quantile": "nearest", "lower", "higher",
	// "midpoint" or "linear".
	Interpolation string
}

// AggRanges reduces each row range [starts[i], starts[i]+lens[i]) of s
// with op. It backs rolling_*_by, DataFrame.Rolling and
// DataFrame.GroupByDynamic. Supported ops: sum, mean, min, max, std,
// var, median, quantile, count, null_count, len, first, last and
// n_unique. Empty windows and windows shorter than MinPeriods give
// null (sum of an all-null window is 0, like polars).
func AggRanges(s *Series, op string, starts, lens []int, o RangeAggOptions, opts ...Option) (*Series, error) {
	cfg := resolve(opts)
	arr := s.Chunk(0)
	n := len(starts)
	name := s.Name()
	// With MinPeriods == 0 (group-by semantics) an empty window still
	// yields a value where one exists: sum 0, count 0.
	short := func(i int) bool { return o.MinPeriods > 0 && lens[i] < o.MinPeriods }
	switch op {
	case "len", "count", "null_count":
		return buildFixed(name, n, cfg.alloc, arrow.PrimitiveTypes.Uint32, nil, func(out []uint32, _ []byte) error {
			for i := range n {
				c := lens[i]
				if op != "len" && arr.NullN() > 0 {
					nulls := 0
					for j := starts[i]; j < starts[i]+lens[i]; j++ {
						if arr.IsNull(j) {
							nulls++
						}
					}
					if op == "count" {
						c -= nulls
					} else {
						c = nulls
					}
				} else if op == "null_count" {
					c = 0
				}
				out[i] = uint32(c)
			}
			return nil
		})
	case "first", "last":
		idx := make([]int, n)
		for i := range n {
			switch {
			case lens[i] == 0:
				idx[i] = -1
			case op == "first":
				idx[i] = starts[i]
			default:
				idx[i] = starts[i] + lens[i] - 1
			}
		}
		return takeNullable(s, idx, cfg.alloc)
	case "n_unique":
		return buildFixed(name, n, cfg.alloc, arrow.PrimitiveTypes.Uint32, nil, func(out []uint32, _ []byte) error {
			for i := range n {
				seen := map[any]struct{}{}
				for j := starts[i]; j < starts[i]+lens[i]; j++ {
					if arr.IsNull(j) {
						seen[nil] = struct{}{}
						continue
					}
					seen[cellKey(arr, j)] = struct{}{}
				}
				out[i] = uint32(len(seen))
			}
			return nil
		})
	}

	dt := s.DType()
	isTemporal := dt.IsTemporal()
	if (op == "min" || op == "max") && (dt.IsInteger() || isTemporal) {
		vals := physicalInt64(arr)
		outDT := dt.Arrow()
		if dt.IsInteger() {
			outDT = arrow.PrimitiveTypes.Int64
		}
		best := func(i int) (int64, bool) {
			var b int64
			found := false
			for j := starts[i]; j < starts[i]+lens[i]; j++ {
				if arr.IsNull(j) {
					continue
				}
				v := vals[j]
				if !found || (op == "min" && v < b) || (op == "max" && v > b) {
					b, found = v, true
				}
			}
			return b, found
		}
		if dt.IsDate() || dt.ID() == arrow.TIME32 {
			return buildFixed(name, n, cfg.alloc, outDT, nil, func(out []int32, valid []byte) error {
				for i := range n {
					v, ok := best(i)
					if !ok || short(i) {
						clearValid(valid, i)
						continue
					}
					out[i] = int32(v)
				}
				return nil
			})
		}
		return buildFixed(name, n, cfg.alloc, outDT, nil, func(out []int64, valid []byte) error {
			for i := range n {
				v, ok := best(i)
				if !ok || short(i) {
					clearValid(valid, i)
					continue
				}
				out[i] = v
			}
			return nil
		})
	}
	if op == "sum" && dt.IsInteger() {
		vals := physicalInt64(arr)
		return buildFixed(name, n, cfg.alloc, arrow.PrimitiveTypes.Int64, nil, func(out []int64, valid []byte) error {
			for i := range n {
				if short(i) {
					clearValid(valid, i)
					continue
				}
				var sum int64
				for j := starts[i]; j < starts[i]+lens[i]; j++ {
					if arr.IsValid(j) {
						sum += vals[j]
					}
				}
				out[i] = sum
			}
			return nil
		})
	}
	if isTemporal || !(dt.IsNumeric() || dt.IsBool()) {
		if (op == "min" || op == "max") && dt.IsString() {
			return stringMinMaxRanges(s, op, starts, lens, cfg.alloc)
		}
		return nil, fmt.Errorf("rolling/dynamic aggregation %q not supported for dtype %s", op, dt)
	}
	fv := floatValues(arr)
	var scratch []float64
	return buildFixed(name, n, cfg.alloc, arrow.PrimitiveTypes.Float64, nil, func(out []float64, valid []byte) error {
		for i := range n {
			if short(i) {
				clearValid(valid, i)
				continue
			}
			scratch = scratch[:0]
			for j := starts[i]; j < starts[i]+lens[i]; j++ {
				if arr.IsValid(j) {
					scratch = append(scratch, fv[j])
				}
			}
			v, ok := reduceFloats(op, scratch, o)
			if !ok {
				clearValid(valid, i)
				continue
			}
			out[i] = v
		}
		return nil
	})
}

func floatValues(arr arrow.Array) []float64 {
	switch a := arr.(type) {
	case *array.Float64:
		return a.Float64Values()
	case *array.Float32:
		src := a.Float32Values()
		out := make([]float64, len(src))
		for i, v := range src {
			out[i] = float64(v)
		}
		return out
	case *array.Boolean:
		out := make([]float64, a.Len())
		for i := range out {
			if a.Value(i) {
				out[i] = 1
			}
		}
		return out
	}
	iv := physicalInt64(arr)
	out := make([]float64, len(iv))
	for i, v := range iv {
		out[i] = float64(v)
	}
	return out
}

// reduceFloats applies op to the non-null values of one window.
func reduceFloats(op string, xs []float64, o RangeAggOptions) (float64, bool) {
	n := len(xs)
	switch op {
	case "sum":
		var s float64
		for _, x := range xs {
			s += x
		}
		return s, true
	case "mean":
		if n == 0 {
			return 0, false
		}
		var s float64
		for _, x := range xs {
			s += x
		}
		return s / float64(n), true
	case "min", "max":
		if n == 0 {
			return 0, false
		}
		b := xs[0]
		for _, x := range xs[1:] {
			if (op == "min" && x < b) || (op == "max" && x > b) {
				b = x
			}
		}
		return b, true
	case "std", "var":
		if n-o.Ddof <= 0 {
			return 0, false
		}
		var mean float64
		for _, x := range xs {
			mean += x
		}
		mean /= float64(n)
		var ss float64
		for _, x := range xs {
			d := x - mean
			ss += d * d
		}
		v := ss / float64(n-o.Ddof)
		if op == "std" {
			return math.Sqrt(v), true
		}
		return v, true
	case "median":
		return quantileOf(xs, 0.5, "linear")
	case "quantile":
		interp := o.Interpolation
		if interp == "" {
			interp = "nearest"
		}
		return quantileOf(xs, o.Quantile, interp)
	}
	return 0, false
}

// quantileOf sorts xs in place and interpolates the q-th quantile the
// way polars does.
func quantileOf(xs []float64, q float64, interp string) (float64, bool) {
	n := len(xs)
	if n == 0 {
		return 0, false
	}
	slices.Sort(xs)
	pos := float64(n-1) * q
	switch interp {
	case "nearest":
		return xs[int(math.Round(pos))], true
	case "lower":
		return xs[int(math.Floor(pos))], true
	case "higher":
		return xs[int(math.Ceil(pos))], true
	case "midpoint":
		lo, hi := xs[int(math.Floor(pos))], xs[int(math.Ceil(pos))]
		return (lo + hi) / 2, true
	}
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return xs[lo], true
	}
	f := pos - float64(lo)
	return xs[lo] + (xs[hi]-xs[lo])*f, true
}

func cellKey(arr arrow.Array, i int) any {
	switch a := arr.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	}
	if v := physicalInt64(arr); v != nil {
		return v[i]
	}
	return fmt.Sprint(i)
}

func stringMinMaxRanges(s *Series, op string, starts, lens []int, mem memory.Allocator) (*Series, error) {
	a := s.Chunk(0).(*array.String)
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for i := range starts {
		best := ""
		found := false
		for j := starts[i]; j < starts[i]+lens[i]; j++ {
			if a.IsNull(j) {
				continue
			}
			v := a.Value(j)
			if !found || (op == "min" && v < best) || (op == "max" && v > best) {
				best, found = v, true
			}
		}
		if found {
			b.Append(best)
		} else {
			b.AppendNull()
		}
	}
	return New(s.Name(), b.NewArray())
}

// takeNullable gathers rows of s at idx; -1 produces null.
func takeNullable(s *Series, idx []int, mem memory.Allocator) (*Series, error) {
	arr := s.Chunk(0)
	n := len(idx)
	dt := s.DType()
	switch a := arr.(type) {
	case *array.String:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, j := range idx {
			if j < 0 || a.IsNull(j) {
				b.AppendNull()
			} else {
				b.Append(a.Value(j))
			}
		}
		return New(s.Name(), b.NewArray())
	case *array.Boolean:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, j := range idx {
			if j < 0 || a.IsNull(j) {
				b.AppendNull()
			} else {
				b.Append(a.Value(j))
			}
		}
		return New(s.Name(), b.NewArray())
	case *array.Float64:
		vals := a.Float64Values()
		return buildFixed(s.Name(), n, mem, dt.Arrow(), nil, func(out []float64, valid []byte) error {
			for i, j := range idx {
				if j < 0 || a.IsNull(j) {
					clearValid(valid, i)
					continue
				}
				out[i] = vals[j]
			}
			return nil
		})
	}
	vals := physicalInt64(arr)
	if vals == nil && arr.Len() > 0 {
		return nil, fmt.Errorf("first/last not supported for dtype %s", dt)
	}
	switch dt.ID() {
	case arrow.INT64, arrow.TIMESTAMP, arrow.DURATION, arrow.TIME64, arrow.DATE64:
		return buildFixed(s.Name(), n, mem, dt.Arrow(), nil, func(out []int64, valid []byte) error {
			for i, j := range idx {
				if j < 0 || arr.IsNull(j) {
					clearValid(valid, i)
					continue
				}
				out[i] = vals[j]
			}
			return nil
		})
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		return buildFixed(s.Name(), n, mem, dt.Arrow(), nil, func(out []int32, valid []byte) error {
			for i, j := range idx {
				if j < 0 || arr.IsNull(j) {
					clearValid(valid, i)
					continue
				}
				out[i] = int32(vals[j])
			}
			return nil
		})
	}
	// Other integer widths come back as Int64.
	return buildFixed(s.Name(), n, mem, dtype.Int64().Arrow(), nil, func(out []int64, valid []byte) error {
		for i, j := range idx {
			if j < 0 || arr.IsNull(j) {
				clearValid(valid, i)
				continue
			}
			out[i] = vals[j]
		}
		return nil
	})
}
