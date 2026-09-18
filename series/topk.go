package series

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TopKIndices returns the row indices of the k largest (descending) or
// smallest (ascending) non-null elements, in output order. Ties run in
// input order (stable), matching polars top_k and DataFrame.TopK.
func (s *Series) TopKIndices(k int, descending bool) ([]int, error) {
	return topKIndices(s, k, descending)
}

// topKIndices returns the row indices of the k largest (descending) or
// smallest (ascending) non-null elements, in output order.
//
// Small k takes a binary-heap path: O(n log k) with one comparison per
// row in the common reject case, vs O(n log n) closure comparisons for
// ArgSort. The heap replicates the sort order exactly, including tie
// direction (input order both ways) and float NaN placement (largest).
// Anything outside int64/float64, or k covering a large fraction of
// the input, stays on ArgSort.
func topKIndices(s *Series, k int, descending bool) ([]int, error) {
	if k <= 0 {
		return []int{}, nil
	}
	chunk := s.Chunk(0)
	n := chunk.Len()
	nn := n - s.NullCount()
	if k >= nn {
		return topKIndicesSort(s, k, nn, descending)
	}
	switch a := chunk.(type) {
	case *array.Int64:
		if k*heapTopKDivisor < nn {
			return heapTopKInt64(a, n, k, descending), nil
		}
	case *array.Float64:
		if k*heapTopKDivisor < nn {
			return heapTopKFloat64(a, n, k, descending), nil
		}
	}
	return topKIndicesSort(s, k, nn, descending)
}

// heapTopKDivisor tunes the heap/argsort crossover: heap while
// k*heapTopKDivisor < nn.
const heapTopKDivisor = 2

// topKIndicesSort is the argsort-based path: ascending with nulls last
// for bottom-k; a stable value-descending pass over the non-null
// prefix for top-k (ties keep input order).
func topKIndicesSort(s *Series, k, nn int, descending bool) ([]int, error) {
	idx, err := s.ArgSort()
	if err != nil {
		return nil, err
	}
	if k > nn {
		k = nn
	}
	if !descending {
		return idx[:k], nil
	}
	head := idx[:nn]
	cmp, err := descRank(s.Chunk(0))
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(head, cmp)
	return head[:k], nil
}

// descRank orders row indices by value descending, NaN first, input
// order on ties. Supports the ArgSort dtypes.
func descRank(chunk arrow.Array) (func(a, b int) int, error) {
	switch a := chunk.(type) {
	case *array.Int64:
		vals := a.Int64Values()
		return func(x, y int) int {
			switch {
			case vals[x] > vals[y]:
				return -1
			case vals[x] < vals[y]:
				return 1
			default:
				return 0
			}
		}, nil
	case *array.Float64:
		vals := a.Float64Values()
		return func(x, y int) int {
			xv, yv := vals[x], vals[y]
			xNaN, yNaN := xv != xv, yv != yv
			switch {
			case xNaN && yNaN:
				return 0
			case xNaN:
				return -1
			case yNaN:
				return 1
			case xv > yv:
				return -1
			case xv < yv:
				return 1
			default:
				return 0
			}
		}, nil
	case *array.String:
		return func(x, y int) int {
			xv, yv := a.Value(x), a.Value(y)
			switch {
			case xv > yv:
				return -1
			case xv < yv:
				return 1
			default:
				return 0
			}
		}, nil
	}
	return nil, fmt.Errorf("series: TopK unsupported for dtype %s", chunk.DataType())
}

// heapTopKInt64 streams values through a size-k heap whose root is the
// current worst kept element. Output order is value rank with input
// order on ties, matching the sort path exactly. Comparisons are fully
// inlined per direction: the reject path costs one direct comparison.
func heapTopKInt64(a *array.Int64, n, k int, descending bool) []int {
	vals := a.Int64Values()
	hasNulls := a.NullN() > 0
	h := make([]int, 0, k)
	for i := range n {
		if hasNulls && a.IsNull(i) {
			continue
		}
		if len(h) < k {
			h = append(h, i)
			for j := len(h) - 1; j > 0; {
				p := (j - 1) >> 1
				if !int64Worse(descending, vals, h[j], h[p]) {
					break
				}
				h[j], h[p] = h[p], h[j]
				j = p
			}
			continue
		}
		if int64Worse(descending, vals, i, h[0]) {
			continue
		}
		h[0] = i
		for j := 0; ; {
			l, r := 2*j+1, 2*j+2
			m := j
			if l < len(h) && int64Worse(descending, vals, h[l], h[m]) {
				m = l
			}
			if r < len(h) && int64Worse(descending, vals, h[r], h[m]) {
				m = r
			}
			if m == j {
				break
			}
			h[j], h[m] = h[m], h[j]
			j = m
		}
	}
	sortHeapInt64(h, vals, descending)
	return h
}

// int64Worse orders int64 indices by output rank ascending: worse
// means a smaller value, or an equal value at a larger index. Plain
// function (inlinable) shared by the int64 loops.
func int64Worse(descending bool, vals []int64, x, y int) bool {
	if vals[x] != vals[y] {
		if descending {
			return vals[x] < vals[y]
		}
		return vals[x] > vals[y]
	}
	return x > y
}

// heapTopKFloat64 mirrors heapTopKInt64 with NaN ranking largest.
func heapTopKFloat64(a *array.Float64, n, k int, descending bool) []int {
	vals := a.Float64Values()
	hasNulls := a.NullN() > 0
	h := make([]int, 0, k)
	if descending {
		for i := range n {
			if hasNulls && a.IsNull(i) {
				continue
			}
			if len(h) < k {
				h = append(h, i)
				for j := len(h) - 1; j > 0; {
					p := (j - 1) >> 1
					if !floatWorse(true, vals, h[j], h[p]) {
						break
					}
					h[j], h[p] = h[p], h[j]
					j = p
				}
				continue
			}
			if floatWorse(true, vals, i, h[0]) {
				continue
			}
			h[0] = i
			for j := 0; ; {
				l, r := 2*j+1, 2*j+2
				m := j
				if l < len(h) && floatWorse(true, vals, h[l], h[m]) {
					m = l
				}
				if r < len(h) && floatWorse(true, vals, h[r], h[m]) {
					m = r
				}
				if m == j {
					break
				}
				h[j], h[m] = h[m], h[j]
				j = m
			}
		}
	} else {
		for i := range n {
			if hasNulls && a.IsNull(i) {
				continue
			}
			if len(h) < k {
				h = append(h, i)
				for j := len(h) - 1; j > 0; {
					p := (j - 1) >> 1
					if !floatWorse(false, vals, h[j], h[p]) {
						break
					}
					h[j], h[p] = h[p], h[j]
					j = p
				}
				continue
			}
			if floatWorse(false, vals, i, h[0]) {
				continue
			}
			h[0] = i
			for j := 0; ; {
				l, r := 2*j+1, 2*j+2
				m := j
				if l < len(h) && floatWorse(false, vals, h[l], h[m]) {
					m = l
				}
				if r < len(h) && floatWorse(false, vals, h[r], h[m]) {
					m = r
				}
				if m == j {
					break
				}
				h[j], h[m] = h[m], h[j]
				j = m
			}
		}
	}
	sortHeapFloat64(h, vals, descending)
	return h
}

// floatWorse orders float indices by output rank ascending with NaN
// largest. Plain function (inlinable) shared by the float64 loops.
func floatWorse(descending bool, vals []float64, x, y int) bool {
	xv, yv := vals[x], vals[y]
	xNaN, yNaN := xv != xv, yv != yv
	if xNaN != yNaN {
		if descending {
			return yNaN
		}
		return xNaN
	}
	if xNaN {
		return x > y
	}
	if descending {
		if xv != yv {
			return xv < yv
		}
		return x > y
	}
	if xv != yv {
		return xv > yv
	}
	return x > y
}

// sortHeapInt64 orders the kept indices best-first: value rank with
// input order on ties.
func sortHeapInt64(h []int, vals []int64, descending bool) {
	slices.SortFunc(h, func(x, y int) int {
		if vals[x] != vals[y] {
			if descending {
				if vals[x] > vals[y] {
					return -1
				}
				return 1
			}
			if vals[x] < vals[y] {
				return -1
			}
			return 1
		}
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	})
}

// sortHeapFloat64 mirrors sortHeapInt64 with NaN largest.
func sortHeapFloat64(h []int, vals []float64, descending bool) {
	slices.SortFunc(h, func(x, y int) int {
		xv, yv := vals[x], vals[y]
		xNaN, yNaN := xv != xv, yv != yv
		switch {
		case xNaN && yNaN:
		case xNaN:
			return -1
		case yNaN:
			return 1
		case xv != yv:
			if descending {
				if xv > yv {
					return -1
				}
				return 1
			}
			if xv < yv {
				return -1
			}
			return 1
		}
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	})
}
