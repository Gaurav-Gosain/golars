package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Flatten returns the flat child values that the rows of a list (or
// array) column cover, plus row offsets relative to that child (n+1
// entries; a null row spans an empty range). list.eval and similar
// consumers evaluate on the child and rebuild with ListFromOffsets.
func (s *Series) Flatten() (*Series, []int, error) {
	v, err := newNestedView(s, "flatten", s.DType().IsFixedList())
	if err != nil {
		return nil, nil, err
	}
	defer v.release()
	offsets := make([]int, v.n+1)
	lo, hi := 0, 0
	if v.n > 0 {
		lo, _ = v.rng(0)
		_, hi = v.rng(v.n - 1)
	}
	for i := range v.n {
		s0, e0 := v.rng(i)
		offsets[i] = s0 - lo
		offsets[i+1] = e0 - lo
	}
	child := array.NewSlice(v.values, int64(lo), int64(hi))
	out, err := New(v.name, child)
	if err != nil {
		child.Release()
		return nil, nil, err
	}
	return out, offsets, nil
}

// ListFromOffsets builds a List column named name whose row i holds
// child[offsets[i]:offsets[i+1]]. Rows where template (a list or array
// column of the same length) is null stay null.
func ListFromOffsets(name string, template *Series, offsets []int, child *Series, opts ...Option) (*Series, error) {
	mem := resolve(opts).alloc
	n := template.Len()
	if len(offsets) != n+1 {
		return nil, fmt.Errorf("series.ListFromOffsets: %d offsets for %d rows", len(offsets), n)
	}
	ca, err := child.Consolidated()
	if err != nil {
		return nil, err
	}
	defer ca.Release()
	ta, err := template.Consolidated()
	if err != nil {
		return nil, err
	}
	defer ta.Release()
	lb := newListOffsetsBuilder(mem, n)
	idx := make([]int, 0, ca.Len())
	identity := true
	for i := range n {
		valid := ta.IsValid(i)
		if valid {
			for j := offsets[i]; j < offsets[i+1]; j++ {
				if j != len(idx) {
					identity = false
				}
				idx = append(idx, j)
			}
		}
		lb.closeRow(len(idx), valid)
	}
	var arr arrow.Array
	if identity && len(idx) == ca.Len() {
		ca.Retain()
		arr = ca
	} else {
		arr, err = takeArrow(ca, idx, mem)
		if err != nil {
			lb.release()
			return nil, err
		}
	}
	return New(name, lb.newListArray(arr))
}
