package dataframe_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Concat borrows its inputs: the caller still releases every input and
// the result, and the checked allocator sees no leak or double free.
func TestConcatBorrowsInputs(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	mk := func(vals ...int64) *dataframe.DataFrame {
		s, _ := series.FromInt64("a", vals, nil, series.WithAllocator(mem))
		df, err := dataframe.New(s)
		if err != nil {
			t.Fatal(err)
		}
		return df
	}
	a, b := mk(1, 2), mk(3)
	one, err := dataframe.Concat(a)
	if err != nil {
		t.Fatal(err)
	}
	two, err := dataframe.Concat(a, b)
	if err != nil {
		t.Fatal(err)
	}
	stacked, err := a.VStack(b)
	if err != nil {
		t.Fatal(err)
	}
	a.Release()
	b.Release()
	if one.Height() != 2 || two.Height() != 3 || stacked.Height() != 3 {
		t.Errorf("heights = %d, %d, %d; want 2, 3, 3", one.Height(), two.Height(), stacked.Height())
	}
	one.Release()
	two.Release()
	stacked.Release()

	// All-empty inputs (zero-chunk columns) concatenate to an empty frame.
	e1, _ := dataframe.New(series.Empty("a", dtype.Int64()))
	e2, _ := dataframe.New(series.Empty("a", dtype.Int64()))
	defer e1.Release()
	defer e2.Release()
	empty, err := dataframe.Concat(e1, e2)
	if err != nil {
		t.Fatalf("Concat of empty frames: %v", err)
	}
	defer empty.Release()
	if empty.Height() != 0 || empty.Width() != 1 {
		t.Errorf("shape = %dx%d, want 0x1", empty.Height(), empty.Width())
	}
}
