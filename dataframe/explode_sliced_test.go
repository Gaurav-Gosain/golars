package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// Exploding a sliced list column must read offsets relative to the
// slice. Found by internal/difftest: filter then explode returned
// nulls. polars: [[9], [-2]] sliced to the last row explodes to [-2].
func TestExplodeSlicedList(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	vb := b.ValueBuilder().(*array.Int32Builder)
	b.Append(true)
	vb.Append(9)
	b.AppendNull()
	b.Append(true)
	vb.Append(-2)
	vb.Append(5)
	arr := b.NewArray()
	b.Release()
	full, err := series.New("l", arr)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Release()
	for _, tc := range []struct {
		off, n int
		want   []any
	}{
		{2, 1, []any{int32(-2), int32(5)}},
		{1, 2, []any{nil, int32(-2), int32(5)}},
	} {
		sl, err := full.Slice(tc.off, tc.n)
		if err != nil {
			t.Fatal(err)
		}
		df, err := dataframe.New(sl)
		if err != nil {
			t.Fatal(err)
		}
		out, err := df.Explode(context.Background(), "l")
		if err != nil {
			t.Fatal(err)
		}
		col := out.ColumnAt(0)
		var got []any
		for _, ch := range col.Chunks() {
			a := ch.(*array.Int32)
			for i := range a.Len() {
				if a.IsNull(i) {
					got = append(got, nil)
				} else {
					got = append(got, a.Value(i))
				}
			}
		}
		if len(got) != len(tc.want) {
			t.Fatalf("slice(%d, %d): got %v, want %v", tc.off, tc.n, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("slice(%d, %d): got %v, want %v", tc.off, tc.n, got, tc.want)
			}
		}
		out.Release()
		df.Release()
	}
}
