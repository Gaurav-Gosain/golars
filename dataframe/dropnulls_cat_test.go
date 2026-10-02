package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// DropNulls on a subset must keep every column, including a
// categorical one, even when all rows are dropped. polars keeps
// {c0: cat, c3: i64} with zero rows. Found by internal/difftest.
func TestDropNullsKeepsCategoricalColumn(t *testing.T) {
	idx := array.NewUint32Builder(memory.DefaultAllocator)
	idx.AppendNull()
	ia := idx.NewArray()
	sb := array.NewStringBuilder(memory.DefaultAllocator)
	da := sb.NewArray()
	cat := array.NewDictionaryArray(dtype.Categorical().Arrow(), ia, da)
	ia.Release()
	da.Release()
	c0, err := series.New("c0", cat)
	if err != nil {
		t.Fatal(err)
	}
	c3, err := series.FromInt64("c3", []int64{0}, []bool{false})
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(c0, c3)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := df.DropNulls(context.Background(), "c3")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Width() != 2 || out.Height() != 0 {
		t.Fatalf("got %d columns, %d rows: %v", out.Width(), out.Height(), out.Schema().Names())
	}
}
