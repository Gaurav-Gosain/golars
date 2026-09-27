package dataframe_test

import (
	"context"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Pivot labels and cells must index the whole column, also when the
// inputs arrive in several chunks.
func TestPivotMultiChunkInput(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	strs := func(name string, parts ...[]string) *series.Series {
		var chunks []arrow.Array
		for _, p := range parts {
			b := array.NewStringBuilder(alloc)
			b.AppendValues(p, nil)
			chunks = append(chunks, b.NewArray())
			b.Release()
		}
		s, err := series.New(name, chunks...)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	id := strs("id", []string{"A"}, []string{"A", "B", "B"})
	on := strs("on", []string{"x"}, []string{"é", "x", "zz"})
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4}, nil, series.WithAllocator(alloc))
	df, err := dataframe.New(id, on, v)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	out, err := df.Pivot(context.Background(), []string{"id"}, "on", "v", dataframe.PivotSum)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got, want := out.ColumnNames(), []string{"id", "x", "é", "zz"}; !slices.Equal(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}
