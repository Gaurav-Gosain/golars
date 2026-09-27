package ipc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
)

// When a later column fails to build, earlier columns are released
// through their Series and the failing column's raw chunks exactly
// once. A double release panics or trips the checked allocator.
func TestBuildDataFrameErrorPathReleasesOnce(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ib := array.NewInt64Builder(mem)
	ib.AppendValues([]int64{1, 2}, nil)
	good := ib.NewArray()
	ib.Release()

	fb := array.NewFloat64Builder(mem)
	fb.AppendValues([]float64{1}, nil)
	f := fb.NewArray()
	fb.Release()
	ib = array.NewInt64Builder(mem)
	ib.AppendValues([]int64{3}, nil)
	i := ib.NewArray()
	ib.Release()

	sch := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	// Column b mixes float64 and int64 chunks, so series.New fails.
	_, err := buildDataFrameFromChunks(sch, [][]arrow.Array{{good}, {f, i}})
	if err == nil {
		t.Fatal("want an error for mixed chunk dtypes")
	}
}
