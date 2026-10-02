package ipc_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/io/ipc"
	"github.com/Gaurav-Gosain/golars/series"
)

func catChunk(t *testing.T, vals ...string) arrow.Array {
	t.Helper()
	mem := memory.DefaultAllocator
	idx := array.NewUint32Builder(mem)
	defer idx.Release()
	dict := array.NewStringBuilder(mem)
	defer dict.Release()
	for i, v := range vals {
		dict.Append(v)
		idx.Append(uint32(i))
	}
	ia, da := idx.NewArray(), dict.NewArray()
	defer ia.Release()
	defer da.Release()
	return array.NewDictionaryArray(dtype.Categorical().Arrow(), ia, da)
}

// Writing a categorical column whose first chunk is empty panicked in
// arrow's Concatenate. Found by internal/difftest.
func TestIPCWriteCategoricalEmptyChunk(t *testing.T) {
	s, err := series.New("c", catChunk(t), catChunk(t, "a"), catChunk(t, "b"))
	if err != nil {
		t.Fatal(err)
	}
	df, err := dataframe.New(s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()
	var buf bytes.Buffer
	if err := ipc.Write(context.Background(), &buf, df); err != nil {
		t.Fatal(err)
	}
	back, err := ipc.Read(context.Background(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	if got := catIPCCells(back.ColumnAt(0)); got != "a,b" {
		t.Fatalf("got %s", got)
	}
}
