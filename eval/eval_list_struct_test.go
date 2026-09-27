package eval_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/eval"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func makeListDF(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	mem := memory.DefaultAllocator
	b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	vb := b.ValueBuilder().(*array.Int64Builder)
	b.Append(true)
	vb.Append(1)
	vb.Append(2)
	vb.Append(3)
	b.Append(true)
	vb.Append(10)
	arr := b.NewListArray()
	b.Release()
	s, err := series.New("vs", arr)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := series.FromInt64("id", []int64{1, 2}, nil)
	df, err := dataframe.New(id, s)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestEvalListSum(t *testing.T) {
	df := makeListDF(t)
	defer df.Release()
	out, err := eval.Eval(context.Background(), eval.Default(), expr.Col("vs").List().Sum(), df)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.Value(0) != 6 || arr.Value(1) != 10 {
		t.Errorf("got [%d,%d] want [6,10]", arr.Value(0), arr.Value(1))
	}
}

func TestEvalListLen(t *testing.T) {
	df := makeListDF(t)
	defer df.Release()
	out, err := eval.Eval(context.Background(), eval.Default(), expr.Col("vs").List().Len(), df)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Uint32)
	if arr.Value(0) != 3 || arr.Value(1) != 1 {
		t.Errorf("got [%d,%d] want [3,1]", arr.Value(0), arr.Value(1))
	}
}

func TestEvalStructField(t *testing.T) {
	mem := memory.DefaultAllocator
	fields := []arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		{Name: "y", Type: arrow.BinaryTypes.String},
	}
	b := array.NewStructBuilder(mem, arrow.StructOf(fields...))
	xb := b.FieldBuilder(0).(*array.Int64Builder)
	yb := b.FieldBuilder(1).(*array.StringBuilder)
	b.Append(true)
	xb.Append(7)
	yb.Append("a")
	b.Append(true)
	xb.Append(9)
	yb.Append("b")
	s, _ := series.New("payload", b.NewArray())
	b.Release()
	id, _ := series.FromInt64("id", []int64{1, 2}, nil)
	df, _ := dataframe.New(id, s)
	defer df.Release()

	out, err := eval.Eval(context.Background(), eval.Default(),
		expr.Col("payload").Struct().Field("x"), df)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.Value(0) != 7 || arr.Value(1) != 9 {
		t.Errorf("got [%d,%d] want [7,9]", arr.Value(0), arr.Value(1))
	}
}
