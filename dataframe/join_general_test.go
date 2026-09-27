package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

// A left join with no matches null-extends nested right columns. The
// all-null runs come from the pooled allocator, whose recycled memory
// once left garbage list offsets and struct child strings behind
// (found by internal/difftest).
func TestJoinNullExtendsNestedColumns(t *testing.T) {
	ctx := context.Background()
	for range 5 {
		lk, _ := series.FromInt64("k", []int64{1, 2, 3, 4}, nil)
		left, _ := dataframe.New(lk)

		rk, _ := series.FromInt64("k", []int64{9, 8}, nil)
		st := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Nullable: true})
		sb := array.NewStructBuilder(memory.DefaultAllocator, st)
		for _, v := range []string{"xxxxxxxxxxxxxxxxxxxx", "yyyyyyyyyyyyyyyy"} {
			sb.Append(true)
			sb.FieldBuilder(0).(*array.StringBuilder).Append(v)
		}
		ss, _ := series.New("s", sb.NewArray())
		sb.Release()
		lb := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
		for range 2 {
			lb.Append(true)
			lb.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		}
		ls, _ := series.New("l", lb.NewArray())
		lb.Release()
		right, _ := dataframe.New(rk, ss, ls)

		out, err := left.Join(ctx, right, []string{"k"}, dataframe.LeftJoin)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"s", "l"} {
			c, _ := out.Column(name)
			if c.NullCount() != 4 {
				t.Fatalf("%s: %d nulls, want 4", name, c.NullCount())
			}
			a, _ := c.Consolidated()
			for i := range a.Len() {
				if got := a.ValueStr(i); got != array.NullValueStr {
					t.Fatalf("%s row %d: %q, want null", name, i, got)
				}
			}
			a.Release()
		}
		out.Release()
		left.Release()
		right.Release()
	}
}

// Multi-chunk categorical payloads with an empty chunk used to fail in
// arrow's Concatenate while gathering.
func TestJoinCategoricalWithEmptyChunk(t *testing.T) {
	ctx := context.Background()
	empty := array.NewDictionaryArray(dtype.Categorical().Arrow().(*arrow.DictionaryType),
		array.NewUint32Builder(memory.DefaultAllocator).NewArray(),
		array.NewStringBuilder(memory.DefaultAllocator).NewArray())
	vals, _ := series.FromString("c", []string{"a", "b"}, nil)
	cat, err := compute.Cast(ctx, vals, dtype.Categorical())
	if err != nil {
		t.Fatal(err)
	}
	full, _ := cat.Consolidated()
	c, err := series.New("c", empty, full)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := series.FromInt64("k", []int64{1, 2}, nil)
	left, _ := dataframe.New(k, c)
	defer left.Release()
	rk, _ := series.FromInt64("k", []int64{2}, nil)
	right, _ := dataframe.New(rk)
	defer right.Release()
	for _, how := range []dataframe.JoinType{dataframe.InnerJoin, dataframe.FullJoin, dataframe.SemiJoin} {
		out, err := left.Join(ctx, right, []string{"k"}, how)
		if err != nil {
			t.Fatalf("%s: %v", how, err)
		}
		out.Release()
	}
}
