package lazy_test

import (
	"context"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestJoinOnDifferentKeyNames(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	o := series.WithAllocator(mem)
	ck, _ := series.FromInt64("c_custkey", []int64{1, 2, 3}, nil, o)
	cn, _ := series.FromString("c_name", []string{"a", "b", "c"}, nil, o)
	cust, _ := dataframe.New(ck, cn)
	defer cust.Release()
	ok, _ := series.FromInt64("o_orderkey", []int64{10, 11, 12, 13}, nil, o)
	oc, _ := series.FromInt64("o_custkey", []int64{2, 1, 2, 9}, nil, o)
	orders, _ := dataframe.New(ok, oc)
	defer orders.Release()

	out, err := lazy.FromDataFrame(cust).
		JoinOn(lazy.FromDataFrame(orders), "c_custkey", "o_custkey", dataframe.InnerJoin).
		Filter(expr.Col("o_orderkey").Gt(expr.LitInt64(10))).
		Select(expr.Col("c_custkey"), expr.Col("c_name"), expr.Col("o_orderkey")).
		Collect(context.Background(), lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := out.ColumnNames(); len(got) != 3 || got[0] != "c_custkey" {
		t.Fatalf("columns = %v", got)
	}
	sorted, err := out.SortBy(context.Background(), []string{"o_orderkey"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Release()
	keys, _ := sorted.Column("c_custkey")
	want := []int64{1, 2}
	if keys.Len() != len(want) {
		t.Fatalf("rows = %d, want %d\n%s", keys.Len(), len(want), sorted)
	}
	for i, w := range want {
		if v, _ := keys.Get(i); v != w {
			t.Fatalf("row %d: c_custkey = %v, want %d", i, v, w)
		}
	}
}
