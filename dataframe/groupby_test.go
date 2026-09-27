package dataframe_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestGroupBySumSingleKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k, _ := series.FromString("k", []string{"a", "b", "a", "b", "a"}, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{1, 10, 2, 20, 3}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(ctx,
		[]expr.Expr{expr.Col("v").Sum().Alias("v_sum")},
		dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatalf("Agg: %v", err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Errorf("Height = %d, want 2", out.Height())
	}
	if out.Width() != 2 {
		t.Errorf("Width = %d, want 2", out.Width())
	}

	keyCol, _ := out.Column("k")
	keys := keyCol.Chunk(0).(*array.String)
	if keys.Value(0) != "a" || keys.Value(1) != "b" {
		t.Errorf("keys = [%q, %q], want [a, b]", keys.Value(0), keys.Value(1))
	}

	sumCol, _ := out.Column("v_sum")
	sums := sumCol.Chunk(0).(*array.Int64).Int64Values()
	if sums[0] != 6 || sums[1] != 30 {
		t.Errorf("sums = %v, want [6, 30]", sums)
	}
}

func TestGroupByMultipleAggregations(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k, _ := series.FromString("region", []string{"n", "s", "n", "s", "n"}, nil, series.WithAllocator(mem))
	v, _ := series.FromFloat64("v", []float64{10, 20, 30, 40, 50}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("region").Agg(ctx, []expr.Expr{
		expr.Col("v").Sum().Alias("s"),
		expr.Col("v").Mean().Alias("m"),
		expr.Col("v").Min().Alias("lo"),
		expr.Col("v").Max().Alias("hi"),
		expr.Col("v").Count().Alias("n"),
	}, dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatalf("Agg: %v", err)
	}
	defer out.Release()

	if out.Height() != 2 {
		t.Errorf("Height = %d, want 2", out.Height())
	}

	// Groups come out sorted by region ascending: n (rows 0,2,4 = 10,30,50), s (rows 1,3 = 20,40).
	reg, _ := out.Column("region")
	regs := reg.Chunk(0).(*array.String)
	if regs.Value(0) != "n" || regs.Value(1) != "s" {
		t.Errorf("regions = [%q, %q], want [n, s]", regs.Value(0), regs.Value(1))
	}

	sumCol, _ := out.Column("s")
	ss := sumCol.Chunk(0).(*array.Float64).Float64Values()
	if ss[0] != 90 || ss[1] != 60 {
		t.Errorf("sum = %v, want [90, 60]", ss)
	}

	meanCol, _ := out.Column("m")
	ms := meanCol.Chunk(0).(*array.Float64).Float64Values()
	if ms[0] != 30.0 || ms[1] != 30.0 {
		t.Errorf("mean = %v, want [30, 30]", ms)
	}

	loCol, _ := out.Column("lo")
	los := loCol.Chunk(0).(*array.Float64).Float64Values()
	if los[0] != 10 || los[1] != 20 {
		t.Errorf("min = %v, want [10, 20]", los)
	}

	hiCol, _ := out.Column("hi")
	his := hiCol.Chunk(0).(*array.Float64).Float64Values()
	if his[0] != 50 || his[1] != 40 {
		t.Errorf("max = %v, want [50, 40]", his)
	}

	countCol, _ := out.Column("n")
	ns := countCol.Chunk(0).(*array.Uint32).Uint32Values() // polars: u32
	if ns[0] != 3 || ns[1] != 2 {
		t.Errorf("count = %v, want [3, 2]", ns)
	}
}

func TestGroupByMultiKey(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	region, _ := series.FromString("region", []string{"n", "n", "s", "s", "n", "s"}, nil, series.WithAllocator(mem))
	year, _ := series.FromInt64("year", []int64{2024, 2025, 2024, 2025, 2024, 2024}, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5, 6}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(region, year, v)
	defer df.Release()

	out, err := df.GroupBy("region", "year").Agg(ctx,
		[]expr.Expr{expr.Col("v").Sum().Alias("v_sum")},
		dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatalf("Agg: %v", err)
	}
	defer out.Release()

	if out.Height() != 4 {
		t.Errorf("Height = %d, want 4", out.Height())
	}

	// Expected groups after sort by (region, year): (n,2024)=1+5=6, (n,2025)=2, (s,2024)=3+6=9, (s,2025)=4.
	regCol, _ := out.Column("region")
	yrCol, _ := out.Column("year")
	sumCol, _ := out.Column("v_sum")

	regs := regCol.Chunk(0).(*array.String)
	yrs := yrCol.Chunk(0).(*array.Int64).Int64Values()
	ss := sumCol.Chunk(0).(*array.Int64).Int64Values()

	got := []struct {
		r string
		y int64
		s int64
	}{
		{regs.Value(0), yrs[0], ss[0]},
		{regs.Value(1), yrs[1], ss[1]},
		{regs.Value(2), yrs[2], ss[2]},
		{regs.Value(3), yrs[3], ss[3]},
	}
	want := []struct {
		r string
		y int64
		s int64
	}{
		{"n", 2024, 6},
		{"n", 2025, 2},
		{"s", 2024, 9},
		{"s", 2025, 4},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestGroupByNullKeysFormGroup(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k, _ := series.FromString("k",
		[]string{"a", "", "a", ""},
		[]bool{true, false, true, false},
		series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{1, 2, 3, 4}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(ctx,
		[]expr.Expr{expr.Col("v").Sum().Alias("v_sum")},
		dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatalf("Agg: %v", err)
	}
	defer out.Release()

	// Expect 2 groups: {a: 1+3=4, null: 2+4=6}.
	if out.Height() != 2 {
		t.Errorf("Height = %d, want 2", out.Height())
	}
	keyCol, _ := out.Column("k")
	sumCol, _ := out.Column("v_sum")

	keys := keyCol.Chunk(0).(*array.String)
	sums := sumCol.Chunk(0).(*array.Int64).Int64Values()
	// Nulls-last: "a" (ok), then null.
	if keys.IsValid(0) && keys.Value(0) != "a" {
		t.Errorf("first key = %q valid=%v, want a", keys.Value(0), keys.IsValid(0))
	}
	if keys.IsValid(1) {
		t.Errorf("second key should be null")
	}
	if sums[0] != 4 || sums[1] != 6 {
		t.Errorf("sums = %v, want [4, 6]", sums)
	}
}

// TestGroupByNonAggExprWithoutEvaluator checks the dataframe package on
// its own: without the eval package installed as GenericAgg, a
// non-aggregation expression is rejected. With eval linked in, polars
// semantics apply instead (a list column per group).
func TestGroupByNonAggExprWithoutEvaluator(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k, _ := series.FromInt64("k", []int64{1, 2}, nil, series.WithAllocator(mem))
	v, _ := series.FromInt64("v", []int64{10, 20}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(ctx,
		[]expr.Expr{expr.Col("v").Add(expr.LitInt64(1))},
		dataframe.WithGroupByAllocator(mem))
	if dataframe.GenericAgg == nil {
		if err == nil {
			out.Release()
			t.Error("expected error for non-aggregation expression")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	col, _ := out.Column("v")
	if got := col.DType().String(); got != "list[i64]" {
		t.Errorf("dtype = %s, want list[i64]", got)
	}
}
