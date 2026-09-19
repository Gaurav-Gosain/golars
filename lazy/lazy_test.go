package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func buildDF(t *testing.T, mem interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, nil, series.WithAllocator(mem))
	c, _ := series.FromString("c", []string{"a", "b", "a", "b", "a", "b", "a", "b", "a", "b"}, nil, series.WithAllocator(mem))
	df, err := dataframe.New(a, b, c)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return df
}

func TestSelectAndFilter(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(5))).
		Select(expr.Col("a"), expr.Col("b")).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 5 {
		t.Errorf("Height = %d, want 5", out.Height())
	}
	if out.Width() != 2 {
		t.Errorf("Width = %d, want 2", out.Width())
	}
	aCol, _ := out.Column("a")
	vals := aCol.Chunk(0).(*array.Int64).Int64Values()
	for i, want := range []int64{6, 7, 8, 9, 10} {
		if vals[i] != want {
			t.Errorf("a[%d] = %d, want %d", i, vals[i], want)
		}
	}
}

func TestSelectParallelErrorReleases(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	// Eight expressions force the parallel projection path; the
	// missing column fails it. The checked allocator fails the test
	// if any completed column leaks.
	exprs := []expr.Expr{
		expr.Col("a"), expr.Col("b"), expr.Col("c"),
		expr.Col("a"), expr.Col("b"), expr.Col("c"),
		expr.Col("a"), expr.Col("nope"),
	}
	if _, err := lazy.FromDataFrame(df).Select(exprs...).Collect(ctx, lazy.WithExecAllocator(mem)); err == nil {
		t.Fatal("expected projection error for missing column")
	}
}

func TestWithColumnsAndSort(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	// (a * b) alias "x", sort by x desc, limit 3.
	out, err := lazy.FromDataFrame(df).
		WithColumns(expr.Col("a").Mul(expr.Col("b")).Alias("x")).
		Sort("x", true).
		Head(3).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if out.Height() != 3 {
		t.Errorf("Height = %d, want 3", out.Height())
	}
	x, _ := out.Column("x")
	vals := x.Chunk(0).(*array.Int64).Int64Values()
	// a*b for a=1..10 gives [10, 40, 90, 160, 250, 360, 490, 640, 810, 1000].
	// desc sorted top 3: 1000, 810, 640.
	want := []int64{1000, 810, 640}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("x[%d] = %d, want %d", i, vals[i], want[i])
		}
	}
}

func TestExplainSections(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(5))).
		Select(expr.Col("a"), expr.Col("b"))

	out, err := lf.Explain()
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	for _, section := range []string{"Logical plan", "Optimizer", "Optimized plan"} {
		if !strings.Contains(out, section) {
			t.Errorf("explain missing section %q in:\n%s", section, out)
		}
	}
}

func TestPredicatePushdownIntoScan(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	// Filter on top of Scan should push into Scan.
	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(5))).
		Select(expr.Col("a"))

	opt := lazy.DefaultOptimizer()
	optimized, _, err := opt.Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	explain := lazy.Explain(optimized)
	if !strings.Contains(explain, "predicate=") {
		t.Errorf("expected predicate pushed into scan:\n%s", explain)
	}
}

func TestProjectionPushdownIntoScan(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	lf := lazy.FromDataFrame(df).Select(expr.Col("a"))
	optimized, _, err := lazy.DefaultOptimizer().Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	explain := lazy.Explain(optimized)
	if !strings.Contains(explain, "projection=[a]") {
		t.Errorf("expected projection=[a] in scan:\n%s", explain)
	}
}

func TestSlicePushdownIntoScan(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		Select(expr.Col("a"), expr.Col("b")).
		Head(3)
	optimized, _, err := lazy.DefaultOptimizer().Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	explain := lazy.Explain(optimized)
	if !strings.Contains(explain, "slice=(0,3)") {
		t.Errorf("expected slice=(0,3) pushed into scan:\n%s", explain)
	}
}

func TestSimplifyConstantFolding(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	// Lit(2) + Lit(3) should fold to Lit(5).
	e := expr.LitInt64(2).Add(expr.LitInt64(3)).Alias("x")
	lf := lazy.FromDataFrame(df).Select(e)
	optimized, _, err := lazy.DefaultOptimizer().Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	explain := lazy.Explain(optimized)
	if strings.Contains(explain, "(2 + 3)") {
		t.Errorf("expected 2+3 folded; got:\n%s", explain)
	}
	if !strings.Contains(explain, "5") {
		t.Errorf("expected folded 5 in plan; got:\n%s", explain)
	}

	// And confirm execution gives the right answer: all 10 rows = 5.
	out, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()
	x, _ := out.Column("x")
	vals := x.Chunk(0).(*array.Int64).Int64Values()
	for i, v := range vals {
		if v != 5 {
			t.Errorf("x[%d] = %d, want 5", i, v)
		}
	}
}

func TestSimplifyBooleanIdentities(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	// (a > 0) AND true  -> (a > 0)
	e := expr.Col("a").GtLit(int64(0)).And(expr.LitBool(true))
	lf := lazy.FromDataFrame(df).Filter(e).Select(expr.Col("a"))
	optimized, _, err := lazy.DefaultOptimizer().Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	explain := lazy.Explain(optimized)
	if strings.Contains(explain, "and true") {
		t.Errorf("AND true should be simplified away:\n%s", explain)
	}
}

func TestPassLogOrder(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)

	df := buildDF(t, mem)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		Filter(expr.Col("a").GtLit(int64(0))).
		Select(expr.Col("a"))

	_, traces, err := lazy.DefaultOptimizer().Optimize(lf.Plan())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	want := []string{
		"simplify/fix",
		"type-coerce/fix",
		"cse",
		"slice-pushdown/fix",
		"predicate-pushdown/fix",
		"projection-pushdown",
	}
	if len(traces) != len(want) {
		t.Fatalf("trace count = %d, want %d", len(traces), len(want))
	}
	for i, name := range want {
		if traces[i].Name != name {
			t.Errorf("trace[%d] = %q, want %q", i, traces[i].Name, name)
		}
	}
}

func TestOptimizedAndUnoptimizedAgree(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	lf := lazy.FromDataFrame(df).
		WithColumns(expr.Col("a").Add(expr.Col("b")).Alias("sum")).
		Filter(expr.Col("sum").GtLit(int64(50))).
		Select(expr.Col("a"), expr.Col("sum")).
		Sort("sum", false).
		Head(5)

	optRes, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("optimized Collect: %v", err)
	}
	defer optRes.Release()

	unopt, err := lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("unoptimized Collect: %v", err)
	}
	defer unopt.Release()

	if optRes.Height() != unopt.Height() {
		t.Errorf("heights differ: opt=%d unopt=%d", optRes.Height(), unopt.Height())
	}
	a1, _ := optRes.Column("a")
	a2, _ := unopt.Column("a")
	v1 := a1.Chunk(0).(*array.Int64).Int64Values()
	v2 := a2.Chunk(0).(*array.Int64).Int64Values()
	if len(v1) != len(v2) {
		t.Fatalf("a column lengths differ: %d vs %d", len(v1), len(v2))
	}
	for i := range v1 {
		if v1[i] != v2[i] {
			t.Errorf("a[%d] opt=%d unopt=%d", i, v1[i], v2[i])
		}
	}
}

func TestRenameAndDrop(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	df := buildDF(t, mem)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		Rename("a", "alpha").
		Drop("c").
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	if !out.Contains("alpha") {
		t.Error("output missing alpha")
	}
	if out.Contains("c") {
		t.Error("output still has c")
	}
}

func TestSortByMultipleKeys(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()

	k1, _ := series.FromInt64("k1", []int64{2, 1, 2, 1}, nil, series.WithAllocator(mem))
	k2, _ := series.FromString("k2", []string{"b", "c", "a", "d"}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(k1, k2)
	defer df.Release()

	out, err := lazy.FromDataFrame(df).
		SortBy([]string{"k1", "k2"}, []compute.SortOptions{{}, {}}).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	defer out.Release()

	k1c, _ := out.Column("k1")
	vs := k1c.Chunk(0).(*array.Int64).Int64Values()
	if len(vs) != 4 || vs[0] != 1 || vs[1] != 1 || vs[2] != 2 || vs[3] != 2 {
		t.Errorf("k1 sorted = %v, want [1,1,2,2]", vs)
	}
}
