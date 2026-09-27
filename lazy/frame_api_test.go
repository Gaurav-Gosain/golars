package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
)

func TestLazySchemaWithoutExecution(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := baseFixture(t, mem)
	defer df.Release()
	lf := lazy.FromDataFrame(df).
		Explode("l").
		Select(expr.Col("k"), expr.Col("l"), expr.Col("a").Cast(dtype.Float64()).Alias("af")).
		GroupBy("k").MaintainOrder().Sum()

	cols, err := lf.Columns()
	if err != nil {
		t.Fatal(err)
	}
	testutil.AssertStrings(t, cols, []string{"k", "l", "af"})
	dts, err := lf.DTypes()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(dts))
	for i, d := range dts {
		got[i] = d.String()
	}
	testutil.AssertStrings(t, got, []string{"str", "i64", "f64"})
	if w, err := lf.Width(); err != nil || w != 3 {
		t.Fatalf("Width = %d, %v", w, err)
	}
	sch, err := lf.CollectSchema()
	if err != nil {
		t.Fatal(err)
	}
	out, err := lf.Collect(context.Background(), lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if !sch.Equal(out.Schema()) {
		t.Fatalf("CollectSchema %s != collected %s", sch, out.Schema())
	}
}

func TestLazyInspectPipeCloneMapBatches(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := lxFrame(t, lxCol(t, mem, "a", lxI64, 1, 2, 3))
	defer df.Release()
	lf := lazy.FromDataFrame(df)

	seen := 0
	inspected := lf.Inspect(func(d *dataframe.DataFrame) { seen = d.Height() }).
		Filter(expr.Col("a").Gt(expr.LitInt64(1)))
	out, err := inspected.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
	if seen != 3 {
		t.Fatalf("Inspect saw %d rows, want 3 (filter must stay above)", seen)
	}

	piped := lf.Pipe(func(x lazy.LazyFrame) lazy.LazyFrame { return x.Head(2) })
	clone := piped.Clone()
	out, err = clone.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	framerepr.AssertFrame(t, out, "a:i64\n1\n2")
	out.Release()

	doubled, _ := schema.New(schema.Field{Name: "b", DType: dtype.Int64()})
	mapped := lf.MapBatches(func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		c, _ := d.Column("a")
		r := c.Rename("b")
		return dataframe.New(r)
	}, lazy.MapBatchesOptions{Schema: doubled})
	if sch, _ := mapped.CollectSchema(); !sch.Equal(doubled) {
		t.Fatalf("MapBatches schema = %s", sch)
	}
	inferred := lf.MapBatches(func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return d.WithRowIndex("i", 0)
	}, lazy.MapBatchesOptions{InferSchema: true})
	if cols, _ := inferred.Columns(); len(cols) != 2 || cols[0] != "i" {
		t.Fatalf("inferred MapBatches columns = %v", cols)
	}
	out, err = mapped.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	framerepr.AssertFrame(t, out, "b:i64\n1\n2\n3")
	out.Release()
}

func TestLazyProfileBatchesAsync(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := lxFrame(t, lxCol(t, mem, "a", lxI64, 1, 2, 3, 4, 5))
	defer df.Release()
	lf := lazy.FromDataFrame(df).Filter(expr.Col("a").Gt(expr.LitInt64(1))).Shift(1, nil)

	out, timings, err := lf.Profile(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	defer timings.Release()
	if got := timings.Schema().String(); !strings.Contains(got, "node") || timings.Width() != 3 {
		t.Fatalf("timings schema = %s", got)
	}
	nodeCol, _ := timings.Column("node")
	if nodeCol.DType().String() != "str" || timings.Height() < 2 {
		t.Fatalf("timings = %s", framerepr.Frame(timings))
	}
	if first, _ := timings.ItemAt(0, "node"); first != "optimization" {
		t.Fatalf("first timing row = %v", first)
	}
	framerepr.AssertFrame(t, out, "a:i64\nnull\n2\n3\n4")

	var heights []int
	for batch, err := range lf.CollectBatches(ctx, 3, lazy.WithExecAllocator(mem)) {
		if err != nil {
			t.Fatal(err)
		}
		heights = append(heights, batch.Height())
		batch.Release()
	}
	testutil.AssertEqualAny(t, heights, []int{3, 1})

	res := <-lf.CollectAsync(ctx, lazy.WithExecAllocator(mem))
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	framerepr.AssertFrame(t, res.DataFrame, "a:i64\nnull\n2\n3\n4")
	res.DataFrame.Release()

	desc, err := lazy.FromDataFrame(df).Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if desc.Width() != 2 {
		t.Fatalf("Describe width = %d", desc.Width())
	}
	desc.Release()

	explain := lf.ExplainString()
	if !strings.Contains(explain, "SHIFT n=1") {
		t.Fatalf("explain misses the frame op:\n%s", explain)
	}
}

func TestLazyMatchToSchemaAndCasts(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	df := lxFrame(t, lxCol(t, mem, "a", lxI64, 1, 2), lxCol(t, mem, "s", lxStr, "x", "y"))
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	target, _ := schema.New(
		schema.Field{Name: "s", DType: dtype.String()},
		schema.Field{Name: "a", DType: dtype.Int64()},
		schema.Field{Name: "z", DType: dtype.Float64()},
	)
	m := lf.MatchToSchema(target, dataframe.MatchToSchemaOptions{InsertMissing: true})
	out, err := m.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	framerepr.AssertFrame(t, out, "s:str|a:i64|z:f64\n\"x\"|1|null\n\"y\"|2|null")
	out.Release()

	all := lf.Select(expr.Col("a")).CastAll(dtype.Float64(), true)
	out, err = all.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	framerepr.AssertFrame(t, out, "a:f64\n1.0\n2.0")
	out.Release()

	byType := lf.CastDTypes([]dataframe.DTypeCast{{From: dtype.Int64(), To: dtype.String()}}, true)
	if sch, _ := byType.CollectSchema(); sch.Field(0).DType.String() != "str" {
		t.Fatalf("CastDTypes schema = %s", sch)
	}
}
