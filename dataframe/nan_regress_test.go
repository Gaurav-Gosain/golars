package dataframe_test

import (
	"context"
	"math"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func nanFrame(t *testing.T, cols ...*series.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func colRepr(t *testing.T, df *dataframe.DataFrame, name string) string {
	t.Helper()
	c, err := df.Column(name)
	if err != nil {
		t.Fatalf("column %q: %v (schema %v)", name, err, df.Schema())
	}
	return testutil.PyRepr(c.Chunk(0))
}

// polars 1.39:
//
//	df = pl.DataFrame({"g": [1, 1, 1, 2, 2, 3], "x": [1.0, nan, 3.0, nan, nan, None]})
//	df.group_by("g", maintain_order=True).agg(max, min) -> mx [3.0, nan, None], mn [1.0, nan, None]
func TestGroupByMinMaxNaN(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	nan := math.NaN()
	opt := series.WithAllocator(mem)
	g, _ := series.FromInt64("g", []int64{1, 1, 1, 2, 2, 3}, nil, opt)
	x, _ := series.FromFloat64("x", []float64{1, nan, 3, nan, nan, 0}, []bool{true, true, true, true, true, false}, opt)
	df := nanFrame(t, g, x)
	defer df.Release()
	X := expr.Col("x")
	// max+min alone takes the fused scan; adding first forces the
	// per-spec hash path.
	for name, aggs := range map[string][]expr.Expr{
		"fused": {X.Max().Alias("mx"), X.Min().Alias("mn")},
		"hash":  {X.Max().Alias("mx"), X.Min().Alias("mn"), X.First().Alias("f")},
	} {
		out, err := df.GroupBy("g").Agg(context.Background(), aggs, dataframe.WithGroupByAllocator(mem))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		sorted, err := out.Sort(context.Background(), "g", false)
		out.Release()
		if err != nil {
			t.Fatal(err)
		}
		if got := colRepr(t, sorted, "mx"); got != "[3.0, nan, None]" {
			t.Errorf("%s mx = %s", name, got)
		}
		if got := colRepr(t, sorted, "mn"); got != "[1.0, nan, None]" {
			t.Errorf("%s mn = %s", name, got)
		}
		sorted.Release()
	}
}

// Float keys take the sort-based group path, which used to name the
// first/last output after the source column instead of the alias.
func TestGroupByFloatKeyFirstKeepsAlias(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	k, _ := series.FromFloat64("k", []float64{1, 1, 2}, nil, opt)
	v, _ := series.FromInt64("v", []int64{1, 2, 3}, nil, opt)
	df := nanFrame(t, k, v)
	defer df.Release()
	out, err := df.GroupBy("k").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").First().Alias("first_v"), expr.Col("v").Last().Alias("last_v")},
		dataframe.WithGroupByAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if !out.Contains("first_v") || !out.Contains("last_v") {
		t.Errorf("schema = %v, want first_v and last_v", out.Schema())
	}
}

// polars joins NaN keys with NaN keys and -0.0 with 0.0:
// [1.0, nan, -0.0] inner-join [nan, 1.0, 0.0] matches all three rows.
func TestJoinNaNKeys(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	nan := math.NaN()
	opt := series.WithAllocator(mem)
	lk, _ := series.FromFloat64("k", []float64{1, nan, math.Copysign(0, -1)}, nil, opt)
	la, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, opt)
	rk, _ := series.FromFloat64("k", []float64{nan, 1, 0}, nil, opt)
	rb, _ := series.FromInt64("b", []int64{10, 30, 40}, nil, opt)
	l := nanFrame(t, lk, la)
	defer l.Release()
	r := nanFrame(t, rk, rb)
	defer r.Release()
	for _, how := range []dataframe.JoinType{dataframe.InnerJoin, dataframe.LeftJoin} {
		out, err := l.Join(context.Background(), r, []string{"k"}, how, dataframe.WithJoinAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if got := colRepr(t, out, "b"); got != "[30, 10, 40]" {
			t.Errorf("%s join b = %s, want [30, 10, 40]", how, got)
		}
		out.Release()
	}
}
