package lazy_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func pushdownFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4, 5}, nil)
	b, _ := series.FromInt64("b", []int64{10, 20, 30, 40, 50}, nil)
	df, err := dataframe.New(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// Filters and slices must not move below nodes whose expressions read
// other rows, and a filter must not move below a slice. Expected values
// come from polars 1.39.
func TestPushdownRespectsRowDependence(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := pushdownFrame(t)
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	a := expr.Col("a")
	cases := []struct {
		name string
		plan lazy.LazyFrame
		col  string
		want []any
	}{
		{"head then filter", lf.Head(2).Filter(a.GtLit(int64(1))), "a", []any{int64(2)}},
		{"cum_sum then filter", lf.WithColumns(a.CumSum().Alias("cs")).Filter(a.GtLit(int64(3))), "cs", []any{int64(10), int64(15)}},
		{"shift then filter", lf.WithColumns(a.Shift(1).Alias("s")).Filter(a.GtLit(int64(3))), "s", []any{int64(3), int64(4)}},
		{"select sum then filter", lf.Select(a, a.Sum().Alias("tot")).Filter(a.GtLit(int64(3))), "tot", []any{int64(15), int64(15)}},
		{"cum_sum then slice", lf.WithColumns(a.CumSum().Alias("cs")).Slice(3, 2), "cs", []any{int64(10), int64(15)}},
		{"filter then agg filter", lf.Filter(a.GtLit(int64(1))).Filter(a.Gt(a.Mean())), "a", []any{int64(4), int64(5)}},
		{"elementwise with_columns", lf.WithColumns(a.MulLit(int64(2)).Alias("d")).Filter(a.GtLit(int64(3))), "d", []any{int64(8), int64(10)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := c.plan.Collect(context.Background(), lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			m, err := out.ToMap()
			if err != nil {
				t.Fatal(err)
			}
			if got := m[c.col]; !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s = %v, want %v", c.col, got, c.want)
			}
		})
	}
}

// Elementwise projections still let the filter reach the scan.
func TestPushdownThroughElementwise(t *testing.T) {
	df := pushdownFrame(t)
	defer df.Release()
	a := expr.Col("a")
	plan := lazy.FromDataFrame(df).
		WithColumns(a.MulLit(int64(2)).Alias("d"), expr.Col("b").Abs().Alias("e")).
		Filter(a.GtLit(int64(3))).Plan()
	opt, _, err := lazy.DefaultOptimizer().Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	w, ok := opt.(lazy.WithColumns)
	if !ok {
		t.Fatalf("root = %T, want WithColumns", opt)
	}
	if _, ok := w.Input.(lazy.DataFrameScan); !ok {
		t.Fatalf("filter not pushed into the scan: input = %T", w.Input)
	}

	plan = lazy.FromDataFrame(df).WithColumns(a.CumSum().Alias("cs")).Filter(a.GtLit(int64(3))).Plan()
	opt, _, err = lazy.DefaultOptimizer().Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := opt.(lazy.Filter); !ok {
		t.Fatalf("root = %T, want the filter to stay above cum_sum", opt)
	}
}

// polars clamps lazy slices: slice(-2, 5) keeps the last two rows and a
// slice past the end is empty.
func TestLazySliceClamps(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := pushdownFrame(t)
	defer df.Release()
	lf := lazy.FromDataFrame(df)
	a := expr.Col("a")
	cases := []struct {
		name string
		plan lazy.LazyFrame
		want []any
	}{
		{"slice(-2, 5)", lf.Slice(-2, 5), []any{int64(4), int64(5)}},
		{"slice(10, 2)", lf.Slice(10, 2), []any{}},
		{"filter.slice(3, 2)", lf.Filter(a.GtLit(int64(3))).Slice(3, 2), []any{}},
		{"col.slice(-2, 5)", lf.Select(a.Slice(-2, 5)), []any{int64(4), int64(5)}},
	}
	for _, c := range cases {
		out, err := c.plan.Collect(context.Background(), lazy.WithExecAllocator(mem))
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		m, _ := out.ToMap()
		got := m["a"]
		if got == nil {
			got = []any{}
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
		out.Release()
	}
}

// polars broadcasts scalar results: select(a, a.sum()) and
// with_columns(a.sum()) repeat the sum on every row.
func TestScalarBroadcast(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	df := pushdownFrame(t)
	defer df.Release()
	a := expr.Col("a")
	for name, plan := range map[string]lazy.LazyFrame{
		"select":       lazy.FromDataFrame(df).Select(a, a.Sum().Alias("tot")),
		"with_columns": lazy.FromDataFrame(df).WithColumns(a.Sum().Alias("tot")),
	} {
		out, err := plan.Collect(context.Background(), lazy.WithExecAllocator(mem))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		m, _ := out.ToMap()
		want := []any{int64(15), int64(15), int64(15), int64(15), int64(15)}
		if !reflect.DeepEqual(m["tot"], want) {
			t.Errorf("%s tot = %v, want %v", name, m["tot"], want)
		}
		out.Release()
	}
	// All-scalar selects stay one row.
	out, err := lazy.FromDataFrame(df).Select(a.Sum().Alias("s"), a.Mean().Alias("m")).Collect(context.Background(), lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 1 {
		t.Errorf("height = %d, want 1", out.Height())
	}
}
