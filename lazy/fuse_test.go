package lazy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestFuseWithColumns(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	b, _ := series.FromInt64("b", []int64{10, 20, 30}, nil, series.WithAllocator(mem))
	df, _ := dataframe.New(a, b)
	defer df.Release()
	A, B := expr.Col("a"), expr.Col("b")
	cases := []struct {
		name  string
		lf    lazy.LazyFrame
		fused bool
	}{
		{"independent", lazy.FromDataFrame(df).WithColumns(A.Mul(expr.LitInt64(2)).Alias("a2")).
			WithColumns(B.Add(expr.LitInt64(1)).Alias("b1")), true},
		{"reads new column", lazy.FromDataFrame(df).WithColumns(A.Mul(expr.LitInt64(2)).Alias("a2")).
			WithColumns(expr.Col("a2").Add(B).Alias("c")), false},
		{"reads overwritten column", lazy.FromDataFrame(df).WithColumns(A.Mul(expr.LitInt64(2)).Alias("a")).
			WithColumns(A.Add(expr.LitInt64(1)).Alias("c")), false},
		{"later output wins", lazy.FromDataFrame(df).WithColumns(A.Mul(expr.LitInt64(2)).Alias("x")).
			WithColumns(B.Mul(expr.LitInt64(3)).Alias("x")), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.lf.ExplainString()
			opt := plan[strings.Index(plan, "Optimized plan"):]
			if got := strings.Count(opt, "WITH_COLUMNS") == 1; got != tc.fused {
				t.Fatalf("fused = %v, want %v\n%s", got, tc.fused, opt)
			}
			got, err := tc.lf.Collect(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer got.Release()
			want, err := tc.lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()
			if got.String() != want.String() {
				t.Fatalf("got\n%s\nwant\n%s", got, want)
			}
		})
	}
}
