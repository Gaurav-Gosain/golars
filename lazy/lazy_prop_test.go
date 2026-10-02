package lazy_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// propFrame has an id column (0..n-1) and four data columns with nulls:
// a i64, b f64 (with NaN and -0.0), s str, g i64 group key.
func propFrame(t *testing.T, mem memory.Allocator, r *rand.Rand, n int) *dataframe.DataFrame {
	t.Helper()
	mk := func(name string, dt arrow.DataType, vals []any) *series.Series {
		s, err := series.FromValues(name, dt, vals, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ids := make([]any, n)
	g := make([]any, n)
	for i := range n {
		ids[i] = int64(i)
		if r.IntN(10) != 0 {
			g[i] = int64(r.IntN(4))
		}
	}
	df, err := dataframe.New(
		mk("id", arrow.PrimitiveTypes.Int64, ids),
		mk("a", arrow.PrimitiveTypes.Int64, testutil.RandValues(r, arrow.PrimitiveTypes.Int64, n, 0.2)),
		mk("b", arrow.PrimitiveTypes.Float64, testutil.RandValues(r, arrow.PrimitiveTypes.Float64, n, 0.2)),
		mk("s", arrow.BinaryTypes.String, testutil.RandValues(r, arrow.BinaryTypes.String, n, 0.2)),
		mk("g", arrow.PrimitiveTypes.Int64, g),
	)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

type planGen struct {
	r    *rand.Rand
	cols []string // columns currently in the frame
	desc []string
}

func (p *planGen) has(c string) bool { return slices.Contains(p.cols, c) }

func (p *planGen) pred() (expr.Expr, bool) {
	var opts []func() expr.Expr
	if p.has("a") {
		opts = append(opts,
			func() expr.Expr { return expr.Col("a").Gt(expr.LitInt64(int64(p.r.IntN(5) - 2))) },
			func() expr.Expr { return expr.Col("a").IsNull() },
			func() expr.Expr { return expr.Col("a").Lt(expr.LitInt64(0)).Not() })
	}
	if p.has("b") {
		opts = append(opts,
			func() expr.Expr { return expr.Col("b").Lt(expr.LitFloat64(0.5)) },
			func() expr.Expr { return expr.Col("b").Eq(expr.LitFloat64(math.NaN())) })
	}
	if p.has("s") {
		opts = append(opts, func() expr.Expr { return expr.Col("s").Eq(expr.LitString("a")) })
	}
	if p.has("id") {
		opts = append(opts, func() expr.Expr { return expr.Col("id").Gt(expr.LitInt64(int64(p.r.IntN(20)))) })
	}
	if len(opts) == 0 {
		return expr.Expr{}, false
	}
	e := opts[p.r.IntN(len(opts))]()
	if p.r.IntN(3) == 0 {
		e = e.And(opts[p.r.IntN(len(opts))]())
	} else if p.r.IntN(4) == 0 {
		e = e.Or(opts[p.r.IntN(len(opts))]())
	}
	return e, true
}

func (p *planGen) step(lf lazy.LazyFrame) (lazy.LazyFrame, bool) {
	switch p.r.IntN(7) {
	case 0, 1:
		if e, ok := p.pred(); ok {
			p.desc = append(p.desc, "filter "+e.String())
			return lf.Filter(e), true
		}
	case 2:
		var es []expr.Expr
		if p.has("a") && p.has("id") {
			es = append(es, expr.Col("a").Add(expr.Col("id")).Alias("a"))
		}
		if p.has("b") {
			es = append(es, expr.Col("b").Mul(expr.LitFloat64(2)).Alias("c"))
			if !p.has("c") {
				p.cols = append(p.cols, "c")
			}
		}
		if p.has("a") && p.r.IntN(2) == 0 {
			es = append(es, expr.Col("a").FillNull(int64(0)).CumSum().Alias("cs"))
			if !p.has("cs") {
				p.cols = append(p.cols, "cs")
			}
		}
		if len(es) > 0 {
			p.desc = append(p.desc, fmt.Sprintf("with_columns %v", es))
			return lf.WithColumns(es...), true
		}
	case 3:
		keep := []string{}
		for _, c := range p.cols {
			if c == "id" || p.r.IntN(3) != 0 {
				keep = append(keep, c)
			}
		}
		if len(keep) > 0 {
			p.cols = keep
			var es []expr.Expr
			for _, c := range keep {
				es = append(es, expr.Col(c))
			}
			p.desc = append(p.desc, fmt.Sprintf("select %v", keep))
			return lf.Select(es...), true
		}
	case 4:
		by := p.cols[p.r.IntN(len(p.cols))]
		keys := []string{by}
		opts := []compute.SortOptions{{Descending: p.r.IntN(2) == 0}}
		if p.r.IntN(2) == 0 {
			opts[0].Nulls = compute.NullsLast
		}
		p.desc = append(p.desc, fmt.Sprintf("sort %v %+v", keys, opts))
		return lf.SortBy(keys, opts), true
	case 5:
		off, n := p.r.IntN(10), p.r.IntN(40)
		p.desc = append(p.desc, fmt.Sprintf("slice(%d, %d)", off, n))
		return lf.Slice(off, n), true
	case 6:
		n := p.r.IntN(30)
		p.desc = append(p.desc, fmt.Sprintf("tail(%d)", n))
		return lf.Tail(n), true
	}
	return lf, false
}

func frameDump(df *dataframe.DataFrame) string {
	var b strings.Builder
	for _, c := range df.Columns() {
		fmt.Fprintf(&b, "%s %s %v\n", c.Name(), c.DType(), c.ToList())
	}
	return b.String()
}

func framesIdentical(a, b *dataframe.DataFrame) bool {
	if a.Height() != b.Height() || a.Width() != b.Width() {
		return false
	}
	for i := range a.Width() {
		ca, cb := a.ColumnAt(i), b.ColumnAt(i)
		if ca.Name() != cb.Name() || !ca.DType().Equal(cb.DType()) {
			return false
		}
		va, vb := ca.ToList(), cb.ToList()
		for j := range va {
			if !testutil.ValuesIdentical(va[j], vb[j]) {
				return false
			}
		}
	}
	return true
}

// propIters is raised locally to hunt for rarer failures.
var propIters = 500

// TestLazyModesAgreeProp runs random filter / with_columns / select /
// sort / slice pipelines three ways (optimized, unoptimized and
// streaming with tiny morsels) and requires identical results. The
// optimizer's pushdowns and the streaming engine must never change the
// answer.
func TestLazyModesAgreeProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(61, 62))
	sizes := []int{0, 1, 2, 15, 16, 17, 63, 64, 65, 200}
	for iter := range propIters {
		n := sizes[r.IntN(len(sizes))]
		df := propFrame(t, mem, r, n)
		p := &planGen{r: r, cols: []string{"id", "a", "b", "s", "g"}}
		lf := lazy.FromDataFrame(df)
		for range 1 + r.IntN(5) {
			lf, _ = p.step(lf)
		}
		if r.IntN(4) == 0 && p.has("g") && p.has("a") {
			aggs := []expr.Expr{expr.Col("a").Sum().Alias("asum"), expr.Col("a").Max().Alias("amax"), expr.Len().Alias("n")}
			if r.IntN(2) == 0 {
				p.desc = append(p.desc, "group_by(g, maintain_order).agg(a.sum, a.max, len)")
				lf = lf.GroupBy("g").MaintainOrder().Agg(aggs...)
			} else {
				// golars sorts groups by key without maintain_order, so
				// the modes still have to agree exactly.
				p.desc = append(p.desc, "group_by(g).agg(a.sum, a.max, len)")
				lf = lf.GroupBy("g").Agg(aggs...)
			}
		}
		desc := fmt.Sprintf("iter %d n=%d plan %s", iter, n, strings.Join(p.desc, " | "))
		want, err := lf.CollectUnoptimized(ctx, lazy.WithExecAllocator(mem))
		if err != nil {
			t.Fatalf("%s: unoptimized: %v", desc, err)
		}
		modes := []struct {
			name string
			run  func() (*dataframe.DataFrame, error)
		}{
			{"optimized", func() (*dataframe.DataFrame, error) { return lf.Collect(ctx, lazy.WithExecAllocator(mem)) }},
			{"streaming", func() (*dataframe.DataFrame, error) {
				return lf.Collect(ctx, lazy.WithExecAllocator(mem), lazy.WithStreaming(), lazy.WithStreamingMorselRows(7))
			}},
		}
		for _, m := range modes {
			got, err := m.run()
			if err != nil {
				t.Fatalf("%s: %s: %v", desc, m.name, err)
			}
			if !framesIdentical(got, want) {
				t.Fatalf("%s: %s differs\n got:\n%s want:\n%s", desc, m.name, frameDump(got), frameDump(want))
			}
			got.Release()
		}
		want.Release()
		df.Release()
	}
}
