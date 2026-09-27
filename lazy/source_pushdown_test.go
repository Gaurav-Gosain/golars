package lazy_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/schema"
)

// recordingSource wraps df in a projected source and records the column
// lists the executor asks for.
type recordingSource struct {
	mu    sync.Mutex
	calls [][]string
}

func (r *recordingSource) frame(df *dataframe.DataFrame, known *schema.Schema) lazy.LazyFrame {
	return lazy.FromSourceProjected("test", known, func(_ context.Context, cols []string) (*dataframe.DataFrame, error) {
		r.mu.Lock()
		r.calls = append(r.calls, slices.Clone(cols))
		r.mu.Unlock()
		if cols == nil {
			return df.Clone(), nil
		}
		return df.Select(cols...)
	})
}

func (r *recordingSource) last() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[len(r.calls)-1]
}

func TestSourceProjectionPushdown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := buildDF(t, mem)
	defer df.Release()

	cases := []struct {
		name  string
		known bool
		build func(lazy.LazyFrame) lazy.LazyFrame
		want  []string // nil: every column
		h, w  int      // h < 0 skips the height check
	}{
		{
			name: "filter then select",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame {
				return lf.Filter(expr.Col("a").GtLit(int64(5))).Select(expr.Col("b"))
			},
			want: []string{"a", "b"}, h: 5, w: 1,
		},
		{
			name:  "known schema keeps schema order",
			known: true,
			build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Select(expr.Col("c"), expr.Col("a")) },
			want:  []string{"a", "c"}, h: 10, w: 2,
		},
		{
			name: "aggregate under sort (root is not a projection)",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame {
				return lf.GroupBy("c").Agg(expr.Col("a").Sum().Alias("s")).
					SortBy([]string{"c"}, []compute.SortOptions{{}})
			},
			want: []string{"a", "c"}, h: 2, w: 2,
		},
		{
			name: "aggregate with computed input",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame {
				return lf.GroupBy("c").Agg(expr.Col("a").Mul(expr.Col("b")).Sum().Alias("s"))
			},
			want: []string{"a", "b", "c"}, h: 2, w: 2,
		},
		{
			name:  "with_columns at root reads everything",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.WithColumns(expr.Col("a").Alias("z")) },
			want:  nil, h: 10, w: 4,
		},
		{
			name:  "literal-only select keeps the height",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Select(expr.LitInt64(1).Alias("one")) },
			want:  nil, h: -1, w: 1,
		},
		{
			name: "select above with_columns drops unused inputs",
			build: func(lf lazy.LazyFrame) lazy.LazyFrame {
				return lf.WithColumns(expr.Col("a").Add(expr.LitInt64(1)).Alias("a1")).Select(expr.Col("a1"))
			},
			want: []string{"a"}, h: 10, w: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingSource{}
			var known *schema.Schema
			if tc.known {
				known = df.Schema()
			}
			out, err := tc.build(rec.frame(df, known)).Collect(ctx, lazy.WithExecAllocator(mem))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if got := rec.last(); !slices.Equal(got, tc.want) {
				t.Errorf("source read %v, want %v", got, tc.want)
			}
			if (tc.h >= 0 && out.Height() != tc.h) || out.Width() != tc.w {
				t.Errorf("shape = (%d,%d), want (%d,%d)", out.Height(), out.Width(), tc.h, tc.w)
			}
		})
	}
}

// TestSourceWithoutLoadColumnsStillProjects: a plain FromSource loader
// has no column-subset entry point, so the executor selects after load.
func TestSourceWithoutLoadColumnsStillProjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := buildDF(t, mem)
	defer df.Release()
	lf := lazy.FromSource("plain", nil, func(context.Context) (*dataframe.DataFrame, error) { return df.Clone(), nil })
	q := lf.Select(expr.Col("b").Sum().Alias("s"))
	plan, err := q.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "projection=[b]") {
		t.Errorf("optimized plan lacks the source projection:\n%s", plan)
	}
	out, err := q.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 1 || out.Width() != 1 {
		t.Errorf("shape = (%d,%d)", out.Height(), out.Width())
	}
}

// TestSourceOutputIsRechunked: multi-chunk source output (one chunk per
// row group from a file reader) is consolidated once at the scan.
func TestSourceOutputIsRechunked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := buildDF(t, mem)
	defer df.Release()
	lf := lazy.FromSource("chunks", nil, func(context.Context) (*dataframe.DataFrame, error) {
		return dataframe.Concat(df, df, df)
	})
	out, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 30 || out.Width() != 3 {
		t.Fatalf("shape = (%d,%d)", out.Height(), out.Width())
	}
	for _, c := range out.Columns() {
		if c.NumChunks() != 1 {
			t.Errorf("column %s has %d chunks", c.Name(), c.NumChunks())
		}
	}
	want, _ := dataframe.Concat(df, df, df)
	defer want.Release()
	if !out.Equals(want) {
		t.Errorf("rechunked frame differs from its input")
	}
}

// TestJoinChildrenStillPrune: a join is a pushdown barrier for its own
// output, but projections below it keep working.
func TestJoinChildrenStillPrune(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := buildDF(t, mem)
	defer df.Release()
	rec := &recordingSource{}
	left := rec.frame(df, nil).Select(expr.Col("a"), expr.Col("b"))
	right := lazy.FromDataFrame(df).Select(expr.Col("a"), expr.Col("c"))
	out, err := left.Join(right, []string{"a"}, dataframe.InnerJoin).Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := rec.last(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("left source read %v, want [a b]", got)
	}
	if out.Height() != 10 || out.Width() != 3 {
		t.Errorf("shape = (%d,%d), want (10,3)", out.Height(), out.Width())
	}
}
