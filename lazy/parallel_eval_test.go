package lazy_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// intFrame builds columns a, b, c of height n with a sprinkling of
// nulls in b. chunks > 1 splits the frame into that many vertical
// pieces so kernels see multi-chunk input.
func intFrame(t *testing.T, mem memory.Allocator, n, chunks int) *dataframe.DataFrame {
	t.Helper()
	build := func(lo, hi int) *dataframe.DataFrame {
		a := make([]int64, hi-lo)
		b := make([]int64, hi-lo)
		c := make([]int64, hi-lo)
		bv := make([]bool, hi-lo)
		for i := range a {
			r := lo + i
			a[i] = int64(r)
			b[i] = int64(r%97 - 40)
			c[i] = int64(r * 3 % 1001)
			bv[i] = r%13 != 0
		}
		sa, _ := series.FromInt64("a", a, nil, series.WithAllocator(mem))
		sb, _ := series.FromInt64("b", b, bv, series.WithAllocator(mem))
		sc, _ := series.FromInt64("c", c, nil, series.WithAllocator(mem))
		df, err := dataframe.New(sa, sb, sc)
		if err != nil {
			t.Fatal(err)
		}
		return df
	}
	if chunks <= 1 || n < chunks {
		return build(0, n)
	}
	parts := make([]*dataframe.DataFrame, chunks)
	step := n / chunks
	for i := range parts {
		hi := (i + 1) * step
		if i == chunks-1 {
			hi = n
		}
		parts[i] = build(i*step, hi)
	}
	out, err := dataframe.Concat(parts...)
	for _, p := range parts {
		p.Release()
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sixExprs() []expr.Expr {
	a, b, c := expr.Col("a"), expr.Col("b"), expr.Col("c")
	return []expr.Expr{
		a.Add(b).Alias("s"),
		a.Mul(c).Alias("m"),
		b.Sub(c).Alias("d"),
		a.Gt(c).Alias("g"),
		b.IsNull().Alias("n"),
		a.Mul(b).Add(c).Alias("f"),
	}
}

// TestParallelEvalMatchesSerial checks the fan-out path against one
// single-expression select per output (always serial), across the row
// cutoff and for multi-chunk input.
func TestParallelEvalMatchesSerial(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, n := range []int{0, 1, 8191, 8192, 20000} {
		for _, chunks := range []int{1, 3} {
			t.Run(fmt.Sprintf("n=%d,chunks=%d", n, chunks), func(t *testing.T) {
				mem := testutil.NewCheckedAllocator(t)
				df := intFrame(t, mem, n, chunks)
				defer df.Release()
				exprs := sixExprs()

				sel, err := lazy.FromDataFrame(df).Select(exprs...).Collect(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				defer sel.Release()
				wc, err := lazy.FromDataFrame(df).WithColumns(exprs...).Collect(ctx, lazy.WithExecAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				defer wc.Release()

				if got, want := wc.Width(), df.Width()+len(exprs); got != want {
					t.Fatalf("with_columns width = %d, want %d", got, want)
				}
				for i, e := range exprs {
					one, err := lazy.FromDataFrame(df).Select(e).Collect(ctx, lazy.WithExecAllocator(mem))
					if err != nil {
						t.Fatal(err)
					}
					want := one.ColumnAt(0)
					if got := sel.ColumnAt(i); !got.Equal(want) {
						t.Errorf("select col %d (%s) differs from serial", i, e)
					}
					if got := wc.ColumnAt(df.Width() + i); !got.Equal(want) {
						t.Errorf("with_columns col %d (%s) differs from serial", i, e)
					}
					one.Release()
				}
			})
		}
	}
}

// TestWithColumnsReadsInputNotSiblings pins polars semantics: every
// expression in one with_columns call sees the input frame, even when
// an earlier expression in the same call overwrites a column it reads.
func TestWithColumnsReadsInputNotSiblings(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, n := range []int{5, 20000} {
		mem := testutil.NewCheckedAllocator(t)
		df := intFrame(t, mem, n, 1)
		out, err := lazy.FromDataFrame(df).
			WithColumns(
				expr.Col("a").Mul(expr.LitInt64(100)).Alias("a"),
				expr.Col("a").Add(expr.LitInt64(1)).Alias("a1"),
			).
			Collect(ctx, lazy.WithExecAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if out.Width() != 4 {
			t.Fatalf("n=%d: width = %d, want 4 (a replaced in place, a1 appended)", n, out.Width())
		}
		if names := out.Schema().Names(); names[0] != "a" || names[3] != "a1" {
			t.Fatalf("n=%d: column order = %v", n, names)
		}
		want, _ := lazy.FromDataFrame(df).Select(expr.Col("a").Add(expr.LitInt64(1)).Alias("a1")).Collect(ctx, lazy.WithExecAllocator(mem))
		if !out.ColumnAt(3).Equal(want.ColumnAt(0)) {
			t.Errorf("n=%d: a1 did not read the input column a", n)
		}
		want.Release()
		out.Release()
		df.Release()
	}
}

// TestWithColumnsDuplicateOutputLastWins keeps the in-order apply rule
// for repeated output names.
func TestWithColumnsDuplicateOutputLastWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := intFrame(t, mem, 20000, 1)
	defer df.Release()
	out, err := lazy.FromDataFrame(df).
		WithColumns(
			expr.Col("a").Add(expr.LitInt64(1)).Alias("x"),
			expr.Col("c").Alias("x"),
		).
		Collect(ctx, lazy.WithExecAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	x, _ := out.Column("x")
	c, _ := out.Column("c")
	xc := x.Rename("c")
	defer xc.Release()
	if out.Width() != 4 || !xc.Equal(c) {
		t.Errorf("width=%d, x equals c: %v", out.Width(), xc.Equal(c))
	}
}

// TestParallelEvalErrorIsDeterministic: with several failing
// expressions the error names the first one in list order, whatever
// the goroutine schedule, and no buffers leak.
func TestParallelEvalErrorIsDeterministic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	df := intFrame(t, mem, 20000, 2)
	defer df.Release()
	exprs := []expr.Expr{
		expr.Col("a").Add(expr.Col("b")).Alias("ok0"),
		expr.Col("missing_first").Alias("bad1"),
		expr.Col("a").Mul(expr.Col("c")).Alias("ok2"),
		expr.Col("missing_second").Alias("bad3"),
	}
	for range 20 {
		for _, build := range []func(lazy.LazyFrame) lazy.LazyFrame{
			func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.Select(exprs...) },
			func(lf lazy.LazyFrame) lazy.LazyFrame { return lf.WithColumns(exprs...) },
		} {
			_, err := build(lazy.FromDataFrame(df)).Collect(ctx, lazy.WithExecAllocator(mem))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), "missing_first") {
				t.Fatalf("error %q does not name the first failing expression", err)
			}
		}
	}
}

func TestParallelEvalCancelled(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	df := intFrame(t, mem, 20000, 1)
	defer df.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := lazy.FromDataFrame(df).WithColumns(sixExprs()...).Collect(ctx, lazy.WithExecAllocator(mem))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
