// Parity tests for DataFrame.JoinWhere against polars 1.39.3. Polars
// does not define a row order for join_where, so both the recorded
// polars output and golars' output are compared with data rows sorted.

package dataframe_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func jwFrames(t *testing.T, mem memory.Allocator) (*dataframe.DataFrame, *dataframe.DataFrame) {
	east := jnFrame(t,
		jnCol(t, mem, "id", 100, 101, 102),
		jnCol(t, mem, "dur", 120, 140, 160),
		jnCol(t, mem, "rev", 12, 8, 7),
		jnCol(t, mem, "cores", 2, 8, 4))
	west := jnFrame(t,
		jnCol(t, mem, "t_id", 404, 498, 676, 742),
		jnCol(t, mem, "time", 90, 130, 150, 170),
		jnCol(t, mem, "cost", 9, 13, 15, 16),
		jnCol(t, mem, "cores", 4, 2, 1, 4))
	return east, west
}

func jnWhere(t *testing.T, name string, l, r *dataframe.DataFrame, mem memory.Allocator, preds []expr.Expr, opts ...dataframe.JoinOption) {
	t.Helper()
	opts = append(opts, dataframe.WithJoinAllocator(mem))
	got, err := l.JoinWhere(context.Background(), r, preds, opts...)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	defer got.Release()
	jnCheck(t, name, got, true)
}

func TestParityJoinWhere(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	east, west := jwFrames(t, mem)
	defer east.Release()
	defer west.Release()
	c := expr.Col
	cases := []struct {
		name  string
		preds []expr.Expr
	}{
		{"jw_one_ineq", []expr.Expr{c("dur").Lt(c("time"))}},
		{"jw_one_le", []expr.Expr{c("cores").Le(c("cores_right"))}},
		{"jw_two_ineq", []expr.Expr{c("dur").Lt(c("time")), c("rev").Lt(c("cost"))}},
		{"jw_two_ineq_ge", []expr.Expr{c("dur").Ge(c("time")), c("rev").Gt(c("cost"))}},
		{"jw_reversed", []expr.Expr{c("time").Gt(c("dur"))}},
		{"jw_eq_ineq", []expr.Expr{c("cores").Eq(c("cores_right")), c("dur").Lt(c("time"))}},
		{"jw_ne", []expr.Expr{c("cores").Ne(c("cores_right"))}},
		{"jw_three", []expr.Expr{c("dur").Lt(c("time")), c("rev").Lt(c("cost")), c("cores").Le(c("cores_right"))}},
		{"jw_lit", []expr.Expr{c("dur").Lt(c("time")), c("cost").Gt(expr.LitInt64(14))}},
		{"jw_empty", []expr.Expr{c("dur").Gt(c("time")), c("id").Gt(c("t_id"))}},
		// Predicates combined with And are split like separate ones.
		{"jw_two_ineq", []expr.Expr{c("dur").Lt(c("time")).And(c("rev").Lt(c("cost")))}},
	}
	for _, tc := range cases {
		jnWhere(t, tc.name, east, west, mem, tc.preds)
	}
	jnWhere(t, "jw_suffix", east, west, mem, []expr.Expr{c("cores").Gt(c("cores_X"))}, dataframe.WithJoinSuffix("_X"))

	if _, err := east.JoinWhere(context.Background(), west, nil); err == nil {
		t.Fatal("expected error without predicates")
	}
	if _, err := east.JoinWhere(context.Background(), west, []expr.Expr{c("zzz").Lt(c("time"))}); err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestParityJoinWhereNulls(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	c := expr.Col
	l := jnFrame(t, jnCol(t, mem, "a", 1, nil, 3, 5))
	defer l.Release()
	r := jnFrame(t, jnCol(t, mem, "b", 2, nil, 4))
	defer r.Release()
	jnWhere(t, "jw_nulls_lt", l, r, mem, []expr.Expr{c("a").Lt(c("b"))})
	jnWhere(t, "jw_nulls_ne", l, r, mem, []expr.Expr{c("a").Ne(c("b"))})
	jnWhere(t, "jw_nulls_eq", l, r, mem, []expr.Expr{c("a").Eq(c("b"))})

	l2 := jnFrame(t, jnCol(t, mem, "a", 1, nil, 3), jnCol(t, mem, "x", 5, 5, nil))
	defer l2.Release()
	r2 := jnFrame(t, jnCol(t, mem, "b", 2, 4, nil), jnCol(t, mem, "y", 4, nil, 1))
	defer r2.Release()
	jnWhere(t, "jw_two_ineq_nulls", l2, r2, mem, []expr.Expr{c("a").Lt(c("b")), c("x").Gt(c("y"))})
}

func TestParityJoinWhereFloatsStringsDups(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	c := expr.Col
	lf := jnFrame(t, jnCol(t, mem, "a", 1.0, math.NaN(), 3.0))
	defer lf.Release()
	rf := jnFrame(t, jnCol(t, mem, "b", 2.0, math.NaN()))
	defer rf.Release()
	jnWhere(t, "jw_nan_lt", lf, rf, mem, []expr.Expr{c("a").Lt(c("b"))})
	jnWhere(t, "jw_nan_ge", lf, rf, mem, []expr.Expr{c("a").Ge(c("b"))})

	ls := jnFrame(t, jnCol(t, mem, "a", "a", "c", "b"))
	defer ls.Release()
	rs := jnFrame(t, jnCol(t, mem, "b", "b", "c"))
	defer rs.Release()
	jnWhere(t, "jw_string", ls, rs, mem, []expr.Expr{c("a").Le(c("b"))})

	ld := jnFrame(t, jnCol(t, mem, "a", 1, 2, 2, 3), jnCol(t, mem, "x", 5, 5, 6, 7))
	defer ld.Release()
	rd := jnFrame(t, jnCol(t, mem, "b", 2, 2, 3), jnCol(t, mem, "y", 5, 6, 6))
	defer rd.Release()
	jnWhere(t, "jw_dups_two", ld, rd, mem, []expr.Expr{c("a").Le(c("b")), c("x").Ge(c("y"))})

	li := jnFrame(t, jnCol(t, mem, "a", 1, 3))
	defer li.Release()
	if _, err := li.JoinWhere(context.Background(), rf, []expr.Expr{c("a").Lt(c("b"))}); err == nil ||
		!strings.Contains(err.Error(), "cannot compare") {
		t.Fatalf("int vs float should error, got %v", err)
	}
}

// TestJoinWhereLargeMatchesBrute checks the parallel IEJoin, sort and
// hash paths against a brute-force count on inputs large enough to fan
// out across workers.
func TestJoinWhereLargeMatchesBrute(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	const n, m = 40_000, 150
	mk := func(name string, k int, f func(i int) int64) *series.Series {
		v := make([]int64, k)
		for i := range v {
			v[i] = f(i)
		}
		s, err := series.FromInt64(name, v, nil, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	la := func(i int) int64 { return int64((i * 7919) % 10007) }
	lx := func(i int) int64 { return int64((i * 104729) % 1009) }
	lk := func(i int) int64 { return int64(i % 13) }
	rb := func(j int) int64 { return int64((j * 6007) % 10007) }
	ry := func(j int) int64 { return int64((j * 3001) % 1009) }
	rk := func(j int) int64 { return int64(j % 11) }
	l := jnFrame(t, mk("a", n, la), mk("x", n, lx), mk("k", n, lk))
	defer l.Release()
	r := jnFrame(t, mk("b", m, rb), mk("y", m, ry), mk("k", m, rk))
	defer r.Release()
	c := expr.Col

	type check struct {
		preds []expr.Expr
		keep  func(i, j int) bool
	}
	checks := []check{
		{[]expr.Expr{c("a").Lt(c("b")), c("x").Ge(c("y"))}, func(i, j int) bool { return la(i) < rb(j) && lx(i) >= ry(j) }},
		{[]expr.Expr{c("a").Ge(c("b")), c("x").Lt(c("y"))}, func(i, j int) bool { return la(i) >= rb(j) && lx(i) < ry(j) }},
		{[]expr.Expr{c("a").Gt(c("b"))}, func(i, j int) bool { return la(i) > rb(j) }},
		{[]expr.Expr{c("k").Eq(c("k_right")), c("a").Le(c("b"))}, func(i, j int) bool { return lk(i) == rk(j) && la(i) <= rb(j) }},
	}
	for ci, ch := range checks {
		out, err := l.JoinWhere(context.Background(), r, ch.preds, dataframe.WithJoinAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		for i := range n {
			for j := range m {
				if ch.keep(i, j) {
					want++
				}
			}
		}
		if out.Height() != want {
			t.Fatalf("check %d: got %d rows, want %d", ci, out.Height(), want)
		}
		// Spot check that every emitted pair satisfies the predicates.
		a := out.ColumnAt(0).Chunk(0).(*array.Int64)
		b := out.ColumnAt(3).Chunk(0).(*array.Int64)
		for row := 0; row < out.Height(); row += 1 + out.Height()/500 {
			switch ci {
			case 2:
				if a.Value(row) <= b.Value(row) {
					t.Fatalf("check %d row %d violates predicate", ci, row)
				}
			}
		}
		out.Release()
	}
}
