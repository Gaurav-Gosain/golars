// Parity tests for DataFrame.JoinAsof against polars 1.39.3. Expected
// frames live in testdata/join_asof_where_parity.txt and were produced by
// polars with the canonical repr in internal/framerepr (the generator
// mirrors every case below).

package dataframe_test

import (
	"bufio"
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/framerepr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

var (
	jnOnce     sync.Once
	jnExpected map[string]string
)

func jnWant(t *testing.T, name string) string {
	t.Helper()
	jnOnce.Do(func() {
		jnExpected = map[string]string{}
		f, err := os.Open("testdata/join_asof_where_parity.txt")
		if err != nil {
			return
		}
		defer f.Close()
		var cur string
		var lines []string
		flush := func() {
			if cur != "" {
				jnExpected[cur] = strings.Join(lines, "\n")
			}
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if rest, ok := strings.CutPrefix(line, "=== "); ok {
				flush()
				cur, lines = rest, nil
				continue
			}
			lines = append(lines, line)
		}
		flush()
	})
	want, ok := jnExpected[name]
	if !ok {
		t.Fatalf("no expected output for %q", name)
	}
	return want
}

// jnCheck compares got with the polars output recorded under name. When
// sorted is set, data rows are compared as a sorted multiset because the
// operation does not define a row order.
func jnCheck(t *testing.T, name string, got *dataframe.DataFrame, sorted bool) {
	t.Helper()
	g := framerepr.Frame(got)
	if sorted {
		lines := strings.Split(g, "\n")
		slices.Sort(lines[1:])
		g = strings.Join(lines, "\n")
	}
	if want := jnWant(t, name); g != want {
		t.Errorf("%s mismatch\ngot:\n%s\nwant:\n%s", name, g, want)
	}
}

// jnCol builds a column from Go values; nil entries are nulls. The dtype
// follows the first non-nil value: int -> i64, float64 -> f64,
// string -> str.
func jnCol(t *testing.T, mem memory.Allocator, name string, vals ...any) *series.Series {
	t.Helper()
	valid := make([]bool, len(vals))
	var kind any
	for i, v := range vals {
		valid[i] = v != nil
		if v != nil && kind == nil {
			kind = v
		}
	}
	var s *series.Series
	var err error
	switch kind.(type) {
	case float64:
		out := make([]float64, len(vals))
		for i, v := range vals {
			if v != nil {
				out[i] = v.(float64)
			}
		}
		s, err = series.FromFloat64(name, out, valid, series.WithAllocator(mem))
	case string:
		out := make([]string, len(vals))
		for i, v := range vals {
			if v != nil {
				out[i] = v.(string)
			}
		}
		s, err = series.FromString(name, out, valid, series.WithAllocator(mem))
	default:
		out := make([]int64, len(vals))
		for i, v := range vals {
			if v != nil {
				out[i] = int64(v.(int))
			}
		}
		s, err = series.FromInt64(name, out, valid, series.WithAllocator(mem))
	}
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func jnFrame(t *testing.T, cols ...*series.Series) *dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func jnDatetime(t *testing.T, mem memory.Allocator, name string, ts ...time.Time) *series.Series {
	t.Helper()
	b := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer b.Release()
	for _, v := range ts {
		b.Append(arrow.Timestamp(v.UnixMicro()))
	}
	s, err := series.New(name, b.NewArray())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func jnDate(t *testing.T, mem memory.Allocator, name string, ds ...time.Time) *series.Series {
	t.Helper()
	b := array.NewDate32Builder(mem)
	defer b.Release()
	for _, v := range ds {
		b.Append(arrow.Date32FromTime(v))
	}
	s, err := series.New(name, b.NewArray())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func jnAsof(t *testing.T, name string, l, r *dataframe.DataFrame, opts dataframe.AsofOptions) {
	t.Helper()
	got, err := l.JoinAsof(context.Background(), r, opts)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	defer got.Release()
	jnCheck(t, name, got, false)
}

func asofBaseFrames(t *testing.T, mem memory.Allocator) (*dataframe.DataFrame, *dataframe.DataFrame) {
	l := jnFrame(t,
		jnCol(t, mem, "t", 1, 5, 10, 15, 20),
		jnCol(t, mem, "a", "x", "y", "z", "w", "v"))
	r := jnFrame(t,
		jnCol(t, mem, "t", 2, 5, 11, 30),
		jnCol(t, mem, "b", 100, 200, 300, 400),
		jnCol(t, mem, "a", "r1", "r2", "r3", "r4"))
	return l, r
}

func TestParityJoinAsofStrategies(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	l, r := asofBaseFrames(t, mem)
	defer l.Release()
	defer r.Release()
	for _, st := range []dataframe.AsofStrategy{dataframe.AsofBackward, dataframe.AsofForward, dataframe.AsofNearest} {
		base := "asof_" + st.String()
		jnAsof(t, base, l, r, dataframe.AsofOptions{On: "t", Strategy: st, Allocator: mem})
		jnAsof(t, base+"_noexact", l, r, dataframe.AsofOptions{On: "t", Strategy: st, DisallowExactMatches: true, Allocator: mem})
		jnAsof(t, base+"_tol1", l, r, dataframe.AsofOptions{On: "t", Strategy: st, Tolerance: 1, Allocator: mem})
	}
	jnAsof(t, "asof_nocoalesce", l, r, dataframe.AsofOptions{On: "t", NoCoalesce: true, Allocator: mem})
	jnAsof(t, "asof_suffix", l, r, dataframe.AsofOptions{On: "t", Suffix: "_R", Allocator: mem})
	rr, err := r.Rename("t", "rt")
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	jnAsof(t, "asof_left_on", l, rr, dataframe.AsofOptions{LeftOn: "t", RightOn: "rt", Allocator: mem})
}

func TestParityJoinAsofNullKeys(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	l := jnFrame(t, jnCol(t, mem, "t", nil, 1, 6))
	defer l.Release()
	r := jnFrame(t, jnCol(t, mem, "t", nil, 2, 5), jnCol(t, mem, "b", 1, 2, 3))
	defer r.Release()
	jnAsof(t, "asof_nulls_first", l, r, dataframe.AsofOptions{On: "t", Strategy: dataframe.AsofNearest, Allocator: mem})

	// Polars requires nulls first; a trailing null fails the check.
	bad := jnFrame(t, jnCol(t, mem, "t", 1, 6, nil))
	defer bad.Release()
	if _, err := bad.JoinAsof(context.Background(), r, dataframe.AsofOptions{On: "t"}); err == nil {
		t.Fatal("expected sortedness error for trailing null key")
	}
}

func TestParityJoinAsofUnsortedErrors(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	l := jnFrame(t, jnCol(t, mem, "t", 10, 1, 5))
	defer l.Release()
	r := jnFrame(t, jnCol(t, mem, "t", 2, 5), jnCol(t, mem, "b", 1, 2))
	defer r.Release()
	_, err := l.JoinAsof(context.Background(), r, dataframe.AsofOptions{On: "t"})
	if err == nil || !strings.Contains(err.Error(), "not sorted") {
		t.Fatalf("want sortedness error, got %v", err)
	}
	out, err := l.JoinAsof(context.Background(), r, dataframe.AsofOptions{On: "t", SkipSortednessCheck: true, Allocator: mem})
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
}

func TestParityJoinAsofBy(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	l := jnFrame(t,
		jnCol(t, mem, "g", "a", "b", "a", "b", nil),
		jnCol(t, mem, "t", 1, 2, 3, 4, 5),
		jnCol(t, mem, "v", 1, 2, 3, 4, 5))
	defer l.Release()
	r := jnFrame(t,
		jnCol(t, mem, "g", "a", "b", "a", nil),
		jnCol(t, mem, "t", 0, 3, 2, 1),
		jnCol(t, mem, "w", 10, 20, 30, 40))
	defer r.Release()
	for _, st := range []dataframe.AsofStrategy{dataframe.AsofBackward, dataframe.AsofForward, dataframe.AsofNearest} {
		jnAsof(t, "asof_by_"+st.String(), l, r, dataframe.AsofOptions{On: "t", By: []string{"g"}, Strategy: st, Allocator: mem})
	}
	rh, err := r.Rename("g", "h")
	if err != nil {
		t.Fatal(err)
	}
	defer rh.Release()
	jnAsof(t, "asof_by_left_right", l, rh, dataframe.AsofOptions{
		On: "t", ByLeft: []string{"g"}, ByRight: []string{"h"}, NoCoalesce: true, Allocator: mem,
	})

	lm := jnFrame(t,
		jnCol(t, mem, "g", 1, 1, 2, 3),
		jnCol(t, mem, "h", "x", "y", "x", "x"),
		jnCol(t, mem, "t", 5, 5, 5, 5))
	defer lm.Release()
	rm := jnFrame(t,
		jnCol(t, mem, "g", 1, 1, 2, 2),
		jnCol(t, mem, "h", "x", "y", "x", "x"),
		jnCol(t, mem, "t", 1, 2, 3, 6),
		jnCol(t, mem, "w", 1, 2, 3, 4))
	defer rm.Release()
	jnAsof(t, "asof_multi_by", lm, rm, dataframe.AsofOptions{On: "t", By: []string{"g", "h"}, Strategy: dataframe.AsofNearest, Allocator: mem})
}

func TestParityJoinAsofTies(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	l := jnFrame(t, jnCol(t, mem, "t", 4, 5, 6))
	defer l.Release()
	r := jnFrame(t, jnCol(t, mem, "t", 3, 3, 7, 7), jnCol(t, mem, "b", 1, 2, 3, 4))
	defer r.Release()
	jnAsof(t, "asof_nearest_ties", l, r, dataframe.AsofOptions{On: "t", Strategy: dataframe.AsofNearest, Allocator: mem})

	// Backward picks the last duplicate, forward the first.
	r2 := jnFrame(t, jnCol(t, mem, "t", 3, 3, 7), jnCol(t, mem, "b", 1, 2, 3))
	defer r2.Release()
	l1 := jnFrame(t, jnCol(t, mem, "t", 4))
	defer l1.Release()
	l2 := jnFrame(t, jnCol(t, mem, "t", 2))
	defer l2.Release()
	back, err := l1.JoinAsof(context.Background(), r2, dataframe.AsofOptions{On: "t", Allocator: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	fwd, err := l2.JoinAsof(context.Background(), r2, dataframe.AsofOptions{On: "t", Strategy: dataframe.AsofForward, Allocator: mem})
	if err != nil {
		t.Fatal(err)
	}
	defer fwd.Release()
	fwd2, err := fwd.Rename("t", "t2")
	if err != nil {
		t.Fatal(err)
	}
	defer fwd2.Release()
	fwd3, err := fwd2.Rename("b", "b2")
	if err != nil {
		t.Fatal(err)
	}
	defer fwd3.Release()
	both, err := back.HStack(fwd3)
	if err != nil {
		t.Fatal(err)
	}
	defer both.Release()
	jnCheck(t, "asof_dup_back_fwd", both, false)
}

func TestParityJoinAsofTemporal(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	day := func(d, h, m int) time.Time { return time.Date(2020, 1, d, h, m, 0, 0, time.UTC) }
	l := jnFrame(t, jnDatetime(t, mem, "t", day(1, 10, 0), day(1, 12, 0), day(2, 12, 0)))
	defer l.Release()
	r := jnFrame(t, jnDatetime(t, mem, "t", day(1, 9, 0), day(1, 11, 30)), jnCol(t, mem, "b", 1, 2))
	defer r.Release()
	jnAsof(t, "asof_dt_1h", l, r, dataframe.AsofOptions{On: "t", Tolerance: "1h", Allocator: mem})
	jnAsof(t, "asof_dt_30m", l, r, dataframe.AsofOptions{On: "t", Tolerance: 30 * time.Minute, Allocator: mem})

	date := func(y, mo, d int) time.Time { return time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC) }
	ld := jnFrame(t, jnDate(t, mem, "t", date(2020, 1, 1), date(2020, 1, 5), date(2020, 1, 9)))
	defer ld.Release()
	rd := jnFrame(t, jnDate(t, mem, "t", date(2019, 12, 30), date(2020, 1, 3)), jnCol(t, mem, "b", 1, 2))
	defer rd.Release()
	jnAsof(t, "asof_date_2d", ld, rd, dataframe.AsofOptions{On: "t", Tolerance: "2d", Allocator: mem})
	// An integer tolerance on a date key counts days, as in polars.
	jnAsof(t, "asof_date_2d", ld, rd, dataframe.AsofOptions{On: "t", Tolerance: 2, Allocator: mem})
	if _, err := ld.JoinAsof(context.Background(), rd, dataframe.AsofOptions{On: "t", Tolerance: "1mo"}); err == nil {
		t.Fatal("calendar tolerance should be rejected")
	}
}

func TestParityJoinAsofNumericKinds(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	lf := jnFrame(t, jnCol(t, mem, "t", 1.0, 2.5, 2.6))
	defer lf.Release()
	rf := jnFrame(t, jnCol(t, mem, "t", 1.0, 2.0, 3.0), jnCol(t, mem, "b", 1, 2, 3))
	defer rf.Release()
	jnAsof(t, "asof_float_nearest", lf, rf, dataframe.AsofOptions{On: "t", Tolerance: 0.45, Strategy: dataframe.AsofNearest, Allocator: mem})

	i32, err := series.FromInt32("t", []int32{1, 5}, nil, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	l32 := jnFrame(t, i32)
	defer l32.Release()
	r64 := jnFrame(t, jnCol(t, mem, "t", 2, 5, 11), jnCol(t, mem, "b", 1, 2, 3))
	defer r64.Release()
	jnAsof(t, "asof_i32_i64", l32, r64, dataframe.AsofOptions{On: "t", Allocator: mem})

	empty := r64.Clear()
	defer empty.Release()
	l2 := jnFrame(t, jnCol(t, mem, "t", 1, 5))
	defer l2.Release()
	jnAsof(t, "asof_empty_right", l2, empty, dataframe.AsofOptions{On: "t", Allocator: mem})

	if _, err := lf.JoinAsof(context.Background(), r64, dataframe.AsofOptions{On: "t"}); err == nil {
		t.Fatal("float vs int key should error")
	}

	lc := jnFrame(t, jnCol(t, mem, "t", 1, 5), jnCol(t, mem, "rt", 0, 0))
	defer lc.Release()
	rc := jnFrame(t, jnCol(t, mem, "rt", 2, 5), jnCol(t, mem, "b", 1, 2))
	defer rc.Release()
	jnAsof(t, "asof_key_collision", lc, rc, dataframe.AsofOptions{LeftOn: "t", RightOn: "rt", Allocator: mem})
}

// TestJoinAsofLargeMatchesSerial cross-checks the parallel partitioned
// scan (with and without by groups) against a straightforward reference.
func TestJoinAsofLargeMatchesSerial(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	const n, m = 150_000, 40_000
	lt := make([]int64, n)
	lg := make([]int64, n)
	for i := range lt {
		lt[i] = int64(i) * 3
		lg[i] = int64(i % 7)
	}
	rt := make([]int64, m)
	rg := make([]int64, m)
	rv := make([]int64, m)
	for j := range rt {
		rt[j] = int64(j)*11 + 1
		rg[j] = int64(j % 5)
		rv[j] = int64(j)
	}
	mk := func(name string, v []int64) *series.Series {
		s, err := series.FromInt64(name, v, nil, series.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	l := jnFrame(t, mk("t", lt), mk("g", lg))
	defer l.Release()
	r := jnFrame(t, mk("t", rt), mk("g", rg), mk("v", rv))
	defer r.Release()

	for _, st := range []dataframe.AsofStrategy{dataframe.AsofBackward, dataframe.AsofForward, dataframe.AsofNearest} {
		for _, by := range [][]string{nil, {"g"}} {
			out, err := l.JoinAsof(context.Background(), r, dataframe.AsofOptions{On: "t", By: by, Strategy: st, Tolerance: 20, Allocator: mem})
			if err != nil {
				t.Fatal(err)
			}
			got := out.ColumnAt(out.Width() - 1).Chunk(0).(*array.Int64)
			for i := 0; i < n; i += 997 {
				want := int64(-1)
				best := int64(-1)
				for j := range m {
					if by != nil && rg[j] != lg[i] {
						continue
					}
					d := lt[i] - rt[j]
					ok := false
					switch st {
					case dataframe.AsofBackward:
						ok = d >= 0 && d <= 20 && (best < 0 || rt[j] >= rt[best])
					case dataframe.AsofForward:
						ok = d <= 0 && -d <= 20 && best < 0
					case dataframe.AsofNearest:
						ad := max(d, -d)
						if ad <= 20 {
							if best < 0 {
								ok = true
							} else {
								bd := max(lt[i]-rt[best], rt[best]-lt[i])
								ok = ad <= bd
							}
						}
					}
					if ok {
						best = int64(j)
						want = rv[j]
					}
				}
				if (want < 0) != got.IsNull(i) || (want >= 0 && got.Value(i) != want) {
					t.Fatalf("strategy %s by=%v row %d: got %v want %d", st, by, i, got.ValueStr(i), want)
				}
			}
			out.Release()
		}
	}
}
