package dataframe

// Tests against the vectors generated from the Lean model in verify/.
// See docs/verification.md.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

func loadVerified(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("../testdata/verified/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func vInt64Series(t *testing.T, name string, vals []*int64) *series.Series {
	t.Helper()
	v := make([]int64, len(vals))
	valid := make([]bool, len(vals))
	for i, p := range vals {
		if p != nil {
			v[i], valid[i] = *p, true
		}
	}
	s, err := series.FromInt64(name, v, valid)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func vFloatSeries(t *testing.T, name string, vals []*float64) *series.Series {
	t.Helper()
	v := make([]float64, len(vals))
	valid := make([]bool, len(vals))
	for i, p := range vals {
		if p != nil {
			v[i], valid[i] = *p, true
		}
	}
	s, err := series.FromFloat64(name, v, valid)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func hexFloats(t *testing.T, raw []*string) []*float64 {
	t.Helper()
	out := make([]*float64, len(raw))
	for i, h := range raw {
		if h == nil {
			continue
		}
		u, err := strconv.ParseUint(*h, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		f := math.Float64frombits(u)
		out[i] = &f
	}
	return out
}

// cell reads row i of a column as a string: "null", an integer, or a
// float formatted with %v (NaN prints as NaN).
func cell(t *testing.T, df *DataFrame, name string, i int) string {
	t.Helper()
	col, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	arr, err := col.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	if arr.IsNull(i) {
		return "null"
	}
	switch a := arr.(type) {
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(a.Value(i)), 10)
	case *array.Uint64:
		return strconv.FormatUint(a.Value(i), 10)
	case *array.Float64:
		return fmt.Sprint(a.Value(i))
	}
	t.Fatalf("column %s: unexpected type %s", name, arr.DataType())
	return ""
}

func optStr(p *int64) string {
	if p == nil {
		return "null"
	}
	return strconv.FormatInt(*p, 10)
}

func TestVerifiedGroupBy(t *testing.T) {
	var cases []struct {
		Keys   []*int64 `json:"keys"`
		Values []*int64 `json:"values"`
		Groups []struct {
			Key *int64 `json:"key"`
			Agg struct {
				Sum   int64  `json:"sum"`
				Count int64  `json:"count"`
				Min   *int64 `json:"min"`
				Max   *int64 `json:"max"`
			} `json:"agg"`
		} `json:"groups"`
	}
	loadVerified(t, "groupby.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		df, err := New(vInt64Series(t, "k", c.Keys), vInt64Series(t, "v", c.Values))
		if err != nil {
			t.Fatal(err)
		}
		out, err := df.GroupBy("k").Agg(ctx, []expr.Expr{
			expr.Col("v").Sum().Alias("sum"),
			expr.Col("v").Count().Alias("count"),
			expr.Col("v").Min().Alias("min"),
			expr.Col("v").Max().Alias("max"),
		})
		if err != nil {
			t.Fatal(err)
		}
		var got, want []string
		for i := range out.Height() {
			got = append(got, fmt.Sprintf("%s:%s,%s,%s,%s", cell(t, out, "k", i), cell(t, out, "sum", i),
				cell(t, out, "count", i), cell(t, out, "min", i), cell(t, out, "max", i)))
		}
		for _, g := range c.Groups {
			want = append(want, fmt.Sprintf("%s:%d,%d,%s,%s", optStr(g.Key), g.Agg.Sum, g.Agg.Count,
				optStr(g.Agg.Min), optStr(g.Agg.Max)))
		}
		// Hash and sort group-by agree as multisets (proved in the model),
		// so the group order is not compared.
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("case %d:\n got  %v\n want %v", ci, got, want)
		}
		out.Release()
		df.Release()
	}
}

func TestVerifiedGroupByFloatMinMax(t *testing.T) {
	var cases []struct {
		Keys   []*int64          `json:"keys"`
		Values []json.RawMessage `json:"values"`
		Groups []struct {
			Key *int64          `json:"key"`
			Min json.RawMessage `json:"min"`
			Max json.RawMessage `json:"max"`
		} `json:"groups"`
	}
	loadVerified(t, "groupby_float.json", &cases)
	fv := func(r json.RawMessage) *float64 {
		switch string(r) {
		case "null":
			return nil
		case `"nan"`:
			f := math.NaN()
			return &f
		}
		i, err := strconv.ParseInt(string(r), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		f := float64(i)
		return &f
	}
	fs := func(p *float64) string {
		if p == nil {
			return "null"
		}
		return fmt.Sprint(*p)
	}
	ctx := context.Background()
	for ci, c := range cases {
		vals := make([]*float64, len(c.Values))
		for i, r := range c.Values {
			vals[i] = fv(r)
		}
		df, err := New(vInt64Series(t, "k", c.Keys), vFloatSeries(t, "v", vals))
		if err != nil {
			t.Fatal(err)
		}
		out, err := df.GroupBy("k").Agg(ctx, []expr.Expr{
			expr.Col("v").Min().Alias("min"),
			expr.Col("v").Max().Alias("max"),
		})
		if err != nil {
			t.Fatal(err)
		}
		var got, want []string
		for i := range out.Height() {
			got = append(got, cell(t, out, "k", i)+":"+cell(t, out, "min", i)+","+cell(t, out, "max", i))
		}
		for _, g := range c.Groups {
			want = append(want, optStr(g.Key)+":"+fs(fv(g.Min))+","+fs(fv(g.Max)))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("case %d:\n got  %v\n want %v", ci, got, want)
		}
		out.Release()
		df.Release()
	}
}

func TestVerifiedJoins(t *testing.T) {
	var cases []struct {
		Float    bool              `json:"float"`
		Left     []json.RawMessage `json:"left"`
		Right    []json.RawMessage `json:"right"`
		Inner    [][2]int          `json:"inner"`
		LeftJoin [][2]*int         `json:"left_join"`
	}
	loadVerified(t, "joins.json", &cases)
	keyCol := func(raw []json.RawMessage, float bool) *series.Series {
		if float {
			hs := make([]*string, len(raw))
			for i, r := range raw {
				if err := json.Unmarshal(r, &hs[i]); err != nil {
					t.Fatal(err)
				}
			}
			return vFloatSeries(t, "k", hexFloats(t, hs))
		}
		is := make([]*int64, len(raw))
		for i, r := range raw {
			if err := json.Unmarshal(r, &is[i]); err != nil {
				t.Fatal(err)
			}
		}
		return vInt64Series(t, "k", is)
	}
	ids := func(name string, n int) *series.Series {
		v := make([]int64, n)
		for i := range v {
			v[i] = int64(i)
		}
		s, err := series.FromInt64(name, v, nil)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ctx := context.Background()
	for ci, c := range cases {
		left, err := New(keyCol(c.Left, c.Float), ids("a", len(c.Left)))
		if err != nil {
			t.Fatal(err)
		}
		right, err := New(keyCol(c.Right, c.Float), ids("b", len(c.Right)))
		if err != nil {
			t.Fatal(err)
		}
		pairs := func(how JoinType) []string {
			out, err := left.Join(ctx, right, []string{"k"}, how)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			var got []string
			for i := range out.Height() {
				got = append(got, cell(t, out, "a", i)+","+cell(t, out, "b", i))
			}
			slices.Sort(got)
			return got
		}
		var wantInner, wantLeft []string
		for _, p := range c.Inner {
			wantInner = append(wantInner, fmt.Sprintf("%d,%d", p[0], p[1]))
		}
		for _, p := range c.LeftJoin {
			b := "null"
			if p[1] != nil {
				b = strconv.Itoa(*p[1])
			}
			wantLeft = append(wantLeft, fmt.Sprintf("%d,%s", *p[0], b))
		}
		slices.Sort(wantInner)
		slices.Sort(wantLeft)
		if got := pairs(InnerJoin); !slices.Equal(got, wantInner) {
			t.Fatalf("case %d (float=%v) inner:\n got  %v\n want %v", ci, c.Float, got, wantInner)
		}
		if got := pairs(LeftJoin); !slices.Equal(got, wantLeft) {
			t.Fatalf("case %d (float=%v) left:\n got  %v\n want %v", ci, c.Float, got, wantLeft)
		}
		left.Release()
		right.Release()
	}
}

func TestVerifiedSortBy(t *testing.T) {
	var cases []struct {
		A         []*int64  `json:"a"`
		B         []*string `json:"b"`
		C         []*int64  `json:"c"`
		Desc      []bool    `json:"desc"`
		NullsLast []bool    `json:"nulls_last"`
		Perm      []int     `json:"perm"`
	}
	loadVerified(t, "sort_multi.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		n := len(c.A)
		id := make([]int64, n)
		for i := range id {
			id[i] = int64(i)
		}
		ids, err := series.FromInt64("id", id, nil)
		if err != nil {
			t.Fatal(err)
		}
		cols := []*series.Series{vInt64Series(t, "a", c.A)}
		keys := []string{"a"}
		so := []compute.SortOptions{{}, {}}
		if c.B != nil {
			cols = append(cols, vFloatSeries(t, "b", hexFloats(t, c.B)))
			keys = append(keys, "b")
			so = make([]compute.SortOptions, 3)
			for i := range so {
				so[i].Descending = c.Desc[i]
				if c.NullsLast[i] {
					so[i].Nulls = compute.NullsLast
				}
			}
		}
		cols = append(cols, vInt64Series(t, "c", c.C), ids)
		keys = append(keys, "c")
		df, err := New(cols...)
		if err != nil {
			t.Fatal(err)
		}
		out, err := df.SortBy(ctx, keys, so)
		if err != nil {
			t.Fatal(err)
		}
		col, _ := out.Column("id")
		arr, _ := col.Consolidated()
		got := make([]int, n)
		for i := range n {
			got[i] = int(arr.(*array.Int64).Value(i))
		}
		arr.Release()
		if !slices.Equal(got, c.Perm) {
			t.Fatalf("case %d: got %v want %v", ci, got, c.Perm)
		}
		out.Release()
		df.Release()
	}
}
