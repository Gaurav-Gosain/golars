package lazy

// Tests against the vectors generated from the Lean model in verify/.
// See docs/verification.md.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
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

// vnode is one node of a serialized model expression, predicate or plan.
type vnode struct {
	Op        string          `json:"op"`
	I         int             `json:"i"`
	V         json.RawMessage `json:"v"`
	A         *vnode          `json:"a"`
	B         *vnode          `json:"b"`
	P         *vnode          `json:"p"`
	Q         *vnode          `json:"q"`
	E         *vnode          `json:"e"`
	Es        []*vnode        `json:"es"`
	Off       int             `json:"off"`
	Len       int             `json:"len"`
	Desc      bool            `json:"desc"`
	NullsLast bool            `json:"nulls_last"`
	Input     *vnode          `json:"input"`
}

func vcol(i int) string { return "c" + strconv.Itoa(i) }

// buildVExpr maps a model expression to golars. Column i of the model is
// the golars column c<i>.
func buildVExpr(t *testing.T, n *vnode) expr.Expr {
	t.Helper()
	switch n.Op {
	case "col":
		return expr.Col(vcol(n.I))
	case "lit":
		v, err := strconv.ParseInt(string(n.V), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return expr.Lit(v)
	case "add":
		return buildVExpr(t, n.A).Add(buildVExpr(t, n.B))
	case "mul":
		return buildVExpr(t, n.A).Mul(buildVExpr(t, n.B))
	case "fill_null":
		return buildVExpr(t, n.A).FillNullExpr(buildVExpr(t, n.B))
	case "neg":
		return buildVExpr(t, n.A).Neg()
	case "abs":
		return buildVExpr(t, n.A).Abs()
	case "cum_sum":
		return buildVExpr(t, n.A).CumSum()
	case "shift":
		return buildVExpr(t, n.A).Shift(1)
	case "sum":
		return buildVExpr(t, n.A).Sum()
	case "gt":
		return buildVExpr(t, n.A).Gt(buildVExpr(t, n.B))
	case "lt":
		return buildVExpr(t, n.A).Lt(buildVExpr(t, n.B))
	case "eq":
		return buildVExpr(t, n.A).Eq(buildVExpr(t, n.B))
	case "and":
		return buildVExpr(t, n.P).And(buildVExpr(t, n.Q))
	case "or":
		return buildVExpr(t, n.P).Or(buildVExpr(t, n.Q))
	case "not":
		return buildVExpr(t, n.P).Not()
	case "is_null":
		return buildVExpr(t, n.A).IsNull()
	case "litb":
		return expr.LitBool(string(n.V) == "true")
	}
	t.Fatalf("unknown op %q", n.Op)
	return expr.Expr{}
}

func buildVPlan(t *testing.T, n *vnode, scan LazyFrame) LazyFrame {
	t.Helper()
	if n.Op == "scan" {
		return scan
	}
	in := buildVPlan(t, n.Input, scan)
	switch n.Op {
	case "filter":
		return in.Filter(buildVExpr(t, n.P))
	case "with_col":
		return in.WithColumns(buildVExpr(t, n.E).Alias(vcol(n.I)))
	case "select":
		es := make([]expr.Expr, len(n.Es))
		for k, e := range n.Es {
			es[k] = buildVExpr(t, e).Alias(vcol(k))
		}
		return in.Select(es...)
	case "slice":
		return in.Slice(n.Off, n.Len)
	case "sort":
		so := compute.SortOptions{Descending: n.Desc}
		if n.NullsLast {
			so.Nulls = compute.NullsLast
		}
		return in.SortBy([]string{vcol(n.I)}, []compute.SortOptions{so})
	}
	t.Fatalf("unknown plan op %q", n.Op)
	return LazyFrame{}
}

// TestVerifiedIsElementwise compares expr.IsElementwise with the model's
// classification, which the model proves sound (an accepted expression
// is computed row by row).
func TestVerifiedIsElementwise(t *testing.T) {
	var cases []struct {
		Kind        string `json:"kind"`
		E           *vnode `json:"e"`
		Elementwise bool   `json:"elementwise"`
	}
	loadVerified(t, "elementwise.json", &cases)
	for i, c := range cases {
		e := buildVExpr(t, c.E)
		if got := expr.IsElementwise(e); got != c.Elementwise {
			t.Fatalf("case %d: IsElementwise(%s) = %v, model says %v", i, e, got, c.Elementwise)
		}
	}
}

func vrowsFrame(t *testing.T, rows [][]*int64, width int) *dataframe.DataFrame {
	t.Helper()
	cols := make([]*series.Series, width)
	for j := range width {
		vals := make([]int64, len(rows))
		valid := make([]bool, len(rows))
		for i, r := range rows {
			if r[j] != nil {
				vals[i], valid[i] = *r[j], true
			}
		}
		s, err := series.FromInt64(vcol(j), vals, valid)
		if err != nil {
			t.Fatal(err)
		}
		cols[j] = s
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func frameRows(t *testing.T, df *dataframe.DataFrame) [][]*int64 {
	t.Helper()
	out := make([][]*int64, df.Height())
	for i := range out {
		out[i] = make([]*int64, df.Width())
	}
	for j := range df.Width() {
		col := df.ColumnAt(j)
		if col.Name() != vcol(j) {
			t.Fatalf("column %d is %q, want %q", j, col.Name(), vcol(j))
		}
		arr, err := col.Consolidated()
		if err != nil {
			t.Fatal(err)
		}
		a, ok := arr.(*array.Int64)
		if !ok {
			t.Fatalf("column %s has type %s, want int64", col.Name(), arr.DataType())
		}
		for i := range out {
			if a.IsValid(i) {
				v := a.Value(i)
				out[i][j] = &v
			}
		}
		arr.Release()
	}
	return out
}

func fmtRows(rows [][]*int64) string {
	s := "["
	for _, r := range rows {
		s += "["
		for j, v := range r {
			if j > 0 {
				s += " "
			}
			if v == nil {
				s += "null"
			} else {
				s += strconv.FormatInt(*v, 10)
			}
		}
		s += "]"
	}
	return s + "]"
}

// TestVerifiedPlans runs every model plan three ways: optimized by
// golars, unoptimized, and the rewritten plan of the model's optimizer
// (proved sound) unoptimized. All must equal the model's result.
func TestVerifiedPlans(t *testing.T) {
	var cases []struct {
		Rows      [][]*int64 `json:"rows"`
		Plan      *vnode     `json:"plan"`
		Optimized *vnode     `json:"optimized"`
		Result    [][]*int64 `json:"result"`
	}
	loadVerified(t, "plans.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		df := vrowsFrame(t, c.Rows, 3)
		scan := FromDataFrame(df)
		want := fmtRows(c.Result)
		run := func(name string, lf LazyFrame, optimized bool) {
			t.Helper()
			var out *dataframe.DataFrame
			var err error
			if optimized {
				out, err = lf.Collect(ctx)
			} else {
				out, err = lf.CollectUnoptimized(ctx)
			}
			if err != nil {
				t.Fatalf("case %d %s: %v\nplan: %s", ci, name, err, lf.plan)
			}
			defer out.Release()
			if got := fmtRows(frameRows(t, out)); got != want {
				t.Fatalf("case %d %s:\n got  %s\n want %s\nrows %s\nplan %s", ci, name, got, want,
					fmtRows(c.Rows), fmt.Sprint(lf.plan))
			}
		}
		run("optimized", buildVPlan(t, c.Plan, scan), true)
		run("unoptimized", buildVPlan(t, c.Plan, scan), false)
		run("model-rewritten", buildVPlan(t, c.Optimized, scan), false)
		df.Release()
	}
}
