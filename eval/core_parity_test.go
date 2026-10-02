package eval_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

// parityColumn and parityCase are filled in by the generated file
// core_parity_gen_test.go (see testdata/core_parity_gen.py).
type parityColumn struct {
	Name   string
	Type   arrow.DataType
	Values []any
}

type parityCase struct {
	Name    string
	Frame   string
	Kind    string // select, sorted, agg, agg_all, over
	Key     string
	Expr    expr.Expr
	DType   string
	Want    any
	Cols    []any
	WantErr bool
}

func ptr[T any](v T) *T { return &v }

// addFold is the fold function used by the generated fold cases.
var addFold expr.FoldFunc = func(acc, x *series.Series) (*series.Series, error) {
	return compute.Add(context.Background(), acc, x)
}

func parityFrame(t *testing.T, name string, opts ...series.Option) *dataframe.DataFrame {
	t.Helper()
	spec, ok := coreParityFrames[name]
	if !ok {
		t.Fatalf("unknown frame %q", name)
	}
	cols := make([]*series.Series, len(spec))
	for i, c := range spec {
		s, err := series.FromValues(c.Name, c.Type, c.Values, opts...)
		if err != nil {
			t.Fatalf("frame %s column %s: %v", name, c.Name, err)
		}
		cols[i] = s
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func TestCoreParity(t *testing.T) {
	for _, tc := range coreParityCases {
		t.Run(tc.Name, func(t *testing.T) {
			mem := testutil.NewCheckedAllocator(t)
			df := parityFrame(t, tc.Frame, series.WithAllocator(mem))
			defer df.Release()
			ctx := context.Background()
			lf := lazy.FromDataFrame(df)
			switch tc.Kind {
			case "select", "sorted":
				lf = lf.Select(tc.Expr)
			case "agg", "agg_all":
				lf = lf.GroupBy(tc.Key).Agg(tc.Expr).Sort(tc.Key, false)
			case "over":
				lf = lf.Select(tc.Expr.Over(tc.Key))
			default:
				t.Fatalf("unknown kind %q", tc.Kind)
			}
			out, err := lf.Collect(ctx, lazy.WithExecAllocator(mem))
			if tc.WantErr {
				if err == nil {
					out.Release()
					t.Fatalf("expected an error, polars raised one")
				}
				return
			}
			if err != nil {
				t.Fatalf("collect: %v", err)
			}
			defer out.Release()
			if tc.Kind == "agg_all" {
				rows, err := out.Rows()
				if err != nil {
					t.Fatal(err)
				}
				var got []any
				for _, r := range rows {
					row := make([]any, 0, len(r)-1)
					for _, v := range r[1:] {
						row = append(row, normalizeCell(v))
					}
					got = append(got, row)
				}
				if !parityEqual(got, tc.Want) {
					t.Fatalf("rows\n got  %v\n want %v", fmtAny(got), fmtAny(tc.Want))
				}
				return
			}
			col := out.ColumnAt(0)
			if tc.Kind == "agg" {
				col = out.ColumnAt(1)
			}
			if got := col.DType().String(); got != tc.DType {
				t.Errorf("dtype = %s, want %s", got, tc.DType)
			}
			got := col.ToList()
			want, _ := tc.Want.([]any)
			if tc.Kind == "sorted" {
				sortAny(got)
				sortAny(want)
			}
			if !parityEqual(got, want) {
				t.Fatalf("values\n got  %v\n want %v", fmtAny(got), fmtAny(want))
			}
		})
	}
}

// normalizeCell maps DataFrame.Rows cell values onto ToList's types.
func normalizeCell(v any) any {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case uint32:
		return uint64(x)
	case float32:
		return float64(x)
	}
	return v
}

func sortAny(v []any) {
	sort.SliceStable(v, func(i, j int) bool { return fmtAny(v[i]) < fmtAny(v[j]) })
}

func fmtAny(v any) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case float64:
		if math.IsNaN(x) {
			return "NaN"
		}
		return fmt.Sprintf("%v", x)
	case []any:
		s := "["
		for i, e := range x {
			if i > 0 {
				s += " "
			}
			s += fmtAny(e)
		}
		return s + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		s := "{"
		for i, k := range keys {
			if i > 0 {
				s += " "
			}
			s += k + ":" + fmtAny(x[k])
		}
		return s + "}"
	}
	return fmt.Sprintf("%v", v)
}

// parityEqual compares ToList output with generated expectations:
// numbers compare by value across int/uint/float, NaN equals NaN, and
// floats allow a tiny relative error.
func parityEqual(got, want any) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	if gf, ok := asFloat(got); ok {
		wf, ok := asFloat(want)
		if !ok {
			return false
		}
		if math.IsNaN(gf) || math.IsNaN(wf) {
			return math.IsNaN(gf) && math.IsNaN(wf)
		}
		if gf == wf {
			return true
		}
		return math.Abs(gf-wf) <= 1e-9*math.Max(math.Abs(gf), math.Abs(wf))
	}
	switch g := got.(type) {
	case []any:
		w, ok := want.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range g {
			if !parityEqual(g[i], w[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		w, ok := want.(map[string]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for k, v := range g {
			if !parityEqual(v, w[k]) {
				return false
			}
		}
		return true
	}
	return got == want
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	}
	return 0, false
}
