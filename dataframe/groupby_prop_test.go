package dataframe_test

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

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// propSeries builds a Series from vals in one of three layouts: one
// chunk, several chunks, or a slice of a larger array (non-zero
// offset).
func propSeries(t testing.TB, mem memory.Allocator, r *rand.Rand, name string, dt arrow.DataType, vals []any) *series.Series {
	t.Helper()
	build := func(v []any) *series.Series {
		s, err := series.FromValues(name, dt, v, series.WithAllocator(mem))
		if err != nil {
			t.Fatalf("FromValues(%s): %v", dt, err)
		}
		return s
	}
	switch r.IntN(3) {
	case 1:
		if len(vals) < 2 {
			return build(vals)
		}
		cut := 1 + r.IntN(len(vals)-1)
		a, b := build(vals[:cut]), build(vals[cut:])
		ca, cb := a.Chunks()[0], b.Chunks()[0]
		ca.Retain()
		cb.Retain()
		a.Release()
		b.Release()
		s, err := series.New(name, ca, cb)
		if err != nil {
			t.Fatal(err)
		}
		return s
	case 2:
		pre := r.IntN(5)
		padded := append(testutil.RandValues(r, dt, pre, 0.3), vals...)
		padded = append(padded, testutil.RandValues(r, dt, r.IntN(5), 0.3)...)
		full := build(padded)
		defer full.Release()
		s, err := full.Slice(pre, len(vals))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	return build(vals)
}

// canonKey renders a key value so that values polars groups together
// (NaN with NaN, -0.0 with 0.0, null with null) map to the same string.
func canonKey(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case float64:
		if math.IsNaN(x) {
			return "f:NaN"
		}
		if x == 0 {
			return "f:0"
		}
		return fmt.Sprintf("f:%v", x)
	}
	return fmt.Sprintf("%T:%v", v, v)
}

func rowKey(cols [][]any, i int) string {
	parts := make([]string, len(cols))
	for k, c := range cols {
		parts[k] = canonKey(c[i])
	}
	return strings.Join(parts, "|")
}

var groupKeyTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	arrow.FixedWidthTypes.Date32,
}

func randGroupFloat(r *rand.Rand) any {
	switch r.IntN(12) {
	case 0:
		return nil
	case 1:
		return math.NaN()
	case 2:
		return math.Inf(1 - 2*r.IntN(2))
	case 3:
		return math.Copysign(0, -1)
	}
	return (r.Float64() - 0.5) * 1000
}

type groupRef struct {
	firstRow int
	n        int
	isum     int64
	icount   int
	fsum     float64
	fabs     float64
}

func TestGroupBySumProp(t *testing.T) {
	t.Parallel()
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(21, 22))
	sizes := append(slices.Clone(testutil.PropSizes), 1000, 4096)
	for iter := range 300 {
		n := sizes[r.IntN(len(sizes))]
		nk := 1 + r.IntN(2)
		keyVals := make([][]any, nk)
		var cols []*series.Series
		var keyNames []string
		var keyTypes []string
		for k := range nk {
			dt := groupKeyTypes[r.IntN(len(groupKeyTypes))]
			keyVals[k] = testutil.RandValues(r, dt, n, 0.15)
			name := fmt.Sprintf("k%d", k)
			s := propSeries(t, mem, r, name, dt, keyVals[k])
			keyVals[k] = s.ToList()
			cols = append(cols, s)
			keyNames = append(keyNames, name)
			keyTypes = append(keyTypes, dt.String())
		}
		iv := make([]any, n)
		fv := make([]any, n)
		for i := range n {
			if r.IntN(8) != 0 {
				iv[i] = int64(r.IntN(2001) - 1000)
			}
			fv[i] = randGroupFloat(r)
		}
		cols = append(cols,
			propSeries(t, mem, r, "iv", arrow.PrimitiveTypes.Int64, iv),
			propSeries(t, mem, r, "fv", arrow.PrimitiveTypes.Float64, fv))
		df, err := dataframe.New(cols...)
		if err != nil {
			t.Fatal(err)
		}

		ref := map[string]*groupRef{}
		var order []string
		var totalI int64
		for i := range n {
			key := rowKey(keyVals, i)
			g := ref[key]
			if g == nil {
				g = &groupRef{firstRow: i}
				ref[key] = g
				order = append(order, key)
			}
			g.n++
			if x, ok := iv[i].(int64); ok {
				g.isum += x
				g.icount++
				totalI += x
			}
			if x, ok := fv[i].(float64); ok {
				g.fsum += x
				g.fabs += math.Abs(x)
			}
		}

		maintain := r.IntN(2) == 0
		gb := df.GroupBy(keyNames...)
		if maintain {
			gb = gb.MaintainOrder()
		}
		desc := fmt.Sprintf("iter %d n=%d keys=%v maintain=%v", iter, n, keyTypes, maintain)
		out, err := gb.Agg(ctx, []expr.Expr{
			expr.Col("iv").Sum().Alias("isum"),
			expr.Col("iv").Count().Alias("icount"),
			expr.Col("fv").Sum().Alias("fsum"),
			expr.Len().Alias("n"),
		}, dataframe.WithGroupByAllocator(mem))
		if err != nil {
			t.Fatalf("%s: %v", desc, err)
		}
		outKeys := make([][]any, nk)
		for k, name := range keyNames {
			c, err := out.Column(name)
			if err != nil {
				t.Fatal(err)
			}
			outKeys[k] = c.ToList()
		}
		if out.Height() != len(ref) {
			seen := map[string]int{}
			for row := range out.Height() {
				key := rowKey(outKeys, row)
				if prev, dup := seen[key]; dup {
					t.Fatalf("%s: %d groups, want %d; key %q appears at rows %d and %d (%v)",
						desc, out.Height(), len(ref), key, prev, row, fmtKeyBits(outKeys, row))
				}
				seen[key] = row
			}
			t.Fatalf("%s: %d groups, want %d", desc, out.Height(), len(ref))
		}
		get := func(name string) []any {
			c, err := out.Column(name)
			if err != nil {
				t.Fatalf("%s: %v", desc, err)
			}
			return c.ToList()
		}
		isum, icount, fsum, cnt := get("isum"), get("icount"), get("fsum"), get("n")
		var gotTotal int64
		var gotRows uint64
		for row := range out.Height() {
			key := rowKey(outKeys, row)
			g := ref[key]
			if g == nil {
				t.Fatalf("%s: output group %q not in input", desc, key)
			}
			if maintain && order[row] != key {
				t.Fatalf("%s: maintain_order row %d is %q want %q", desc, row, key, order[row])
			}
			if isum[row] != g.isum {
				t.Fatalf("%s: group %q sum %v want %d", desc, key, isum[row], g.isum)
			}
			if icount[row] != uint64(g.icount) {
				t.Fatalf("%s: group %q count %v want %d", desc, key, icount[row], g.icount)
			}
			if cnt[row] != uint64(g.n) {
				t.Fatalf("%s: group %q len %v want %d", desc, key, cnt[row], g.n)
			}
			fs, _ := fsum[row].(float64)
			if !floatSumClose(fs, g.fsum, g.fabs) {
				t.Fatalf("%s: group %q float sum %v want %v", desc, key, fsum[row], g.fsum)
			}
			gotTotal += isum[row].(int64)
			gotRows += cnt[row].(uint64)
		}
		if gotTotal != totalI || gotRows != uint64(n) {
			t.Fatalf("%s: totals %d/%d want %d/%d", desc, gotTotal, gotRows, totalI, n)
		}
		out.Release()
		df.Release()
	}
}

func fmtKeyBits(cols [][]any, row int) string {
	var parts []string
	for _, c := range cols {
		if f, ok := c[row].(float64); ok {
			parts = append(parts, fmt.Sprintf("%v(bits %x)", f, math.Float64bits(f)))
			continue
		}
		parts = append(parts, fmt.Sprint(c[row]))
	}
	return strings.Join(parts, ", ")
}

// floatSumClose compares sums that may differ only by summation order.
// NaN must match NaN and infinities must match exactly.
func floatSumClose(got, want, absSum float64) bool {
	if math.IsNaN(want) || math.IsInf(want, 0) {
		return math.IsNaN(got) == math.IsNaN(want) && (math.IsNaN(got) || got == want)
	}
	return math.Abs(got-want) <= 1e-9*absSum+1e-12
}
