// Behavioural parity with polars' tests/unit/operations/test_comparison.py.
// See NOTICE for license & attribution. Fresh Go tests against golars'
// API driven by the polars scenario catalog; no code copied.
//
// Comparison semantic:
//   - null op anything   = null  (three-valued logic)
//   - NaN != NaN         = true  (IEEE 754)
//   - NaN compared to any other float (including itself under ==) = false
//   - length mismatch    = error
//
// Scenarios NOT ported (feature-gated on polars-only behaviour):
//   - Struct equality            (no Struct dtype)
//   - Categorical compare        (no Categorical dtype)
//   - Binary compare             (no Binary dtype)
//   - Date/Duration compare      (no temporal dtypes)
//   - List broadcast compare     (no List dtype)
//   - Literal-downcast flooring  (literal kernels are uint/int/float only)

package compute_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/series"
)

// Polars: test_comparison_nulls_single: a single null on either
// side propagates to a null in the boolean result.
func TestParityCompareNullPropagatesOnLhs(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 0, 3},
		[]bool{true, false, true}, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", []int64{1, 2, 4},
		nil, series.WithAllocator(alloc))
	defer b.Release()

	out, err := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	// [0] 1 == 1 → true
	// [1] null == 2 → null
	// [2] 3 == 4 → false
	if !arr.IsValid(0) || !arr.Value(0) {
		t.Fatal("[0] should be true")
	}
	if arr.IsValid(1) {
		t.Fatal("[1] should be null (lhs null)")
	}
	if !arr.IsValid(2) || arr.Value(2) {
		t.Fatal("[2] should be false")
	}
}

// Polars: null on rhs also yields null.
func TestParityCompareNullPropagatesOnRhs(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", []int64{1, 0, 3},
		[]bool{true, false, true}, series.WithAllocator(alloc))
	defer b.Release()

	out, _ := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	if !arr.IsValid(0) || !arr.Value(0) {
		t.Fatal("[0] 1==1 should be true")
	}
	if arr.IsValid(1) {
		t.Fatal("[1] rhs null → null")
	}
	if !arr.IsValid(2) || !arr.Value(2) {
		t.Fatal("[2] 3==3 should be true")
	}
}

// Polars: both-null yields null (not true, not false).
func TestParityCompareBothNullIsNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{0, 2},
		[]bool{false, true}, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", []int64{0, 2},
		[]bool{false, true}, series.WithAllocator(alloc))
	defer b.Release()

	out, _ := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	if arr.IsValid(0) {
		t.Fatal("[0] both null → null")
	}
	if !arr.IsValid(1) || !arr.Value(1) {
		t.Fatal("[1] 2==2 → true")
	}
}

// polars compares floats with a total order, not IEEE: NaN == NaN.
func TestParityCompareNaNInequalityHolds(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a",
		[]float64{math.NaN(), 1.0, math.NaN()},
		nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromFloat64("b",
		[]float64{math.NaN(), 1.0, 2.0},
		nil, series.WithAllocator(alloc))
	defer b.Release()

	eq, err := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer eq.Release()
	arr := eq.Chunk(0).(*array.Boolean)
	// polars total order: NaN == NaN is true.
	want := []bool{true, true, false}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("Eq[%d] got %v want %v", i, arr.Value(i), w)
		}
	}

	ne, _ := compute.Ne(context.Background(), a, b, compute.WithAllocator(alloc))
	defer ne.Release()
	arr = ne.Chunk(0).(*array.Boolean)
	want = []bool{false, false, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("Ne[%d] got %v want %v", i, arr.Value(i), w)
		}
	}
}

// polars orders NaN above every number:
// [NaN, 1, 2] < [1, NaN, NaN] == [False, True, True].
func TestParityCompareNaNOrderIsTotal(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromFloat64("a",
		[]float64{math.NaN(), 1.0, 2.0},
		nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromFloat64("b",
		[]float64{1.0, math.NaN(), math.NaN()},
		nil, series.WithAllocator(alloc))
	defer b.Release()

	for _, tc := range []struct {
		name string
		op   func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error)
		want []bool
	}{
		{"Lt", compute.Lt, []bool{false, true, true}},
		{"Le", compute.Le, []bool{false, true, true}},
		{"Gt", compute.Gt, []bool{true, false, false}},
		{"Ge", compute.Ge, []bool{true, false, false}},
	} {
		out, err := tc.op(context.Background(), a, b, compute.WithAllocator(alloc))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		arr := out.Chunk(0).(*array.Boolean)
		for i, w := range tc.want {
			if arr.Value(i) != w {
				t.Errorf("%s[%d] = %v, want %v", tc.name, i, arr.Value(i), w)
			}
		}
		out.Release()
	}
}

// Polars: length mismatch between lhs and rhs is a hard error.
func TestParityCompareLengthMismatchErrors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", []int64{1, 2}, nil, series.WithAllocator(alloc))
	defer b.Release()

	if _, err := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc)); err == nil {
		t.Fatal("length mismatch must error")
	}
}

// Polars: string equality: exact byte-level equality, not locale.
func TestParityCompareStringEq(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromString("a",
		[]string{"alpha", "beta", "gamma"},
		nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromString("b",
		[]string{"alpha", "BETA", "gamma"},
		nil, series.WithAllocator(alloc))
	defer b.Release()

	out, err := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	want := []bool{true, false, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

// Polars: bool equality: true/false compared row-wise.
func TestParityCompareBoolEq(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromBool("a", []bool{true, false, true, false}, nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromBool("b", []bool{true, true, false, false}, nil, series.WithAllocator(alloc))
	defer b.Release()

	out, _ := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	want := []bool{true, false, false, true}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Fatalf("[%d]=%v want %v", i, arr.Value(i), w)
		}
	}
}

// Polars: test_comparison_literal_behavior_matches_nonliteral_behavior -
// the scalar-literal kernels (GtLit etc.) agree with a full-series
// broadcast. Row-wise: GtLit(s, c) == Gt(s, broadcast(c)).
func TestParityCompareLiteralMatchesBroadcast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{-1, 0, 1, 5, 10, 100}, nil, series.WithAllocator(alloc))
	defer s.Release()

	// Build a broadcast series with the literal value.
	n := s.Len()
	bcastVals := make([]int64, n)
	for i := range bcastVals {
		bcastVals[i] = 5
	}
	bcast, _ := series.FromInt64("t", bcastVals, nil, series.WithAllocator(alloc))
	defer bcast.Release()

	lit, err := compute.GtLit(context.Background(), s, int64(5), compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer lit.Release()

	bc, err := compute.Gt(context.Background(), s, bcast, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Release()

	la := lit.Chunk(0).(*array.Boolean)
	ba := bc.Chunk(0).(*array.Boolean)
	for i := range la.Len() {
		if la.Value(i) != ba.Value(i) {
			t.Fatalf("[%d] GtLit=%v Gt-broadcast=%v (src=%d)", i, la.Value(i), ba.Value(i), s.Chunk(0).(*array.Int64).Value(i))
		}
	}
}

// Polars: empty series compare yields an empty boolean series.
func TestParityCompareEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", nil, nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", nil, nil, series.WithAllocator(alloc))
	defer b.Release()

	out, err := compute.Eq(context.Background(), a, b, compute.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("len=%d want 0", out.Len())
	}
}

// Polars: the six operators on a representative 3-element sample
// produce the expected truth vectors.
func TestParityCompareAllSixOperators(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	a, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer a.Release()
	b, _ := series.FromInt64("b", []int64{2, 2, 2}, nil, series.WithAllocator(alloc))
	defer b.Release()

	checks := []struct {
		name string
		op   func(context.Context, *series.Series, *series.Series, ...compute.Option) (*series.Series, error)
		want [3]bool
	}{
		{"Eq", compute.Eq, [3]bool{false, true, false}},
		{"Ne", compute.Ne, [3]bool{true, false, true}},
		{"Lt", compute.Lt, [3]bool{true, false, false}},
		{"Le", compute.Le, [3]bool{true, true, false}},
		{"Gt", compute.Gt, [3]bool{false, false, true}},
		{"Ge", compute.Ge, [3]bool{false, true, true}},
	}
	for _, c := range checks {
		out, err := c.op(context.Background(), a, b, compute.WithAllocator(alloc))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		arr := out.Chunk(0).(*array.Boolean)
		for i, w := range c.want {
			if arr.Value(i) != w {
				t.Fatalf("%s[%d]=%v want %v", c.name, i, arr.Value(i), w)
			}
		}
		out.Release()
	}
}
