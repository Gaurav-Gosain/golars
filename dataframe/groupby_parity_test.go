// Behavioural parity with polars' tests/unit/operations/aggregation/
// test_aggregations.py groupby scenarios. See NOTICE for license &
// attribution. Fresh Go tests against golars' API driven by the
// polars scenario catalog; no code copied.
//
// Scenarios NOT ported (feature-gated on polars-only behaviour):
//   - Quantile / Median                 (no Quantile kernel yet)
//   - Rolling aggregations              (no Rolling ops)
//   - List / Struct / Categorical keys  (no such dtypes)
//   - DynamicGroupBy / GroupByRolling   (time-windowed groupby)
//   - Expression-based agg filters      (no .filter() inside agg)

package dataframe_test

import (
	"cmp"
	"context"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/series"
)

// Shared fixture: a small 2-key × 1-value table used across every
// aggregation variant.
func groupByFixture(t *testing.T, alloc memory.Allocator) *dataframe.DataFrame {
	t.Helper()
	region, _ := series.FromString("region",
		[]string{"eu", "eu", "us", "us", "us", "ap"},
		nil, series.WithAllocator(alloc))
	value, _ := series.FromInt64("v",
		[]int64{10, 20, 5, 15, 25, 50},
		nil, series.WithAllocator(alloc))
	df, err := dataframe.New(region, value)
	if err != nil {
		t.Fatal(err)
	}
	return df
}

// rowsByKey reads the output of a groupby into a map[key]sum-of-ints
// for easy assertion regardless of row-order inside the result.
func rowsByStringKey(t *testing.T, df *dataframe.DataFrame, keyCol, valCol string) map[string]int64 {
	t.Helper()
	k, err := df.Column(keyCol)
	if err != nil {
		t.Fatal(err)
	}
	v, err := df.Column(valCol)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]int64, df.Height())
	kArr := k.Chunk(0).(*array.String)
	vArr := v.Chunk(0).(*array.Int64)
	for i := range kArr.Len() {
		out[kArr.Value(i)] = vArr.Value(i)
	}
	return out
}

// Polars: group_by("k").agg(pl.col("v").sum()): every key shows
// up once, paired with its sum. Row order is not guaranteed.
func TestParityGroupBySum(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := groupByFixture(t, alloc)
	defer df.Release()

	out, err := df.GroupBy("region").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("total")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Fatalf("height=%d want 3 (eu/us/ap)", out.Height())
	}
	got := rowsByStringKey(t, out, "region", "total")
	want := map[string]int64{"eu": 30, "us": 45, "ap": 50}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("sum[%q]=%d want %d", k, got[k], v)
		}
	}
}

// Polars: multiple aggregations in a single agg() call produce one
// output column per aggregate, all keyed by the group key.
func TestParityGroupByMultiAgg(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := groupByFixture(t, alloc)
	defer df.Release()

	out, err := df.GroupBy("region").Agg(context.Background(), []expr.Expr{
		expr.Col("v").Sum().Alias("s"),
		expr.Col("v").Min().Alias("lo"),
		expr.Col("v").Max().Alias("hi"),
		expr.Col("v").Count().Alias("n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if got := out.Width(); got != 5 {
		t.Fatalf("width=%d want 5 (region + 4 aggs)", got)
	}
	if got := out.Height(); got != 3 {
		t.Fatalf("height=%d want 3", got)
	}
	// Spot-check the US group.
	us := rowsByStringKey(t, out, "region", "s")
	if us["us"] != 45 {
		t.Fatalf("us sum = %d want 45", us["us"])
	}
	lo := rowsByStringKey(t, out, "region", "lo")
	if lo["us"] != 5 {
		t.Fatalf("us min = %d want 5", lo["us"])
	}
	hi := rowsByStringKey(t, out, "region", "hi")
	if hi["us"] != 25 {
		t.Fatalf("us max = %d want 25", hi["us"])
	}
	n := rowsByStringKey(t, out, "region", "n")
	if n["us"] != 3 {
		t.Fatalf("us count = %d want 3", n["us"])
	}
}

// Polars: groupby on two keys treats each distinct (k1, k2) pair as
// its own group. Output width = 2 keys + N aggs.
func TestParityGroupByMultiKey(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	region, _ := series.FromString("region",
		[]string{"eu", "eu", "eu", "us", "us"},
		nil, series.WithAllocator(alloc))
	prod, _ := series.FromString("product",
		[]string{"A", "A", "B", "A", "A"},
		nil, series.WithAllocator(alloc))
	qty, _ := series.FromInt64("qty",
		[]int64{1, 2, 3, 4, 5},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(region, prod, qty)
	defer df.Release()

	out, err := df.GroupBy("region", "product").Agg(context.Background(),
		[]expr.Expr{expr.Col("qty").Sum().Alias("total")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	// Distinct pairs: (eu,A), (eu,B), (us,A) → 3 groups.
	if out.Height() != 3 {
		t.Fatalf("height=%d want 3 (distinct key pairs)", out.Height())
	}
	if out.Width() != 3 {
		t.Fatalf("width=%d want 3 (region, product, total)", out.Width())
	}
}

// Multi-key hash path against a brute-force oracle: randomized
// string+int64 keys with nulls on both sides, every scalar agg op,
// output order pinned to ascending-keys/nulls-last.
func TestParityGroupByMultiKeyBruteForce(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	rng := rand.New(rand.NewSource(11))
	const n = 500
	regions := make([]string, n)
	regionValid := make([]bool, n)
	years := make([]int64, n)
	yearValid := make([]bool, n)
	vals := make([]int64, n)
	valValid := make([]bool, n)
	words := []string{"eu", "us", "ap", "eu-west", ""}
	for i := range regions {
		regions[i] = words[rng.Intn(len(words))]
		regionValid[i] = rng.Intn(8) != 0
		years[i] = 2020 + int64(rng.Intn(6))
		yearValid[i] = rng.Intn(8) != 0
		vals[i] = rng.Int63n(1000)
		valValid[i] = rng.Intn(8) != 0
	}
	region, _ := series.FromString("region", regions, regionValid, series.WithAllocator(alloc))
	year, _ := series.FromInt64("year", years, yearValid, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", vals, valValid, series.WithAllocator(alloc))
	df, _ := dataframe.New(region, year, v)
	defer df.Release()

	type key struct {
		region  string
		hasReg  bool
		year    int64
		hasYear bool
	}
	type acc struct {
		sum       int64
		count     int64
		nullCount int64
		minVal    int64
		maxVal    int64
		hasVal    bool
		// First/last are row-based (null-aware), matching Take at the
		// group boundary: the first/last input row of the group.
		first     int64
		firstOK   bool
		firstSeen bool
		last      int64
		lastOK    bool
	}
	groups := map[key]*acc{}
	order := []key{}
	for i := range n {
		k := key{regions[i], regionValid[i], years[i], yearValid[i]}
		if !k.hasReg {
			k.region = ""
		}
		if !k.hasYear {
			k.year = 0
		}
		a, ok := groups[k]
		if !ok {
			a = &acc{minVal: math.MaxInt64, maxVal: math.MinInt64}
			groups[k] = a
			order = append(order, k)
		}
		if !a.firstSeen {
			a.first, a.firstOK, a.firstSeen = vals[i], valValid[i], true
		}
		a.last, a.lastOK = vals[i], valValid[i]
		if !valValid[i] {
			a.nullCount++
			continue
		}
		x := vals[i]
		a.sum += x
		a.count++
		if !a.hasVal || x < a.minVal {
			a.minVal = x
		}
		if !a.hasVal || x > a.maxVal {
			a.maxVal = x
		}
		a.hasVal = true
	}
	// Sort groups the way the engine must emit them.
	slices.SortFunc(order, func(a, b key) int {
		if a.hasReg != b.hasReg {
			if a.hasReg {
				return -1
			}
			return 1
		}
		if a.hasReg && a.region != b.region {
			return strings.Compare(a.region, b.region)
		}
		if a.hasYear != b.hasYear {
			if a.hasYear {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.year, b.year)
	})

	aggs := []expr.Expr{
		expr.Col("v").Sum().Alias("s"),
		expr.Col("v").Mean().Alias("m"),
		expr.Col("v").Min().Alias("mi"),
		expr.Col("v").Max().Alias("ma"),
		expr.Col("v").Count().Alias("c"),
		expr.Col("v").NullCount().Alias("nc"),
		expr.Col("v").First().Alias("f"),
		expr.Col("v").Last().Alias("l"),
	}
	out, err := df.GroupBy("region", "year").Agg(context.Background(), aggs)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != len(order) {
		t.Fatalf("height=%d want %d", out.Height(), len(order))
	}

	col := func(name string) *array.Int64 {
		s, err := out.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		return s.Chunk(0).(*array.Int64)
	}
	regArr, _ := out.Column("region")
	regVals := regArr.Chunk(0).(*array.String)
	yrArr, _ := out.Column("year")
	yrVals := yrArr.Chunk(0).(*array.Int64)
	sArr := col("s")
	ms, err := out.Column("m")
	if err != nil {
		t.Fatal(err)
	}
	mVals := ms.Chunk(0).(*array.Float64)
	miArr, maArr := col("mi"), col("ma")
	cArr, ncArr := col("c"), col("nc")
	fArr, lArr := col("f"), col("l")
	for i, k := range order {
		if regVals.IsValid(i) != k.hasReg || (k.hasReg && regVals.Value(i) != k.region) {
			t.Fatalf("row %d region mismatch", i)
		}
		if yrVals.IsValid(i) != k.hasYear || (k.hasYear && yrVals.Value(i) != k.year) {
			t.Fatalf("row %d year mismatch", i)
		}
		a := groups[k]
		if v := sArr.Value(i); v != a.sum {
			t.Fatalf("row %d sum=%d want %d", i, v, a.sum)
		}
		if v := cArr.Value(i); v != a.count {
			t.Fatalf("row %d count=%d want %d", i, v, a.count)
		}
		if v := ncArr.Value(i); v != a.nullCount {
			t.Fatalf("row %d nullcount=%d want %d", i, v, a.nullCount)
		}
		if a.count == 0 {
			if mVals.IsValid(i) || miArr.IsValid(i) || maArr.IsValid(i) || fArr.IsValid(i) || lArr.IsValid(i) {
				t.Fatalf("row %d all-null group must emit nulls", i)
			}
			continue
		}
		if !mVals.IsValid(i) {
			t.Fatalf("row %d mean unexpectedly null", i)
		}
		if got, want := mVals.Value(i), float64(a.sum)/float64(a.count); math.Abs(got-want) > 1e-9 {
			t.Fatalf("row %d mean=%v want %v", i, got, want)
		}
		if v := miArr.Value(i); v != a.minVal {
			t.Fatalf("row %d min=%d want %d", i, v, a.minVal)
		}
		if v := maArr.Value(i); v != a.maxVal {
			t.Fatalf("row %d max=%d want %d", i, v, a.maxVal)
		}
		if fArr.IsValid(i) != a.firstOK || (a.firstOK && fArr.Value(i) != a.first) {
			t.Fatalf("row %d first=(%d,%v) want (%d,%v)",
				i, fArr.Value(i), fArr.IsValid(i), a.first, a.firstOK)
		}
		if lArr.IsValid(i) != a.lastOK || (a.lastOK && lArr.Value(i) != a.last) {
			t.Fatalf("row %d last=(%d,%v) want (%d,%v)",
				i, lArr.Value(i), lArr.IsValid(i), a.last, a.lastOK)
		}
	}
}

// Polars: sum over a group whose values are all-null yields 0 (the
// additive identity: matches `col.sum()` returning 0 for empty).
func TestParityGroupByAllNullsInOneGroupSumsToZero(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	region, _ := series.FromString("r",
		[]string{"a", "a", "b", "b"},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{10, 20, 0, 0},
		[]bool{true, true, false, false},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(region, v)
	defer df.Release()

	out, err := df.GroupBy("r").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("s")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	got := rowsByStringKey(t, out, "r", "s")
	if got["a"] != 30 {
		t.Fatalf("a sum = %d want 30", got["a"])
	}
	if got["b"] != 0 {
		t.Fatalf("b sum = %d want 0 (all-null group → additive identity)", got["b"])
	}
}

// Polars: count aggregation gives the non-null count per group; the
// null_count aggregation gives the null total.
func TestParityGroupByCountVsNullCount(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	region, _ := series.FromString("r",
		[]string{"a", "a", "a", "b", "b"},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{1, 0, 3, 0, 5},
		[]bool{true, false, true, false, true},
		series.WithAllocator(alloc))
	df, _ := dataframe.New(region, v)
	defer df.Release()

	out, err := df.GroupBy("r").Agg(context.Background(), []expr.Expr{
		expr.Col("v").Count().Alias("n"),
		expr.Col("v").NullCount().Alias("nc"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	n := rowsByStringKey(t, out, "r", "n")
	nc := rowsByStringKey(t, out, "r", "nc")
	if n["a"] != 2 || n["b"] != 1 {
		t.Fatalf("count: a=%d (want 2), b=%d (want 1)", n["a"], n["b"])
	}
	if nc["a"] != 1 || nc["b"] != 1 {
		t.Fatalf("null_count: a=%d (want 1), b=%d (want 1)", nc["a"], nc["b"])
	}
}

// Polars: groupby on an empty frame yields an empty frame with the
// correct schema (key column + aggregate columns).
func TestParityGroupByEmpty(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	r, _ := series.FromString("r", nil, nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v", nil, nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(r, v)
	defer df.Release()

	out, err := df.GroupBy("r").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("s")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 0 {
		t.Fatalf("empty groupby height = %d, want 0", out.Height())
	}
	if out.Width() != 2 {
		t.Fatalf("empty groupby width = %d, want 2 (r, s)", out.Width())
	}
}

// Polars: groupby on an int key behaves identically to string keys -
// distinct values each become a group. Smoke test so we know the
// int64 hash path and the string hash path stay in sync.
func TestParityGroupByIntKey(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromInt64("k",
		[]int64{1, 1, 2, 2, 3},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{10, 20, 5, 15, 100},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("s")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	if out.Height() != 3 {
		t.Fatalf("height=%d want 3", out.Height())
	}

	kArr := out.ColumnAt(0).Chunk(0).(*array.Int64)
	sArr, _ := out.Column("s")
	sVals := sArr.Chunk(0).(*array.Int64)
	sums := map[int64]int64{}
	for i := range kArr.Len() {
		sums[kArr.Value(i)] = sVals.Value(i)
	}
	want := map[int64]int64{1: 30, 2: 20, 3: 100}
	for k, v := range want {
		if sums[k] != v {
			t.Fatalf("sum[%d]=%d want %d", k, sums[k], v)
		}
	}
}

// Polars: first / last aggregates return the first or last value
// within the group (in source row order). Useful for "any"-style
// semantics when per-group ordering matters.
func TestParityGroupByFirstLast(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k",
		[]string{"a", "a", "a", "b", "b"},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{1, 2, 3, 10, 20},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(context.Background(), []expr.Expr{
		expr.Col("v").First().Alias("f"),
		expr.Col("v").Last().Alias("l"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()

	f := rowsByStringKey(t, out, "k", "f")
	l := rowsByStringKey(t, out, "k", "l")
	if f["a"] != 1 || l["a"] != 3 {
		t.Fatalf("a first/last = %d/%d, want 1/3", f["a"], l["a"])
	}
	if f["b"] != 10 || l["b"] != 20 {
		t.Fatalf("b first/last = %d/%d, want 10/20", f["b"], l["b"])
	}
}

// Polars: groupby on floats works (no-NaN case). NaN keys are
// skipped in polars by default; golars does the same.
func TestParityGroupByFloatKey(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromFloat64("k",
		[]float64{1.5, 1.5, 2.5, 2.5, 3.5},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{1, 2, 10, 20, 100},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("s")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 3 {
		t.Fatalf("height=%d want 3 (1.5, 2.5, 3.5)", out.Height())
	}
}

// Polars: single-group sanity: when every row has the same key,
// output is a single row whose aggregate covers the whole column.
func TestParityGroupBySingleGroup(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	k, _ := series.FromString("k",
		[]string{"x", "x", "x"},
		nil, series.WithAllocator(alloc))
	v, _ := series.FromInt64("v",
		[]int64{1, 2, 3},
		nil, series.WithAllocator(alloc))
	df, _ := dataframe.New(k, v)
	defer df.Release()

	out, err := df.GroupBy("k").Agg(context.Background(),
		[]expr.Expr{expr.Col("v").Sum().Alias("s")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Height() != 1 {
		t.Fatalf("single-group height=%d want 1", out.Height())
	}
	s, _ := out.Column("s")
	arr := s.Chunk(0).(*array.Int64)
	if arr.Value(0) != 6 {
		t.Fatalf("single-group sum = %d, want 6", arr.Value(0))
	}
}
