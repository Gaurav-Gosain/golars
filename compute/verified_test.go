package compute

// Tests against the vectors generated from the Lean model in verify/.
// See docs/verification.md. The vectors are the model's output; these
// tests check that the Go implementation agrees with it exactly.

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func loadVectors(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("../testdata/verified/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func parseBits(t *testing.T, s string) uint64 {
	t.Helper()
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

type verifiedSortCase struct {
	DType     string            `json:"dtype"`
	Values    []json.RawMessage `json:"values"`
	Desc      bool              `json:"desc"`
	NullsLast bool              `json:"nulls_last"`
	Perm      []int             `json:"perm"`
}

// buildVerifiedSeries decodes a vector column: hex bit patterns for f64,
// integers for i64, null for a missing value.
func buildVerifiedSeries(t *testing.T, name, dtype string, raw []json.RawMessage, opts ...series.Option) *series.Series {
	t.Helper()
	valid := make([]bool, len(raw))
	switch dtype {
	case "f64":
		vals := make([]float64, len(raw))
		for i, r := range raw {
			var s *string
			if err := json.Unmarshal(r, &s); err != nil {
				t.Fatal(err)
			}
			if s != nil {
				vals[i] = math.Float64frombits(parseBits(t, *s))
				valid[i] = true
			}
		}
		out, err := series.FromFloat64(name, vals, valid, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	default:
		vals := make([]int64, len(raw))
		for i, r := range raw {
			var v *int64
			if err := json.Unmarshal(r, &v); err != nil {
				t.Fatal(err)
			}
			if v != nil {
				vals[i] = *v
				valid[i] = true
			}
		}
		out, err := series.FromInt64(name, vals, valid, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
}

// seriesBits returns the raw 64-bit words and validity of an int64 or
// float64 series.
func seriesBits(t *testing.T, s *series.Series) ([]uint64, []bool) {
	t.Helper()
	arr, err := s.Consolidated()
	if err != nil {
		t.Fatal(err)
	}
	defer arr.Release()
	n := arr.Len()
	out := make([]uint64, n)
	valid := make([]bool, n)
	for i := range n {
		valid[i] = arr.IsValid(i)
		if !valid[i] {
			continue
		}
		switch a := arr.(type) {
		case *array.Float64:
			out[i] = math.Float64bits(a.Value(i))
		case *array.Int64:
			out[i] = uint64(a.Value(i))
		default:
			t.Fatalf("unexpected array %T", arr)
		}
	}
	return out, valid
}

func TestVerifiedSortIndices(t *testing.T) {
	var cases []verifiedSortCase
	loadVectors(t, "sort.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		alloc := testutil.NewCheckedAllocator(t)
		s := buildVerifiedSeries(t, "x", c.DType, c.Values, series.WithAllocator(alloc))
		so := SortOptions{Descending: c.Desc}
		if c.NullsLast {
			so.Nulls = NullsLast
		}
		idx, err := SortIndices(ctx, s, so, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		testutil.AssertEqualAny(t, idx, c.Perm)
		if t.Failed() {
			t.Fatalf("case %d (%s n=%d desc=%v nulls_last=%v)", ci, c.DType, len(c.Values), c.Desc, c.NullsLast)
		}
		multi, err := SortIndicesMulti(ctx, []*series.Series{s}, []SortOptions{so}, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		testutil.AssertEqualAny(t, multi, c.Perm)
		s.Release()
	}
}

// TestVerifiedSortValues checks the value sort (radix fast paths when
// there are no nulls) bit for bit against the stable order of the model:
// ties between 0.0 and -0.0 and between NaN payloads keep input order.
func TestVerifiedSortValues(t *testing.T) {
	var cases []verifiedSortCase
	loadVectors(t, "sort.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		alloc := testutil.NewCheckedAllocator(t)
		s := buildVerifiedSeries(t, "x", c.DType, c.Values, series.WithAllocator(alloc))
		so := SortOptions{Descending: c.Desc}
		if c.NullsLast {
			so.Nulls = NullsLast
		}
		out, err := Sort(ctx, s, so, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		inBits, inValid := seriesBits(t, s)
		gotBits, gotValid := seriesBits(t, out)
		for i, p := range c.Perm {
			if gotValid[i] != inValid[p] || (inValid[p] && gotBits[i] != inBits[p]) {
				t.Fatalf("case %d (%s n=%d desc=%v nulls_last=%v): row %d got %016x (valid %v), want %016x (valid %v)",
					ci, c.DType, len(c.Values), c.Desc, c.NullsLast, i, gotBits[i], gotValid[i], inBits[p], inValid[p])
			}
		}
		out.Release()
		s.Release()
	}
}

func TestVerifiedSortBig(t *testing.T) {
	var c struct {
		Table    []string `json:"table"`
		Idx      []int    `json:"idx"`
		Perm     []int    `json:"perm"`
		PermDesc []int    `json:"perm_desc"`
	}
	loadVectors(t, "sort_big.json", &c)
	vals := make([]float64, len(c.Idx))
	for i, k := range c.Idx {
		vals[i] = math.Float64frombits(parseBits(t, c.Table[k]))
	}
	ctx := context.Background()
	alloc := testutil.NewCheckedAllocator(t)
	s, err := series.FromFloat64("x", vals, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	for _, desc := range []bool{false, true} {
		perm := c.Perm
		if desc {
			perm = c.PermDesc
		}
		out, err := Sort(ctx, s, SortOptions{Descending: desc}, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := seriesBits(t, out)
		for i, p := range perm {
			if got[i] != math.Float64bits(vals[p]) {
				t.Fatalf("desc=%v row %d: got %016x want %016x", desc, i, got[i], math.Float64bits(vals[p]))
			}
		}
		out.Release()
		idx, err := SortIndices(ctx, s, SortOptions{Descending: desc}, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		testutil.AssertEqualAny(t, idx, perm)
	}
}

type verifiedMultiCase struct {
	A         []json.RawMessage `json:"a"`
	B         []json.RawMessage `json:"b"`
	C         []json.RawMessage `json:"c"`
	Desc      []bool            `json:"desc"`
	NullsLast []bool            `json:"nulls_last"`
	Perm      []int             `json:"perm"`
}

// verifiedMultiKeys returns the key columns and options of a multi-key
// case. Cases without "b" are the all-int64 ascending fast path.
func verifiedMultiKeys(t *testing.T, c verifiedMultiCase, opts ...series.Option) ([]*series.Series, []SortOptions) {
	t.Helper()
	if c.B == nil {
		a := buildVerifiedSeries(t, "a", "i64", c.A, opts...)
		cc := buildVerifiedSeries(t, "c", "i64", c.C, opts...)
		return []*series.Series{a, cc}, []SortOptions{{}, {}}
	}
	cols := []*series.Series{
		buildVerifiedSeries(t, "a", "i64", c.A, opts...),
		buildVerifiedSeries(t, "b", "f64", c.B, opts...),
		buildVerifiedSeries(t, "c", "i64", c.C, opts...),
	}
	so := make([]SortOptions, 3)
	for i := range so {
		so[i].Descending = c.Desc[i]
		if c.NullsLast[i] {
			so[i].Nulls = NullsLast
		}
	}
	return cols, so
}

func TestVerifiedSortMulti(t *testing.T) {
	var cases []verifiedMultiCase
	loadVectors(t, "sort_multi.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		alloc := testutil.NewCheckedAllocator(t)
		cols, so := verifiedMultiKeys(t, c, series.WithAllocator(alloc))
		idx, err := SortIndicesMulti(ctx, cols, so, WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		testutil.AssertEqualAny(t, idx, c.Perm)
		if t.Failed() {
			t.Fatalf("case %d", ci)
		}
		for _, col := range cols {
			col.Release()
		}
	}
}

func kleeneSeries(t *testing.T, vals []*bool, opts ...series.Option) *series.Series {
	t.Helper()
	v := make([]bool, len(vals))
	valid := make([]bool, len(vals))
	for i, p := range vals {
		if p != nil {
			v[i], valid[i] = *p, true
		}
	}
	s, err := series.FromBool("x", v, valid, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestVerifiedKleene(t *testing.T) {
	var rows []struct {
		A, B, And, Or, NotA *bool
	}
	var raw []map[string]*bool
	loadVectors(t, "kleene.json", &raw)
	for _, r := range raw {
		rows = append(rows, struct{ A, B, And, Or, NotA *bool }{r["a"], r["b"], r["and"], r["or"], r["not_a"]})
	}
	alloc := testutil.NewCheckedAllocator(t)
	col := func(f func(i int) *bool) []*bool {
		out := make([]*bool, len(rows))
		for i := range rows {
			out[i] = f(i)
		}
		return out
	}
	a := kleeneSeries(t, col(func(i int) *bool { return rows[i].A }), series.WithAllocator(alloc))
	b := kleeneSeries(t, col(func(i int) *bool { return rows[i].B }), series.WithAllocator(alloc))
	defer a.Release()
	defer b.Release()
	ctx := context.Background()
	check := func(name string, s *series.Series, want []*bool) {
		t.Helper()
		defer s.Release()
		arr, err := s.Consolidated()
		if err != nil {
			t.Fatal(err)
		}
		defer arr.Release()
		ba := arr.(*array.Boolean)
		for i, w := range want {
			if (w == nil) != ba.IsNull(i) || (w != nil && *w != ba.Value(i)) {
				t.Fatalf("%s row %d: a=%v b=%v got null=%v value=%v", name, i, rows[i].A, rows[i].B, ba.IsNull(i), ba.Value(i))
			}
		}
	}
	and, err := And(ctx, a, b, WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	check("and", and, col(func(i int) *bool { return rows[i].And }))
	or, err := Or(ctx, a, b, WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	check("or", or, col(func(i int) *bool { return rows[i].Or }))
	not, err := Not(ctx, a, WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	check("not", not, col(func(i int) *bool { return rows[i].NotA }))

	// The filter rule: a null mask entry drops the row.
	idx, err := series.FromInt64("i", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8}, nil, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Release()
	and2, err := And(ctx, a, b, WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer and2.Release()
	kept, err := Filter(ctx, idx, and2, WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer kept.Release()
	got, _ := seriesBits(t, kept)
	var want []uint64
	for i, r := range rows {
		if r.And != nil && *r.And {
			want = append(want, uint64(i))
		}
	}
	testutil.AssertEqualAny(t, got, want)
}

type verifiedAgg struct {
	Sum   int64  `json:"sum"`
	Count int    `json:"count"`
	Min   *int64 `json:"min"`
	Max   *int64 `json:"max"`
}

// TestVerifiedAggChunks runs the reductions on multi-chunk series: the
// model proves merged per-chunk partials equal the whole-column result.
func TestVerifiedAggChunks(t *testing.T) {
	var cases []struct {
		Chunks   [][]json.RawMessage `json:"chunks"`
		Agg      verifiedAgg         `json:"agg"`
		Partials []verifiedAgg       `json:"partials"`
	}
	loadVectors(t, "agg_chunks.json", &cases)
	ctx := context.Background()
	for ci, c := range cases {
		alloc := testutil.NewCheckedAllocator(t)
		var chunks []arrow.Array
		var parts []*series.Series
		for _, raw := range c.Chunks {
			p := buildVerifiedSeries(t, "x", "i64", raw, series.WithAllocator(alloc))
			parts = append(parts, p)
			chunks = append(chunks, p.Chunk(0))
		}
		s, err := series.New("x", chunks...)
		if err != nil {
			t.Fatal(err)
		}
		checkAgg := func(what string, s *series.Series, want verifiedAgg) {
			t.Helper()
			sum, err := SumInt64(ctx, s, WithAllocator(alloc))
			if err != nil {
				t.Fatal(err)
			}
			if sum != want.Sum || Count(s) != want.Count {
				t.Fatalf("case %d %s: sum %d count %d, want %d %d", ci, what, sum, Count(s), want.Sum, want.Count)
			}
			mn, ok, err := MinInt64(ctx, s, WithAllocator(alloc))
			if err != nil {
				t.Fatal(err)
			}
			if ok != (want.Min != nil) || (ok && mn != *want.Min) {
				t.Fatalf("case %d %s: min %d %v, want %v", ci, what, mn, ok, want.Min)
			}
			mx, ok, err := MaxInt64(ctx, s, WithAllocator(alloc))
			if err != nil {
				t.Fatal(err)
			}
			if ok != (want.Max != nil) || (ok && mx != *want.Max) {
				t.Fatalf("case %d %s: max %d %v, want %v", ci, what, mx, ok, want.Max)
			}
		}
		checkAgg("whole", s, c.Agg)
		for i, p := range parts {
			checkAgg("chunk "+strconv.Itoa(i), p, c.Partials[i])
			p.Release()
		}
		s.Release()
	}
}
