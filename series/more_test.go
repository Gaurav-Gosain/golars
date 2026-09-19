package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestRankMethods(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{3, 1, 2, 2, 5}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	cases := []struct {
		m    series.RankMethod
		want []float64
	}{
		{series.RankAverage, []float64{4, 1, 2.5, 2.5, 5}},
		{series.RankMin, []float64{4, 1, 2, 2, 5}},
		{series.RankMax, []float64{4, 1, 3, 3, 5}},
		{series.RankDense, []float64{3, 1, 2, 2, 4}},
		{series.RankOrdinal, []float64{4, 1, 2, 3, 5}},
	}
	for _, c := range cases {
		got, err := s.Rank(c.m, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		arr := got.Chunk(0).(*array.Float64)
		for i, w := range c.want {
			if arr.Value(i) != w {
				t.Errorf("method=%d [%d] = %v, want %v", c.m, i, arr.Value(i), w)
			}
		}
		got.Release()
	}
}

func TestRankWithNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{3, 0, 2, 0, 2},
		[]bool{true, false, true, false, true}, series.WithAllocator(alloc))
	defer s.Release()

	// Valid values [3,2,2] with a tie; nulls stay null.
	cases := []struct {
		m    series.RankMethod
		want []float64
	}{
		{series.RankAverage, []float64{3, 0, 1.5, 0, 1.5}},
		{series.RankMin, []float64{3, 0, 1, 0, 1}},
		{series.RankMax, []float64{3, 0, 2, 0, 2}},
		{series.RankDense, []float64{2, 0, 1, 0, 1}},
		{series.RankOrdinal, []float64{3, 0, 1, 0, 2}},
	}
	for _, c := range cases {
		got, err := s.Rank(c.m, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		arr := got.Chunk(0).(*array.Float64)
		for i, w := range c.want {
			if i == 1 || i == 3 {
				if arr.IsValid(i) {
					t.Errorf("method=%d [%d] should be null", c.m, i)
				}
				continue
			}
			if !arr.IsValid(i) || arr.Value(i) != w {
				t.Errorf("method=%d [%d] = %v, want %v", c.m, i, arr.Value(i), w)
			}
		}
		got.Release()
	}
}

func TestSearchSortedAndIndexOf(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	sorted, _ := series.FromInt64("v", []int64{1, 3, 5, 7, 9}, nil,
		series.WithAllocator(alloc))
	defer sorted.Release()

	cases := []struct {
		v    int64
		want int
	}{{0, 0}, {1, 0}, {2, 1}, {5, 2}, {10, 5}}
	for _, c := range cases {
		got, err := sorted.SearchSorted(c.v)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("SearchSorted(%d) = %d, want %d", c.v, got, c.want)
		}
	}

	u, _ := series.FromString("s", []string{"a", "b", "c", "b"}, nil,
		series.WithAllocator(alloc))
	defer u.Release()
	if idx, _ := u.IndexOf("b"); idx != 1 {
		t.Errorf("IndexOf(b) = %d, want 1", idx)
	}
	if idx, _ := u.IndexOf("z"); idx != -1 {
		t.Errorf("IndexOf(z) = %d, want -1", idx)
	}
}

func TestIsUniqueIsDuplicated(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s", []string{"a", "b", "a", "c"}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	u, _ := s.IsUnique(series.WithAllocator(alloc))
	defer u.Release()
	d, _ := s.IsDuplicated(series.WithAllocator(alloc))
	defer d.Release()

	uArr := u.Chunk(0).(*array.Boolean)
	dArr := d.Chunk(0).(*array.Boolean)
	for i, wUnique := range []bool{false, true, false, true} {
		if uArr.Value(i) != wUnique {
			t.Errorf("IsUnique[%d] = %v, want %v", i, uArr.Value(i), wUnique)
		}
		if dArr.Value(i) == wUnique {
			t.Errorf("IsDuplicated[%d] should complement IsUnique", i)
		}
	}
}

func TestIsFirstLastDistinct(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s", []string{"a", "b", "a", "c"}, nil,
		series.WithAllocator(alloc))
	defer s.Release()

	first, _ := s.IsFirstDistinct(series.WithAllocator(alloc))
	defer first.Release()
	fArr := first.Chunk(0).(*array.Boolean)
	// a, b, c are first at 0, 1, 3. a at 2 is not first.
	for i, want := range []bool{true, true, false, true} {
		if fArr.Value(i) != want {
			t.Errorf("IsFirstDistinct[%d] = %v, want %v", i, fArr.Value(i), want)
		}
	}

	last, _ := s.IsLastDistinct(series.WithAllocator(alloc))
	defer last.Release()
	lArr := last.Chunk(0).(*array.Boolean)
	// Last occurrences: a at 2, b at 1, c at 3.
	for i, want := range []bool{false, true, true, true} {
		if lArr.Value(i) != want {
			t.Errorf("IsLastDistinct[%d] = %v, want %v", i, lArr.Value(i), want)
		}
	}
}

func TestExtendConstantAndHash(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()

	ext, err := s.ExtendConstant(int64(9), 3, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer ext.Release()
	if ext.Len() != 5 {
		t.Errorf("ExtendConstant len = %d, want 5", ext.Len())
	}
	arr := ext.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 2, 9, 9, 9} {
		if arr.Value(i) != w {
			t.Errorf("ext[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}

	// Type mismatch errors.
	if _, err := s.ExtendConstant("str", 1, series.WithAllocator(alloc)); err == nil {
		t.Errorf("dtype-mismatched ExtendConstant should error")
	}

	// Hash returns uint64 series of same length; equal inputs hash equally.
	h, err := s.Hash(series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	if h.Len() != s.Len() {
		t.Errorf("Hash len = %d, want %d", h.Len(), s.Len())
	}
	if h.DType().String() != "u64" {
		t.Errorf("Hash dtype = %s, want u64", h.DType())
	}
}

func TestFirstNonNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a",
		[]int64{0, 0, 7},
		[]bool{false, false, true},
		series.WithAllocator(alloc))
	defer s.Release()
	v, i, err := s.FirstNonNull()
	if err != nil {
		t.Fatal(err)
	}
	if i != 2 || v.(int64) != 7 {
		t.Errorf("FirstNonNull = (%v, %d), want (7, 2)", v, i)
	}

	// All-null.
	all, _ := series.FromInt64("b",
		[]int64{0},
		[]bool{false},
		series.WithAllocator(alloc))
	defer all.Release()
	v2, i2, err := all.FirstNonNull()
	if err != nil {
		t.Fatal(err)
	}
	if v2 != nil || i2 != -1 {
		t.Errorf("FirstNonNull all-null = (%v, %d), want (nil, -1)", v2, i2)
	}
}

func TestApplyInt64(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(alloc))
	defer s.Release()

	out, err := s.ApplyInt64(func(v int64) int64 { return v * v }, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	for i, w := range []int64{1, 4, 9} {
		if arr.Value(i) != w {
			t.Errorf("square[%d] = %d, want %d", i, arr.Value(i), w)
		}
	}

	// Wrong dtype errors.
	str, _ := series.FromString("s", []string{"a"}, nil, series.WithAllocator(alloc))
	defer str.Release()
	if _, err := str.ApplyInt64(func(v int64) int64 { return v }, series.WithAllocator(alloc)); err == nil {
		t.Errorf("ApplyInt64 on string series should error")
	}
}

func TestStrBeforeAfterSplitNth(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("email",
		[]string{"ada@x.com", "bob@y.org", "nohost"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	before, _ := s.Str().Before("@", series.WithAllocator(alloc))
	defer before.Release()
	after, _ := s.Str().After("@", series.WithAllocator(alloc))
	defer after.Release()
	bArr := before.Chunk(0).(*array.String)
	aArr := after.Chunk(0).(*array.String)
	want := []struct{ b, a string }{
		{"ada", "x.com"}, {"bob", "y.org"}, {"nohost", ""},
	}
	for i, w := range want {
		if bArr.Value(i) != w.b {
			t.Errorf("Before[%d] = %q, want %q", i, bArr.Value(i), w.b)
		}
		if aArr.Value(i) != w.a {
			t.Errorf("After[%d] = %q, want %q", i, aArr.Value(i), w.a)
		}
	}

	// SplitNth.
	parts0, _ := s.Str().SplitNth("@", 0, series.WithAllocator(alloc))
	defer parts0.Release()
	parts1, _ := s.Str().SplitNth("@", 1, series.WithAllocator(alloc))
	defer parts1.Release()
	if parts0.Chunk(0).(*array.String).Value(0) != "ada" {
		t.Errorf("SplitNth(0)[0] = %q", parts0.Chunk(0).(*array.String).Value(0))
	}
	if parts1.Chunk(0).(*array.String).Value(2) != "" {
		t.Errorf("SplitNth(1)[nohost] = %q, want empty",
			parts1.Chunk(0).(*array.String).Value(2))
	}
}

func TestStrSplitWide(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("addr",
		[]string{"1-main-st", "2-oak-ave", "only"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	cols, err := s.Str().SplitWide("-", 3, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cols {
		defer c.Release()
	}
	if len(cols) != 3 {
		t.Fatalf("got %d cols, want 3", len(cols))
	}
	if cols[0].Name() != "addr_0" {
		t.Errorf("col[0] name = %q", cols[0].Name())
	}
	c0 := cols[0].Chunk(0).(*array.String)
	if c0.Value(2) != "only" {
		t.Errorf("c0[2] = %q, want only", c0.Value(2))
	}
	c1 := cols[1].Chunk(0).(*array.String)
	if c1.IsValid(2) && c1.Value(2) != "" {
		t.Errorf("c1[2] expected empty-or-null on under-split row")
	}
}

func TestStrIsNumeric(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"123", "-4.5", "+6", "abc", "", "1.2.3", "."},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	got, _ := s.Str().IsNumeric(series.WithAllocator(alloc))
	defer got.Release()
	arr := got.Chunk(0).(*array.Boolean)
	want := []bool{true, true, true, false, false, false, false}
	for i, w := range want {
		if arr.Value(i) != w {
			t.Errorf("IsNumeric[%d] = %v, want %v", i, arr.Value(i), w)
		}
	}
}

func TestSeriesSampleFrac(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("v", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.SampleFrac(0.3, false, 42, series.WithAllocator(alloc))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 3 {
		t.Errorf("len = %d, want 3", out.Len())
	}
}
