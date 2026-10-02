package series_test

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func mustValues(t *testing.T, name string, dt arrow.DataType, vals []any, opts ...series.Option) *series.Series {
	t.Helper()
	s, err := series.FromValues(name, dt, vals, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func listEq(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if xf, ok := x.(float64); ok {
			if yf, ok := y.(float64); ok && math.IsNaN(xf) && math.IsNaN(yf) {
				continue
			}
		}
		if xl, ok := x.([]any); ok {
			yl, ok := y.([]any)
			if !ok || !listEq(xl, yl) {
				return false
			}
			continue
		}
		if x != y {
			return false
		}
	}
	return true
}

func TestSeriesAccessors(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s := mustValues(t, "x", arrow.PrimitiveTypes.Int64, []any{1, nil, 3}, series.WithAllocator(mem))
	defer s.Release()
	if got := s.ToList(); !listEq(got, []any{int64(1), nil, int64(3)}) {
		t.Fatalf("ToList = %v", got)
	}
	if v, _ := s.Get(-1); v != int64(3) {
		t.Fatalf("Get(-1) = %v", v)
	}
	if _, err := s.Get(3); err == nil {
		t.Fatal("Get out of bounds should fail")
	}
	if _, err := s.Item(); err == nil {
		t.Fatal("Item on length 3 should fail")
	}
	if s.First() != int64(1) || s.Last() != int64(3) {
		t.Fatalf("First/Last = %v/%v", s.First(), s.Last())
	}
	if s.Shape() != [1]int{3} || s.NChunks() != 1 || !slices.Equal(s.ChunkLengths(), []int{3}) {
		t.Fatalf("shape/chunks = %v %d %v", s.Shape(), s.NChunks(), s.ChunkLengths())
	}
	c, err := s.Clear(2, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if c.Len() != 2 || c.NullCount() != 2 || c.DType().String() != "i64" {
		t.Fatalf("Clear = %s", c.Summary())
	}
	nf, err := s.NewFromIndex(2, 3, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer nf.Release()
	if !listEq(nf.ToList(), []any{int64(3), int64(3), int64(3)}) {
		t.Fatalf("NewFromIndex = %v", nf.ToList())
	}
	one := mustValues(t, "one", arrow.PrimitiveTypes.Int64, []any{5}, series.WithAllocator(mem))
	defer one.Release()
	if v, err := one.Item(); err != nil || v != int64(5) {
		t.Fatalf("Item = %v %v", v, err)
	}
	empty := mustValues(t, "e", arrow.PrimitiveTypes.Int64, nil, series.WithAllocator(mem))
	defer empty.Release()
	if empty.First() != nil || !empty.IsEmpty() {
		t.Fatal("empty First should be nil")
	}
}

func TestSeriesEquals(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	a := mustValues(t, "a", arrow.PrimitiveTypes.Float64, []any{1.0, math.NaN(), nil}, series.WithAllocator(mem))
	defer a.Release()
	b := mustValues(t, "b", arrow.PrimitiveTypes.Float64, []any{1.0, math.NaN(), nil}, series.WithAllocator(mem))
	defer b.Release()
	f32 := mustValues(t, "b", arrow.PrimitiveTypes.Float32, []any{1.0, math.NaN(), nil}, series.WithAllocator(mem))
	defer f32.Release()
	ints := mustValues(t, "i", arrow.PrimitiveTypes.Int64, []any{1, 2}, series.WithAllocator(mem))
	defer ints.Release()
	floats := mustValues(t, "f", arrow.PrimitiveTypes.Float64, []any{1.0, 2.0}, series.WithAllocator(mem))
	defer floats.Release()
	// polars: a.equals(b) True, check_names False, f32 True, check_dtypes False.
	if !a.Equals(b) || a.Equals(b, series.EqualsOptions{CheckNames: true}) {
		t.Fatal("equals names")
	}
	if !a.Equals(f32) || a.Equals(f32, series.EqualsOptions{CheckDTypes: true}) {
		t.Fatal("equals dtypes")
	}
	if !ints.Equals(floats) {
		t.Fatal("int and float with equal values should be equal")
	}
	if a.Equals(b, series.EqualsOptions{NullUnequal: true}) {
		t.Fatal("null_equal=False should fail on nulls")
	}
}

func TestSeriesAppendZipScatter(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s := mustValues(t, "a", arrow.PrimitiveTypes.Int64, []any{1, nil, 3}, opt)
	defer s.Release()
	mask := mustValues(t, "m", arrow.FixedWidthTypes.Boolean, []any{true, false, nil}, opt)
	defer mask.Release()
	other := mustValues(t, "o", arrow.PrimitiveTypes.Int64, []any{10, 20, 30}, opt)
	defer other.Release()
	z, err := s.ZipWith(mask, other, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Release()
	if !listEq(z.ToList(), []any{int64(1), int64(20), int64(30)}) {
		t.Fatalf("ZipWith = %v", z.ToList())
	}
	vals := mustValues(t, "v", arrow.PrimitiveTypes.Int64, []any{7, 8}, opt)
	defer vals.Release()
	sc, err := s.Scatter([]int{0, -1}, vals, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Release()
	if !listEq(sc.ToList(), []any{int64(7), nil, int64(8)}) {
		t.Fatalf("Scatter = %v", sc.ToList())
	}
	ap, err := s.Append(other, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Release()
	if ap.Len() != 6 || ap.Name() != "a" {
		t.Fatalf("Append = %s", ap.Summary())
	}
}

func TestSeriesShrinkDtype(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	cases := []struct {
		dt   arrow.DataType
		vals []any
		want string
	}{
		{arrow.PrimitiveTypes.Int64, []any{1, 2, 300}, "i16"},
		{arrow.PrimitiveTypes.Int64, []any{-1, 2, 300}, "i16"},
		{arrow.PrimitiveTypes.Int64, []any{1, nil}, "i8"},
		{arrow.PrimitiveTypes.Int64, []any{int64(1) << 40}, "i64"},
		{arrow.PrimitiveTypes.Uint64, []any{1, 2, 3}, "u8"},
		{arrow.PrimitiveTypes.Float64, []any{1.5, 2.0}, "f32"},
	}
	for _, c := range cases {
		s := mustValues(t, "x", c.dt, c.vals, opt)
		out, err := s.ShrinkDtype(opt)
		s.Release()
		if err != nil {
			t.Fatal(err)
		}
		if out.DType().String() != c.want {
			t.Errorf("%v: dtype %s, want %s", c.vals, out.DType(), c.want)
		}
		out.Release()
	}
}

func TestSeriesGatherAllDtypes(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	for _, dt := range []arrow.DataType{
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Float32, arrow.BinaryTypes.String, arrow.FixedWidthTypes.Boolean,
	} {
		var vals []any
		switch dt.ID() {
		case arrow.STRING:
			vals = []any{"a", nil, "c"}
		case arrow.BOOL:
			vals = []any{true, nil, false}
		default:
			vals = []any{1, nil, 3}
		}
		s := mustValues(t, "x", dt, vals, opt)
		g, err := s.Gather([]int{2, -1, 0, 1}, opt)
		if err != nil {
			t.Fatal(err)
		}
		got := g.ToList()
		if got[1] != nil || got[3] != nil || got[0] == nil || got[2] == nil {
			t.Errorf("%s gather = %v", dt, got)
		}
		g.Release()
		s.Release()
	}
	// Nested: gather from a list column built with Implode.
	base := mustValues(t, "x", arrow.PrimitiveTypes.Int64, []any{1, 2, 3}, opt)
	defer base.Release()
	lst, err := base.Implode(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer lst.Release()
	g, err := lst.Gather([]int{0, -1, 0}, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Release()
	want := []any{[]any{int64(1), int64(2), int64(3)}, nil, []any{int64(1), int64(2), int64(3)}}
	if !listEq(g.ToList(), want) {
		t.Fatalf("list gather = %v", g.ToList())
	}
	ex, err := g.Explode(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ex.Release()
	if !listEq(ex.ToList(), []any{int64(1), int64(2), int64(3), nil, int64(1), int64(2), int64(3)}) {
		t.Fatalf("explode = %v", ex.ToList())
	}
}

func TestSeriesMapValues(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	s := mustValues(t, "s", arrow.BinaryTypes.String, []any{"ab", nil, "c"}, opt)
	defer s.Release()
	out, err := series.MapValues(s, func(v string) int64 { return int64(len(v)) }, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.DType().String() != "i64" || !listEq(out.ToList(), []any{int64(2), nil, int64(1)}) {
		t.Fatalf("MapValues = %s %v", out.DType(), out.ToList())
	}
	up, err := series.MapValues(s, strings.ToUpper, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer up.Release()
	if !listEq(up.ToList(), []any{"AB", nil, "C"}) {
		t.Fatalf("MapValues upper = %v", up.ToList())
	}
	if _, err := series.MapValues(s, func(v int64) int64 { return v }); err == nil {
		t.Fatal("reading strings as int64 should fail")
	}
}

func TestSeriesArgSortMulti(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	a := mustValues(t, "a", arrow.PrimitiveTypes.Int64, []any{1, nil, 3, nil}, opt)
	defer a.Release()
	b := mustValues(t, "b", arrow.PrimitiveTypes.Int64, []any{10, 20, nil, nil}, opt)
	defer b.Release()
	// polars: pl.arg_sort_by(["a","b"], descending=[True, False]) -> [3, 1, 2, 0]
	idx, err := series.ArgSortKeys([]*series.Series{a, b}, []bool{true, false}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(idx, []int{3, 1, 2, 0}) {
		t.Fatalf("argsort = %v", idx)
	}
}
