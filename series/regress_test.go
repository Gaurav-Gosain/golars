package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

// A Series with zero chunks (Empty, a zero-length slice) must not panic
// in Chunk(0) or in the aggregations that read it.
func TestZeroChunkSeries(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, series.WithAllocator(mem))
	defer s.Release()
	sliced, err := s.Slice(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sliced.Release()
	empty := series.Empty("a", dtype.Int64())
	defer empty.Release()
	for _, x := range []*series.Series{empty, sliced} {
		if got := x.Chunk(0).Len(); got != 0 {
			t.Fatalf("Chunk(0).Len() = %d, want 0", got)
		}
		if _, err := x.Sum(); err != nil {
			t.Errorf("Sum: %v", err)
		}
		_, _ = x.Min()
		_, _ = x.Quantile(0.5)
		_, _ = x.Any()
		u, err := x.Unique(series.WithAllocator(mem))
		if err != nil {
			t.Errorf("Unique: %v", err)
		} else {
			u.Release()
		}
	}
}

// polars: pl.Series([1, 2, 3]).shift(10) -> [None, None, None], and the
// same for every dtype. The old path released the null array twice.
func TestShiftPastLengthIsNull(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	i, _ := series.FromInt64("a", []int64{1, 2, 3}, nil, opt)
	defer i.Release()
	u, _ := series.FromUint32("u", []uint32{1, 2, 3}, nil, opt)
	defer u.Release()
	for _, s := range []*series.Series{i, u} {
		for _, p := range []int{3, 10, -3, -10} {
			out, err := s.Shift(p, opt)
			if err != nil {
				t.Fatalf("Shift(%d): %v", p, err)
			}
			if out.Len() != 3 || out.NullCount() != 3 {
				t.Errorf("%s Shift(%d): len=%d nulls=%d, want 3 and 3", s.DType(), p, out.Len(), out.NullCount())
			}
			if !arrow.TypeEqual(out.Chunked().DataType(), s.Chunked().DataType()) {
				t.Errorf("Shift(%d) dtype = %s, want %s", p, out.DType(), s.DType())
			}
			out.Release()
		}
	}
}

// polars: pl.Series([4, None, 16]).sqrt() / .pow(0.5) / .abs() keep the null.
func TestMathKernelsKeepNulls(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	opt := series.WithAllocator(mem)
	i, _ := series.FromInt64("i", []int64{4, 9, 16}, []bool{true, false, true}, opt)
	defer i.Release()
	f, _ := series.FromFloat64("f", []float64{-4, 9, 16.5}, []bool{true, false, true}, opt)
	defer f.Release()
	cases := map[string]func() (*series.Series, error){
		"i.Sqrt":    func() (*series.Series, error) { return i.Sqrt(opt) },
		"i.Pow":     func() (*series.Series, error) { return i.Pow(0.5, opt) },
		"i.Log":     func() (*series.Series, error) { return i.Log(opt) },
		"f.Pow":     func() (*series.Series, error) { return f.Pow(2, opt) },
		"f.Abs":     func() (*series.Series, error) { return f.Abs(opt) },
		"f.Sign":    func() (*series.Series, error) { return f.Sign(opt) },
		"f.Round":   func() (*series.Series, error) { return f.Round(0, opt) },
		"f.Clip":    func() (*series.Series, error) { return f.Clip(0, 10, opt) },
		"f.Apply":   func() (*series.Series, error) { return f.ApplyFloat64(func(v float64) float64 { return v }, opt) },
		"f.Arctan2": func() (*series.Series, error) { return f.Arctan2(i, opt) },
	}
	for name, fn := range cases {
		out, err := fn()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if out.NullCount() != 1 || !out.Chunk(0).IsNull(1) {
			t.Errorf("%s: nulls=%d, want row 1 null", name, out.NullCount())
		}
		out.Release()
	}
	out, _ := i.Pow(0.5, opt)
	defer out.Release()
	if got := testutil.PyRepr(out.Chunk(0)); got != "[2.0, None, 4.0]" {
		t.Errorf("Pow(0.5) = %s", got)
	}
}

// polars slice_offsets: negative offsets count from the end and the
// window clamps to the length.
func TestClampSlice(t *testing.T) {
	cases := []struct{ off, length, n, wantOff, wantLen int }{
		{-2, 5, 5, 3, 2},
		{10, 2, 5, 5, 0},
		{-10, 3, 5, 0, 0},
		{-10, 7, 5, 0, 2},
		{1, -1, 5, 1, 4},
		{3, 2, 2, 2, 0},
		{0, 0, 0, 0, 0},
	}
	for _, c := range cases {
		off, length := series.ClampSlice(c.off, c.length, c.n)
		if off != c.wantOff || length != c.wantLen {
			t.Errorf("ClampSlice(%d, %d, %d) = (%d, %d), want (%d, %d)", c.off, c.length, c.n, off, length, c.wantOff, c.wantLen)
		}
	}
}

// polars: [NaN, NaN].min() is NaN and [1, NaN, 3].max() is 3.
func TestSeriesMinMaxNaN(t *testing.T) {
	nan := math.NaN()
	all, _ := series.FromFloat64("x", []float64{nan, nan}, nil)
	defer all.Release()
	mixed, _ := series.FromFloat64("x", []float64{1, nan, 3}, nil)
	defer mixed.Release()
	if v, _ := all.Min(); !math.IsNaN(v) {
		t.Errorf("all-NaN Min = %v, want NaN", v)
	}
	if v, _ := all.Max(); !math.IsNaN(v) {
		t.Errorf("all-NaN Max = %v, want NaN", v)
	}
	if v, _ := mixed.Max(); v != 3 {
		t.Errorf("Max = %v, want 3", v)
	}
	if v, _ := mixed.Min(); v != 1 {
		t.Errorf("Min = %v, want 1", v)
	}
}

// polars keeps NaN in the median, ordered above every number:
// [1, NaN, 3] -> 3, [1, NaN] -> NaN, [1, 2, NaN, NaN] -> NaN.
func TestMedianKeepsNaN(t *testing.T) {
	nan := math.NaN()
	cases := []struct {
		vals []float64
		want float64
	}{
		{[]float64{1, nan, 3}, 3},
		{[]float64{1, nan}, nan},
		{[]float64{1, 2, nan, nan}, nan},
		{[]float64{4, 1, 2}, 2},
	}
	for _, c := range cases {
		s, _ := series.FromFloat64("x", c.vals, nil)
		got, err := s.Median()
		s.Release()
		if err != nil {
			t.Fatal(err)
		}
		if !(got == c.want || (math.IsNaN(got) && math.IsNaN(c.want))) {
			t.Errorf("Median(%v) = %v, want %v", c.vals, got, c.want)
		}
	}
}

// polars: pl.Series(["hello", "world", None, "héllo"]).str.find("llo")
// == [2, None, None, 3] (u32, byte offsets), and count_matches is u32.
func TestStrFindNullWhenAbsent(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromString("s", []string{"hello", "world", "", "héllo"}, []bool{true, true, false, true}, series.WithAllocator(mem))
	defer s.Release()
	f, err := s.Str().Find("llo", series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	if got := testutil.PyRepr(f.Chunk(0)); f.DType().String() != "u32" || got != "[2, None, None, 3]" {
		t.Errorf("find = %s %s, want u32 [2, None, None, 3]", f.DType(), got)
	}
	c, err := s.Str().CountMatches("l", series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if got := testutil.PyRepr(c.Chunk(0)); c.DType().String() != "u32" || got != "[2, 1, None, 2]" {
		t.Errorf("count_matches = %s %s, want u32 [2, 1, None, 2]", c.DType(), got)
	}
}

// polars: df.top_k(4, by="a") with a=[3, None, 1, 5, None] gives
// [5, 3, 1, None] and bottom_k(4) gives [1, 3, 5, None].
func TestTopKFillsWithNulls(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	s, _ := series.FromInt64("a", []int64{3, 0, 1, 5, 0}, []bool{true, false, true, true, false}, series.WithAllocator(mem))
	defer s.Release()
	top, err := s.TopK(4, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer top.Release()
	if got := testutil.PyRepr(top.Chunk(0)); got != "[5, 3, 1, None]" {
		t.Errorf("TopK(4) = %s", got)
	}
	bot, err := s.BottomK(4, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer bot.Release()
	if got := testutil.PyRepr(bot.Chunk(0)); got != "[1, 3, 5, None]" {
		t.Errorf("BottomK(4) = %s", got)
	}
	two, _ := s.TopK(2, series.WithAllocator(mem))
	defer two.Release()
	if got := testutil.PyRepr(two.Chunk(0)); got != "[5, 3]" {
		t.Errorf("TopK(2) = %s", got)
	}
}
