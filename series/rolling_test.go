package series_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestRollingSum(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.RollingSum(series.RollingOptions{WindowSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	// First 2 rows < min_periods=window: null
	if arr.IsValid(0) || arr.IsValid(1) {
		t.Fatal("expected first two rows to be null")
	}
	want := []float64{6, 9, 12}
	for i, v := range want {
		if arr.Value(i+2) != v {
			t.Fatalf("idx %d: got %v want %v", i+2, arr.Value(i+2), v)
		}
	}
}

func TestRollingMean(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromFloat64("x", []float64{1, 2, 3, 4, 5}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := s.RollingMean(series.RollingOptions{WindowSize: 3, MinPeriods: 1})
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	// min_periods=1 so first two rows are means of partial windows.
	want := []float64{1, 1.5, 2, 3, 4}
	for i, v := range want {
		if math.Abs(arr.Value(i)-v) > 1e-12 {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestRollingMinMax(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{3, 1, 4, 1, 5, 9, 2, 6}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mn, _ := s.RollingMin(series.RollingOptions{WindowSize: 3})
	defer mn.Release()
	mx, _ := s.RollingMax(series.RollingOptions{WindowSize: 3})
	defer mx.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	mxArr := mx.Chunk(0).(*array.Float64)
	wantMin := []float64{1, 1, 1, 1, 2, 2}
	wantMax := []float64{4, 4, 5, 9, 9, 9}
	for i, v := range wantMin {
		if mnArr.Value(i+2) != v {
			t.Fatalf("min idx %d: got %v want %v", i+2, mnArr.Value(i+2), v)
		}
		if mxArr.Value(i+2) != wantMax[i] {
			t.Fatalf("max idx %d: got %v want %v", i+2, mxArr.Value(i+2), wantMax[i])
		}
	}
}

func TestRollingMeanFastMatchesGeneric(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	vals := []float64{3.5, -1.25, 7.0, 0.5, 9.75, 2.0, 4.25}
	allValid := []bool{true, true, true, true, true, true, true}
	fastSrc, _ := series.FromFloat64("x", vals, nil, series.WithAllocator(alloc))
	defer fastSrc.Release()
	genSrc, _ := series.FromFloat64("x", vals, allValid, series.WithAllocator(alloc))
	defer genSrc.Release()
	for _, w := range []int{1, 2, 3, 7, 9} {
		fast, err := fastSrc.RollingMean(series.RollingOptions{WindowSize: w, MinPeriods: 1})
		if err != nil {
			t.Fatal(err)
		}
		gen, err := genSrc.RollingMean(series.RollingOptions{WindowSize: w, MinPeriods: 1})
		if err != nil {
			fast.Release()
			t.Fatal(err)
		}
		fa := fast.Chunk(0).(*array.Float64)
		ga := gen.Chunk(0).(*array.Float64)
		for i := range vals {
			if fa.IsValid(i) != ga.IsValid(i) {
				fast.Release()
				gen.Release()
				t.Fatalf("w=%d idx %d: valid %v vs %v", w, i, fa.IsValid(i), ga.IsValid(i))
			}
			if fa.IsValid(i) && math.Abs(fa.Value(i)-ga.Value(i)) > 1e-12 {
				fast.Release()
				gen.Release()
				t.Fatalf("w=%d idx %d: %v vs %v", w, i, fa.Value(i), ga.Value(i))
			}
		}
		fast.Release()
		gen.Release()
	}
}

func TestRollingMinMaxNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x",
		[]int64{3, 0, 4, 1, 0, 9, 2, 6},
		[]bool{true, false, true, true, false, true, true, true},
		series.WithAllocator(alloc))
	defer s.Release()
	mn, err := s.RollingMin(series.RollingOptions{WindowSize: 3, MinPeriods: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Release()
	mx, err := s.RollingMax(series.RollingOptions{WindowSize: 3, MinPeriods: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer mx.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	mxArr := mx.Chunk(0).(*array.Float64)
	// Windows: [3,x,4] [x,4,1] [4,1,x] [1,x,9] [x,9,2] [9,2,6].
	wantMin := []float64{3, 1, 1, 1, 2, 2}
	wantMax := []float64{4, 4, 4, 9, 9, 9}
	for i := range wantMin {
		if !mnArr.IsValid(i+2) || mnArr.Value(i+2) != wantMin[i] {
			t.Fatalf("min idx %d: got valid=%v value=%v want %v",
				i+2, mnArr.IsValid(i+2), mnArr.Value(i+2), wantMin[i])
		}
		if !mxArr.IsValid(i+2) || mxArr.Value(i+2) != wantMax[i] {
			t.Fatalf("max idx %d: got valid=%v value=%v want %v",
				i+2, mxArr.IsValid(i+2), mxArr.Value(i+2), wantMax[i])
		}
	}
}

func TestRollingMinMaxMinPeriods(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromInt64("x", []int64{3, 1, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mn, err := s.RollingMin(series.RollingOptions{WindowSize: 3, MinPeriods: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	want := []float64{3, 1, 1}
	for i, v := range want {
		if !mnArr.IsValid(i) || mnArr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, mnArr.Value(i), v)
		}
	}
}

func TestRollingMinMaxEdges(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Window size 1 echoes the input.
	s, _ := series.FromInt64("x", []int64{5, 2, 8}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mn, _ := s.RollingMin(series.RollingOptions{WindowSize: 1})
	defer mn.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	for i, v := range []float64{5, 2, 8} {
		if !mnArr.IsValid(i) || mnArr.Value(i) != v {
			t.Fatalf("w=1 idx %d: got %v want %v", i, mnArr.Value(i), v)
		}
	}

	// Window larger than the input: everything null at default periods.
	big, _ := s.RollingMax(series.RollingOptions{WindowSize: 9})
	defer big.Release()
	bigArr := big.Chunk(0).(*array.Float64)
	for i := 0; i < 3; i++ {
		if bigArr.IsValid(i) {
			t.Fatalf("w>n idx %d: expected null", i)
		}
	}

	// Empty input.
	e, _ := series.FromInt64("e", nil, nil, series.WithAllocator(alloc))
	defer e.Release()
	out, err := e.RollingMin(series.RollingOptions{WindowSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Len() != 0 {
		t.Fatalf("empty: got len %d", out.Len())
	}
}

func TestRollingMinNaNLeavesWindow(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// NaN surfaces only while it is the oldest entry; once it slides
	// out the true min resumes instead of a stale NaN.
	s, _ := series.FromFloat64("x",
		[]float64{1, math.NaN(), 2, 3, 4}, nil, series.WithAllocator(alloc))
	defer s.Release()
	mn, err := s.RollingMin(series.RollingOptions{WindowSize: 2, MinPeriods: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer mn.Release()
	mnArr := mn.Chunk(0).(*array.Float64)
	// i=2 covers [NaN,2]: NaN is the oldest entry, so it surfaces.
	// From i=3 the NaN has slid out and the true min resumes.
	for i, v := range []float64{1, 1, math.NaN(), 2, 3} {
		if !mnArr.IsValid(i) {
			t.Fatalf("idx %d: expected valid", i)
		}
		if math.IsNaN(v) {
			if !math.IsNaN(mnArr.Value(i)) {
				t.Fatalf("idx %d: got %v want NaN", i, mnArr.Value(i))
			}
			continue
		}
		if mnArr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, mnArr.Value(i), v)
		}
	}
}

func TestRollingMinMaxAgainstBruteForce(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	rng := rand.New(rand.NewSource(7))
	const n = 300
	vals := make([]int64, n)
	valid := make([]bool, n)
	for i := range vals {
		vals[i] = rng.Int63n(50)
		valid[i] = rng.Intn(4) != 0
	}
	s, _ := series.FromInt64("x", vals, valid, series.WithAllocator(alloc))
	defer s.Release()

	brute := func(w, mp int, isMin bool) ([]float64, []bool) {
		out := make([]float64, n)
		ok := make([]bool, n)
		for i := range n {
			lo := max(0, i-w+1)
			best := 0.0
			count := 0
			for j := lo; j <= i; j++ {
				if !valid[j] {
					continue
				}
				v := float64(vals[j])
				if count == 0 {
					best = v
				} else if isMin && v < best {
					best = v
				} else if !isMin && v > best {
					best = v
				}
				count++
			}
			if count >= mp {
				out[i], ok[i] = best, true
			}
		}
		return out, ok
	}

	for _, w := range []int{1, 2, 5, 32, 400} {
		for _, mp := range []int{0, 1, w} {
			for _, isMin := range []bool{true, false} {
				var got *series.Series
				var err error
				if isMin {
					got, err = s.RollingMin(series.RollingOptions{WindowSize: w, MinPeriods: mp})
				} else {
					got, err = s.RollingMax(series.RollingOptions{WindowSize: w, MinPeriods: mp})
				}
				if err != nil {
					t.Fatal(err)
				}
				arr := got.Chunk(0).(*array.Float64)
				// resolve() clamps mp<=0 to w; mirror that here.
				effMP := mp
				if effMP <= 0 {
					effMP = w
				}
				want, wantOK := brute(w, effMP, isMin)
				for i := range n {
					if arr.IsValid(i) != wantOK[i] {
						got.Release()
						t.Fatalf("w=%d mp=%d min=%v idx %d: valid=%v want %v",
							w, mp, isMin, i, arr.IsValid(i), wantOK[i])
					}
					if wantOK[i] && arr.Value(i) != want[i] {
						got.Release()
						t.Fatalf("w=%d mp=%d min=%v idx %d: got %v want %v",
							w, mp, isMin, i, arr.Value(i), want[i])
					}
				}
				got.Release()
			}
		}
	}
}

func TestRollingStd(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Window of all equal values has std=0.
	s, _ := series.FromFloat64("x", []float64{2, 2, 2, 2, 2}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, _ := s.RollingStd(series.RollingOptions{WindowSize: 3})
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	for i := 2; i < 5; i++ {
		if math.Abs(arr.Value(i)) > 1e-9 {
			t.Fatalf("idx %d: got %v want 0", i, arr.Value(i))
		}
	}
}
