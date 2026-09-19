package series_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestTopKBottomKAgainstBruteForce(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	rngVals := []float64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, math.NaN(), 2, 7, 1}
	valid := make([]bool, len(rngVals))
	for i := range valid {
		valid[i] = i%5 != 4
	}
	s, _ := series.FromFloat64("x", rngVals, valid, series.WithAllocator(alloc))
	defer s.Release()

	// Brute force over the argsort-equivalent order: ascending stable
	// with NaN last, nulls last. Values only (ties share values).
	type elem struct {
		v   float64
		i   int
		nan bool
	}
	var elems []elem
	for i, v := range rngVals {
		if !valid[i] {
			continue
		}
		elems = append(elems, elem{v, i, math.IsNaN(v)})
	}
	less := func(a, b elem) bool {
		if a.nan != b.nan {
			return b.nan
		}
		if a.nan {
			return a.i < b.i
		}
		if a.v != b.v {
			return a.v < b.v
		}
		return a.i < b.i
	}
	for i := 1; i < len(elems); i++ {
		for j := i; j > 0 && less(elems[j], elems[j-1]); j-- {
			elems[j], elems[j-1] = elems[j-1], elems[j]
		}
	}
	nn := len(elems)
	for _, k := range []int{0, 1, 2, 5, nn - 1, nn, nn + 3} {
		top, err := s.TopK(k, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		kk := min(k, nn)
		arr := top.Chunk(0).(*array.Float64)
		if arr.Len() != kk {
			top.Release()
			t.Fatalf("k=%d: len=%d want %d", k, arr.Len(), kk)
		}
		for i := range kk {
			want := elems[nn-1-i]
			got := arr.Value(i)
			if math.IsNaN(want.v) {
				if !math.IsNaN(got) {
					top.Release()
					t.Fatalf("k=%d idx %d: got %v want NaN", k, i, got)
				}
				continue
			}
			if got != want.v {
				top.Release()
				t.Fatalf("k=%d idx %d: got %v want %v", k, i, got, want.v)
			}
		}
		top.Release()

		bot, err := s.BottomK(k, series.WithAllocator(alloc))
		if err != nil {
			t.Fatal(err)
		}
		barr := bot.Chunk(0).(*array.Float64)
		if barr.Len() != kk {
			bot.Release()
			t.Fatalf("k=%d bottom len=%d want %d", k, barr.Len(), kk)
		}
		for i := range kk {
			want := elems[i]
			got := barr.Value(i)
			if math.IsNaN(want.v) {
				if !math.IsNaN(got) {
					bot.Release()
					t.Fatalf("k=%d bottom idx %d: got %v want NaN", k, i, got)
				}
				continue
			}
			if got != want.v {
				bot.Release()
				t.Fatalf("k=%d bottom idx %d: got %v want %v", k, i, got, want.v)
			}
		}
		bot.Release()
	}
}
