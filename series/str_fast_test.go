package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestStrFastPath(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"Alpha", "beta", "Gamma delta", "", "x"},
		[]bool{true, true, true, true, false},
		series.WithAllocator(alloc))
	defer s.Release()

	lower, _ := s.Str().Lower(series.WithAllocator(alloc))
	defer lower.Release()
	lowerArr := lower.Chunk(0).(*array.String)
	want := []string{"alpha", "beta", "gamma delta", "", ""}
	for i, w := range want {
		if i == 4 {
			if lowerArr.IsValid(i) {
				t.Errorf("Lower[4] should be null")
			}
			continue
		}
		if lowerArr.Value(i) != w {
			t.Errorf("Lower[%d] = %q, want %q", i, lowerArr.Value(i), w)
		}
	}

	upper, _ := s.Str().Upper(series.WithAllocator(alloc))
	defer upper.Release()
	upperArr := upper.Chunk(0).(*array.String)
	wantU := []string{"ALPHA", "BETA", "GAMMA DELTA", "", ""}
	for i, w := range wantU {
		if i == 4 {
			continue
		}
		if upperArr.Value(i) != w {
			t.Errorf("Upper[%d] = %q, want %q", i, upperArr.Value(i), w)
		}
	}

	lens, _ := s.Str().LenBytes(series.WithAllocator(alloc))
	defer lens.Release()
	lensArr := lens.Chunk(0).(*array.Int64)
	wantL := []int64{5, 4, 11, 0, 0}
	for i, w := range wantL {
		if i == 4 {
			if lensArr.IsValid(i) {
				t.Errorf("LenBytes[4] should be null")
			}
			continue
		}
		if lensArr.Value(i) != w {
			t.Errorf("LenBytes[%d] = %d, want %d", i, lensArr.Value(i), w)
		}
	}

	contains, _ := s.Str().Contains("amma", series.WithAllocator(alloc))
	defer contains.Release()
	carr := contains.Chunk(0).(*array.Boolean)
	wantC := []bool{false, false, true, false, false}
	for i, w := range wantC {
		if i == 4 {
			continue
		}
		if carr.Value(i) != w {
			t.Errorf("Contains[%d] = %v, want %v", i, carr.Value(i), w)
		}
	}

	starts, _ := s.Str().StartsWith("G", series.WithAllocator(alloc))
	defer starts.Release()
	sarr := starts.Chunk(0).(*array.Boolean)
	wantS := []bool{false, false, true, false, false}
	for i, w := range wantS {
		if i == 4 {
			continue
		}
		if sarr.Value(i) != w {
			t.Errorf("StartsWith[%d] = %v, want %v", i, sarr.Value(i), w)
		}
	}

	ends, _ := s.Str().EndsWith("ta", series.WithAllocator(alloc))
	defer ends.Release()
	earr := ends.Chunk(0).(*array.Boolean)
	wantE := []bool{false, true, true, false, false}
	for i, w := range wantE {
		if i == 4 {
			continue
		}
		if earr.Value(i) != w {
			t.Errorf("EndsWith[%d] = %v, want %v", i, earr.Value(i), w)
		}
	}
}

func TestStrLike(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"MEDIUM POLISHED STEEL", "LARGE BRUSHED COPPER", "MEDIUM ANODIZED TIN", "SMALL POLISHED COPPER"},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	cases := []struct {
		pat   string
		wants []bool
	}{
		{"MEDIUM POLISHED%", []bool{true, false, false, false}},
		{"%COPPER", []bool{false, true, false, true}},
		{"%POLISHED%", []bool{true, false, false, true}},
		{"M_DIUM%", []bool{true, false, true, false}},
		{"SMALL POLISHED COPPER", []bool{false, false, false, true}},
		{"", []bool{false, false, false, false}},
		{"%", []bool{true, true, true, true}},
	}
	for _, tc := range cases {
		res, err := s.Str().Like(tc.pat, series.WithAllocator(alloc))
		if err != nil {
			t.Fatalf("Like(%q): %v", tc.pat, err)
		}
		arr := res.Chunk(0).(*array.Boolean)
		for i, w := range tc.wants {
			if arr.Value(i) != w {
				t.Errorf("Like(%q)[%d] = %v, want %v", tc.pat, i, arr.Value(i), w)
			}
		}
		res.Release()
	}
}

func TestStrHeadTailFind(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"alpha", "beta", "ga", ""},
		nil, series.WithAllocator(alloc))
	defer s.Release()

	head, _ := s.Str().Head(3, series.WithAllocator(alloc))
	defer head.Release()
	wantH := []string{"alp", "bet", "ga", ""}
	harr := head.Chunk(0).(*array.String)
	for i, w := range wantH {
		if harr.Value(i) != w {
			t.Errorf("Head(3)[%d] = %q, want %q", i, harr.Value(i), w)
		}
	}

	tail, _ := s.Str().Tail(2, series.WithAllocator(alloc))
	defer tail.Release()
	wantT := []string{"ha", "ta", "ga", ""}
	tarr := tail.Chunk(0).(*array.String)
	for i, w := range wantT {
		if tarr.Value(i) != w {
			t.Errorf("Tail(2)[%d] = %q, want %q", i, tarr.Value(i), w)
		}
	}

	find, _ := s.Str().Find("a", series.WithAllocator(alloc))
	defer find.Release()
	// polars: "alpha" -> 0, "beta" -> 3, "ga" -> 1, "" -> null (u32).
	wantF := []uint32{0, 3, 1}
	farr := find.Chunk(0).(*array.Uint32)
	for i, w := range wantF {
		if farr.Value(i) != w {
			t.Errorf("Find('a')[%d] = %d, want %d", i, farr.Value(i), w)
		}
	}
	if farr.IsValid(3) {
		t.Errorf("Find('a')[3] = %d, want null", farr.Value(3))
	}
}
