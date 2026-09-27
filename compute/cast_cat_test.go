package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Gaurav-Gosain/golars/compute"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/series"
)

func catCellStrings(s *series.Series) []string {
	a := s.Chunk(0)
	out := make([]string, a.Len())
	for i := range out {
		if a.IsNull(i) {
			out[i] = "<null>"
		} else {
			out[i] = a.ValueStr(i)
		}
	}
	return out
}

func TestCastStringCategoricalRoundTrip(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	opt := compute.WithAllocator(mem)
	s, _ := series.FromString("c", []string{"b", "", "a", "b", "", "héllo"},
		[]bool{true, false, true, true, true, true}, series.WithAllocator(mem))
	defer s.Release()

	c, err := compute.Cast(ctx, s, dtype.Categorical(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if !c.DType().IsCategorical() || c.Name() != "c" {
		t.Fatalf("dtype %s name %q", c.DType(), c.Name())
	}
	// Codes follow first appearance, like polars' to_physical.
	d := c.Chunk(0).(*array.Dictionary)
	for i, want := range []int{0, -1, 1, 0, 2, 3} {
		if want < 0 {
			if !d.IsNull(i) {
				t.Fatalf("row %d should be null", i)
			}
			continue
		}
		if got := d.GetValueIndex(i); got != want {
			t.Fatalf("code[%d] = %d want %d", i, got, want)
		}
	}

	back, err := compute.Cast(ctx, c, dtype.String(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer back.Release()
	testutil.AssertStrings(t, catCellStrings(back), []string{"b", "<null>", "a", "b", "", "héllo"})

	codes, err := compute.Cast(ctx, c, dtype.Int64(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer codes.Release()
	testutil.AssertStrings(t, catCellStrings(codes), []string{"0", "<null>", "1", "0", "2", "3"})
}

func TestCastEnum(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	opt := compute.WithAllocator(mem)
	e := dtype.Enum([]string{"b", "a", "c"})

	s, _ := series.FromString("e", []string{"a", "", "c", "a"}, []bool{true, false, true, true}, series.WithAllocator(mem))
	defer s.Release()
	en, err := compute.Cast(ctx, s, e, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Release()
	if en.DType().String() != "enum" {
		t.Fatalf("dtype = %s", en.DType())
	}
	phys, err := compute.Cast(ctx, en, dtype.Uint32(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer phys.Release()
	testutil.AssertStrings(t, catCellStrings(phys), []string{"1", "<null>", "2", "1"})

	// Enum -> Categorical -> String keeps values.
	cat, err := compute.Cast(ctx, en, dtype.Categorical(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Release()
	str, err := compute.Cast(ctx, cat, dtype.String(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer str.Release()
	testutil.AssertStrings(t, catCellStrings(str), []string{"a", "<null>", "c", "a"})

	// Enum -> other Enum remaps codes.
	e2 := dtype.Enum([]string{"c", "a", "b"})
	re, err := compute.Cast(ctx, en, e2, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Release()
	phys2, err := compute.Cast(ctx, re, dtype.Int64(), opt)
	if err != nil {
		t.Fatal(err)
	}
	defer phys2.Release()
	testutil.AssertStrings(t, catCellStrings(phys2), []string{"1", "<null>", "0", "1"})
}

func TestCastEnumUnknownErrors(t *testing.T) {
	ctx := context.Background()
	e := dtype.Enum([]string{"x", "y", "z"})
	s, _ := series.FromString("s", []string{"x", "q", "z"}, nil)
	defer s.Release()
	_, err := compute.Cast(ctx, s, e)
	want := "conversion from `str` to `enum` failed in column 's' for 1 out of 3 values: [\"q\"]"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v", err)
	}

	cat, _ := compute.Cast(ctx, s, dtype.Categorical())
	defer cat.Release()
	_, err = compute.Cast(ctx, cat, e)
	want = "conversion from `cat` to `enum` failed in column 's' for 1 out of 3 values: [\"q\"]"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v", err)
	}

	// A category that no row uses does not fail the cast.
	sub, _ := compute.Filter(ctx, cat, mustBool(t, []bool{true, false, true}))
	defer sub.Release()
	ok, err := compute.Cast(ctx, sub, e)
	if err != nil {
		t.Fatalf("unused category should not fail: %v", err)
	}
	ok.Release()
}

func mustBool(t *testing.T, v []bool) *series.Series {
	t.Helper()
	s, err := series.FromBool("m", v, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Release)
	return s
}

func TestCategoricalFilterTakeCompare(t *testing.T) {
	ctx := context.Background()
	mem := testutil.NewCheckedAllocator(t)
	opt := compute.WithAllocator(mem)
	s, err := series.NewCategorical("c", []string{"b", "", "a", "b"}, []bool{true, false, true, true}, series.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()

	eq, err := compute.EqLit(ctx, s, "b", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer eq.Release()
	testutil.AssertStrings(t, catCellStrings(eq), []string{"true", "<null>", "false", "true"})

	ne, err := compute.NeLit(ctx, s, "b", opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ne.Release()
	testutil.AssertStrings(t, catCellStrings(ne), []string{"false", "<null>", "true", "false"})

	f, err := compute.Filter(ctx, s, eq, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	if !f.DType().IsCategorical() {
		t.Fatalf("filter dtype = %s", f.DType())
	}
	testutil.AssertStrings(t, catCellStrings(f), []string{"b", "b"})

	tk, err := compute.Take(ctx, s, []int{3, 1, 2}, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer tk.Release()
	testutil.AssertStrings(t, catCellStrings(tk), []string{"b", "<null>", "a"})

	// Column vs column compare on two categoricals with different
	// dictionaries goes through the decoded strings.
	other, _ := series.NewCategorical("o", []string{"a", "x", "a", "b"}, nil, series.WithAllocator(mem))
	defer other.Release()
	cmp, err := compute.Eq(ctx, s, other, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer cmp.Release()
	testutil.AssertStrings(t, catCellStrings(cmp), []string{"false", "<null>", "true", "true"})
}
