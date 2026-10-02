package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

func TestStrContainsRegex(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s", []string{"abc123", "xy", "000"}, nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Str().ContainsRegex(`\d+`)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	want := []bool{true, false, true}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %v want %v", i, arr.Value(i), v)
		}
	}
}

func TestStrExtract(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"abc-123", "xy-456", "no-match", "foo-789"},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Str().Extract(`^[a-z]+-(\d+)$`, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.String)
	// "no-match" fails the overall regex so it's null.
	if !arr.IsValid(0) || arr.Value(0) != "123" {
		t.Fatalf("idx 0: got %v valid=%v", arr.Value(0), arr.IsValid(0))
	}
	if !arr.IsValid(1) || arr.Value(1) != "456" {
		t.Fatalf("idx 1")
	}
	if arr.IsValid(2) {
		t.Fatalf("idx 2: should be null")
	}
	if !arr.IsValid(3) || arr.Value(3) != "789" {
		t.Fatalf("idx 3")
	}
}

func TestStrCountMatchesRegex(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"aaa", "abcabc", "xyz"},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Str().CountMatchesRegex("a")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Uint32) // polars: u32
	want := []uint32{3, 2, 0}
	for i, v := range want {
		if arr.Value(i) != v {
			t.Fatalf("idx %d: got %d want %d", i, arr.Value(i), v)
		}
	}
}

func TestStrSplitN(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	s, _ := series.FromString("s",
		[]string{"a-b-c", "x-y", "nosep"},
		nil, series.WithAllocator(alloc))
	defer s.Release()
	out, err := s.Str().SplitExactNullShort("-", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.String)
	if !arr.IsValid(0) || arr.Value(0) != "b" {
		t.Fatalf("idx 0: got %v valid=%v", arr.Value(0), arr.IsValid(0))
	}
	if !arr.IsValid(1) || arr.Value(1) != "y" {
		t.Fatalf("idx 1")
	}
	if arr.IsValid(2) {
		t.Fatalf("idx 2: should be null for nosep")
	}
}
