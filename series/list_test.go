package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/series"
)

// buildListInt64 returns a List<Int64> Series with rows:
//
//	[1, 2, 3]
//	[]
//	NULL
//	[10, 20]
func buildListInt64(t *testing.T) *series.Series {
	t.Helper()
	mem := memory.DefaultAllocator
	b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer b.Release()
	vb := b.ValueBuilder().(*array.Int64Builder)
	b.Append(true)
	vb.Append(1)
	vb.Append(2)
	vb.Append(3)
	b.Append(true)
	b.AppendNull()
	b.Append(true)
	vb.Append(10)
	vb.Append(20)
	arr := b.NewListArray()
	s, err := series.New("vals", arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func buildListString(t *testing.T) *series.Series {
	t.Helper()
	mem := memory.DefaultAllocator
	b := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer b.Release()
	vb := b.ValueBuilder().(*array.StringBuilder)
	b.Append(true)
	vb.Append("a")
	vb.Append("b")
	b.Append(true) // empty
	b.AppendNull()
	b.Append(true)
	vb.Append("x")
	arr := b.NewListArray()
	s, err := series.New("tags", arr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestListLen(t *testing.T) {
	s := buildListInt64(t)
	defer s.Release()
	out, err := s.List().Len()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Uint32)
	if arr.Len() != 4 {
		t.Fatalf("len=%d want 4", arr.Len())
	}
	if arr.Value(0) != 3 || arr.Value(1) != 0 {
		t.Errorf("lens got [%d,%d], want [3,0]", arr.Value(0), arr.Value(1))
	}
	if !arr.IsNull(2) {
		t.Errorf("row 2 should be null")
	}
	if arr.Value(3) != 2 {
		t.Errorf("lens[3]=%d want 2", arr.Value(3))
	}
}

func TestListSum(t *testing.T) {
	s := buildListInt64(t)
	defer s.Release()
	out, err := s.List().Sum()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	// Expected (polars): [6, 0 (empty), null (null row), 30]
	if arr.Value(0) != 6 {
		t.Errorf("sum[0]=%d want 6", arr.Value(0))
	}
	if arr.IsNull(1) || arr.Value(1) != 0 || !arr.IsNull(2) {
		t.Errorf("empty list should sum to 0 and the null row stay null")
	}
	if arr.Value(3) != 30 {
		t.Errorf("sum[3]=%d want 30", arr.Value(3))
	}
}

func TestListMean(t *testing.T) {
	s := buildListInt64(t)
	defer s.Release()
	out, err := s.List().Mean()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Float64)
	if arr.Value(0) != 2.0 {
		t.Errorf("mean[0]=%v want 2", arr.Value(0))
	}
	if arr.Value(3) != 15.0 {
		t.Errorf("mean[3]=%v want 15", arr.Value(3))
	}
}

func TestListGet(t *testing.T) {
	s := buildListInt64(t)
	defer s.Release()
	out, err := s.List().Get(1)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Int64)
	if arr.Value(0) != 2 {
		t.Errorf("get(1)[0]=%d want 2", arr.Value(0))
	}
	if !arr.IsNull(1) {
		t.Errorf("empty list should produce null on get")
	}
}

func TestListContains(t *testing.T) {
	s := buildListInt64(t)
	defer s.Release()
	out, err := s.List().Contains(int64(3))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.Boolean)
	if !arr.Value(0) || arr.Value(3) {
		t.Errorf("contains(3)=[%v,.,.,%v], want [true,.,.,false]", arr.Value(0), arr.Value(3))
	}
}

func TestListJoin(t *testing.T) {
	s := buildListString(t)
	defer s.Release()
	out, err := s.List().Join("|", true)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	arr := out.Chunk(0).(*array.String)
	if arr.Value(0) != "a|b" {
		t.Errorf("join[0]=%q want a|b", arr.Value(0))
	}
	if arr.Value(1) != "" {
		t.Errorf("empty list join should be empty string, got %q", arr.Value(1))
	}
}

func TestListNotAList(t *testing.T) {
	s, _ := series.FromInt64("x", []int64{1, 2}, nil)
	defer s.Release()
	if _, err := s.List().Len(); err == nil {
		t.Error("expected error on non-list Series")
	}
}
